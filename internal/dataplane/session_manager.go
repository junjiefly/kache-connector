package dataplane

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"log/slog"
	"sync"
	"time"

	"github.com/junjiefly/kache-connector/internal/errs"
)

type sessionKey struct {
	transport string
	endpoint  string
	capID     string
}

type sessionEntry struct {
	transport DataTransport
	lastUsed  time.Time
	inUse     int
}

type keyLock struct {
	mu      sync.Mutex
	waiters int // guarded by SessionManager.mu
}

// SessionManager keeps one logical session per (endpoint, transport) pair,
// including the capability identity in the key, and reuses them across batches.
type SessionManager struct {
	mu        sync.Mutex
	factories map[string]Factory
	sessions  map[sessionKey]*sessionEntry
	keyMu     map[sessionKey]*keyLock
	idle      time.Duration
	log       *slog.Logger
}

func NewSessionManager(factories []Factory, idle time.Duration, log *slog.Logger) *SessionManager {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	m := &SessionManager{
		factories: map[string]Factory{},
		sessions:  map[sessionKey]*sessionEntry{},
		keyMu:     map[sessionKey]*keyLock{},
		idle:      idle,
		log:       log,
	}
	for _, f := range factories {
		m.factories[f.Transport()] = f
	}
	return m
}

func (m *SessionManager) Factory(transport string) (Factory, bool) {
	f, ok := m.factories[transport]
	return f, ok
}

func keyFor(t DataTarget) sessionKey {
	sum := sha256.Sum256(t.Capability)
	return sessionKey{transport: t.Transport, endpoint: t.Endpoint, capID: hex.EncodeToString(sum[:])}
}

func (m *SessionManager) lockFor(key sessionKey) *keyLock {
	m.mu.Lock()
	defer m.mu.Unlock()
	l := m.keyMu[key]
	if l == nil {
		l = &keyLock{}
		m.keyMu[key] = l
	}
	l.waiters++
	return l
}

func (m *SessionManager) unlockFor(key sessionKey, l *keyLock) {
	l.mu.Unlock()
	m.mu.Lock()
	defer m.mu.Unlock()
	l.waiters--
	m.dropKeyLockLocked(key)
}

// dropKeyLockLocked reclaims a per-key lock once no acquirer holds or waits
// on it and no session exists for the key. Callers hold m.mu.
func (m *SessionManager) dropKeyLockLocked(key sessionKey) {
	if l := m.keyMu[key]; l != nil && l.waiters == 0 && m.sessions[key] == nil {
		delete(m.keyMu, key)
	}
}

// Acquire returns a reusable session and a release function. Passing a
// non-nil error to release discards the session so the next call reconnects.
// Dials are serialized per session key only, so a slow endpoint cannot block
// session acquisition for other endpoints.
func (m *SessionManager) Acquire(ctx context.Context, t DataTarget) (DataTransport, func(error), error) {
	f, ok := m.factories[t.Transport]
	if !ok {
		return nil, nil, errs.New(errs.TransportUnavailable, "transport %q is not enabled", t.Transport)
	}
	key := keyFor(t)
	kl := m.lockFor(key)
	kl.mu.Lock()
	defer m.unlockFor(key, kl)

	m.mu.Lock()
	entry := m.sessions[key]
	if entry != nil {
		entry.inUse++
		entry.lastUsed = time.Now()
		m.mu.Unlock()
	} else {
		m.mu.Unlock()
		tr, err := f.Open(ctx, t)
		if err != nil {
			return nil, nil, err
		}
		entry = &sessionEntry{transport: tr, inUse: 1, lastUsed: time.Now()}
		m.mu.Lock()
		m.sessions[key] = entry
		m.mu.Unlock()
	}

	release := func(opErr error) {
		m.mu.Lock()
		defer m.mu.Unlock()
		entry.inUse--
		entry.lastUsed = time.Now()
		if opErr != nil && m.sessions[key] == entry {
			delete(m.sessions, key)
			m.dropKeyLockLocked(key)
			go func() {
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				defer cancel()
				_ = entry.transport.Close(ctx)
			}()
		}
	}
	return entry.transport, release, nil
}

// CloseIdle closes sessions unused for longer than the idle timeout.
func (m *SessionManager) CloseIdle(now time.Time) {
	m.mu.Lock()
	var stale []*sessionEntry
	for k, e := range m.sessions {
		if e.inUse == 0 && now.Sub(e.lastUsed) > m.idle {
			delete(m.sessions, k)
			m.dropKeyLockLocked(k)
			stale = append(stale, e)
		}
	}
	m.mu.Unlock()
	for _, e := range stale {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_ = e.transport.Close(ctx)
		cancel()
	}
}

func (m *SessionManager) Close(ctx context.Context) error {
	m.mu.Lock()
	entries := make([]*sessionEntry, 0, len(m.sessions))
	for k, e := range m.sessions {
		delete(m.sessions, k)
		m.dropKeyLockLocked(k)
		entries = append(entries, e)
	}
	m.mu.Unlock()
	var firstErr error
	for _, e := range entries {
		if err := e.transport.Close(ctx); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
