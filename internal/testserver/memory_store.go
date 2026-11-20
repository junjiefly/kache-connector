package testserver

import (
	"sync"

	kachev1 "github.com/junjiefly/kache-connector/proto/kache/v1"
)

type entryState int

const (
	stateCommitted entryState = iota
	stateDeleted
)

// storedEntry is the committed metadata for one key. Payload bytes live in the
// data plane (fake or TCP node); the control plane never receives payloads.
type storedEntry struct {
	key          string
	size         uint64
	hash         []byte
	attributes   map[string]string
	blocks       []*kachev1.PhysicalBlock
	lastModified int64
	state        entryState
}

type memoryStore struct {
	mu      sync.Mutex
	entries map[string]*storedEntry
}

func newMemoryStore() *memoryStore {
	return &memoryStore{entries: map[string]*storedEntry{}}
}

func storeKey(namespace, key string) string { return namespace + "\x00" + key }

func (s *memoryStore) get(namespace, key string) *storedEntry {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.entries[storeKey(namespace, key)]
}

func (s *memoryStore) put(namespace string, e *storedEntry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries[storeKey(namespace, e.key)] = e
}

func (s *memoryStore) visible(namespace, key string) bool {
	e := s.get(namespace, key)
	return e != nil && e.state == stateCommitted
}
