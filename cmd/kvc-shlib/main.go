//go:build cgo

// Command kvc-shlib builds the connector C ABI shared library.
//
//	go build -buildmode=c-shared -o build/libkvcache.so ./cmd/kvc-shlib
package main

/*
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

typedef void* kvc_handle_t;

typedef struct {
    const void* data;
    size_t len;
} kvc_buf_t;
*/
import "C"

import (
	"context"
	"errors"
	"math"
	"sync"
	"time"
	"unsafe"

	"google.golang.org/protobuf/proto"

	kvcache "github.com/junjiefly/kache-connector/connector"
	"github.com/junjiefly/kache-connector/internal/errs"
	"github.com/junjiefly/kache-connector/internal/testserver"
	kachev1 "github.com/junjiefly/kache-connector/proto/kache/v1"
)

const (
	abiOK                   C.int32_t = 0
	abiInvalidArgument      C.int32_t = 1
	abiConfigError          C.int32_t = 2
	abiInternalError        C.int32_t = 3
	abiTimeout              C.int32_t = 4
	abiTransportUnavailable C.int32_t = 5
	abiContextCancelled     C.int32_t = 6
	abiMemoryError          C.int32_t = 7
)

var (
	handleMu sync.RWMutex
	handles  = map[uint64]kvcache.Connector{}
	nextID   uint64

	errStrings = map[C.int32_t]*C.char{}
)

func init() {
	for code, text := range map[C.int32_t]string{
		abiOK:                   "OK",
		abiInvalidArgument:      "INVALID_ARGUMENT",
		abiConfigError:          "CONFIG_ERROR",
		abiInternalError:        "INTERNAL_ERROR",
		abiTimeout:              "TIMEOUT",
		abiTransportUnavailable: "TRANSPORT_UNAVAILABLE",
		abiContextCancelled:     "CONTEXT_CANCELLED",
		abiMemoryError:          "MEMORY_ERROR",
	} {
		errStrings[code] = C.CString(text)
	}
}

func opContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 60*time.Second)
}

func lookup(handle C.kvc_handle_t) (kvcache.Connector, bool) {
	if handle == nil {
		return nil, false
	}
	id := *(*uint64)(handle)
	handleMu.RLock()
	defer handleMu.RUnlock()
	c, ok := handles[id]
	return c, ok
}

func abiCode(err error) C.int32_t {
	if err == nil {
		return abiOK
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return abiTimeout
	}
	if errors.Is(err, context.Canceled) {
		return abiContextCancelled
	}
	switch errs.From(err) {
	case errs.InvalidArgument, errs.ChunkTooLarge, errs.BatchTooLarge, errs.PayloadLengthMismatch:
		return abiInvalidArgument
	case errs.TransportUnavailable:
		return abiTransportUnavailable
	default:
		return abiInternalError
	}
}

// abiBytes copies a caller-owned C buffer into Go memory. Lengths beyond
// C.int range would wrap negative and panic inside C.GoBytes, crashing the
// host process; reject them with a code instead (spec §6.1).
func abiBytes(p unsafe.Pointer, n C.size_t) ([]byte, bool) {
	if n > C.size_t(math.MaxInt32) {
		return nil, false
	}
	return C.GoBytes(p, C.int(n)), true
}

func allocBytes(b []byte) (*C.uint8_t, C.size_t) {
	if len(b) == 0 {
		return nil, 0
	}
	p := C.malloc(C.size_t(len(b)))
	if p == nil {
		return nil, 0
	}
	C.memcpy(p, unsafe.Pointer(&b[0]), C.size_t(len(b)))
	return (*C.uint8_t)(p), C.size_t(len(b))
}

func writeResponse(msg proto.Message, response **C.uint8_t, responseLen *C.size_t) C.int32_t {
	b, err := proto.Marshal(msg)
	if err != nil {
		return abiInternalError
	}
	p, n := allocBytes(b)
	if p == nil && len(b) > 0 {
		return abiMemoryError
	}
	*response = p
	*responseLen = n
	return abiOK
}

//export kvc_open
func kvc_open(configJSON *C.char, configLen C.size_t, outHandle *C.kvc_handle_t) C.int32_t {
	if outHandle == nil {
		return abiInvalidArgument
	}
	*outHandle = nil
	if configJSON == nil || configLen == 0 {
		return abiConfigError
	}
	data, ok := abiBytes(unsafe.Pointer(configJSON), configLen)
	if !ok {
		return abiInvalidArgument
	}
	cfg, err := kvcache.ParseConfig(data)
	if err != nil {
		return abiConfigError
	}
	ctx, cancel := opContext()
	defer cancel()
	options := []kvcache.Option{}
	if cfg.ControlPlane.InsecureTestMode {
		options = append(options, kvcache.WithFactories(
			testserver.NewTCPTransportFactory(cfg.Transport.TCP.DialTimeoutMS),
		))
	}
	conn, err := kvcache.New(ctx, *cfg, options...)
	if err != nil {
		if errs.From(err) == errs.InvalidArgument {
			return abiInvalidArgument
		}
		return abiConfigError
	}
	handleMu.Lock()
	nextID++
	id := nextID
	handles[id] = conn
	handleMu.Unlock()
	// The token is a C allocation holding the registry id. It is intentionally
	// not freed on close so that repeated kvc_close calls remain safe.
	tok := C.malloc(C.size_t(8))
	if tok == nil {
		handleMu.Lock()
		delete(handles, id)
		handleMu.Unlock()
		_ = conn.Close(ctx)
		// Spec §6.1: kvc_open failures must report CONFIG_ERROR or
		// INVALID_ARGUMENT, never MEMORY_ERROR.
		return abiConfigError
	}
	*(*uint64)(tok) = id
	*outHandle = C.kvc_handle_t(tok)
	return abiOK
}

//export kvc_version
func kvc_version(major, minor, patch *C.uint32_t) C.int32_t {
	if major == nil || minor == nil || patch == nil {
		return abiInvalidArgument
	}
	*major = 1
	*minor = 0
	*patch = 0
	return abiOK
}

//export kvc_close
func kvc_close(handle C.kvc_handle_t) {
	if handle == nil {
		return
	}
	id := *(*uint64)(handle)
	handleMu.Lock()
	conn, ok := handles[id]
	delete(handles, id)
	handleMu.Unlock()
	if ok {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = conn.Close(ctx)
	}
}

//export kvc_caps
func kvc_caps(handle C.kvc_handle_t, response **C.uint8_t, responseLen *C.size_t) C.int32_t {
	conn, ok := lookup(handle)
	if !ok || response == nil || responseLen == nil {
		return abiInvalidArgument
	}
	ctx, cancel := opContext()
	defer cancel()
	rep, err := conn.Caps(ctx)
	if err != nil {
		return abiCode(err)
	}
	return writeResponse(rep, response, responseLen)
}

//export kvc_match
func kvc_match(handle C.kvc_handle_t, request *C.uint8_t, requestLen C.size_t, response **C.uint8_t, responseLen *C.size_t) C.int32_t {
	conn, ok := lookup(handle)
	if !ok || response == nil || responseLen == nil {
		return abiInvalidArgument
	}
	var req kachev1.MatchBatchRequest
	if request == nil && requestLen > 0 {
		return abiInvalidArgument
	}
	raw, ok := abiBytes(unsafe.Pointer(request), requestLen)
	if !ok {
		return abiInvalidArgument
	}
	if err := proto.Unmarshal(raw, &req); err != nil {
		return abiInvalidArgument
	}
	ctx, cancel := opContext()
	defer cancel()
	rep, err := conn.Match(ctx, &req)
	if err != nil {
		return abiCode(err)
	}
	return writeResponse(rep, response, responseLen)
}

//export kvc_get
func kvc_get(handle C.kvc_handle_t, request *C.uint8_t, requestLen C.size_t, response **C.uint8_t, responseLen *C.size_t, dataBuffers **C.kvc_buf_t, dataCount *C.size_t) C.int32_t {
	conn, ok := lookup(handle)
	if !ok || response == nil || responseLen == nil || dataBuffers == nil || dataCount == nil {
		return abiInvalidArgument
	}
	*dataBuffers = nil
	*dataCount = 0
	var req kachev1.GetBatchRequest
	if request == nil && requestLen > 0 {
		return abiInvalidArgument
	}
	raw, ok := abiBytes(unsafe.Pointer(request), requestLen)
	if !ok {
		return abiInvalidArgument
	}
	if err := proto.Unmarshal(raw, &req); err != nil {
		return abiInvalidArgument
	}
	ctx, cancel := opContext()
	defer cancel()
	rep, err := conn.Get(ctx, &req)
	if err != nil {
		return abiCode(err)
	}
	byKey := map[string]*kachev1.GetResult{}
	for _, r := range rep.GetResults() {
		byKey[r.GetKey()] = r
	}
	n := len(req.GetKeys())
	arr := (*C.kvc_buf_t)(C.malloc(C.size_t(n) * C.size_t(unsafe.Sizeof(C.kvc_buf_t{}))))
	if arr == nil && n > 0 {
		return abiMemoryError
	}
	bufs := unsafe.Slice(arr, n)
	for i := range bufs {
		bufs[i].data = nil
		bufs[i].len = 0
	}
	cleanup := func() {
		for i := range bufs {
			if bufs[i].data != nil {
				C.free(unsafe.Pointer(bufs[i].data))
			}
		}
		C.free(unsafe.Pointer(arr))
	}
	for i, key := range req.GetKeys() {
		r := byKey[key]
		if r == nil || !r.GetHit() || len(r.GetData()) == 0 {
			continue
		}
		p, ln := allocBytes(r.GetData())
		if p == nil {
			cleanup()
			return abiMemoryError
		}
		bufs[i].data = unsafe.Pointer(p)
		bufs[i].len = ln
	}
	// Payloads travel through data_buffers; keep GetResult.data empty.
	out := proto.Clone(rep).(*kachev1.GetBatchReply)
	for _, r := range out.GetResults() {
		r.Data = nil
	}
	if code := writeResponse(out, response, responseLen); code != abiOK {
		cleanup()
		return code
	}
	*dataBuffers = arr
	*dataCount = C.size_t(n)
	return abiOK
}

//export kvc_put
func kvc_put(handle C.kvc_handle_t, request *C.uint8_t, requestLen C.size_t, dataBuffers *C.kvc_buf_t, dataCount C.size_t, response **C.uint8_t, responseLen *C.size_t) C.int32_t {
	conn, ok := lookup(handle)
	if !ok || response == nil || responseLen == nil {
		return abiInvalidArgument
	}
	var req kachev1.PutBatchRequest
	if request == nil && requestLen > 0 {
		return abiInvalidArgument
	}
	raw, ok := abiBytes(unsafe.Pointer(request), requestLen)
	if !ok {
		return abiInvalidArgument
	}
	if err := proto.Unmarshal(raw, &req); err != nil {
		return abiInvalidArgument
	}
	if int(dataCount) != len(req.GetChunks()) {
		return abiInvalidArgument
	}
	if dataBuffers == nil && dataCount > 0 {
		return abiInvalidArgument
	}
	bufs := unsafe.Slice(dataBuffers, int(dataCount))
	for i, ch := range req.GetChunks() {
		if len(ch.GetData()) != 0 {
			return abiInvalidArgument
		}
		if bufs[i].data == nil && bufs[i].len > 0 {
			return abiInvalidArgument
		}
		if uint64(bufs[i].len) != ch.GetSizeBytes() {
			return abiInvalidArgument
		}
		data, ok := abiBytes(unsafe.Pointer(bufs[i].data), bufs[i].len)
		if !ok {
			return abiInvalidArgument
		}
		ch.Data = data
	}
	ctx, cancel := opContext()
	defer cancel()
	rep, err := conn.Put(ctx, &req)
	if err != nil {
		return abiCode(err)
	}
	return writeResponse(rep, response, responseLen)
}

//export kvc_delete
func kvc_delete(handle C.kvc_handle_t, request *C.uint8_t, requestLen C.size_t, response **C.uint8_t, responseLen *C.size_t) C.int32_t {
	conn, ok := lookup(handle)
	if !ok || response == nil || responseLen == nil {
		return abiInvalidArgument
	}
	var req kachev1.DeleteBatchRequest
	if request == nil && requestLen > 0 {
		return abiInvalidArgument
	}
	raw, ok := abiBytes(unsafe.Pointer(request), requestLen)
	if !ok {
		return abiInvalidArgument
	}
	if err := proto.Unmarshal(raw, &req); err != nil {
		return abiInvalidArgument
	}
	ctx, cancel := opContext()
	defer cancel()
	rep, err := conn.Delete(ctx, &req)
	if err != nil {
		return abiCode(err)
	}
	return writeResponse(rep, response, responseLen)
}

//export kvc_free_buffer
func kvc_free_buffer(pointer *C.uint8_t) {
	if pointer != nil {
		C.free(unsafe.Pointer(pointer))
	}
}

//export kvc_free_buffers
func kvc_free_buffers(buffers *C.kvc_buf_t, count C.size_t) {
	if buffers == nil {
		return
	}
	bufs := unsafe.Slice(buffers, int(count))
	for i := range bufs {
		if bufs[i].data != nil {
			C.free(unsafe.Pointer(bufs[i].data))
		}
	}
	C.free(unsafe.Pointer(buffers))
}

//export kvc_error_string
func kvc_error_string(code C.int32_t) *C.char {
	if s, ok := errStrings[code]; ok {
		return s
	}
	return errStrings[abiInternalError]
}

func main() {}
