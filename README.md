# Kache Connector

This repository implements the client-side KV Cache connector used by inference
processes. The open-source repository name is `kache-connector`.

The connector exposes:

- a Go library and `cgo` shared-library boundary for in-process use;
- a thin Python adapter;
- gRPC control-plane calls to KacheControl;
- direct data-plane writes and reads through the public Transport SPI.

The open-source module deliberately does not include the production RDMA/TCP
data-plane adapter. Those adapters depend on the private backend integration and
are injected by the private build with `kvcache.WithFactories`.

KacheControl is not implemented here. This repository contains the connector plus test doubles required for local verification.

## Implementation boundary

The implementing agent must follow these constraints:

1. Keep private backend adapters outside the public connector module.
2. Treat backend block references as opaque bytes.
3. Do not implement backend storage, allocation, GC, or placement logic in this repository.
4. The root module must not import or depend on a private data-plane backend. It defines the Transport SPI only.

Chinese documentation is in [`README.zh-CN.md`](README.zh-CN.md),
[`docs/USAGE.zh-CN.md`](docs/USAGE.zh-CN.md), and
[`docs/PRIVATE_BUILD.zh-CN.md`](docs/PRIVATE_BUILD.zh-CN.md). Runnable examples
are under [`examples/`](examples/README.md). Integration behavior is defined by
the public Go API, checked-in protobuf contract, C ABI, and examples.

## Layout

```text
connector/            public Go API (Config, Connector, options, error codes)
internal/controlclient/  gRPC control-plane client (auth, retry, failover)
internal/dataplane/   transport-neutral orchestration, plan validation,
                      session manager, and plan execution
transport/            public data-plane SPI (Target, Session, Factory)
internal/memory/      bounded buffer pool (backpressure on in-flight bytes)
internal/metrics/     counters and histograms
internal/testserver/  in-memory control-plane server, fake data plane, TCP node
cmd/kvc-shlib/        cgo C ABI shared library (libkvcache.so / kvcache.dll)
cmd/kvc-smoke/        end-to-end smoke binary; -serve hosts the Python test backend
sdk/python/           synchronous ctypes adapter (kvcache_connector package)
proto/kache/v1/       kache.proto + generated Go bindings (checked in)
proto/gen/python/     generated Python bindings (checked in)
tools/generate_proto.py  protoc wrapper
test/integration/     end-to-end Go tests against the local test doubles
private/              nested private build module (private data-plane adapters and
                      production shlib entry point; not part of open-source core)
```

## Build and test

Requirements: Go 1.25+, a C toolchain for cgo (gcc/clang), Python 3.10+ with
`protobuf` and `pytest` for the Python tests. `protoc` plus `protoc-gen-go` and
`protoc-gen-go-grpc` are only needed to regenerate bindings.

```bash
make test     # gofmt check, public Go build/tests, and public integration tests
make smoke    # end-to-end put/get/miss/delete against the local test doubles
make race     # go test -race ./...
```

Private adapter tests and the production shlib are built from `private`:

```bash
cd private
go test ./...
CGO_ENABLED=1 go build -buildmode=c-shared \
  -o ../build/libkvcache.so ./cmd/kvc-shlib
```

RDMA integration tests belong to the private build; the open-source tests use
the fake data plane and never import a private data-plane backend.

### Control plane versus data plane

The open-source connector always talks to KacheControl over gRPC/TCP.
RDMA is a data-plane capability, not a control-plane protocol. After the private
RDMA adapter is injected, it uses opaque server-returned refs to read/write
storage nodes directly. Control-plane traffic never uses the private RDMA data
plane.

## Python adapter

```python
from kvcache_connector import KVCacheConnector, KVChunk

config = {
    "namespace": "llm-cache",
    "control_plane": {"endpoints": ["127.0.0.1:9100"], "auth_token_env": "KVCACHE_AUTH_TOKEN"},
    "transport": {"mode": "auto", "fallback_enabled": True,
                  "rdma": {"enabled": True}, "tcp": {"enabled": True}},
}
with KVCacheConnector.from_config(config) as conn:
    conn.put_batch([KVChunk(key="k1", data=b"...")])
    values = conn.get_values(["k1", "missing"])  # [b"...", None]
```

The adapter loads `build/libkvcache.so` (or `build/kvcache.dll`) by default;
override with the `KVCACHE_LIBRARY` environment variable. To run the Python
smoke test manually against the local test doubles:

```bash
make shlib build
./build/kvc-smoke -serve -endpoint-file /tmp/kvc.json &
KVCACHE_LIBRARY=$PWD/build/libkvcache.so pytest -q sdk/python
```

`get_batch` returns per-key `GetResult` objects (misses carry
`error_code="KEY_NOT_FOUND"`); `get_values` returns `bytes`/`None` and raises
`KVCacheBatchError` (with `.results`) when a key fails to read. Connector-level
failures raise `KVCacheError` subclasses; per-key business errors never raise.

## C ABI

`cmd/kvc-shlib` exports `kvc_open/kvc_close/kvc_version/kvc_caps/kvc_match/
kvc_get/kvc_put/kvc_delete/kvc_free_buffer/kvc_free_buffers/kvc_error_string`.
Requests and responses are protobuf-encoded bytes; `kvc_get` payloads travel
through `kvc_buf_t` arrays (one per input key, `NULL` for misses) and
`kvc_put` payloads through one input buffer per chunk. All returned memory is
C-allocated and must be freed with the matching free function. Return codes:
`0 OK, 1 INVALID_ARGUMENT, 2 CONFIG_ERROR, 3 INTERNAL_ERROR, 4 TIMEOUT,
5 TRANSPORT_UNAVAILABLE, 6 CONTEXT_CANCELLED, 7 MEMORY_ERROR`. Per-key errors
and misses are encoded in the protobuf reply, not the return code.

## Implementation notes

- **Private transport adapters**: the root module contains no default RDMA/TCP
  factory. `kvcache.New` returns `TRANSPORT_UNAVAILABLE` without injection. The
  private module provides private RDMA/TCP adapters and the production shlib
  entry point. The connector core never decodes backend block references.
- **Empty request namespace**: the Python API has no namespace parameter, so
  batch requests arrive with an empty `namespace`. The connector fills it with
  the configured namespace before calling the server; a non-empty namespace
  that differs from the configured one is rejected with `INVALID_ARGUMENT`.
- **RDMA pool knobs**: `transport.rdma.pool_size_bytes` and
  `transport.rdma.max_outstanding` are consumed by the private RDMA factory.
  The open-source core does not have a production RDMA backend, so they are
  parsed and validated in the public package but only take effect after the
  private factories are injected. In-flight connector bytes remain bounded by
  `limits.max_inflight_bytes`.
- **Invalid ReadPlan error code**: the implementation returns
  `INVALID_READ_PLAN` when a server-provided data-plane plan fails local
  validation. Integration tests pin this behavior.
- **Python result `message` fields**: Python result objects follow the checked
  in protobuf contract. `GetResult` and `PutResult` carry `message`; `MatchResult`
  and `DeleteResult` do not.
- **Proto generation paths**: Go bindings are generated next to
  `proto/kache/v1/kache.proto` (not `proto/gen/go`) so the import path
  matches `option go_package = "github.com/junjiefly/kache-connector/proto/kache/v1;kachev1"`.
  Python bindings are generated under `proto/gen/python`. Both are checked in;
  `tools/generate_proto.py` reproduces them. Set `KACHE_PROTOC_DIR` to the
  directory containing the approved Linux protoc and plugins.
