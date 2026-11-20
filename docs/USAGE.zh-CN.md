# Kache Connector 使用说明

## 1. 定位

`kache-connector` 是运行在推理进程内的 KV Cache 客户端库。它负责：

1. 通过 gRPC 访问 KacheControl 控制面；
2. 读取或写入服务端返回的数据面计划；
3. 通过注入的数据面工厂直连数据节点；
4. 校验 chunk、batch、计划、payload 长度和内容哈希。

开源仓库包含 connector 核心、协议、C ABI、Python ctypes 适配器和本地测试双端。生产 RDMA/TCP 数据面工厂不在开源包内，由私有构建注入。

## 2. 快速开始

### 2.1 本地验证

```bash
make smoke
```

`make smoke` 使用内置测试控制面和测试数据面，验证 `Put -> Get -> Miss -> Delete` 全流程，不需要 KacheControl 或存储节点。

### 2.2 Python 本地验证

```bash
make build
CGO_ENABLED=1 go build -buildmode=c-shared \
  -o build/libkvcache.so ./cmd/kvc-shlib
./build/kvc-smoke -serve -endpoint-file /tmp/kvc.json &

PYTHONPATH=$PWD/sdk/python \
KVCACHE_LIBRARY=$PWD/build/libkvcache.so \
python examples/python/basic.py --config-file /tmp/kvc.json
```

注意：公开仓库的 shlib 只在 `insecure_test_mode=true` 且控制面 endpoint 是 loopback 时注入本地 TCP 测试工厂。生产部署必须使用私有构建生成的 shlib。

## 3. Go API

Go 用户直接导入本模块：

```go
import (
    "context"
    "time"

    kvcache "github.com/junjiefly/kache-connector/connector"
    kachev1 "github.com/junjiefly/kache-connector/proto/kache/v1"
)

func main() error {
    ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
    defer cancel()

    cfg := kvcache.Config{
        Namespace: "llm-cache",
        ControlPlane: kvcache.ControlPlaneConfig{
            Endpoints:      []string{"kache-control-1:9122", "kache-control-2:9122"},
            AuthTokenEnv:   "KACHE_CONTROL_TOKEN",
        },
        Transport: kvcache.TransportConfig{
            Mode:             "auto",
            FallbackEnabled:  true,
            RDMA:             kvcache.RDMATransportConfig{Enabled: true},
            TCP:              kvcache.TCPTransportConfig{Enabled: true},
        },
    }

    conn, err := kvcache.New(ctx, cfg, productionFactories())
    if err != nil {
        return err
    }
    defer conn.Close(context.Background())

    data := []byte("kv payload")
    put, err := conn.Put(ctx, &kachev1.PutBatchRequest{
        Chunks: []*kachev1.PutChunk{{
            Key:       "prompt/k1",
            Data:      data,
            SizeBytes: uint64(len(data)),
        }},
    })
    if err != nil {
        return err
    }
    if !put.GetCommitted() {
        return errors.New(put.GetResults()[0].GetMessage())
    }
    return nil
}
```

`productionFactories()` 来自私有数据面适配器，通过 `kvcache.WithFactories` 注入：

```go
conn, err := kvcache.New(ctx, cfg, kvcache.WithFactories(rdmaFactory, tcpFactory))
```

公开核心不提供生产工厂。没有注入工厂时，`kvcache.New` 返回 `TRANSPORT_UNAVAILABLE`。

## 4. Python API

Python 适配器通过 C ABI 调用 Go shlib，不直接处理 RDMA 或数据面计划。

```python
from kvcache_connector import Config, KVCacheConnector, KVChunk

config = Config.from_dict({
    "namespace": "llm-cache",
    "control_plane": {
        "endpoints": ["kache-control-1:9122", "kache-control-2:9122"],
        "timeout_ms": 5000,
        "auth_token_env": "KACHE_CONTROL_TOKEN",
    },
    "transport": {
        "mode": "auto",
        "fallback_enabled": True,
        "rdma": {"enabled": True, "dial_timeout_ms": 2000},
        "tcp": {"enabled": True, "dial_timeout_ms": 2000},
    },
})

with KVCacheConnector.from_config(config) as conn:
    results = conn.put_batch([
        KVChunk(key="prompt/k1", data=b"payload", attributes={"model": "m1"}),
    ])
    if not results[0].ok:
        raise RuntimeError(results[0].error_code)

    hits = conn.match_batch(["prompt/k1"])
    values = conn.get_values(["prompt/k1", "prompt/miss"])
    deleted = conn.delete_batch(["prompt/k1"])
```

方法行为：

| 方法 | 返回语义 |
|---|---|
| `match_batch` | 精确 key 探测。miss 不是异常。 |
| `get_batch` | 精确读取。miss 返回 `hit=False`；数据节点失败返回 `error_code`。 |
| `get_values` | 简化接口。miss 返回 `None`；非 miss 错误抛出 `KVCacheBatchError`。 |
| `put_batch` | 批量写。chunk 级错误在结果中返回；连接器级错误抛异常。 |
| `delete_batch` | 批量删除。missing key 视为已删除。 |

Python `KVChunk` 当前不暴露 `ttl_seconds` 字段。需要自定义 TTL 时使用 Go 低层 API。

## 5. 配置

配置 JSON 会拒绝未知字段。下面是完整 schema：

```json
{
  "namespace": "llm-cache",
  "control_plane": {
    "endpoints": ["kache-control-1:9122", "kache-control-2:9122"],
    "timeout_ms": 5000,
    "auth_token_env": "KACHE_CONTROL_TOKEN",
    "tls": {
      "enabled": false,
      "ca_file": "",
      "cert_file": "",
      "key_file": "",
      "server_name": ""
    },
    "insecure_test_mode": false,
    "caps_refresh_seconds": 60
  },
  "transport": {
    "mode": "auto",
    "fallback_enabled": true,
    "session_idle_timeout_ms": 60000,
    "rdma": {
      "enabled": true,
      "dial_timeout_ms": 2000,
      "pool_size_bytes": 268435456,
      "max_outstanding": 32
    },
    "tcp": {
      "enabled": true,
      "dial_timeout_ms": 2000
    }
  },
  "limits": {
    "max_chunk_bytes": 67108864,
    "max_batch_chunks": 64,
    "max_inflight_bytes": 268435456,
    "max_concurrent_blocks": 16,
    "max_wire_block_bytes": 0
  }
}
```

| 字段 | 默认值 | 说明 |
|---|---:|---|
| `namespace` | 必填 | 连接器绑定的 namespace。 |
| `control_plane.endpoints` | 必填 | KacheControl 地址列表，格式 `host:port`，按顺序容错。 |
| `control_plane.timeout_ms` | 5000 | 一次控制面调用的整体超时。 |
| `control_plane.auth_token_env` | 空 | 环境变量名，不是 token 本身。值作为 `Authorization: Bearer <token>` 发送。 |
| `control_plane.tls.enabled` | false | 启用 gRPC TLS。 |
| `control_plane.tls.ca_file` | 空 | CA PEM 文件。 |
| `control_plane.tls.cert_file/key_file` | 空 | 客户端证书和私钥，必须成对。 |
| `control_plane.tls.server_name` | 空 | TLS SNI/服务端证书校验名。 |
| `control_plane.insecure_test_mode` | false | 只允许 loopback endpoint 的本地测试模式。 |
| `control_plane.caps_refresh_seconds` | 60 | 服务端能力缓存刷新间隔。 |
| `transport.mode` | `auto` | `rdma`、`tcp` 或 `auto`。 |
| `transport.fallback_enabled` | true | RDMA 目标失败且提供 TCP fallback endpoint 时切换 TCP。 |
| `transport.rdma.dial_timeout_ms` | 2000 | 私有 RDMA 工厂建立数据面连接的超时。 |
| `transport.rdma.pool_size_bytes` | 256 MiB | 私有 RDMA 数据面缓冲池预算。 |
| `transport.rdma.max_outstanding` | 32 | 私有 RDMA 数据面最大在途请求数。 |
| `transport.tcp.dial_timeout_ms` | 2000 | TCP 数据面连接超时。 |
| `limits.max_chunk_bytes` | 64 MiB | 本地 chunk 上限。 |
| `limits.max_batch_chunks` | 64 | 本地 batch chunk 数上限。 |
| `limits.max_inflight_bytes` | 256 MiB | batch 内部并发 payload/缓冲预算。 |
| `limits.max_concurrent_blocks` | 16 | 单 batch 并发块数上限。 |
| `limits.max_wire_block_bytes` | 0 | 本地 wire block 上限；0 表示使用服务端能力。 |

本地限制会取本地配置和服务端 `GetCaps` 的较小值。服务端必须允许使用的传输类型；配置 `rdma`/`auto` 时 `rdma.enabled=true`，配置 `tcp`/`auto` 时 `tcp.enabled=true`。

鉴权要求：生产环境必须配置 TLS 或 token。`insecure_test_mode=true` 只适合本地 loopback 测试。数据面节点不提供面向外部客户端的数据加密；跨非信任网络不属于 Kache v1 的安全边界。

## 6. 执行流程

### 6.1 Put

1. connector 校验本地 batch、chunk、key 和总大小；
2. `PreparePutBatch` 只发送元数据，不发送 payload；
3. KacheControl 返回 `put_id`、过期时间和不可变物理块计划；
4. connector 校验计划后，向每个块的全部副本目标并发写入；
5. 全部成功后调用 `CommitPutBatch`；
6. 任一目标失败或过期时调用 `AbortPutBatch`，chunk 返回失败，孤儿物理块由服务端清理机制处理。

`PreparePutBatch` 不重试，避免丢失响应后重复分配块。重试应使用新的 `client_request_id` 重做整个 Put batch。`CommitPutBatch` 粘住 Prepare 成功的 KacheControl 节点；找不到粘性节点时允许按 endpoint 列表重试。

### 6.2 Get

1. connector 发送 `ResolveBatch`；
2. KacheControl 返回每个 key 的 `ReadPlan` 或 miss；
3. connector 按块顺序恢复 chunk，块内优先尝试健康副本；
4. RDMA 目标失败时，如果服务端返回 TCP fallback endpoint，则先切换 TCP；
5. 所有块读取完成后校验 SHA-256 内容哈希。

### 6.3 Match 和 Delete

- `MatchBatch` 是精确 key 探测，不是 listing、prefix 搜索或 glob。
- `DeleteBatch` 按 key 精确删除。missing key 返回已删除，不是错误。

## 7. 协议兼容性

公共 RPC 服务是 `kache.v1.KacheControl`，方法包括：

- `GetCaps`
- `MatchBatch`
- `ResolveBatch`
- `PreparePutBatch`
- `CommitPutBatch`
- `AbortPutBatch`
- `DeleteBatch`
- `ListControlServers`

connector 和 KacheControl 共享消息字段和字段编号。connector 公开 proto 额外包含进程内高级 API DTO；KacheControl 的内部 `RdmaKitRefV1` 消息不复制到 connector 公开 proto。只要服务端和客户端使用字段编号相同的共享消息，gRPC wire 兼容。

服务端返回的 `opaque_ref` 对 connector 是不透明字节。connector 只检查 `ref_kind` 并把字节原样交给数据面工厂，不解析后端物理块布局。

## 8. 错误处理

Python 结果对象中的稳定错误码包括：

| 错误码 | 语义 |
|---|---|
| `INVALID_ARGUMENT` | key、chunk、batch、namespace 或请求结构非法。 |
| `CHUNK_TOO_LARGE` / `BATCH_TOO_LARGE` | 超过本地或服务端限制。 |
| `KEY_NOT_FOUND` | Get/Match/Resolve 的 miss。 |
| `KEY_EXISTS` | 相同 namespace/key 已存在；chunk 不可覆盖。 |
| `PUT_NOT_PREPARED` / `PUT_EXPIRED` | Put 状态不存在、已提交/中止或计划过期。 |
| `INVALID_WRITE_PLAN` / `INVALID_READ_PLAN` | 服务端计划未通过本地校验。 |
| `UNSUPPORTED_REF_KIND` | 当前注入的数据面工厂不支持该引用类型。 |
| `WRITE_TARGET_FAILED` / `READ_TARGET_FAILED` | 数据节点写/读失败。 |
| `HASH_MISMATCH` | 读回数据与内容哈希不一致。 |
| `TRANSPORT_UNAVAILABLE` | 没有可用数据面工厂或控制面连接失败。 |
| `PERMISSION_DENIED` | token、namespace 或 bucket 校验失败。 |
| `SERVER_BUSY` | 服务端繁忙。 |

C ABI 返回码与 Python 异常映射：

| 返回码 | Python 异常 |
|---:|---|
| 0 | 无 |
| 1 | `KVCacheInvalidArgument` |
| 2 | `KVCacheConfigError` |
| 3 | `KVCacheInternalError` |
| 4 | `KVCacheTimeout` |
| 5 | `KVCacheTransportError` |
| 6 | `KVCacheCancelled` |
| 7 | `KVCacheMemoryError` |

按 key 的业务错误不抛异常，必须检查每个 result。

## 9. 构建和测试

公开核心：

```bash
gofmt -w .
go test ./...
make smoke
```

Python：

```bash
python -m pip install protobuf pytest
make build
CGO_ENABLED=1 go build -buildmode=c-shared \
  -o build/libkvcache.so ./cmd/kvc-shlib
PYTHONPATH=$PWD/sdk/python KVCACHE_LIBRARY=$PWD/build/libkvcache.so \
  python -m pytest -q sdk/python
```

重新生成协议时，使用项目要求的 Linux `protoc` 和匹配插件：

```bash
KACHE_PROTOC_DIR=/path/to/protoc-tools make proto
```

不要手工修改 `proto/kache/v1/kache.pb.go`、`proto/kache/v1/kache_grpc.pb.go` 或 `proto/gen/python/kache/v1/kache_pb2.py`。
