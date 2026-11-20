# 私有构建与生产部署说明

## 1. 组件边界

| 组件 | 位置 | 用途 |
|---|---|---|
| connector 核心 | 本仓库公开文件 | Go API、gRPC 客户端、协议校验、C ABI、Python ctypes 封装 |
| Transport SPI | `transport/` | 数据面工厂和会话接口 |
| 私有适配包 | 另行分发的 `private/` | 生产 RDMA/TCP 数据面工厂和生产 shlib 入口 |
| 生产 shlib | `build/libkvcache.so` | 已注入私有数据面工厂的最终共享库 |

公开包不包含生产数据面适配器。直接使用公开 Go 库时，必须由使用方通过 `kvcache.WithFactories` 注入自己的工厂；直接使用公开 shlib 时，它只支持本地测试模式。

## 2. 目录准备

假设公开包已解压到 `kache-connector/`。获得私有适配包后，将私有适配包提供的 `private/` 目录放入同一根目录，并按私有适配包的 Go module `replace` 声明准备对应的私有数据面依赖目录。

典型最终结构如下：

```text
kache-connector/
  connector/
  transport/
  internal/
  proto/
  sdk/
  cmd/
  private/
  go.mod
  go.sum
```

不要把生产 shlib 或私有适配器单独复制到业务机器后随意替换公开 shlib 文件名。应使用私有适配包的构建入口生成最终 shlib。

## 3. 构建

在 Linux 上执行：

```bash
cd private
go test ./...
CGO_ENABLED=1 go build -buildmode=c-shared \
  -o ../build/libkvcache.so ./cmd/kvc-shlib
```

私有模块测试覆盖：

1. 生产 RDMA 工厂与 connector SPI 的适配；
2. TCP fallback 工厂；
3. 生产 shlib 入口注入的工厂；
4. opaque ref 编解码和协议错误映射。

Windows 构建使用私有模块中的入口时，同样需要启用 cgo 和对应的 RDMA/TCP 运行环境。

## 4. 运行时部署

业务进程需要能访问：

1. KacheControl 的 gRPC 地址；
2. 服务端返回的数据节点地址；
3. RDMA 网卡和用户态 RDMA 运行环境；
4. TCP fallback endpoint，如果启用 fallback。

推荐部署：

```text
<deployment-root>/
  libkvcache.so
  connector-config.json
  certificates/
```

启动业务进程前设置：

```bash
export KACHE_CONTROL_TOKEN="<token-from-secret-manager>"
export KVCACHE_LIBRARY=/opt/kache/libkvcache.so
```

不要把 token 写入配置文件。`auth_token_env` 只保存环境变量名。

## 5. 生产配置

生产配置示例：

```json
{
  "namespace": "llm-cache",
  "control_plane": {
    "endpoints": [
      "kache-control-1.internal:9122",
      "kache-control-2.internal:9122"
    ],
    "timeout_ms": 5000,
    "auth_token_env": "KACHE_CONTROL_TOKEN",
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

启用 TLS 时：

```json
{
  "control_plane": {
    "tls": {
      "enabled": true,
      "ca_file": "/opt/kache/certificates/ca.pem",
      "cert_file": "/opt/kache/certificates/client.pem",
      "key_file": "/opt/kache/certificates/client-key.pem",
      "server_name": "kache-control.internal"
    }
  }
}
```

不要在生产配置中使用 `insecure_test_mode=true`。

## 6. 验收检查

私有构建完成后按顺序验证：

```bash
gofmt -w .
go test ./...
cd private && go test ./...
```

然后运行业务接入检查：

1. `Caps` 能拿到 KacheControl 返回的 namespace、chunk/batch/块限制和传输类型；
2. Put 后 Get 能返回相同 payload 和相同 SHA-256；
3. 相同 key 第二次 Put 返回 `KEY_EXISTS`；
4. Delete 后 Get 返回 `KEY_NOT_FOUND`；
5. RDMA 可用时数据节点收到 RDMA 传输；
6. 停用 RDMA 后 fallback 到 TCP；
7. 控制面 token 失败时返回 `PERMISSION_DENIED`；
8. `KVCACHE_LIBRARY` 指向的是私有构建的 shlib。

## 7. 升级约束

升级公开包和私有包时，必须保证协议版本和 `kache.v1` 消息字段编号一致。先在测试环境完成 Put/Get/Delete/fallback 回归，再升级生产。

禁止在业务进程内解析 `opaque_ref`。该字段只应由私有数据面适配器消费。
