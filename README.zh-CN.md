# Kache Connector

这是推理进程使用的 KV Cache 连接器的开源核心实现。它提供：

- Go 库和 `cgo` 共享库边界；
- 简单的 Python 适配器；
- 到 KacheControl 的 gRPC 控制面访问；
- 通过公共 Transport SPI 注入的数据面访问能力。

生产环境使用的 RDMA/TCP 数据面适配器不在开源仓库中。私有构建会通过
`kvcache.WithFactories` 注入这些适配器。开源连接器始终把服务端返回的物理
block 引用视为不透明字节，不会解析内部存储布局。

## 快速开始

仓库内置本地测试控制面和数据面，不需要真实 KacheControl 或存储节点即可验证
基本流程：

```bash
make smoke
```

输出：

```text
smoke OK: put/get/miss/delete passed
```

可运行示例见 [`examples/README.md`](examples/README.md)，Python 示例源码见
[`examples/python/basic.py`](examples/python/basic.py)。

## 核心概念

- **控制面**：连接器通过 gRPC/TCP 访问 KacheControl，完成鉴权、key 匹配、
  block 计划获取、commit/abort 和删除。
- **数据面**：连接器使用服务端返回的 block 计划直连数据节点。开源包只定义
  Transport SPI，不包含生产 RDMA/TCP 工厂。
- **chunk**：KV Cache 的最小业务单元。每个 chunk 是一个不可变对象。
- **namespace**：连接器的逻辑命名空间，从配置中读取，Python API 不显式传入。

完整配置、API、错误码和私有构建说明见
[`docs/USAGE.zh-CN.md`](docs/USAGE.zh-CN.md) 和
[`docs/PRIVATE_BUILD.zh-CN.md`](docs/PRIVATE_BUILD.zh-CN.md)。

## Python 示例

```python
from kvcache_connector import KVCacheConnector, KVChunk

config = {
    "namespace": "llm-cache",
    "control_plane": {
        "endpoints": ["127.0.0.1:9100"],
        "auth_token_env": "KVCACHE_AUTH_TOKEN",
    },
    "transport": {
        "mode": "auto",
        "fallback_enabled": True,
        "rdma": {"enabled": True},
        "tcp": {"enabled": True},
    },
}

with KVCacheConnector.from_config(config) as conn:
    results = conn.put_batch([KVChunk(key="prompt/k1", data=b"hello")])
    if not results[0].ok:
        raise RuntimeError(results[0].error_code)

    print(conn.get_values(["prompt/k1", "prompt/missing"]))

    conn.delete_batch(["prompt/k1"])
```

从源码运行示例时，需要把 `sdk/python` 加入 `PYTHONPATH`。Python 适配器默认
加载 `build/libkvcache.so` 或 `build/kvcache.dll`，也可以用
`KVCACHE_LIBRARY` 环境变量覆盖。完整运行步骤见
[`examples/README.md`](examples/README.md)。

## 构建和测试

依赖：

- Go 1.25+；
- C 工具链，例如 gcc 或 clang；
- Python 3.10+，测试时需要 `protobuf` 和 `pytest`；
- 只有重新生成 protobuf 时才需要 `protoc`、`protoc-gen-go` 和
  `protoc-gen-go-grpc`。

常用命令：

```bash
make test     # gofmt 检查、开源核心 Go 构建/测试
make smoke    # 本地 put/get/miss/delete 端到端测试
make race     # Go race 测试
```

私有适配器和生产 shlib 构建方式：

```bash
cd private
go test ./...
CGO_ENABLED=1 go build -buildmode=c-shared \
  -o ../build/libkvcache.so ./cmd/kvc-shlib
```

## 目录结构

```text
connector/             公共 Go API
transport/             公共数据面 Transport SPI
internal/controlclient/  KacheControl gRPC 客户端
internal/dataplane/      与传输无关的数据面编排
internal/testserver/     本地测试控制面和 fake data plane
cmd/kvc-shlib/           开源 cgo 共享库入口
cmd/kvc-smoke/           本地端到端 smoke 命令
sdk/python/              Python ctypes 适配器
examples/                最小可运行示例
private/                 私有生产适配器和私有 shlib 构建模块，不随开源包发布
```

## 开源边界

开源连接器不包含：

- 生产 RDMA/TCP 数据面适配器；
- 私有数据面依赖及其后端协议；
- 内部 block 布局、ref 编码和 wire protocol；
- 服务端和存储后端的实现。

公开边界以本仓库的 Go API、protobuf 协议、C ABI 和示例为准。

## 许可

本项目基于 Apache License 2.0 开源。详见 [LICENSE](LICENSE) 和 [NOTICE](NOTICE)。
