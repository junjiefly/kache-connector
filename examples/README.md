# Examples

These examples use the checked-in local test doubles. They do not require a
production KacheControl deployment or the private RDMA/TCP adapters.

## Go smoke example

The fastest way to see the full flow is the built-in smoke program:

```bash
go run ./cmd/kvc-smoke
```

It starts a local control plane, injects a fake transport factory, and verifies:

1. put one chunk;
2. read the chunk back;
3. read a missing chunk;
4. delete the chunk.

The corresponding source is [`cmd/kvc-smoke/main.go`](../cmd/kvc-smoke/main.go).

## Python smoke example

The Python adapter calls the cgo shared library. Build both the local server
and shared library first. The Python adapter also needs `protobuf`:

```bash
python -m pip install protobuf
```

```bash
make shlib build
```

Start the local test server in another terminal:

```bash
./build/kvc-smoke -serve -endpoint-file /tmp/kvc.json
```

Then run the Python example:

```bash
PYTHONPATH=$PWD/sdk/python \
  python examples/python/basic.py --config-file /tmp/kvc.json
```

On Linux, Python usually discovers `build/libkvcache.so` automatically. If the
shared library is somewhere else, set `KVCACHE_LIBRARY`:

```bash
KVCACHE_LIBRARY=$PWD/build/libkvcache.so \
PYTHONPATH=$PWD/sdk/python \
  python examples/python/basic.py --config-file /tmp/kvc.json
```

On Windows:

```powershell
$env:KVCACHE_LIBRARY="$PWD\build\kvcache.dll"
$env:PYTHONPATH="$PWD\sdk\python"
python examples/python/basic.py --config-file C:\tmp\kvc.json
```

The example performs a put, a hit, a miss, and a delete against the server
identified by the config file.
