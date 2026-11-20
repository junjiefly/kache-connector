#!/usr/bin/env python3
"""Generate Go and Python bindings from proto/kache/v1/kache.proto.

Requirements: protoc (>=3.21), protoc-gen-go, protoc-gen-go-grpc on PATH.
Outputs:
  proto/kache/v1/kache.pb.go        (Go messages, next to the .proto)
  proto/kache/v1/kache_grpc.pb.go   (Go gRPC stubs)
  proto/gen/python/kache/v1/kache_pb2.py  (Python messages)

Go bindings are generated with paths=source_relative into proto/ so their
directory matches the mandated go_package import path
github.com/junjiefly/kache-connector/proto/kache/v1. The Python adapter loads the generated
module from proto/gen/python (see sdk/python .../_pb.py).

Set KACHE_PROTOC_DIR to a directory containing the pinned protoc,
protoc-gen-go and protoc-gen-go-grpc binaries (Linux); it is prepended to
PATH so generation always uses the approved toolchain.
"""

import os
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PROTO = ROOT / "proto"

PROTOC_DIR = os.environ.get("KACHE_PROTOC_DIR")


def main() -> int:
    env = os.environ.copy()
    if PROTOC_DIR:
        env["PATH"] = PROTOC_DIR + os.pathsep + env.get("PATH", "")
    for tool in ("protoc", "protoc-gen-go", "protoc-gen-go-grpc"):
        if shutil.which(tool, path=env["PATH"]) is None:
            print(f"error: {tool} not found on PATH", file=sys.stderr)
            return 1
    (PROTO / "gen" / "python").mkdir(parents=True, exist_ok=True)
    cmd = [
        "protoc",
        f"--proto_path={PROTO}",
        f"--go_out={PROTO}",
        "--go_opt=paths=source_relative",
        f"--go-grpc_out={PROTO}",
        "--go-grpc_opt=paths=source_relative",
        f"--python_out={PROTO / 'gen' / 'python'}",
        "kache/v1/kache.proto",
    ]
    subprocess.run(cmd, check=True, env=env)
    print("generated Go and Python bindings")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
