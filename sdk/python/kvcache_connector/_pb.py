"""Loads the generated protobuf module from proto/gen/python.

Set KVCACHE_PROTO_PYTHON to override the module path (e.g. when the SDK is
installed separately from the repository)."""

import importlib.util
import os
import sys
from pathlib import Path

_MODULE_NAME = "kache.v1.kache_pb2"


def _module_path() -> Path:
    override = os.environ.get("KVCACHE_PROTO_PYTHON")
    if override:
        return Path(override)
    root = Path(__file__).resolve().parents[3]
    return root / "proto" / "gen" / "python" / "kache" / "v1" / "kache_pb2.py"


def _load():
    if _MODULE_NAME in sys.modules:
        return sys.modules[_MODULE_NAME]
    path = _module_path()
    if not path.exists():
        raise FileNotFoundError(
            f"generated protobuf module not found at {path}; run tools/generate_proto.py"
        )
    spec = importlib.util.spec_from_file_location(_MODULE_NAME, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[_MODULE_NAME] = module
    spec.loader.exec_module(module)
    return module


pb = _load()
