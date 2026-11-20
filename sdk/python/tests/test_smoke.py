"""Python ctypes smoke tests (spec test 30, also covers ABI open/close and
buffer ownership: spec tests 3 and 29).

Requires the built shared library and kvc-smoke binary:

    make shlib build
    pytest -q sdk/python

Override the library location with KVCACHE_LIBRARY.
"""

import contextlib
import ctypes
import hashlib
import json
import os
import subprocess
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from kvcache_connector import Config, KVCacheConnector, KVChunk
from kvcache_connector import errors
from kvcache_connector._pb import pb
from kvcache_connector.connector import _Buf

ROOT = Path(__file__).resolve().parents[3]


def _library_path() -> str | None:
    override = os.environ.get("KVCACHE_LIBRARY")
    if override:
        return override
    for name in ("build/libkvcache.so", "build/kvcache.dll"):
        candidate = ROOT / name
        if candidate.exists():
            return str(candidate)
    return None


@pytest.fixture(scope="session")
def library_path() -> str:
    path = _library_path()
    if path is None:
        pytest.skip("shared library not built; run `make shlib` or set KVCACHE_LIBRARY")
    return path


@contextlib.contextmanager
def _serve(tmp_path_factory, extra_args=()):
    exe = ROOT / "build" / "kvc-smoke"
    if not exe.exists():
        pytest.skip("kvc-smoke not built; run `make build`")
    endpoint_file = tmp_path_factory.mktemp("kvc") / "config.json"
    proc = subprocess.Popen(
        [str(exe), "-serve", "-endpoint-file", str(endpoint_file), *extra_args],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    cfg = None
    deadline = time.time() + 15
    while time.time() < deadline:
        if proc.poll() is not None:
            out = proc.stdout.read().decode("utf-8", "replace") if proc.stdout else ""
            raise RuntimeError(f"kvc-smoke exited early: {out}")
        if endpoint_file.exists():
            try:
                cfg = json.loads(endpoint_file.read_text())
                break
            except json.JSONDecodeError:
                pass
        time.sleep(0.05)
    if cfg is None:
        proc.terminate()
        proc.wait(timeout=5)
        raise RuntimeError("kvc-smoke did not write its config in time")
    try:
        yield cfg
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)


@pytest.fixture(scope="session")
def server_config(tmp_path_factory):
    with _serve(tmp_path_factory) as cfg:
        yield cfg


@pytest.fixture(scope="session")
def failing_server_config(tmp_path_factory):
    with _serve(tmp_path_factory, ("-fail-reads",)) as cfg:
        yield cfg


@pytest.fixture()
def connector(server_config, library_path):
    with KVCacheConnector.from_json(json.dumps(server_config), library_path) as conn:
        yield conn


def _key() -> str:
    return f"py/{uuid.uuid4().hex}"


def test_version(connector):
    assert connector.version() == (1, 0, 0)


def test_caps(connector):
    caps = connector.caps()
    assert caps.max_chunk_bytes > 0
    assert caps.max_batch_chunks > 0
    assert "tcp" in caps.supported_transports


def test_put_get_roundtrip(connector):
    k1, k2, missing = _key(), _key(), _key()
    d1 = b"hello kv cache"
    d2 = bytes(range(256)) * 40
    results = connector.put_batch([
        KVChunk(key=k1, data=d1, attributes={"model": "m1"}),
        KVChunk(key=k2, data=d2, content_hash=hashlib.sha256(d2).digest()),
    ])
    assert [r.ok for r in results] == [True, True]

    got = connector.get_batch([k1, k2, missing])
    assert got[0].hit and got[0].data == d1
    assert got[0].size_bytes == len(d1)
    assert got[0].content_hash == hashlib.sha256(d1).digest()
    assert got[0].attributes == {"model": "m1"}
    assert got[0].last_modified_unix_sec > 0
    assert got[1].hit and got[1].data == d2
    assert not got[2].hit and got[2].data is None
    assert got[2].error_code == "KEY_NOT_FOUND"

    assert connector.get_values([k1, missing]) == [d1, None]


def test_match_and_delete(connector):
    key, missing = _key(), _key()
    connector.put_batch([KVChunk(key=key, data=b"payload")])
    matched = connector.match_batch([key, missing])
    assert matched[0].hit and not matched[1].hit
    assert matched[1].error_code == "KEY_NOT_FOUND"

    deleted = connector.delete_batch([key, missing])
    assert deleted[0].deleted
    # Control treats a missing chunk as already deleted.
    assert deleted[1].deleted and not deleted[1].error_code
    assert connector.get_values([key]) == [None]


def test_existing_key_is_per_key_error(connector):
    key = _key()
    connector.put_batch([KVChunk(key=key, data=b"original")])
    results = connector.put_batch([KVChunk(key=key, data=b"replacement")])
    assert not results[0].ok
    assert results[0].error_code == "KEY_EXISTS"
    assert connector.get_values([key]) == [b"original"]


def test_local_validation_raises(connector):
    with pytest.raises(errors.KVCacheInvalidArgument):
        connector.put_batch([])
    with pytest.raises(errors.KVCacheInvalidArgument):
        connector.put_batch([KVChunk(key="dup", data=b"x"), KVChunk(key="dup", data=b"y")])
    with pytest.raises(errors.KVCacheInvalidArgument):
        connector.put_batch([KVChunk(key=_key(), data=b"")])
    with pytest.raises(errors.KVCacheInvalidArgument):
        connector.get_batch([])
    with pytest.raises(errors.KVCacheInvalidArgument):
        connector.get_batch(["dup", "dup"])
    with pytest.raises(errors.KVCacheInvalidArgument):
        connector.match_batch([])


def test_config_errors(library_path):
    with pytest.raises(errors.KVCacheConfigError):
        KVCacheConnector("{not json", library_path)
    with pytest.raises(errors.KVCacheConfigError):
        KVCacheConnector('{"namespace": "x", "unknown_field": 1}', library_path)
    with pytest.raises(errors.KVCacheConfigError):
        KVCacheConnector(json.dumps({
            "namespace": "x",
            "control_plane": {"endpoints": ["10.0.0.8:9100"], "insecure_test_mode": True},
            "transport": {"mode": "tcp", "tcp": {"enabled": True}},
        }), library_path)


def test_close_idempotent_and_context_manager(server_config, library_path):
    conn = KVCacheConnector.from_json(json.dumps(server_config), library_path)
    conn.close()
    conn.close()  # idempotent
    with pytest.raises(errors.KVCacheInvalidArgument):
        conn.get_batch(["k"])
    with KVCacheConnector.from_json(json.dumps(server_config), library_path) as c2:
        assert c2.caps().max_chunk_bytes > 0


def test_buffer_ownership_repeated_gets(connector):
    key, missing = _key(), _key()
    data = os.urandom(64 * 1024)
    connector.put_batch([KVChunk(key=key, data=data)])
    # Repeated allocation/free of response and data buffers; a double free or
    # a leaked Go pointer would crash or corrupt here.
    for _ in range(300):
        got = connector.get_batch([key, missing])
        assert got[0].data == data
        assert got[1].data is None


def test_concurrent_calls(connector):
    shared = _key()
    payload = b"shared payload"
    connector.put_batch([KVChunk(key=shared, data=payload)])

    def worker(w: int) -> None:
        key = _key()
        data = f"worker-{w}".encode() * 100
        assert connector.put_batch([KVChunk(key=key, data=data)])[0].ok
        for _ in range(10):
            got = connector.get_batch([key, shared])
            assert got[0].data == data
            assert got[1].data == payload

    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(worker, range(8)))


def test_get_values_batch_error_exposes_results(failing_server_config, library_path):
    with KVCacheConnector.from_json(json.dumps(failing_server_config), library_path) as conn:
        key, missing = _key(), _key()
        assert conn.put_batch([KVChunk(key=key, data=b"data")])[0].ok
        # get_batch surfaces the failure as a per-key result, no exception.
        results = conn.get_batch([key, missing])
        assert not results[0].hit and results[0].error_code == "READ_TARGET_FAILED"
        assert not results[1].hit and results[1].error_code == "KEY_NOT_FOUND"
        # get_values raises but keeps partial results; a miss is not a failure.
        with pytest.raises(errors.KVCacheBatchError) as exc:
            conn.get_values([key, missing])
        by_key = {r.key: r for r in exc.value.results}
        assert by_key[key].error_code == "READ_TARGET_FAILED"
        assert by_key[missing].error_code == "KEY_NOT_FOUND"


def test_from_config_roundtrip(server_config, library_path):
    cfg = Config.from_dict(server_config)
    assert Config.from_json(cfg.to_json()) == cfg
    assert cfg.transport.session_idle_timeout_ms == 60000
    with KVCacheConnector.from_config(cfg, library_path) as conn:
        key = _key()
        data = b"from-config payload"
        assert conn.put_batch([KVChunk(key=key, data=data)])[0].ok
        assert conn.get_values([key]) == [data]
    with KVCacheConnector.from_config(server_config, library_path) as conn:
        assert conn.caps().max_chunk_bytes > 0


def test_config_wire_block_limit_roundtrip():
    cfg = Config.from_dict({"namespace": "x", "limits": {"max_wire_block_bytes": 1048576}})
    assert cfg.limits.max_wire_block_bytes == 1048576
    assert Config.from_json(cfg.to_json()) == cfg
    assert Config().limits.max_wire_block_bytes == 0


def test_abi_error_code_mapping():
    for code, cls in [
        (1, errors.KVCacheInvalidArgument),
        (2, errors.KVCacheConfigError),
        (3, errors.KVCacheInternalError),
        (4, errors.KVCacheTimeout),
        (5, errors.KVCacheTransportError),
        (6, errors.KVCacheCancelled),
        (7, errors.KVCacheMemoryError),
    ]:
        with pytest.raises(cls):
            errors.raise_for_code(code, None)
    errors.raise_for_code(0, None)  # OK does not raise


def test_abi_oversized_length_rejected(connector):
    # Lengths beyond C.int range must return INVALID_ARGUMENT, not crash
    # the host process with a Go panic (spec §6.1).
    lib = connector._lib
    h = connector._handle
    huge = 2**31
    resp = ctypes.POINTER(ctypes.c_uint8)()
    resp_len = ctypes.c_size_t()

    for fn in (lib.kvc_match, lib.kvc_delete):
        assert fn(h, b"x", huge, ctypes.byref(resp), ctypes.byref(resp_len)) == 1

    bufs = ctypes.POINTER(_Buf)()
    count = ctypes.c_size_t()
    assert lib.kvc_get(
        h, b"x", huge, ctypes.byref(resp), ctypes.byref(resp_len),
        ctypes.byref(bufs), ctypes.byref(count),
    ) == 1
    assert lib.kvc_put(h, b"x", huge, None, 0, ctypes.byref(resp), ctypes.byref(resp_len)) == 1

    handle = ctypes.c_void_p()
    assert lib.kvc_open(b"{}", huge, ctypes.byref(handle)) == 1
    assert not handle.value

    # Oversized per-chunk buffer length in kvc_put.
    req = pb.PutBatchRequest()
    chunk = req.chunks.add()
    chunk.key = "oversize"
    chunk.size_bytes = huge
    raw = req.SerializeToString()
    buf_array = (_Buf * 1)()
    buf_array[0].data = ctypes.cast(ctypes.c_char_p(b"x"), ctypes.c_void_p)
    buf_array[0].len = huge
    assert lib.kvc_put(
        h, raw, len(raw), buf_array, 1, ctypes.byref(resp), ctypes.byref(resp_len),
    ) == 1
