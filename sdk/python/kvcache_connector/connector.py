"""Synchronous ctypes adapter for the KV Cache connector shared library.

Python never calls RDMA directly and never sees opaque refs, capabilities or
raw storage plans; those stay inside the Go connector."""

from __future__ import annotations

import ctypes
import json
import os
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping, Sequence

from . import errors
from ._pb import pb
from .config import Config


@dataclass(frozen=True)
class KVChunk:
    key: str
    data: bytes
    content_hash: bytes | None = None
    attributes: Mapping[str, str] | None = None


@dataclass(frozen=True)
class Caps:
    max_chunk_bytes: int
    max_batch_chunks: int
    max_batch_bytes: int
    max_concurrent_blocks: int
    max_wire_block_bytes: int
    supported_transports: tuple[str, ...]
    transport_fallback_enabled: bool
    dedup_enabled: bool


@dataclass(frozen=True)
class MatchResult:
    key: str
    hit: bool
    error_code: str | None = None
    attributes: Mapping[str, str] | None = None


@dataclass(frozen=True)
class GetResult:
    key: str
    hit: bool
    data: bytes | None = None
    size_bytes: int = 0
    content_hash: bytes | None = None
    error_code: str | None = None
    message: str | None = None
    attributes: Mapping[str, str] | None = None
    last_modified_unix_sec: int = 0


@dataclass(frozen=True)
class PutResult:
    key: str
    ok: bool
    error_code: str | None = None
    message: str | None = None


@dataclass(frozen=True)
class DeleteResult:
    key: str
    deleted: bool
    error_code: str | None = None


class _Buf(ctypes.Structure):
    _fields_ = [("data", ctypes.c_void_p), ("len", ctypes.c_size_t)]


def _default_library_path() -> str:
    override = os.environ.get("KVCACHE_LIBRARY")
    if override:
        return override
    root = Path(__file__).resolve().parents[3]
    for name in ("build/libkvcache.so", "build/kvcache.dll", "libkvcache.so", "kvcache.dll"):
        candidate = root / name
        if candidate.exists():
            return str(candidate)
    raise errors.KVCacheConfigError(
        "shared library not found; build it with `make shlib` or set KVCACHE_LIBRARY"
    )


def _load_library(path: str) -> ctypes.CDLL:
    lib = ctypes.CDLL(path)
    u8p = ctypes.POINTER(ctypes.c_uint8)
    szp = ctypes.POINTER(ctypes.c_size_t)
    lib.kvc_open.argtypes = [ctypes.c_char_p, ctypes.c_size_t, ctypes.POINTER(ctypes.c_void_p)]
    lib.kvc_open.restype = ctypes.c_int32
    lib.kvc_version.argtypes = [ctypes.POINTER(ctypes.c_uint32)] * 3
    lib.kvc_version.restype = ctypes.c_int32
    lib.kvc_close.argtypes = [ctypes.c_void_p]
    lib.kvc_close.restype = None
    lib.kvc_caps.argtypes = [ctypes.c_void_p, ctypes.POINTER(u8p), szp]
    lib.kvc_caps.restype = ctypes.c_int32
    lib.kvc_match.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_size_t, ctypes.POINTER(u8p), szp]
    lib.kvc_match.restype = ctypes.c_int32
    lib.kvc_get.argtypes = [
        ctypes.c_void_p, ctypes.c_char_p, ctypes.c_size_t,
        ctypes.POINTER(u8p), szp, ctypes.POINTER(ctypes.POINTER(_Buf)), szp,
    ]
    lib.kvc_get.restype = ctypes.c_int32
    lib.kvc_put.argtypes = [
        ctypes.c_void_p, ctypes.c_char_p, ctypes.c_size_t,
        ctypes.POINTER(_Buf), ctypes.c_size_t, ctypes.POINTER(u8p), szp,
    ]
    lib.kvc_put.restype = ctypes.c_int32
    lib.kvc_delete.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_size_t, ctypes.POINTER(u8p), szp]
    lib.kvc_delete.restype = ctypes.c_int32
    lib.kvc_free_buffer.argtypes = [u8p]
    lib.kvc_free_buffer.restype = None
    lib.kvc_free_buffers.argtypes = [ctypes.POINTER(_Buf), ctypes.c_size_t]
    lib.kvc_free_buffers.restype = None
    lib.kvc_error_string.argtypes = [ctypes.c_int32]
    lib.kvc_error_string.restype = ctypes.c_char_p
    return lib


def _opt_str(value: str) -> str | None:
    return value or None


def _opt_bytes(value: bytes) -> bytes | None:
    return value or None


def _attrs(mapping) -> dict[str, str] | None:
    return dict(mapping) if mapping else None


class KVCacheConnector:
    def __init__(self, config_json: str, library_path: str | None = None):
        self._lib = _load_library(library_path or _default_library_path())
        self._lock = threading.RLock()
        self._handle = ctypes.c_void_p()
        raw = config_json.encode("utf-8") if isinstance(config_json, str) else bytes(config_json)
        code = self._lib.kvc_open(raw, len(raw), ctypes.byref(self._handle))
        errors.raise_for_code(code, self._lib.kvc_error_string)
        if not self._handle.value:
            raise errors.KVCacheConfigError("kvc_open returned a null handle")

    @classmethod
    def from_json(cls, config_json: str, library_path: str | None = None) -> "KVCacheConnector":
        return cls(config_json, library_path)

    @classmethod
    def from_config(cls, config: Config | dict, library_path: str | None = None) -> "KVCacheConnector":
        if isinstance(config, Config):
            text = config.to_json()
        else:
            text = json.dumps(config)
        return cls(text, library_path)

    def __enter__(self) -> "KVCacheConnector":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def _check_open(self) -> None:
        if not self._handle.value:
            raise errors.KVCacheInvalidArgument("connector is closed")

    def _read_response(self, resp_ptr, resp_len) -> bytes:
        try:
            if resp_ptr and resp_len.value:
                return ctypes.string_at(resp_ptr, resp_len.value)
            return b""
        finally:
            if resp_ptr:
                self._lib.kvc_free_buffer(resp_ptr)

    def version(self) -> tuple[int, int, int]:
        major, minor, patch = ctypes.c_uint32(), ctypes.c_uint32(), ctypes.c_uint32()
        code = self._lib.kvc_version(ctypes.byref(major), ctypes.byref(minor), ctypes.byref(patch))
        errors.raise_for_code(code, self._lib.kvc_error_string)
        return major.value, minor.value, patch.value

    def caps(self) -> Caps:
        with self._lock:
            self._check_open()
            resp = ctypes.POINTER(ctypes.c_uint8)()
            resp_len = ctypes.c_size_t()
            code = self._lib.kvc_caps(self._handle, ctypes.byref(resp), ctypes.byref(resp_len))
            errors.raise_for_code(code, self._lib.kvc_error_string)
            data = self._read_response(resp, resp_len)
        reply = pb.GetCapsReply()
        reply.ParseFromString(data)
        return Caps(
            max_chunk_bytes=reply.max_chunk_bytes,
            max_batch_chunks=reply.max_batch_chunks,
            max_batch_bytes=reply.max_batch_bytes,
            max_concurrent_blocks=reply.max_concurrent_blocks,
            max_wire_block_bytes=reply.max_wire_block_bytes,
            supported_transports=tuple(reply.supported_transports),
            transport_fallback_enabled=reply.transport_fallback_enabled,
            dedup_enabled=reply.dedup_enabled,
        )

    def match_batch(self, keys: Sequence[str]) -> list[MatchResult]:
        req = pb.MatchBatchRequest(keys=list(keys))
        with self._lock:
            self._check_open()
            raw = req.SerializeToString()
            resp = ctypes.POINTER(ctypes.c_uint8)()
            resp_len = ctypes.c_size_t()
            code = self._lib.kvc_match(self._handle, raw, len(raw), ctypes.byref(resp), ctypes.byref(resp_len))
            errors.raise_for_code(code, self._lib.kvc_error_string)
            data = self._read_response(resp, resp_len)
        reply = pb.MatchBatchReply()
        reply.ParseFromString(data)
        return [
            MatchResult(
                key=r.key, hit=r.hit, error_code=_opt_str(r.error_code),
                attributes=_attrs(r.attributes),
            )
            for r in reply.results
        ]

    def get_batch(self, keys: Sequence[str]) -> list[GetResult]:
        keys = list(keys)
        req = pb.GetBatchRequest(keys=keys)
        with self._lock:
            self._check_open()
            raw = req.SerializeToString()
            resp = ctypes.POINTER(ctypes.c_uint8)()
            resp_len = ctypes.c_size_t()
            bufs = ctypes.POINTER(_Buf)()
            count = ctypes.c_size_t()
            code = self._lib.kvc_get(
                self._handle, raw, len(raw),
                ctypes.byref(resp), ctypes.byref(resp_len),
                ctypes.byref(bufs), ctypes.byref(count),
            )
            errors.raise_for_code(code, self._lib.kvc_error_string)
            # Free in reverse allocation order: the response was allocated
            # after the data buffers (spec §6.2).
            data = self._read_response(resp, resp_len)
            payloads: list[bytes | None] = [None] * len(keys)
            try:
                for i in range(count.value):
                    buf = bufs[i]
                    if buf.data and buf.len:
                        payloads[i] = ctypes.string_at(buf.data, buf.len)
            finally:
                if bufs:
                    self._lib.kvc_free_buffers(bufs, count)
        reply = pb.GetBatchReply()
        reply.ParseFromString(data)
        by_key = {r.key: r for r in reply.results}
        results: list[GetResult] = []
        for i, key in enumerate(keys):
            r = by_key.get(key)
            if r is None:
                raise errors.KVCacheInternalError(f"missing result for key {key!r}")
            results.append(GetResult(
                key=r.key, hit=r.hit, data=payloads[i] if r.hit else None,
                size_bytes=r.size_bytes, content_hash=_opt_bytes(r.content_hash),
                error_code=_opt_str(r.error_code), message=_opt_str(r.message),
                attributes=_attrs(r.attributes),
                last_modified_unix_sec=r.last_modified_unix_sec,
            ))
        return results

    def get_values(self, keys: Sequence[str]) -> list[bytes | None]:
        results = self.get_batch(keys)
        failures = [r for r in results if not r.hit and r.error_code != "KEY_NOT_FOUND"]
        if failures:
            raise errors.KVCacheBatchError(
                f"{len(failures)} key(s) failed to read", results
            )
        return [r.data if r.hit else None for r in results]

    def put_batch(self, items: Sequence[KVChunk]) -> list[PutResult]:
        items = list(items)
        req = pb.PutBatchRequest()
        payloads: list[bytes] = []
        for item in items:
            chunk = req.chunks.add()
            chunk.key = item.key
            chunk.size_bytes = len(item.data)
            if item.content_hash:
                chunk.content_hash = item.content_hash
            if item.attributes:
                chunk.attributes.update(item.attributes)
            payloads.append(item.data)
        buf_array = (_Buf * len(items))()
        for i, payload in enumerate(payloads):
            buf_array[i].data = ctypes.cast(ctypes.c_char_p(payload), ctypes.c_void_p)
            buf_array[i].len = len(payload)
        with self._lock:
            self._check_open()
            raw = req.SerializeToString()
            resp = ctypes.POINTER(ctypes.c_uint8)()
            resp_len = ctypes.c_size_t()
            code = self._lib.kvc_put(
                self._handle, raw, len(raw), buf_array, len(items),
                ctypes.byref(resp), ctypes.byref(resp_len),
            )
            errors.raise_for_code(code, self._lib.kvc_error_string)
            data = self._read_response(resp, resp_len)
        reply = pb.PutBatchReply()
        reply.ParseFromString(data)
        return [
            PutResult(key=r.key, ok=r.ok, error_code=_opt_str(r.error_code), message=_opt_str(r.message))
            for r in reply.results
        ]

    def delete_batch(self, keys: Sequence[str], *, force: bool = False) -> list[DeleteResult]:
        req = pb.DeleteBatchRequest(keys=list(keys), force=force)
        with self._lock:
            self._check_open()
            raw = req.SerializeToString()
            resp = ctypes.POINTER(ctypes.c_uint8)()
            resp_len = ctypes.c_size_t()
            code = self._lib.kvc_delete(self._handle, raw, len(raw), ctypes.byref(resp), ctypes.byref(resp_len))
            errors.raise_for_code(code, self._lib.kvc_error_string)
            data = self._read_response(resp, resp_len)
        reply = pb.DeleteBatchReply()
        reply.ParseFromString(data)
        return [
            DeleteResult(key=r.key, deleted=r.deleted, error_code=_opt_str(r.error_code))
            for r in reply.results
        ]

    def close(self) -> None:
        with self._lock:
            if self._handle.value:
                handle = self._handle
                self._handle = ctypes.c_void_p()
                self._lib.kvc_close(handle)