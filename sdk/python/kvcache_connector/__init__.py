"""KV Cache connector Python adapter."""

from .connector import (
    KVCacheConnector,
    KVChunk,
    Caps,
    MatchResult,
    GetResult,
    PutResult,
    DeleteResult,
)
from .config import Config
from .errors import (
    KVCacheError,
    KVCacheConfigError,
    KVCacheInvalidArgument,
    KVCacheTimeout,
    KVCacheTransportError,
    KVCacheCancelled,
    KVCacheInternalError,
    KVCacheMemoryError,
    KVCacheBatchError,
)

__all__ = [
    "KVCacheConnector",
    "KVChunk",
    "Caps",
    "MatchResult",
    "GetResult",
    "PutResult",
    "DeleteResult",
    "Config",
    "KVCacheError",
    "KVCacheConfigError",
    "KVCacheInvalidArgument",
    "KVCacheTimeout",
    "KVCacheTransportError",
    "KVCacheCancelled",
    "KVCacheInternalError",
    "KVCacheMemoryError",
    "KVCacheBatchError",
]