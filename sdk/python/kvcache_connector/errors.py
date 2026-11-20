"""Connector-level exceptions. Per-key misses and business errors live in
result objects, not exceptions."""


class KVCacheError(Exception):
    def __init__(self, code: str, message: str):
        super().__init__(f"{code}: {message}")
        self.code = code
        self.message = message


class KVCacheConfigError(KVCacheError):
    def __init__(self, message: str):
        super().__init__("CONFIG_ERROR", message)


class KVCacheInvalidArgument(KVCacheError):
    def __init__(self, message: str):
        super().__init__("INVALID_ARGUMENT", message)


class KVCacheTimeout(KVCacheError):
    def __init__(self, message: str):
        super().__init__("TIMEOUT", message)


class KVCacheTransportError(KVCacheError):
    def __init__(self, message: str):
        super().__init__("TRANSPORT_UNAVAILABLE", message)


class KVCacheCancelled(KVCacheError):
    def __init__(self, message: str):
        super().__init__("CONTEXT_CANCELLED", message)


class KVCacheInternalError(KVCacheError):
    def __init__(self, message: str):
        super().__init__("INTERNAL_ERROR", message)


class KVCacheMemoryError(KVCacheError):
    def __init__(self, message: str):
        super().__init__("MEMORY_ERROR", message)


class KVCacheBatchError(KVCacheError):
    """A batch contained read/transport failures. ``results`` exposes the
    per-key outcomes (hits, misses and failures) collected before raising."""

    def __init__(self, message: str, results):
        super().__init__("BATCH_FAILED", message)
        self.results = results


_ABI_EXCEPTIONS = {
    1: KVCacheInvalidArgument,
    2: KVCacheConfigError,
    3: KVCacheInternalError,
    4: KVCacheTimeout,
    5: KVCacheTransportError,
    6: KVCacheCancelled,
    7: KVCacheMemoryError,
}


def raise_for_code(code: int, error_string) -> None:
    if code == 0:
        return
    message = error_string(code) if error_string else f"ABI code {code}"
    if isinstance(message, bytes):
        message = message.decode("utf-8", "replace")
    cls = _ABI_EXCEPTIONS.get(code, KVCacheInternalError)
    raise cls(message)