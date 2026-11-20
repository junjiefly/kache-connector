"""JSON configuration dataclasses mirroring the Go connector config."""

from __future__ import annotations

import json
from dataclasses import dataclass, field, asdict
from typing import Any


@dataclass
class TLSConfig:
    enabled: bool = False
    ca_file: str = ""
    cert_file: str = ""
    key_file: str = ""
    server_name: str = ""


@dataclass
class ControlPlaneConfig:
    endpoints: list[str] = field(default_factory=list)
    timeout_ms: int = 5000
    auth_token_env: str = ""
    tls: TLSConfig = field(default_factory=TLSConfig)
    insecure_test_mode: bool = False
    caps_refresh_seconds: int = 60


@dataclass
class RDMATransportConfig:
    enabled: bool = True
    dial_timeout_ms: int = 2000
    pool_size_bytes: int = 268435456
    max_outstanding: int = 32


@dataclass
class TCPTransportConfig:
    enabled: bool = True
    dial_timeout_ms: int = 2000


@dataclass
class TransportConfig:
    mode: str = "auto"
    fallback_enabled: bool = True
    session_idle_timeout_ms: int = 60000
    rdma: RDMATransportConfig = field(default_factory=RDMATransportConfig)
    tcp: TCPTransportConfig = field(default_factory=TCPTransportConfig)


@dataclass
class LimitsConfig:
    max_chunk_bytes: int = 67108864
    max_batch_chunks: int = 64
    max_inflight_bytes: int = 268435456
    max_concurrent_blocks: int = 16
    max_wire_block_bytes: int = 0


@dataclass
class Config:
    namespace: str = ""
    control_plane: ControlPlaneConfig = field(default_factory=ControlPlaneConfig)
    transport: TransportConfig = field(default_factory=TransportConfig)
    limits: LimitsConfig = field(default_factory=LimitsConfig)

    def to_json(self) -> str:
        return json.dumps(asdict(self))

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Config":
        cp = data.get("control_plane", {})
        tr = data.get("transport", {})
        lim = data.get("limits", {})
        return cls(
            namespace=data.get("namespace", ""),
            control_plane=ControlPlaneConfig(
                endpoints=list(cp.get("endpoints", [])),
                timeout_ms=cp.get("timeout_ms", 5000),
                auth_token_env=cp.get("auth_token_env", ""),
                tls=TLSConfig(**cp.get("tls", {})),
                insecure_test_mode=cp.get("insecure_test_mode", False),
                caps_refresh_seconds=cp.get("caps_refresh_seconds", 60),
            ),
            transport=TransportConfig(
                mode=tr.get("mode", "auto"),
                fallback_enabled=tr.get("fallback_enabled", True),
                session_idle_timeout_ms=tr.get("session_idle_timeout_ms", 60000),
                rdma=RDMATransportConfig(**tr.get("rdma", {})),
                tcp=TCPTransportConfig(**tr.get("tcp", {})),
            ),
            limits=LimitsConfig(**lim),
        )

    @classmethod
    def from_json(cls, text: str) -> "Config":
        return cls.from_dict(json.loads(text))
