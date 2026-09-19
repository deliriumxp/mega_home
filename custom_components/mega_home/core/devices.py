"""Описания устройств объекта — данные конфига (`accesses[]`), не код.

⚠ Никакого перечня классов и черносписочных полей: авторизация — только
`basic`/`digest` как у `connect` (`docs/plan-thin-gateway.md`, «Слушатели»),
учётка — обычные поля описания. Единственный потребитель — слушатели
(`listeners.py`, `listeners_out.py`): они держат подписку без жильца у экрана,
и им нужен адрес устройства ПОСТОЯННО, а не на один вызов бандла.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

AUTH_TYPES = ("none", "basic", "digest")
EVENT_TYPES = ("webhook", "poll", "stream", "mqtt", "tcp", "tcpServer", "udp", "ws")
CALL_TIMEOUT = 30.0
MAX_TIMEOUT = 600.0

@dataclass
class AuthSpec:
    type: str = "none"
    user: str = ""
    password: str = ""

@dataclass
class DeviceDescriptor:
    """Одно устройство объекта: адрес, авторизация, источники событий."""

    id: str
    host: str
    port: int = 443
    tls: bool = False
    timeout: float = CALL_TIMEOUT
    auth: AuthSpec = field(default_factory=AuthSpec)
    events: list[dict[str, Any]] = field(default_factory=list)

def _seconds(value: Any, default: float) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    return min(number, MAX_TIMEOUT) if number > 0 else default

def _auth_of(block: Any) -> AuthSpec:
    if not isinstance(block, dict):
        return AuthSpec()
    kind = str(block.get("type") or "none")
    return AuthSpec(
        type=kind if kind in AUTH_TYPES else "none",
        user=str(block.get("user") or ""),
        password=str(block.get("pass") or ""),
    )

def descriptor_of(block: Any) -> DeviceDescriptor | None:
    """Собрать описание из блока конфига; мусор — «описания нет»."""
    if not isinstance(block, dict):
        return None
    host = str(block.get("host") or "").strip()
    if not host:
        return None
    try:
        port = int(block.get("port") or 443)
    except (TypeError, ValueError):
        return None
    events = block.get("events")
    return DeviceDescriptor(
        id=str(block.get("id") or "device"),
        host=host,
        port=port,
        tls=block.get("tls") is True,
        timeout=_seconds(block.get("timeout"), CALL_TIMEOUT),
        auth=_auth_of(block.get("auth")),
        events=[item for item in events if isinstance(item, dict)] if isinstance(events, list) else [],
    )

class DeviceRegistry:
    """Устройства объекта из конфига — для слушателей (`listeners.py`)."""

    def __init__(self) -> None:
        self._descriptors: dict[str, DeviceDescriptor] = {}

    def apply(self, blocks: Any) -> None:
        """Принять описания из конфига объекта (список блоков `accesses`)."""
        fresh: dict[str, DeviceDescriptor] = {}
        for block in blocks if isinstance(blocks, list) else []:
            descriptor = descriptor_of(block)
            if descriptor is not None:
                fresh[descriptor.id] = descriptor
        self._descriptors = fresh

    def ids(self) -> list[str]:
        return list(self._descriptors)

    def descriptors(self) -> list[DeviceDescriptor]:
        return list(self._descriptors.values())

    def descriptor(self, device: str | None) -> DeviceDescriptor | None:
        if device:
            return self._descriptors.get(device)
        if len(self._descriptors) == 1:
            return next(iter(self._descriptors.values()))
        return None
