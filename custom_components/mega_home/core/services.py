"""Реестр loopback-служб дома: кто слушает петлю и на каком порту.

⚠ Не список из головы (`docs/plan-thin-gateway.md`, замок 3): реестр ПУСТ до
запуска процессов и наполняется ими самими. `go2rtc_embed` регистрирует
«go2rtc» своим API-портом, `sip_bridge` — «asterisk» портом ARI/WS. Останов
процесса снимает запись: адресат `connect`, переставший существовать, обязан
получить честный отказ, а не адрес мёртвого порта.

Единственный читатель — транспорт (`connect.py`, `stream.py`): `host` вида
"go2rtc" резолвится в `(127.0.0.1, порт)`, и это ЕДИНСТВЕННЫЙ способ дойти до
loopback через `connect` — literal loopback-адрес в запросе отклоняется.
"""

from __future__ import annotations

REGISTRY: dict[str, int] = {}

def register(name: str, port: int) -> None:
    """Служба поднялась: доступна для `connect` по имени."""
    REGISTRY[name] = port

def unregister(name: str) -> None:
    """Служба остановлена: адрес по имени больше не резолвится."""
    REGISTRY.pop(name, None)

def resolve(name: str) -> int | None:
    """Порт службы на loopback, если она сейчас поднята."""
    return REGISTRY.get(name)
