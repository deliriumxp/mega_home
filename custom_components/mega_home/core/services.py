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

import socket
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class Service:
    """Поднятая служба: порт на петле и то, что она знает о себе сама.

    ⚠ `tls`, `headers` и `ws_open` ставит ТОТ, кто регистрирует службу, а не
    вызывающий: учётка службы (токен Home Assistant) не покидает дом, как второй
    круг Digest (`docs/home-gateway.md`, «Личность»). Описание вызова от бандла
    их не знает и перебить не может.
    """

    port: int
    tls: bool = False
    headers: dict[str, str] = field(default_factory=dict)
    # Рукопожатие сокета до первого кадра вызывающего (вход по протоколу службы).
    ws_open: Callable[[Any], Awaitable[None]] | None = None
    # Что учётка службы НЕ даёт вызывающему: `(what, value)` — `"path"` запроса или `"frame"`
    # сокета; причина отказа словами или `None`. Ставит регистрирующий, как учётку.
    guard: Callable[[str, str], str | None] | None = None
    # Что недоступно БАНДЛУ жильца (локальная дверь, пересылка менеджером, сессии) — та же форма
    # `(what, value)`. Операции самого менеджера идут мимо: подключение хранилища и создание папок
    # у службы записи — дело инсталлятора, а не телефона с ключом устройства.
    bundle_guard: Callable[[str, str], str | None] | None = None
    # Служба сама ставит метку кэша своим ответам, и ресурс браузеру (`connect.resource`) её не
    # подменяет: служба записи знает, что закрытое окно спрайта и кадр под меткой не меняются, а текущее
    # окно — меняется. У прочих служб метка двери: их ответ — «сейчас» (кадр go2rtc).
    own_cache: bool = False


REGISTRY: dict[str, Service] = {}


def port_busy(probes: tuple[tuple[int, int, str], ...]) -> str:
    """Держит ли КТО-ТО ЖИВОЙ один из портов. Возвращает описание занятого или ''.

    `probes` — (вид сокета, порт, подпись для диагностики).

    ⚠ Проба идёт с `SO_REUSEADDR`, и это принципиально (живой факт 2026-09-19,
    объект после обновления 0.4.0): при перезапуске Home Assistant дом гасит свой
    go2rtc, пока к его API ещё открыты соединения, сервер закрывает их первым,
    и на 127.0.0.1:1985 минуту висят сокеты TIME_WAIT. Проба без `SO_REUSEADDR`
    об них спотыкалась, дом решал «порт занят чужим go2rtc или аддоном» и больше
    не пытался — и весь аптайм объект жил без своего go2rtc. С `SO_REUSEADDR`
    TIME_WAIT не считается, а живой слушатель (LISTEN, чужой UDP) по-прежнему
    даёт EADDRINUSE: Linux не пускает второй сокет поверх активного. Ровно это и
    нужно: «занят» значит «кто-то слушает», а не «кто-то недавно слушал».
    """
    for kind, port, name in probes:
        with socket.socket(socket.AF_INET, kind) as probe:
            probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                probe.bind(("0.0.0.0", port))
            except OSError:
                return name
    return ""

def register(
    name: str,
    port: int,
    *,
    tls: bool = False,
    headers: dict[str, str] | None = None,
    ws_open: Callable[[Any], Awaitable[None]] | None = None,
    guard: Callable[[str, str], str | None] | None = None,
    bundle_guard: Callable[[str, str], str | None] | None = None,
    own_cache: bool = False,
) -> None:
    """Служба поднялась: доступна для `connect` по имени."""
    REGISTRY[name] = Service(port, tls, dict(headers or {}), ws_open, guard, bundle_guard, own_cache)

def unregister(name: str) -> None:
    """Служба остановлена: адрес по имени больше не резолвится."""
    REGISTRY.pop(name, None)

def resolve(name: str) -> int | None:
    """Порт службы на loopback, если она сейчас поднята."""
    service = REGISTRY.get(name)
    return service.port if service else None

def entry(name: str) -> Service | None:
    """Поднятая служба целиком — вместе с тем, что вызов к ней получает от дома."""
    return REGISTRY.get(name)
