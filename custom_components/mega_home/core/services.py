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

REGISTRY: dict[str, int] = {}


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

def register(name: str, port: int) -> None:
    """Служба поднялась: доступна для `connect` по имени."""
    REGISTRY[name] = port

def unregister(name: str) -> None:
    """Служба остановлена: адрес по имени больше не резолвится."""
    REGISTRY.pop(name, None)

def resolve(name: str) -> int | None:
    """Порт службы на loopback, если она сейчас поднята."""
    return REGISTRY.get(name)
