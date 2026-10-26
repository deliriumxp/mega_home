"""Сторож продюсеров своего go2rtc: снимает осиротевшие ffmpeg и держит жёсткий предел.

⚠⚠ Зачем (объект 2026-10-26, два OOM подряд: 204 и 150 ffmpeg при норме 6). Просмотр архива
заводит поток `ffmpeg:<список>#input=playlist`; его продюсер — дочерний ffmpeg go2rtc. Если
WebRTC-потребитель пропал, не завершив переговоры (закрыли шторку, уснул телефон, оборвалась
сеть), go2rtc ещё держит потребителя, а `DELETE /api/streams` снимает только регистрацию: ffmpeg
продолжает читать список и умирает лишь по ICE-таймауту. Серия перемоток копит такие процессы
быстрее, чем они умирают, и контроллер уходит в OOM.

⚠ Сторож — СТРАХОВКА, а не замена уборке: приложение по-прежнему шлёт `DELETE` при закрытии
клипа (`withClose`) и `closeAll` на `pagehide` (`home-session.ts`). Здесь два правила, оба про
факты из `/proc` и ответа go2rtc:

* осиротевший — дочерний ffmpeg, чей вход (`-i`) не принадлежит НИ ОДНОМУ зарегистрированному
  потоку (`GET /api/streams`, у продюсера `source`/`url` или имя потока). Ждём `GRACE_S`, чтобы
  не убить продюсер, который go2rtc завёл за миг до появления в списке;
* предел — не больше `MAX_PRODUCERS` дочерних ffmpeg: при переполнении снимаем самые старые.
  ⚠ Это последняя линия против OOM: она может снять и живой поток, если их действительно
  больше предела, но контроллер с 4 ГБ иначе падает целиком.

⚠ Дети процесса Home Assistant (`record`/`preview`/`motion`) сюда не попадают: сторож смотрит
только детей НАШЕГО go2rtc (pid из `go2rtc_embed`). Чужой go2rtc (HA-управляемый) не трогаем.
"""

from __future__ import annotations

import asyncio
import os
import signal
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import aiohttp

from . import host_load
from .const import LOGGER

# Как часто сверяемся и сколько ждём появления продюсера в `/api/streams`.
EVERY_S = 10.0
GRACE_S = 15.0
# Потолок дочерних ffmpeg go2rtc. В норме их 1–2 (архив), запас — на перемотки и кадры плиток.
MAX_PRODUCERS = 12
# Ответ go2rtc на петле; больше — он занят, лучше пропустить проход, чем копить запросы.
TIMEOUT_S = 5.0


@dataclass(frozen=True)
class Producer:
    """Дочерний ffmpeg go2rtc: pid, старт (секунды от загрузки) и вход из `-i`."""

    pid: int
    start: float
    input: str


def input_of(argv: list[str]) -> str:
    """Вход ffmpeg — значение первого `-i`; пусто — команда не про поток с входом."""
    for number, arg in enumerate(argv[:-1]):
        if arg == "-i":
            return argv[number + 1]
    return ""


def registered_sources(streams: Any) -> set[str]:
    """Всё, чем go2rtc может назвать живой поток: имена, `source` и `url` продюсеров."""
    out: set[str] = set()
    if not isinstance(streams, dict):
        return out
    for name, stream in streams.items():
        if isinstance(name, str) and name:
            out.add(name)
        if not isinstance(stream, dict):
            continue
        for producer in stream.get("producers") or []:
            if not isinstance(producer, dict):
                continue
            for key in ("source", "url"):
                value = producer.get(key)
                if isinstance(value, str) and value:
                    out.add(value)
    return out


def orphans(children: list[Producer], sources: set[str], uptime: float, grace: float = GRACE_S) -> list[Producer]:
    """Дети, чей вход не принадлежит ни одному живому потоку и которые старше `grace`."""
    return [
        child
        for child in children
        if child.input and uptime - child.start >= grace and not any(child.input in source for source in sources)
    ]


def over_cap(children: list[Producer], cap: int = MAX_PRODUCERS) -> list[Producer]:
    """Самые старые дети сверх предела — их снимаем, если перебор не ушёл с сиротами."""
    if len(children) <= cap:
        return []
    return sorted(children, key=lambda child: child.start)[: len(children) - cap]


def _uptime() -> float:
    try:
        return float(Path("/proc/uptime").read_text().split()[0])
    except (OSError, ValueError, IndexError):
        return 0.0


def _scan(go2rtc_pid: int) -> tuple[list[Producer], float]:
    """Дети go2rtc, чей исполняемый файл — ffmpeg; старт — в секундах от загрузки."""
    clk = os.sysconf("SC_CLK_TCK")
    children: list[Producer] = []
    for pid in host_load.pids():
        info = host_load.proc(pid)
        if info is None or info[0] != go2rtc_pid:
            continue
        args = host_load.argv(pid)
        if not args or Path(args[0]).name != "ffmpeg":
            continue
        children.append(Producer(pid=pid, start=info[1] / clk, input=input_of(args)))
    return children, _uptime()


async def _streams(api: str) -> Any:
    timeout = aiohttp.ClientTimeout(total=TIMEOUT_S)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.get(f"{api}/api/streams") as answer:
            answer.raise_for_status()
            return await answer.json()


def _label(child: Producer) -> str:
    """Подпись для журнала: путь списка — именем, поток — словом (в URL бывает учётка)."""
    return "поток" if "://" in child.input else (Path(child.input).name or "ffmpeg")


def _kill(child: Producer, why: str) -> None:
    try:
        os.kill(child.pid, signal.SIGKILL)
    except ProcessLookupError:
        return
    except OSError as err:  # noqa: BLE001 - уборка не повод шуметь
        LOGGER.debug("go2rtc: %s ffmpeg %s не снят: %s", why, child.pid, err)
        return
    LOGGER.warning("go2rtc: снят %s ffmpeg %s (%s)", why, child.pid, _label(child))


async def sweep(api: str, go2rtc_pid: int) -> None:
    """Один проход: снять сирот, затем добить перебор предела."""
    streams = await _streams(api)
    children, uptime = await asyncio.to_thread(_scan, go2rtc_pid)
    dead = orphans(children, registered_sources(streams), uptime)
    for child in dead:
        _kill(child, "осиротевший")
    left = [child for child in children if child.pid not in {item.pid for item in dead}]
    for child in over_cap(left):
        _kill(child, "сверх предела")


async def run(api: str, go2rtc_pid: int) -> None:
    """Цикл сторожа на всё время жизни процесса go2rtc (снимается `async_stop`)."""
    while True:
        await asyncio.sleep(EVERY_S)
        try:
            await sweep(api, go2rtc_pid)
        except asyncio.CancelledError:
            raise
        except Exception as err:  # noqa: BLE001 - go2rtc занят/не ответил: пропускаем проход
            LOGGER.debug("go2rtc: сторож продюсеров не сработал: %s", err)
