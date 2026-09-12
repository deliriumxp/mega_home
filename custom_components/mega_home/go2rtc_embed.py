"""Свой go2rtc для WebRTC — одна схема, без патча HA core.

Поднимает ``:8555`` (UDP+TCP) с ``candidates: [stun:8555]`` тем же бинарём
``go2rtc``, который лежит в образе Home Assistant. HA-управляемый ``:18555/tcp``
не трогаем: он слушает только TCP и только внутри дома, поэтому телефон снаружи
до него не дойдёт никогда — ради этого свой процесс и заведён.

⚠ Три вещи здесь несущие, и каждая стоила отказа просмотра на объекте:

* **stdout вычитывается ВСЕГДА.** Процесс запущен с трубой, а трубу с
  ненулевым буфером надо опустошать: заполнится (64 КБ логов) — go2rtc
  заблокируется на записи в лог, и просмотр умрёт до перезапуска Home
  Assistant. Поэтому чтение живёт отдельной задачей на всё время работы, а
  последние строки хранятся для диагностики;
* **процесс останавливается.** Осиротевший go2rtc держит ``:8555``, и после
  перезапуска HA новый экземпляр слушатель уже не поднимет — «снаружи работало,
  а потом перестало, и лечится только ребутом»;
* **порт проверяется ДО старта.** Занят — значит на объекте уже есть свой
  go2rtc (аддон или docker), и он почти наверняка настроен правильно. Тогда мы
  не поднимаемся вовсе и уступаем штатному пути Home Assistant: подменять
  РАБОЧИЙ путь своим, у которого нет слушателя, — это `srflx 0` в диагностике и
  поиск несуществующей проблемы с NAT.
* **но сначала — СИРОТА (0.2.44).** Отвечающий на НАШЕМ API-порте (1985,
  только петля) — это наш же процесс прошлого запуска, переживший нечистую
  остановку HA. Уступить его «чужому аддону» — значит после каждого такого
  рестарта навсегда остаться с «не поднят go2rtc». Усыновляем: конфиг наш,
  порты его, переговоры продолжаются без единого разрыва.
"""

from __future__ import annotations

import asyncio
import shutil
import socket
import tempfile
from collections import deque
from pathlib import Path

from homeassistant.core import HomeAssistant

from .const import LOGGER

# Порт сигналинга и медиа. Тот же и в кандидате `stun:8555`: наружу объект
# отдаёт ровно этот адрес, и проброс на роутере (если он нужен) делается на него.
WEBRTC_PORT = 8555
# API только для нас, поэтому на петле. 1985, а не 1984: 1984 занимает go2rtc,
# который человек мог поставить сам, и отобрать у него порт мы не вправе.
API_PORT = 1985
# Сколько ждём, пока go2rtc ответит по своему API. Он стартует за десятые доли
# секунды; секунды здесь — запас для слабого железа, а не нормальное ожидание.
READY_TIMEOUT = 10.0
# Сколько строк лога держим для диагностики. Больше не нужно: важен хвост.
LOG_TAIL = 40

_YAML = f"""api:
  listen: "127.0.0.1:{API_PORT}"
webrtc:
  listen: ":{WEBRTC_PORT}"
  candidates: [stun:{WEBRTC_PORT}]
  ice_servers:
    - urls: [stun:stun.l.google.com:19302]
    - urls: [stun:stun.home-assistant.io:3478]
"""

URL = f"http://127.0.0.1:{API_PORT}"

_proc: asyncio.subprocess.Process | None = None
_drain: asyncio.Task[None] | None = None
_tmp: str | None = None
_ready = False
_log: deque[str] = deque(maxlen=LOG_TAIL)


async def async_start(hass: HomeAssistant) -> bool:
    """Поднять свой go2rtc. False — идём штатным путём Home Assistant."""
    global _proc, _tmp, _ready

    if is_running():
        return True
    # ⚠ СИРОТА — раньше проверки портов. Свой go2rtc прошлого запуска HA,
    # переживший нечистую остановку, держит и API, и медиа-порт. Уступить его
    # «чужому аддону» значило бы навсегда остаться с «не поднят go2rtc»:
    # живой факт 2026-09-12 — два обновления подряд, и камера мертва до
    # ребута объекта. Отвечает НАШ API на петле — усыновляем, а не уступаем.
    if await _api_alive(hass):
        _ready = True
        LOGGER.info(
            "Усыновлён go2rtc прошлого запуска (%s): порты его, конфиг наш — "
            "переговоры идут через него",
            URL,
        )
        return True
    binary = await hass.async_add_executor_job(shutil.which, "go2rtc")
    if not binary:
        LOGGER.debug("go2rtc binary not found — WebRTC через HA, без нас")
        return False
    busy = await hass.async_add_executor_job(_ports_busy)
    if busy:
        # ⚠ Не поднимаемся и НЕ жалуемся громко: чужой go2rtc на этом порту —
        # это, как правило, правильно настроенный аддон. Пусть работает он.
        LOGGER.info("Порт %s уже занят — свой go2rtc не поднимаем (%s)", busy, URL)
        return False

    _tmp = await hass.async_add_executor_job(_write_config)
    LOGGER.info("Starting mega_home go2rtc %s :%s", binary, WEBRTC_PORT)
    try:
        _proc = await asyncio.create_subprocess_exec(
            binary,
            "-c",
            _tmp,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
    except OSError as err:
        LOGGER.warning("Failed to start go2rtc: %s", err)
        await async_stop()
        return False

    _log.clear()
    _start_drain()
    _ready = await _await_api(hass)
    if not _ready:
        LOGGER.warning(
            "go2rtc не ответил по %s за %.0f с — WebRTC пойдёт штатным путём HA. Лог: %s",
            URL,
            READY_TIMEOUT,
            " | ".join(_log) or "пусто",
        )
        await async_stop()
        return False
    LOGGER.info("mega_home go2rtc готов: %s, медиа :%s", URL, WEBRTC_PORT)
    return True


async def async_stop() -> None:
    """Снять процесс и убрать за собой. Зовётся при выгрузке и остановке HA.

    ⚠ Усыновлённый процесс НЕ снимается — мы его не поднимали и handle на
    него не имеем. Он переживёт выгрузку, продолжит держать медиа-порт и
    будет усыновлён следующим стартом: камеры при перезапуске HA не мигают.
    """
    global _proc, _tmp, _drain, _ready

    _ready = False
    drain, _drain = _drain, None
    if drain is not None:
        drain.cancel()
    proc, _proc = _proc, None
    if proc is not None and proc.returncode is None:
        try:
            proc.terminate()
            try:
                await asyncio.wait_for(proc.wait(), timeout=5)
            except TimeoutError:
                proc.kill()
                await proc.wait()
        except ProcessLookupError:
            pass
    if _tmp:
        try:
            Path(_tmp).unlink(missing_ok=True)
        except OSError:  # noqa: BLE001 - временный файл, не повод шуметь
            LOGGER.debug("go2rtc config %s not removed", _tmp)
        _tmp = None


def is_running() -> bool:
    """Готов ли СВОЙ go2rtc принимать переговоры.

    ⚠ Живой процесс — этого мало. go2rtc, у которого не поднялся слушатель,
    продолжает работать, и тогда мы уводили бы переговоры на путь без единого
    кандидата — при живом штатном пути HA. Поэтому здесь и флаг готовности,
    поставленный только после ответа его API.

    ⚠ УСЫНОВЛЁННЫЙ (`_proc is None`) жив по факту ответа API при старте:
    своего handle у него нет, тихая смерть посреди работы обнаружится только
    упавшей переговоркой — и лечится перезапуском HA, который усыновит или
    поднимет заново. Редкий случай: усыновляемый уже пережил часы работы.
    """
    return _ready and (_proc is None or _proc.returncode is None)


def log_tail() -> list[str]:
    """Последние строки go2rtc — для диагностики интеграции."""
    return list(_log)


def _ports_busy() -> str:
    """Занят ли нужный порт. Возвращает описание занятого или ''."""
    probes = (
        (socket.SOCK_DGRAM, WEBRTC_PORT, f"UDP {WEBRTC_PORT}"),
        (socket.SOCK_STREAM, WEBRTC_PORT, f"TCP {WEBRTC_PORT}"),
        (socket.SOCK_STREAM, API_PORT, f"TCP {API_PORT}"),
    )
    for kind, port, name in probes:
        with socket.socket(socket.AF_INET, kind) as probe:
            # Без SO_REUSEADDR: нам нужен честный ответ «порт занят», а не
            # возможность встать рядом с чужим слушателем.
            try:
                probe.bind(("0.0.0.0", port))
            except OSError:
                return name
    return ""


def _write_config() -> str:
    tmp = tempfile.NamedTemporaryFile(
        prefix="mega_home_go2rtc_", suffix=".yaml", delete=False
    )
    tmp.write(_YAML.encode())
    tmp.close()
    return tmp.name


def _start_drain() -> None:
    """Читать stdout до самого конца — иначе труба заполнится и go2rtc встанет."""
    global _drain

    _drain = asyncio.ensure_future(_read_output())


async def _read_output() -> None:
    proc = _proc
    if proc is None or proc.stdout is None:
        return
    try:
        async for line in proc.stdout:
            text = line.decode(errors="ignore").strip()
            if text:
                _log.append(text)
                LOGGER.debug("go2rtc: %s", text)
    except asyncio.CancelledError:
        raise
    except Exception:  # noqa: BLE001 - чтение лога не должно ронять интеграцию
        LOGGER.debug("go2rtc output reader stopped", exc_info=True)


async def _await_api(hass: HomeAssistant) -> bool:
    """Дождаться ответа API — это и есть «поднялся», а не «процесс не умер»."""
    from homeassistant.helpers.aiohttp_client import async_get_clientsession

    session = async_get_clientsession(hass)
    loop = asyncio.get_running_loop()
    deadline = loop.time() + READY_TIMEOUT
    while loop.time() < deadline:
        if _proc is None or _proc.returncode is not None:
            return False
        try:
            async with session.get(f"{URL}/api/streams", timeout=_timeout(1)) as answer:
                if answer.status < 500:
                    return True
        except Exception:  # noqa: BLE001 - ещё не поднялся, это нормально
            pass
        await asyncio.sleep(0.2)
    return False


async def _api_alive(hass: HomeAssistant) -> bool:
    """Отвечает ли чей-то go2rtc на НАШЕМ API-порте (петля, один запрос).

    ⚠ Порт 1985 выбран среди незанятых и наружу не слушается вовсе, поэтому
    ответивший на нём — практически наверняка наш же процесс прошлого запуска.
    Шов для спек: настоящий HTTP здесь не тестируется.
    """
    from homeassistant.helpers.aiohttp_client import async_get_clientsession

    session = async_get_clientsession(hass)
    try:
        async with session.get(f"{URL}/api/streams", timeout=_timeout(1)) as answer:
            return answer.status < 500
    except Exception:  # noqa: BLE001 - не отвечает, значит нечего усыновлять
        return False


def _timeout(seconds: float):  # noqa: ANN201
    import aiohttp

    return aiohttp.ClientTimeout(total=seconds)
