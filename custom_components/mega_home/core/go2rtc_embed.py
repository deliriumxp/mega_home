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
* **порт проверяется ДО старта.** Занят — значит его держит чужой go2rtc
  (аддон или docker) или помеха прошлого запуска. Тогда мы не поднимаемся,
  службу «go2rtc» не регистрируем и говорим причину в паспорте дома
  (`state()["why"]` → `integration.go2rtc.why`), а старт повторяем в фоне:
  помеха бывает временной. Штатного пути видео у HA за нами больше нет —
  переговоры ведёт бандл только с нашей службой (`docs/plan-thin-gateway.md`).
* **но сначала — СИРОТА.** Отвечающий на НАШЕМ API-порте (1985, только петля)
  — это наш же процесс прошлого запуска, переживший перезапуск HA в том же
  контейнере. Уступить его «чужому аддону» — значит после каждого такого
  рестарта навсегда остаться с «не поднят go2rtc». ⚠ И не усыновлять: его
  stdout — труба в умерший процесс HA, и Go выходит по SIGPIPE на первой же
  строке лога (объект 2026-10-24: через минуту после старта HA, на первых
  сессиях камер). Снимаем по pid и поднимаем свой — как любой супервизор;
* **смерть во время работы — не до перезапуска HA.** Читатель лога видит конец
  трубы: служба снимается из реестра (иначе `connect` упирается в закрытый
  порт), причина с хвостом лога — в журнал и паспорт, старт — обычным повтором.
"""

from __future__ import annotations

import asyncio
import os
import shutil
import signal
import socket
import tempfile
import time
from collections import deque
from pathlib import Path
from typing import Any


from . import go2rtc_guard, host_load, services
from .const import LOGGER
from .host import Host

# Порт сигналинга и медиа. Тот же и в кандидате `stun:8555`: наружу объект
# отдаёт ровно этот адрес, и проброс на роутере (если он нужен) делается на него.
WEBRTC_PORT = 8555
# API только для нас, поэтому на петле. 1985, а не 1984: 1984 занимает go2rtc,
# который человек мог поставить сам, и отобрать у него порт мы не вправе.
API_PORT = 1985
# RTSP только на петле: из него поток берёт запись дома (`recorder.py`) — тем же
# подключением к камере, что у живого видео. ⚠ Без явного адреса go2rtc слушает
# `:8554` на ВСЕХ адресах без учётки (проверено 1.9.14), и любой в сети объекта
# забирал поток любой заведённой камеры. 1986, а не 18554: тот занят go2rtc
# самого Home Assistant.
RTSP_PORT = 1986
# Шаблон входа «список кусков ffconcat» для воспроизведения записи: источник
# `ffmpeg:<список>#input=playlist`. ⚠ Только конфигом: источник `exec:` go2rtc
# через API не принимает («source from insecure producer», 1.9.14), а момент
# начала едет строкой `inpoint` в самом списке, не аргументом.
PLAYLIST_INPUT = "playlist"
# Сколько ждём, пока go2rtc ответит по своему API. Он стартует за десятые доли
# секунды; секунды здесь — запас для слабого железа, а не нормальное ожидание.
READY_TIMEOUT = 10.0
# Сколько строк лога держим для диагностики. Больше не нужно: важен хвост.
LOG_TAIL = 40

_YAML = f"""api:
  listen: "127.0.0.1:{API_PORT}"
rtsp:
  listen: "127.0.0.1:{RTSP_PORT}"
ffmpeg:
  {PLAYLIST_INPUT}: "-re -f concat -safe 0 -i {{input}}"
webrtc:
  listen: ":{WEBRTC_PORT}"
  candidates: [stun:{WEBRTC_PORT}]
  ice_servers:
    - urls: [stun:stun.l.google.com:19302]
    - urls: [stun:stun.home-assistant.io:3478]
"""

URL = f"http://127.0.0.1:{API_PORT}"
# Имя временного конфига — по нему же нагрузка находит усыновлённый процесс (`host_load.find`).
CONFIG_PREFIX = "mega_home_go2rtc_"

# Повторы старта, когда порт занят или API не ответил. ⚠ Одна попытка на весь
# аптайм — то, что оставило объект без своего go2rtc после обновления 0.4.0:
# помеха длилась минуту (TIME_WAIT прежнего запуска), а отказ жил до ребута.
RETRY_EVERY = 20.0
RETRY_ATTEMPTS = 15
# Сколько ждём выхода сироты после SIGTERM и затем после SIGKILL.
ORPHAN_STOP_S = 5.0

_env: Host | None = None
_proc: asyncio.subprocess.Process | None = None
_drain: asyncio.Task[None] | None = None
_retry: asyncio.Task[None] | None = None
_guard: asyncio.Task[None] | None = None
_tmp: str | None = None
_ready = False
_log: deque[str] = deque(maxlen=LOG_TAIL)


async def async_start(env: Host) -> bool:
    """Поднять свой go2rtc. False — не поднят, причина в `state()["why"]`."""
    global _env, _proc, _tmp, _ready, _why, _guard

    if is_running():
        return True
    _env = env
    # ⚠ СИРОТА — раньше проверки портов: свой go2rtc прошлого запуска HA держит и API, и
    # медиа-порт (живой факт 2026-09-12 — уступка «чужому аддону» оставила камеру мёртвой до
    # ребута). Снимается, а не усыновляется: без читателя трубы он умрёт сам на первой строке лога.
    if await _api_alive(env):
        problem = await env.run(_stop_orphan)
        if problem:
            _why = problem
            LOGGER.warning("%s", _why)
            _schedule_retry(env)
            return False
        LOGGER.info("Снят go2rtc прошлого запуска HA (%s): поднимаем свой", URL)
    binary = await env.run(shutil.which, "go2rtc")
    if not binary:
        _why = "в системе нет go2rtc: запись и удалённая камера не заработают"
        LOGGER.warning("%s", _why)
        return False
    busy = await env.run(_ports_busy)
    if busy:
        # ⚠ Не поднимаемся и НЕ жалуемся громко: чужой go2rtc на этом порту —
        # это, как правило, правильно настроенный аддон. Пусть работает он.
        # Но и не сдаёмся навсегда: помеха бывает временной (`_schedule_retry`).
        _why = f"порт {busy} занят чужим go2rtc или аддоном — свой не поднимаем"
        LOGGER.info("%s (%s)", _why, URL)
        _schedule_retry(env)
        return False

    _tmp = await env.run(_write_config)
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
        _why = f"go2rtc не запустился: {err}"
        LOGGER.warning("%s", _why)
        await async_stop()
        return False

    _log.clear()
    _start_drain()
    host_load.track("go2rtc", _proc.pid)
    _ready = await _await_api(env)
    if not _ready:
        _why = (
            f"go2rtc не ответил по {URL} за {READY_TIMEOUT:.0f} с. "
            f"Его лог: {' | '.join(_log) or 'пусто'}"
        )
        LOGGER.warning("%s", _why)
        await async_stop()
        _schedule_retry(env)
        return False
    _why = ""
    services.register("go2rtc", API_PORT)
    LOGGER.info("mega_home go2rtc готов: %s, медиа :%s", URL, WEBRTC_PORT)
    _guard = env.spawn(go2rtc_guard.run(URL, _proc.pid), "mega_home go2rtc: сторож продюсеров")
    return True


def _schedule_retry(env: Host) -> None:
    """Повторять старт в фоне, пока помеха не уйдёт или не кончатся попытки."""
    global _retry

    if _retry is not None and not _retry.done():
        return
    _retry = env.spawn(_retry_start(env), "mega_home go2rtc: повтор старта")


async def _retry_start(env: Host, attempts: int = RETRY_ATTEMPTS) -> bool:
    """Цикл повторов: каждая попытка — обычный `async_start`, без своей логики.

    ⚠ Неудачный `async_start` снова зовёт `_schedule_retry`, тот видит живую
    задачу (эту) и второй цикл не заводит — повторы не размножаются.
    """
    for _ in range(attempts):
        await asyncio.sleep(RETRY_EVERY)
        if is_running() or await async_start(env):
            return True
    LOGGER.warning("go2rtc так и не поднялся за %d попыток: %s", attempts, _why)
    return False


async def async_stop() -> None:
    """Снять процесс и убрать за собой. Зовётся при выгрузке и остановке HA."""
    global _proc, _tmp, _drain, _ready, _retry, _guard

    _ready = False
    guard, _guard = _guard, None
    if guard is not None and not guard.done():
        guard.cancel()
    services.unregister("go2rtc")
    host_load.untrack("go2rtc")
    drain, _drain = _drain, None
    if drain is not None:
        drain.cancel()
    # ⚠ Повторы снимаются только снаружи (выгрузка, остановка HA): изнутри
    # `async_start` зовёт `async_stop` при неответившем API, и снять ими самого
    # себя нельзя — цикл повторов оборвался бы на первой же неудаче.
    if _retry is not None and _retry is not asyncio.current_task():
        if not _retry.done():
            _retry.cancel()
        _retry = None
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
    продолжает работать, и переговоры уходили бы на путь без единого
    кандидата. Поэтому здесь и флаг готовности, поставленный только после
    ответа его API.
    """
    return _ready and _proc is not None and _proc.returncode is None


def log_tail() -> list[str]:
    """Последние строки go2rtc — для диагностики интеграции."""
    return list(_log)


# Почему свой go2rtc не поднялся. ⚠ Причин ЧЕТЫРЕ, и лечатся они по-разному:
# нет бинарника (ставить), порт занят чужим аддоном (так и задумано), не
# ответил за срок (смотреть его лог), упал при запуске (смотреть ошибку ОС).
# Наружу же торчало одно слово «не поднят», а сами строки уезжали в журнал Home
# Assistant уровнем debug — то есть инсталлятор не видел ничего (живой отчёт
# 2026-09-13: «go2rtc не поднялся», и дальше некуда идти).
_why: str = "не запускался"


def state() -> dict[str, Any]:
    """Состояние своего go2rtc для диагностики записи объекта."""
    return {
        "running": is_running(),
        "why": "" if is_running() else _why,
        "api": URL,
        "media_port": WEBRTC_PORT,
        "log": log_tail(),
    }


def _ports_busy() -> str:
    """Занят ли нужный порт живым слушателем (`services.port_busy`: TIME_WAIT не в счёт)."""
    return services.port_busy(
        (
            (socket.SOCK_DGRAM, WEBRTC_PORT, f"UDP {WEBRTC_PORT}"),
            (socket.SOCK_STREAM, WEBRTC_PORT, f"TCP {WEBRTC_PORT}"),
            (socket.SOCK_STREAM, API_PORT, f"TCP {API_PORT}"),
        )
    )


def _write_config() -> str:
    tmp = tempfile.NamedTemporaryFile(
        prefix=CONFIG_PREFIX, suffix=".yaml", delete=False
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
        # Конец трубы — процесс вышел. Остановка снимает читателя ДО процесса, сюда не доходя.
        await proc.wait()
    except asyncio.CancelledError:
        raise
    except Exception:  # noqa: BLE001 - чтение лога не должно ронять интеграцию
        LOGGER.debug("go2rtc output reader stopped", exc_info=True)
        return
    _exited(proc)


def _exited(proc: asyncio.subprocess.Process) -> None:
    """Поднятый процесс умер посреди работы. Смерть при старте разбирает сам `async_start`."""
    global _ready, _why

    if proc is not _proc or not _ready or _env is None:
        return
    _ready = False
    services.unregister("go2rtc")
    host_load.untrack("go2rtc", proc.pid)
    _why = f"go2rtc завершился ({proc.returncode}) во время работы. Его лог: {' | '.join(_log) or 'пусто'}"
    LOGGER.warning("%s", _why)
    _env.spawn(_after_exit(_env), "mega_home go2rtc: перезапуск после падения")


async def _after_exit(env: Host) -> None:
    await async_stop()
    _schedule_retry(env)


def _gone(pid: int) -> bool:
    """Процесса нет или он зомби: порты он уже отпустил (`proc(5)`, поле состояния)."""
    try:
        text = Path(f"/proc/{pid}/stat").read_text()
    except OSError:
        return True
    return text[text.rindex(")") + 2 :][:1] in ("Z", "X")


def _stop_orphan() -> str:
    """Снять свой go2rtc прошлого запуска HA (в исполнителе); пусто — снят."""
    pid = host_load.find(CONFIG_PREFIX)
    if pid is None:
        return f"на {URL} отвечает go2rtc прошлого запуска HA, а его процесса не видно — снимет перезапуск контейнера HA"
    for sig in (signal.SIGTERM, signal.SIGKILL):
        try:
            os.kill(pid, sig)
        except ProcessLookupError:
            return ""
        except OSError as err:
            return f"go2rtc прошлого запуска HA (pid {pid}) не снялся: {err}"
        deadline = time.monotonic() + ORPHAN_STOP_S
        while time.monotonic() < deadline:
            if _gone(pid):
                return ""
            time.sleep(0.1)
    return f"go2rtc прошлого запуска HA (pid {pid}) не завершился"


async def _await_api(env: Host) -> bool:
    """Дождаться ответа API — это и есть «поднялся», а не «процесс не умер»."""
    session = env.session()
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


async def _api_alive(env: Host) -> bool:
    """Отвечает ли чей-то go2rtc на НАШЕМ API-порте (петля, один запрос).

    ⚠ Порт 1985 выбран среди незанятых и наружу не слушается вовсе, поэтому
    ответивший на нём — практически наверняка наш же процесс прошлого запуска.
    Шов для спек: настоящий HTTP здесь не тестируется.
    """
    session = env.session()
    try:
        async with session.get(f"{URL}/api/streams", timeout=_timeout(1)) as answer:
            return answer.status < 500
    except Exception:  # noqa: BLE001 - не отвечает, значит нечего усыновлять
        return False


def _timeout(seconds: float):  # noqa: ANN201
    import aiohttp

    return aiohttp.ClientTimeout(total=seconds)
