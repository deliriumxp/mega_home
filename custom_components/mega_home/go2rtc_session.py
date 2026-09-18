"""Сессии СВОЕЙ go2rtc: переговоры WebRTC с любым источником, trickle, уборка.

Зачем отдельно от `webrtc.py`. Своя go2rtc — наш тракт видео, и HA ему не нужен:
источник — строка (RTSP камеры, эфемерный токен клипа Trassir), сессия — наш
ws к go2rtc. В `webrtc.py` остаётся только то, что действительно про камеры
Home Assistant: провайдер HA, сущность камеры, кадр-постер через HA.

⚠ Модуль в ядре без HA (`tests/test_core_without_ha.py`): среда приходит
хозяином (`host.py`), а не `hass`.

⚠ Обмен по умолчанию ОДНОРАЗОВЫЙ (ответ + кандидаты одним пакетом) — почему,
написано в `webrtc.py`; trickle включает только тот, кто его попросил.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from http import HTTPStatus
from secrets import token_hex
from typing import Any

from .const import LOGGER
from .host import Host
from .ops_base import OpError

# Сколько ждём ОТВЕТА камеры. Щедро: go2rtc за это время успевает открыть RTSP у
# самой камеры, а это единственный шаг здесь, который зависит от железа.
ANSWER_TIMEOUT = 6.0
# Сколько после ответа собираем кандидатов, прежде чем отдать пакет.
#
# ⚠ Ждать обязательно: go2rtc отдаёт ответ СРАЗУ, а свой srflx-кандидат (адрес,
# по которому его видно снаружи) присылает следом, узнав его у STUN. Отдать
# ответ без кандидатов значит отдать соединение, которому некуда встать.
# Полторы секунды — с запасом: обмен со STUN укладывается в десятые доли.
CANDIDATE_WINDOW = 1.5
# ⚠ Отдельное, БОЛЬШЕЕ окно для случая «внешнего адреса ещё нет вовсе».
#
# Живой отчёт с объекта 2026-09-09: «Кандидаты дома: host 2», ICE застрял в
# `checking`, переговоры 2644 мс — то есть окно в 1.5 с истекло, и дом ответил
# ОДНИМИ host-кандидатами, по которым телефону снаружи идти некуда. Повторное
# открытие той же камеры проходило нормально: первый раз go2rtc идёт к STUN
# холодным (резолв имени плюс два сервера), дальше адрес у него уже есть.
#
# ⚠ Это НЕ «подождём подольше на всякий случай»: дожидаемся мы только там, где
# иначе гарантированно отдали бы бесполезный ответ. Есть srflx — работает
# прежнее окно, и ждать нечего.
#
# ⚠ 6, а не 4: go2rtc ищет адрес у STUN синхронно и по очереди, с сетевым
# таймаутом 3 с на каждый сервер (в списке их два — Google и HA). Первый сервер
# на объекте может молчать, и тогда окно в 4 с истекало ровно на середине
# второго — дом снова отдавал ответ без srflx. Шесть секунд закрывают худший
# случай; после первого захода адрес кэширован 5 минут и работает короткое окно.
CANDIDATE_WINDOW_COLD = 6.0
# Пауза после появления внешнего адреса, прежде чем отдать пакет.
#
# ⚠ Зеркало раннего выхода браузера (`GATHER_GRACE_MS` в `webrtc-stream.ts`
# менеджера): ответ уже есть, srflx докатился — ждать всё окно ради опоздавших
# кандидатов значит держать лишнюю секунду переговоров на КАЖДОЕ открытие
# камеры. Без `srflx` (дому снаружи отдать нечего) ждём всё окно, как раньше.
CANDIDATE_GRACE = 0.4

# Живые ws-сессии СВОЕГО go2rtc: session_id → (когда открыта, Go2RtcWsClient).
#
# ⚠ Соединение обязано пережить этот запрос: go2rtc держит поток (RTSP-сессию
# камеры) ровно до закрытия ws. Закрытие — по `close` от жильца; иначе каждая
# попытка просмотра оставляла бы камеру занятой до перезапуска HA.
_own_sessions: dict[str, tuple[float, Any]] = {}

# Trickle: session_id → очередь ещё НЕ отданных телефону кандидатов дома.
#
# ⚠ Состояние эфемерное и живёт ровно от ответа до закрытия сессии — это цена
# trickle поверх запрос-ответ, от которой одноразовая схема уходила сознательно.
# Очередь — ТОТ ЖЕ список, что наполняет ws-подписка `_on_msg`, поэтому досылать
# кандидатов телефону не требует второй подписки.
_trickle: dict[str, list[dict[str, Any]]] = {}

# Сколько сессия живёт без закрытия. Телефон, у которого убили приложение,
# `close` не пришлёт НИКОГДА, а ws держим мы — значит и поток с камеры держим
# мы, и никакие таймауты go2rtc тут не помогут. Просмотр дольше этого срока —
# случай редкий, и переоткрыть его дешевле, чем держать камеру занятой сутками.
SESSION_TTL = 3600.0


def _cannot_stream(what: str) -> str:
    return "Дом не смог начать трансляцию" + (f" {what}" if what else "")


async def _drop_own(session_id: str) -> None:
    """Убрать сессию своего go2rtc: закрыть ws и отпустить камеру."""
    _trickle.pop(session_id, None)
    entry = _own_sessions.pop(session_id, None)
    if entry is None:
        return
    await _close_own(entry[1])


def _expire_own(env: Host) -> None:
    """Убрать сессии, о закрытии которых никто не сообщил."""
    from time import monotonic

    now = monotonic()
    for session_id in [key for key, (when, _) in _own_sessions.items() if now - when > SESSION_TTL]:
        LOGGER.info("Сессия %s просрочена — отпускаем камеру", session_id)
        _trickle.pop(session_id, None)
        entry = _own_sessions.pop(session_id, None)
        if entry is not None:
            env.spawn(_close_own(entry[1]), "mega_home go2rtc close")


async def async_shutdown() -> None:
    """Отпустить все камеры: интеграцию выгружают или Home Assistant встаёт.

    ⚠ Без этого перезагрузка записи оставляла бы за собой открытые ws к
    go2rtc — то есть занятые камеры, о которых больше некому вспомнить.
    """
    _trickle.clear()
    for session_id in list(_own_sessions):
        entry = _own_sessions.pop(session_id, None)
        if entry is not None:
            await _close_own(entry[1])


def _external_address(line: str) -> str | None:
    """Публичный адрес в строке кандидата — или None.

    ⚠ Типа `srflx` МАЛО. go2rtc свои КОНФИГУРНЫЕ кандидаты (`candidates:
    [stun:8555]`) отдаёт строкой `typ host`, хотя адрес в ней публичный, узнанный
    у STUN (`CandidateICE` в исходниках go2rtc жёстко печатает `typ host`).
    Проверка только по `srflx` не видела внешний путь, и дом ждал окно целиком на
    КАЖДОМ открытии (живой отчёт 2026-09-10: переговоры 6844 мс при готовом
    внешнем адресе). Смотрим на сам адрес.
    """
    from ipaddress import ip_address

    tokens = line.removeprefix("a=").split()
    try:
        at = tokens.index("typ")
    except ValueError:
        return None
    kind = tokens[at + 1] if at + 1 < len(tokens) else ""
    # `srflx`/`relay` — уже «наружу», адрес для вердикта не нужен: строка может
    # прийти и укороченной (без ip/port), а тип всё сказал.
    # Порядок: foundation component transport priority ADDRESS port typ type,
    # то есть адрес — за два токена до `typ`.
    if kind in ("srflx", "relay"):
        return tokens[at - 2] if at >= 2 else kind
    if kind != "host" or at < 2:
        return None
    host = tokens[at - 2]
    try:
        return host if ip_address(host).is_global else None
    except ValueError:
        return None


def _has_srflx(answer: list[str], candidates: list[dict[str, Any]]) -> bool:
    """Есть ли в пакете адрес, по которому дом видно снаружи."""
    lines = list(answer) + [(item.get("candidate") or "") for item in candidates]
    return any(_external_address(line) for line in lines)


async def _wait_candidates(
    new_candidate: asyncio.Event, ready: Callable[[], bool], remote: bool = False
) -> None:
    """Дождаться достаточных кандидатов, но не дольше окна.

    Достаточно — внешний адрес (`srflx`) плюс grace на опоздавших.

    ⚠ Пока внешнего адреса НЕТ, ждём по `CANDIDATE_WINDOW_COLD`, а не по
    обычному окну. Ответ без srflx телефону снаружи бесполезен — ему некуда
    идти, — поэтому короткое окно здесь экономило секунду и стоило всего
    просмотра (живой отчёт с объекта: «Кандидаты дома: host 2», ICE навсегда в
    `checking`, а повторное открытие той же камеры проходило). Как только srflx
    пришёл, всё идёт прежним темпом: ждать больше нечего.
    """
    # ⚠ Дома ждать внешний адрес НЕ НАДО: телефон в той же сети, и host-кандидатов
    # ему довольно. Лишнее ожидание здесь было бы платой за то, чем дома не
    # пользуются.
    window = CANDIDATE_WINDOW_COLD if remote else CANDIDATE_WINDOW
    try:
        async with asyncio.timeout(window):
            while not ready():
                await new_candidate.wait()
                new_candidate.clear()
            await asyncio.sleep(CANDIDATE_GRACE)
    except TimeoutError:
        pass



async def negotiate_source(
    env: Host,
    url: str,
    identifier: str,
    stream_source: str,
    offer_sdp: str,
    what: str = "",
    remote: bool = False,
    skip_list: bool = False,
    trickle: bool = False,
) -> dict[str, Any]:
    """Свести предложение телефона с ЛЮБЫМ источником своего go2rtc.

    `what` — чем закончить отказ («с этой камеры», «запись события»): текст
    видит жилец, и «трансляция не пошла» без предмета читается как поломка
    всего дома.

    ⚠ Вынесено из `webrtc._negotiate_own` ради записи архива: клип Trassir — это тот же
    поток go2rtc, только источник у него эфемерный (`rtsp://…/<token>`), а не
    камера Home Assistant. Второй способ показывать видео мы не заводим —
    ровно поэтому здесь нет ни слова про то, чей это источник
    (docs/trassir-integration-plan.md §3 у менеджера).

    ⚠ `skip_list` — имя потока заведомо НОВОЕ (эфемерный токен клипа или
    запасной live-путь): `GET /api/streams` на критическом пути тогда ничего не
    решает, только добавляет круг через менеджер. Вызывающий знает это точно,
    поэтому и решает он, а не эвристика по имени (переименуют — молча сломается).
    """
    from go2rtc_client import Go2RtcRestClient
    from go2rtc_client.ws import Go2RtcWsClient, WebRTCAnswer as GoAnswer, WebRTCCandidate as GoCand, WsError

    session = env.session()
    rest = Go2RtcRestClient(session, url)
    # Добавить поток, если его нет. ⚠ Для ЗАВЕДОМО нового имени список не
    # спрашиваем: он ответит «нет такого», и это ровно то, что мы уже знаем.
    try:
        if skip_list:
            await rest.streams.add(identifier, [stream_source])
        else:
            streams = await rest.streams.list()
            if identifier not in streams or not any(
                stream_source == p.url for p in streams[identifier].producers
            ):
                await rest.streams.add(identifier, [stream_source])
    except Exception as err:  # noqa: BLE001
        LOGGER.debug("own go2rtc add stream failed: %s", err)
        raise OpError(_cannot_stream(what), HTTPStatus.BAD_GATEWAY) from err

    session_id = token_hex(8)
    answered = asyncio.Event()
    got_candidate = asyncio.Event()
    answer: list[str] = []
    failure: list[str] = []
    candidates: list[dict[str, Any]] = []

    def _on_msg(msg):  # type: ignore[no-untyped-def]
        if isinstance(msg, GoAnswer):
            answer.append(msg.sdp)
            answered.set()
        elif isinstance(msg, GoCand):
            # ⚠ Кандидат go2rtc — СТРОКА, а `addIceCandidate` в браузере
            # отклоняет непустого кандидата без `sdpMid`/`sdpMLineIndex`
            # (TypeError по спеке W3C). Фронтенд глотает отказ каждого
            # кандидата как «минус один путь» — без m-line это значило «минус
            # ВСЕ пути», и ICE не вставал никогда. Ноль — видео-секция оффера;
            # с rtcp-mux обе секции делят один транспорт, так делает и сам HA
            # (`RTCIceCandidateInit`), и фронтенд go2rtc (`sdpMid: '0'`).
            candidates.append({"candidate": str(msg.candidate), "sdpMLineIndex": 0})
            got_candidate.set()
        elif isinstance(msg, WsError):
            failure.append(msg.error)
            answered.set()

    from time import monotonic

    ws = Go2RtcWsClient(session, url, source=identifier)
    ws.subscribe(_on_msg)  # type: ignore[arg-type]
    _expire_own(env)
    _own_sessions[session_id] = (monotonic(), ws)
    try:
        from go2rtc_client.ws import WebRTCOffer

        await ws.send(WebRTCOffer(offer_sdp, []))
    except Exception as err:  # noqa: BLE001
        LOGGER.warning("own go2rtc offer for %s failed: %s", identifier, err)
        await _drop_own(session_id)
        raise OpError(_cannot_stream(what), HTTPStatus.BAD_GATEWAY) from err

    try:
        async with asyncio.timeout(ANSWER_TIMEOUT):
            await answered.wait()
    except TimeoutError as err:
        await _drop_own(session_id)
        raise OpError("Источник не ответил на запрос трансляции", HTTPStatus.GATEWAY_TIMEOUT) from err
    if failure or not answer:
        LOGGER.warning("own go2rtc offer for %s refused: %s", identifier, failure)
        await _drop_own(session_id)
        raise OpError(failure[0] if failure else "Источник не отдал ответ", HTTPStatus.BAD_GATEWAY)
    if trickle:
        # ⚠ Ответ уходит СРАЗУ, окно кандидатов не ждём: телефон доспросит их
        # операцией `webrtc-candidates` (`ops.webrtc_candidates`). Очередь — ТОТ
        # ЖЕ список, что наполняет `_on_msg`, поэтому вторая подписка не нужна.
        _trickle[session_id] = candidates
        # `candidates: []` кладём намеренно: старый потребитель ответа (бандл,
        # не знающий trickle) не должен упасть на `undefined` — он получит
        # пустой список, а сам trickle включает только тот, кто попросил.
        return {
            "sessionId": session_id,
            "answer": answer[0],
            "candidates": [],
            "trickle": True,
        }
    await _wait_candidates(got_candidate, lambda: _has_srflx(answer, candidates), remote)
    return {"sessionId": session_id, "answer": answer[0], "candidates": list(candidates)}


async def async_candidates(
    session_id: str, candidates: list[str] | None
) -> dict[str, Any]:
    """Trickle: принять кандидаты ТЕЛЕФОНА и отдать накопленные ДОМОМ.

    ⚠ Симметрично WHEP `PATCH`: туда — свои кандидаты, оттуда — чужие. `done` —
    соединение go2rtc закрылось (кандидатов больше не будет). Старый дом без
    этой операции сюда не попадёт: её зовёт только приложение, увидевшее
    `trickle: true` в ответе на предложение.
    """
    queue = _trickle.get(session_id)
    entry = _own_sessions.get(session_id)
    if queue is None or entry is None:
        return {"candidates": [], "done": True}
    ws = entry[1]
    # Кандидаты телефона go2rtc принимает тем же ws, что держит поток.
    from go2rtc_client.ws import WebRTCCandidate as GoCand

    for line in candidates or []:
        try:
            await ws.send(GoCand(candidate=str(line)))
        except Exception as err:  # noqa: BLE001 - путь отвалился, не сессия
            LOGGER.debug("candidate to own go2rtc failed: %s", err)
    out = list(queue)
    queue.clear()
    return {"candidates": out, "done": not ws.connected}


def close_own(env: Host, session_id: str) -> bool:
    """Закрыть сессию СВОЕГО go2rtc, ничего не зная про её источник.

    ⚠ Нужно записи архива: у клипа нет сущности камеры, а `close` ниже её
    спрашивает. Возвращает False, если сессия не наша, — тогда закрывать её
    штатным путём Home Assistant (`webrtc.close`).
    """
    _trickle.pop(session_id, None)
    own = _own_sessions.pop(session_id, None)
    if own is None:
        return False
    env.spawn(_close_own(own[1]), "mega_home go2rtc close")
    return True


async def _close_own(client: Any) -> None:
    try:
        await client.close()
    except Exception:  # noqa: BLE001
        LOGGER.debug("own go2rtc ws close failed", exc_info=True)
