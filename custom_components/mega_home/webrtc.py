"""Remote camera viewing: one-shot WebRTC negotiation for a resident who is away.

⚠ ВИДЕО через менеджер НЕ идёт и идти не должно (remote-access.md в репозитории
менеджера): через него едут только SDP и ICE-кандидаты — считанные килобайты на
открытие камеры, — а поток течёт напрямую телефон ↔ go2rtc в этом доме.

Исключение ровно одно и оно посчитано: ОДИН кадр-постер на открытие камеры
(`snapshot`). Переговоры занимают секунды, снаружи у приложения нет ни одного
адреса Home Assistant, и без кадра просмотр открывается чёрным прямоугольником —
человек читает это как «камера не работает». Кадр в сетке плиток — уже поток
(камер несколько, обновление по таймеру), и его здесь нет.

⚠ Обмен ОДНОРАЗОВЫЙ (non-trickle), а не потоковый, и это главное решение файла.
Штатный путь Home Assistant — подписка по вебсокету (`camera/webrtc/offer`), где
ответ и кандидаты приходят россыпью. Канал до менеджера — запрос-ответ с одним
кадром в каждую сторону (`home-requests.ts`), и городить поверх него вторую
подписку значило бы завести на менеджере состояние сессии, таймауты и уборку за
отвалившимся телефоном. Вместо этого предложение уходит УЖЕ с кандидатами
(браузер ждёт окончания сбора), а ответ дома собирается здесь в один пакет:
`answer` + кандидаты, накопленные за `CANDIDATE_WINDOW`. Одно путешествие
туда-обратно, на менеджере не остаётся ничего.

⚠ Что нужно НА ОБЪЕКТЕ, чтобы это заработало (кодом не лечится). Встроенный в
Home Assistant go2rtc запускается с `webrtc: listen: ":18555/tcp"` — TCP и
только внутри дома, — поэтому телефон снаружи до него не дойдёт никогда. Нужен
go2rtc с UDP-слушателем (аддон или docker) и `go2rtc: url: …` в
`configuration.yaml`. STUN настраивать отдельно НЕ нужно: Home Assistant отдаёт
go2rtc свой список ICE-серверов вместе с предложением, а по умолчанию там уже
стоит публичный `stun:stun.home-assistant.io`. Настроенный в HA список
(интеграция `web_rtc`) go2rtc подхватит сам.
"""

from __future__ import annotations

import asyncio
from http import HTTPStatus
from secrets import token_hex
from typing import Any

from homeassistant.core import HomeAssistant, callback

from .const import LOGGER
from .ops import OpError

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

# Живые ws-сессии СВОЕГО go2rtc: session_id → (когда открыта, Go2RtcWsClient).
#
# ⚠ Соединение обязано пережить этот запрос: go2rtc держит поток (RTSP-сессию
# камеры) ровно до закрытия ws. Закрытие — по `close` от жильца; иначе каждая
# попытка просмотра оставляла бы камеру занятой до перезапуска HA.
_own_sessions: dict[str, tuple[float, Any]] = {}

# Сколько сессия живёт без закрытия. Телефон, у которого убили приложение,
# `close` не пришлёт НИКОГДА, а ws держим мы — значит и поток с камеры держим
# мы, и никакие таймауты go2rtc тут не помогут. Просмотр дольше этого срока —
# случай редкий, и переоткрыть его дешевле, чем держать камеру занятой сутками.
SESSION_TTL = 3600.0


async def _drop_own(session_id: str) -> None:
    """Убрать сессию своего go2rtc: закрыть ws и отпустить камеру."""
    entry = _own_sessions.pop(session_id, None)
    if entry is None:
        return
    await _close_own(entry[1])


def _expire_own(hass: HomeAssistant) -> None:
    """Убрать сессии, о закрытии которых никто не сообщил."""
    from time import monotonic

    now = monotonic()
    for session_id in [key for key, (when, _) in _own_sessions.items() if now - when > SESSION_TTL]:
        LOGGER.info("Сессия %s просрочена — отпускаем камеру", session_id)
        entry = _own_sessions.pop(session_id, None)
        if entry is not None:
            hass.async_create_task(_close_own(entry[1]))


async def async_shutdown() -> None:
    """Отпустить все камеры: интеграцию выгружают или Home Assistant встаёт.

    ⚠ Без этого перезагрузка записи оставляла бы за собой открытые ws к
    go2rtc — то есть занятые камеры, о которых больше некому вспомнить.
    """
    for session_id in list(_own_sessions):
        entry = _own_sessions.pop(session_id, None)
        if entry is not None:
            await _close_own(entry[1])


async def negotiate(
    hass: HomeAssistant, entity_id: str, offer_sdp: str
) -> dict[str, Any]:
    """Trade the resident's offer for this camera's answer and ICE candidates."""
    # Свой go2rtc :8555 — без зависимости от HA :18555/tcp
    #
    # ⚠ try укрывает ТОЛЬКО импорт модуля: его отсутствие — нормальное состояние
    # (go2rtc не установлен) и повод идти путём HA-провайдера ниже. Отказ САМОГО
    # go2rtc (OpError) ловить нельзя: он замещался бы попыткой провайдера, а та
    # на доме без go2rtc в HA отвечала «камера не умеет WebRTC» — текст уводил
    # настройщика чинить не то, и настоящий отказ не попадал даже в лог.
    try:
        from .go2rtc_embed import URL as _OWN_URL, is_running as _own_running
    except Exception as err:  # noqa: BLE001
        LOGGER.debug("own go2rtc not used: %s", err)
    else:
        if _own_running():
            return await _negotiate_own(hass, entity_id, offer_sdp, _OWN_URL)

    from homeassistant.components.camera.const import StreamType
    from homeassistant.components.camera.webrtc import (
        WebRTCAnswer,
        WebRTCCandidate,
        WebRTCError,
        WebRTCMessage,
    )
    from homeassistant.exceptions import HomeAssistantError

    camera = _camera(hass, entity_id)
    if StreamType.WEB_RTC not in camera.camera_capabilities.frontend_stream_types:
        # Честный отказ вместо чёрного прямоугольника: у камеры нет провайдера
        # WebRTC (не поднят go2rtc, либо её поток ему не по зубам), и снаружи её
        # не покажет ничто.
        raise OpError(
            "Камера не умеет WebRTC — снаружи её показать нечем",
            HTTPStatus.NOT_IMPLEMENTED,
        )

    session_id = token_hex(8)
    answered = asyncio.Event()
    answer: list[str] = []
    failure: list[str] = []
    candidates: list[dict[str, Any]] = []

    @callback
    def send_message(message: WebRTCMessage) -> None:
        """Собрать то, что камера присылает россыпью, в один пакет."""
        if isinstance(message, WebRTCAnswer):
            answer.append(message.answer)
            answered.set()
        elif isinstance(message, WebRTCCandidate):
            # `to_dict()` — ровно та форма, которую ждёт `addIceCandidate` в
            # браузере (её же отдаёт фронтенду сам Home Assistant).
            candidates.append(message.candidate.to_dict())
        elif isinstance(message, WebRTCError):
            failure.append(message.message)
            answered.set()

    try:
        await camera.async_handle_async_webrtc_offer(offer_sdp, session_id, send_message)
    except HomeAssistantError as err:
        LOGGER.warning("WebRTC offer for %s failed: %s", entity_id, err)
        raise OpError(
            "Дом не смог начать трансляцию с этой камеры", HTTPStatus.BAD_GATEWAY
        ) from err

    try:
        async with asyncio.timeout(ANSWER_TIMEOUT):
            await answered.wait()
    except TimeoutError as err:
        # ⚠ Сессию закрываем на КАЖДОМ выходе с ошибкой: без этого go2rtc держал
        # бы соединение с камерой до перезапуска — по одному на каждую неудачную
        # попытку жильца.
        camera.close_webrtc_session(session_id)
        raise OpError(
            "Камера не ответила на запрос трансляции", HTTPStatus.GATEWAY_TIMEOUT
        ) from err

    if failure or not answer:
        camera.close_webrtc_session(session_id)
        LOGGER.warning("WebRTC offer for %s refused: %s", entity_id, failure)
        raise OpError(
            failure[0] if failure else "Камера не отдала ответ на предложение",
            HTTPStatus.BAD_GATEWAY,
        )

    await asyncio.sleep(CANDIDATE_WINDOW)
    return {
        "sessionId": session_id,
        "answer": answer[0],
        "candidates": list(candidates),
    }


async def _negotiate_own(
    hass: HomeAssistant, entity_id: str, offer_sdp: str, url: str
) -> dict[str, Any]:
    """Offer через свой go2rtc :1985 — без HA :18555/tcp."""
    from homeassistant.exceptions import HomeAssistantError

    camera = _camera(hass, entity_id)
    # Идентификатор как у HA-провайдера — иначе go2rtc не найдёт поток
    try:
        from homeassistant.components.go2rtc.util import get_camera_identifier

        identifier = get_camera_identifier(camera)
    except Exception:
        identifier = entity_id

    # ⚠ Источник — САМА КАМЕРА, хотя так она и отдаёт RTSP дважды: нам и go2rtc
    # самого Home Assistant. Брать поток у HA-шного go2rtc
    # (`rtsp://127.0.0.1:18554/<identifier>`) пробовали 2026-09-08 и отказались:
    # поток под этим именем появляется у него, только когда камеру ПОСМОТРЕЛИ
    # штатным интерфейсом HA — его заводит их WebRTC-провайдер, а наш путь этого
    # провайдера не зовёт никогда. На обычном объекте потока там нет, подписка
    # уходит в никуда, и жилец вместо камеры получает «Дом не смог начать
    # трансляцию». Проверить наличие потока заранее нечем: API HA-шного go2rtc
    # при выключенном UI слушает только unix-сокет. Экономия одного RTSP-сеанса
    # не стоит выключенной камеры; вернёмся, если появится дешёвая и точная
    # проверка.
    stream_source = await camera.stream_source()
    if not stream_source:
        raise OpError("Камера недоступна в Home Assistant", HTTPStatus.NOT_FOUND)
    # generic камера — нужен ffmpeg префикс как у HA провайдера
    if camera.platform.platform_name == "generic" and not stream_source.startswith("ffmpeg:"):
        stream_source = "ffmpeg:" + stream_source

    from go2rtc_client import Go2RtcRestClient
    from go2rtc_client.ws import Go2RtcWsClient, WebRTCAnswer as GoAnswer, WebRTCCandidate as GoCand, WsError
    from homeassistant.helpers.aiohttp_client import async_get_clientsession

    session = async_get_clientsession(hass)
    rest = Go2RtcRestClient(session, url)
    # Добавить поток если его нет
    try:
        streams = await rest.streams.list()
        if identifier not in streams or not any(stream_source == p.url for p in streams[identifier].producers):
            await rest.streams.add(identifier, [stream_source])
    except Exception as err:  # noqa: BLE001
        LOGGER.debug("own go2rtc add stream failed: %s", err)
        raise OpError("Дом не смог начать трансляцию с этой камеры", HTTPStatus.BAD_GATEWAY) from err

    session_id = token_hex(8)
    answered = asyncio.Event()
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
        elif isinstance(msg, WsError):
            failure.append(msg.error)
            answered.set()

    from time import monotonic

    ws = Go2RtcWsClient(session, url, source=identifier)
    ws.subscribe(_on_msg)  # type: ignore[arg-type]
    _expire_own(hass)
    _own_sessions[session_id] = (monotonic(), ws)
    try:
        from go2rtc_client.ws import WebRTCOffer

        await ws.send(WebRTCOffer(offer_sdp, []))
    except Exception as err:  # noqa: BLE001
        LOGGER.warning("own go2rtc offer for %s failed: %s", entity_id, err)
        await _drop_own(session_id)
        raise OpError("Дом не смог начать трансляцию с этой камеры", HTTPStatus.BAD_GATEWAY) from err

    try:
        async with asyncio.timeout(ANSWER_TIMEOUT):
            await answered.wait()
    except TimeoutError as err:
        await _drop_own(session_id)
        raise OpError("Камера не ответила на запрос трансляции", HTTPStatus.GATEWAY_TIMEOUT) from err
    if failure or not answer:
        LOGGER.warning("own go2rtc offer for %s refused: %s", entity_id, failure)
        await _drop_own(session_id)
        raise OpError(failure[0] if failure else "Камера не отдала ответ", HTTPStatus.BAD_GATEWAY)
    await asyncio.sleep(CANDIDATE_WINDOW)
    return {"sessionId": session_id, "answer": answer[0], "candidates": list(candidates)}


# Предел кадра-постера. Больше — отказ, а не обрезанная картинка: кадр едет
# кадром вебсокета до менеджера, и переросший его закрыл бы канал в дом целиком.
# Масштабирование в Home Assistant «по возможности» (нужен Pillow/turbojpeg), так
# что рассчитывать на просимый размер нельзя — только проверять полученный.
MAX_SNAPSHOT_BYTES = 400_000
# Ширина постера. Его показывают, пока идут переговоры, и он же лежит под
# потоком — разрешение камеры здесь не нужно, нужен узнаваемый кадр.
SNAPSHOT_WIDTH = 640

# Кадр моложе этого отдаём как есть и камеру не тревожим.
#
# ⚠ Кэш здесь не про экономию, а про СМЫСЛ постера. Снять кадр стоит секунду с
# лишним: у камеры без отдельного снапшот-адреса Home Assistant поднимает под
# него ffmpeg и ждёт ключевого кадра. Постер, который добывается столько же,
# сколько прикрываемые им переговоры, не прикрывает ничего — просмотр всё равно
# открывался пустым на несколько секунд (жалоба 2026-09-08).
SNAPSHOT_FRESH = 20.0
# Старше этого не показываем: постер должен быть похож на то, что во дворе
# сейчас, а не на то, что было полчаса назад.
SNAPSHOT_USABLE = 300.0
# Как часто греем кадр ФОНОМ, пока приложение открыто. Реже, чем «свежесть»:
# опрос состояний идёт раз в 3 с, и грей мы по тому же порогу — на доме с
# пятью камерами это был бы вечный ffmpeg по кругу ради кадра, на который
# никто, возможно, не посмотрит.
WARM_INTERVAL = 60.0

# entity_id → (когда снят, тип, байты).
_frames: dict[str, tuple[float, str, bytes]] = {}
# Какие кадры сейчас снимаются: без этого опрос состояний раз в 3 с завёл бы
# по граббер на каждый заход.
_grabbing: set[str] = set()


async def snapshot(hass: HomeAssistant, entity_id: str) -> tuple[str, bytes]:
    """One still frame, so the viewer does not open on black.

    ⚠ Живёт рядом с переговорами, потому что это ТА ЖЕ функция: снаружи у
    приложения нет ни одного адреса Home Assistant, а WebRTC стартует секунды —
    и без кадра просмотр открывается чёрным прямоугольником, что читается как
    «камера не работает». Один кадр на открытие камеры, а не поток: плитка в
    сетке снаружи по-прежнему обходится глифом (docs/remote-access.md).

    ⚠ Свежий кадр отдаётся ИЗ ПАМЯТИ, не дожидаясь камеры, а обновляется он
    фоном (см. `SNAPSHOT_FRESH`). Иначе постер стоил бы ровно тех секунд, ради
    которых он и заведён.

    ⚠ Отдаёт СЫРЫЕ байты, не base64: до 2026-09-08 кадр кодировался здесь и
    сразу декодировался в `relay_api._dispatch`, чтобы `relay_api.handle`
    закодировал его ОБРАТНО в base64 для менеджера, — два лишних прохода по
    кадру до 400 КБ на каждое открытие камеры. Кому нужен base64 (перенос
    через менеджер), кодирует сам на границе; локальная дверь (`http.py`)
    отдаёт эти байты браузеру как есть.
    """
    from time import monotonic

    cached = _frames.get(entity_id)
    if cached is not None and monotonic() - cached[0] <= SNAPSHOT_USABLE:
        # На эту камеру СЕЙЧАС смотрят: обновляем сразу, а не по общему сроку.
        warm(hass, entity_id, SNAPSHOT_FRESH)
        return cached[1], cached[2]
    frame = await _grab(hass, entity_id)
    return frame[1], frame[2]


def warm(hass: HomeAssistant, entity_id: str, max_age: float = WARM_INTERVAL) -> None:
    """Снять кадр ЗАРАНЕЕ, фоном, если он старше `max_age`.

    ⚠ Зовётся с опроса состояний (`ops.states`): приложение открыто, значит
    камеру могут открыть в любую секунду, и кадр к этому моменту должен уже
    лежать. Отказ камеры сюда не выносится — это подготовка, а не запрос
    жильца: не снялось, так снимется при открытии, с обычным сообщением.
    """
    from time import monotonic

    if entity_id in _grabbing:
        return
    cached = _frames.get(entity_id)
    if cached is not None and monotonic() - cached[0] <= max_age:
        return
    hass.async_create_task(_warm(hass, entity_id))


async def _warm(hass: HomeAssistant, entity_id: str) -> None:
    try:
        await _grab(hass, entity_id)
    except OpError as err:
        LOGGER.debug("Warming %s failed: %s", entity_id, err.message)


async def _grab(hass: HomeAssistant, entity_id: str) -> tuple[float, str, bytes]:
    """Один настоящий кадр с камеры — и в память."""
    from time import monotonic

    from homeassistant.components.camera import async_get_image
    from homeassistant.exceptions import HomeAssistantError

    _grabbing.add(entity_id)
    try:
        image = await async_get_image(hass, entity_id, width=SNAPSHOT_WIDTH)
    except HomeAssistantError as err:
        LOGGER.debug("Snapshot of %s failed: %s", entity_id, err)
        raise OpError("Камера не отдала кадр", HTTPStatus.BAD_GATEWAY) from err
    finally:
        _grabbing.discard(entity_id)

    if len(image.content) > MAX_SNAPSHOT_BYTES:
        LOGGER.warning(
            "Snapshot of %s is %d bytes — too large to send", entity_id, len(image.content)
        )
        raise OpError("Кадр камеры слишком большой", HTTPStatus.REQUEST_ENTITY_TOO_LARGE)
    frame = (monotonic(), image.content_type, image.content)
    _frames[entity_id] = frame
    return frame


def close(hass: HomeAssistant, entity_id: str, session_id: str) -> dict[str, Any]:
    """Drop a session the resident is done with.

    ⚠ Без этого дом узнаёт о закрытом просмотре только по развалу соединения, а
    до тех пор держит поток с камеры. Телефон, у которого приложение убили на
    ходу, всё равно оставит сессию висеть — на такой случай у go2rtc свои
    таймауты, — но обычный «закрыл шторку» обязан убирать за собой сразу.

    ⚠ Сессия СВОЕГО go2rtc закрывается через ws-реестр (`_own_sessions`):
    `close_webrtc_session` камеры знает только провайдеров HA и про неё молчит.
    """
    own = _own_sessions.pop(session_id, None)
    if own is not None:
        hass.async_create_task(_close_own(own[1]))
        return {"closed": True}
    _camera(hass, entity_id).close_webrtc_session(session_id)
    return {"closed": True}


async def _close_own(client: Any) -> None:
    try:
        await client.close()
    except Exception:  # noqa: BLE001
        LOGGER.debug("own go2rtc ws close failed", exc_info=True)


def _camera(hass: HomeAssistant, entity_id: str):
    """The camera entity, or a refusal the resident can read."""
    from homeassistant.components.camera.helper import get_camera_from_entity_id
    from homeassistant.exceptions import HomeAssistantError

    try:
        return get_camera_from_entity_id(hass, entity_id)
    except HomeAssistantError as err:
        # Текст Home Assistant английский («Camera is off»), жильцу он не нужен —
        # в лог его, а на экран свою формулировку.
        LOGGER.debug("Camera %s is not available: %s", entity_id, err)
        raise OpError(
            "Камера недоступна в Home Assistant", HTTPStatus.NOT_FOUND
        ) from err
