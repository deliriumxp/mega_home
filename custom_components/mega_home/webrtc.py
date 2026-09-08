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


async def negotiate(
    hass: HomeAssistant, entity_id: str, offer_sdp: str
) -> dict[str, Any]:
    """Trade the resident's offer for this camera's answer and ICE candidates."""
    # Свой go2rtc :8555 — без зависимости от HA :18555/tcp
    try:
        from .go2rtc_embed import URL as _OWN_URL, is_running as _own_running

        if _own_running():
            return await _negotiate_own(hass, entity_id, offer_sdp, _OWN_URL)
    except Exception as err:  # noqa: BLE001
        LOGGER.debug("own go2rtc not used: %s", err)

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
            candidates.append(msg.candidate.to_dict() if hasattr(msg.candidate, "to_dict") else {"candidate": str(msg.candidate)})
        elif isinstance(msg, WsError):
            failure.append(msg.error)
            answered.set()

    ws = Go2RtcWsClient(session, url, source=identifier)
    ws.subscribe(_on_msg)  # type: ignore[arg-type]
    try:
        from go2rtc_client.ws import WebRTCOffer

        await ws.send(WebRTCOffer(offer_sdp, []))
    except Exception as err:  # noqa: BLE001
        LOGGER.warning("own go2rtc offer for %s failed: %s", entity_id, err)
        raise OpError("Дом не смог начать трансляцию с этой камеры", HTTPStatus.BAD_GATEWAY) from err

    try:
        async with asyncio.timeout(ANSWER_TIMEOUT):
            await answered.wait()
    except TimeoutError as err:
        raise OpError("Камера не ответила на запрос трансляции", HTTPStatus.GATEWAY_TIMEOUT) from err
    if failure or not answer:
        LOGGER.warning("own go2rtc offer for %s refused: %s", entity_id, failure)
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


async def snapshot(hass: HomeAssistant, entity_id: str) -> dict[str, Any]:
    """One still frame, so the viewer does not open on black.

    ⚠ Живёт рядом с переговорами, потому что это ТА ЖЕ функция: снаружи у
    приложения нет ни одного адреса Home Assistant, а WebRTC стартует секунды —
    и без кадра просмотр открывается чёрным прямоугольником, что читается как
    «камера не работает». Один кадр на открытие камеры, а не поток: плитка в
    сетке снаружи по-прежнему обходится глифом (docs/remote-access.md).
    """
    from base64 import b64encode

    from homeassistant.components.camera import async_get_image
    from homeassistant.exceptions import HomeAssistantError

    try:
        image = await async_get_image(hass, entity_id, width=SNAPSHOT_WIDTH)
    except HomeAssistantError as err:
        LOGGER.debug("Snapshot of %s failed: %s", entity_id, err)
        raise OpError("Камера не отдала кадр", HTTPStatus.BAD_GATEWAY) from err

    if len(image.content) > MAX_SNAPSHOT_BYTES:
        LOGGER.warning(
            "Snapshot of %s is %d bytes — too large to send", entity_id, len(image.content)
        )
        raise OpError("Кадр камеры слишком большой", HTTPStatus.REQUEST_ENTITY_TOO_LARGE)
    return {
        "contentType": image.content_type,
        "image": b64encode(image.content).decode("ascii"),
    }


def close(hass: HomeAssistant, entity_id: str, session_id: str) -> dict[str, Any]:
    """Drop a session the resident is done with.

    ⚠ Без этого дом узнаёт о закрытом просмотре только по развалу соединения, а
    до тех пор держит поток с камеры. Телефон, у которого приложение убили на
    ходу, всё равно оставит сессию висеть — на такой случай у go2rtc свои
    таймауты, — но обычный «закрыл шторку» обязан убирать за собой сразу.
    """
    _camera(hass, entity_id).close_webrtc_session(session_id)
    return {"closed": True}


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
