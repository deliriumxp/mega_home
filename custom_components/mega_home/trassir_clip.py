"""Просмотр записи события — тем же WebRTC, что и живая камера.

⚠ Своего транспорта у архива НЕТ, и это главное решение всей функции
(docs/trassir-integration-plan.md, §5а у менеджера). Эфемерный токен Trassir
заводится источником в НАШ go2rtc, а дальше идёт ровно тот путь, которым дом уже
отдаёт камеру: предложение телефона → ответ и кандидаты → медиа напрямую, мимо
менеджера. Поэтому в приложении под запись не появляется ни строчки нового
кода, а удалённый просмотр работает по тем же правилам, что и живой.

⚠ Порядок вызовов НЕ переставлять (проверено на живом регистраторе): токен →
кто-то ОТКРЫЛ поток → `archive_command`. Команда, отданная раньше, отвечает
`stream is expired` — текст читается как таймаут и им не является. Здесь поток
открывает go2rtc, когда к нему приходит потребитель, поэтому команда уходит
ПОСЛЕ ответа на предложение WebRTC.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from http import HTTPStatus
from typing import Any

from homeassistant.core import HomeAssistant

from .const import (
    LOGGER,
    TRASSIR_CLIP_LEAD,
    TRASSIR_CLIP_MAX_SECONDS,
    TRASSIR_PING_INTERVAL,
)
from .trassir_client import TrassirError

# Приставка id клипа. Приложение отдаёт его туда же, куда отдаёт id плитки
# камеры, — в `webrtc`; по приставке дом и понимает, что это запись.
CLIP_PREFIX = "trassir:"


@dataclass
class Clip:
    """Один открытый клип: токен Trassir, окно архива и поток go2rtc."""

    token: str
    guid: str
    start_us: int
    stop_us: int
    stream: str
    session_id: str | None = None
    started: bool = False
    # Куда курсор архива встал НА САМОМ ДЕЛЕ: у записи бывают дыры, и «клип
    # начался не с события» — факт регистратора, а не наш промах.
    first_frame: str | None = None
    ping: Any = field(default=None, repr=False)


class ClipSessions:
    """Открытые записи этого дома. Живут ровно пока их смотрят."""

    def __init__(self, gateway: Any) -> None:
        self._gateway = gateway
        self._clips: dict[str, Clip] = {}

    async def async_open(self, event_id: str) -> dict[str, Any]:
        """Подготовить запись к просмотру и вернуть её id приложению.

        Само видео ещё не течёт: токен взят, поток go2rtc назван, а команда
        архива уйдёт, когда телефон подключится (см. заголовок модуля).
        """
        event = self._gateway.event(event_id)
        if event is None:
            raise TrassirError("Событие не найдено")
        client = self._gateway.client
        if client is None:
            raise TrassirError("Видеонаблюдение объекта не настроено")

        settings = self._gateway.settings
        seconds = min(int(settings.get("clipSeconds") or 60), TRASSIR_CLIP_MAX_SECONDS)
        # ⚠ Метка события уходит в окно КАК ЕСТЬ — она в шкале самого Trassir
        # (unix + пояс сервера). Пересчёт по нашим часам сдвинул бы запись на
        # часовой пояс регистратора, а выглядело бы это как «показывает не то».
        start = int(event["timestampUs"]) - TRASSIR_CLIP_LEAD * 1_000_000
        stop = int(event["timestampUs"]) + seconds * 1_000_000

        # ⚠ СУБархив: снаружи это 0.45 против 3 Мбит/с (замер на стенде). Разница
        # «дома/снаружи» не должна быть в наборе функций, а качество клипа
        # события выбирается один раз и в пользу того, что доедет по мобильному.
        token = await client.async_get_video(event["guid"], "archive_sub", "rtsp")
        clip = Clip(
            token=token,
            guid=event["guid"],
            start_us=start,
            stop_us=stop,
            stream=f"trassir_{token}",
        )
        self._clips[f"{CLIP_PREFIX}{token}"] = clip
        return {
            "id": f"{CLIP_PREFIX}{token}",
            "startUs": start,
            "stopUs": stop,
            "cameraName": event.get("cameraName"),
        }

    async def async_offer(self, hass: HomeAssistant, clip_id: str, sdp: str) -> dict[str, Any]:
        """Свести телефон с записью: тот же go2rtc, что и у живой камеры."""
        from .ops import OpError

        clip = self._clips.get(clip_id)
        if clip is None:
            raise OpError("Запись уже закрыта, откройте событие заново", HTTPStatus.NOT_FOUND)
        try:
            from .go2rtc_embed import URL as OWN_URL, is_running
        except Exception as err:  # noqa: BLE001
            raise OpError("Дом не умеет отдавать запись", HTTPStatus.NOT_IMPLEMENTED) from err
        if not is_running():
            # Честный текст: живая камера в этом доме идёт путём Home Assistant,
            # а запись — только своим go2rtc, и молчать об этом нельзя.
            raise OpError(
                "Дом не может отдать запись: не поднят его go2rtc",
                HTTPStatus.SERVICE_UNAVAILABLE,
            )

        from . import webrtc

        settings = self._gateway.settings
        source = f"rtsp://{settings['host']}:{settings['rtspPort']}/{clip.token}"
        answer = await webrtc.negotiate_source(
            hass, OWN_URL, clip.stream, source, sdp, "запись события"
        )
        clip.session_id = answer.get("sessionId")
        await self._async_start(clip)
        # Приложение подписывает клип временем, с которого он реально пошёл:
        # иначе «запись события 14:02» показывала бы 14:06 без объяснений.
        if clip.first_frame:
            answer["firstFrameTs"] = clip.first_frame
        return answer

    async def async_close(self, hass: HomeAssistant, clip_id: str, session_id: str) -> dict[str, Any]:
        """Жилец закрыл запись: снять сессию, поток и токен.

        ⚠ Убирать обязательно и сразу. Забытый клип держит соединение с
        регистратором и поток в go2rtc; предела соединений на объекте может не
        быть вовсе (`connections_per_ip = -1` на стенде), то есть остановить
        это будет некому.
        """
        from . import webrtc

        webrtc.close_own(hass, session_id)
        clip = self._clips.pop(clip_id, None)
        if clip is None:
            return {"closed": True}
        if clip.ping:
            clip.ping.cancel()
        await self._async_drop_stream(clip.stream)
        return {"closed": True}

    def live_stream(self, guid: str) -> tuple[str, str]:
        """Имя потока go2rtc и источник для ЖИВОЙ камеры регистратора.

        ⚠ Токен не нужен вовсе: у live-канала ссылка ПОСТОЯННАЯ
        (`rtsp://host:555/<guid>_m/`), поэтому здесь нет ни сеанса, ни пинга, ни
        уборки — тем и отличается от записи. И идёт она тем же go2rtc и тем же
        WebRTC: камера видеонаблюдения показывается ровно как любая другая.
        """
        settings = self._gateway.settings
        return (
            f"trassir_live_{guid}",
            f"rtsp://{settings['host']}:{settings['rtspPort']}/{guid}_m/",
        )

    def clip_of_session(self, session_id: str) -> str | None:
        """Найти клип по сессии — приложение закрывает просмотр именно ею."""
        for clip_id, clip in self._clips.items():
            if clip.session_id == session_id:
                return clip_id
        return None

    async def _async_start(self, clip: Clip) -> None:
        """Отдать `archive_command` уже открытому потоку и держать токен живым."""
        client = self._gateway.client
        if client is None:
            return
        try:
            answer = await client.async_archive_command(
                clip.token,
                command="play",
                start=clip.start_us,
                stop=clip.stop_us,
                speed=1,
            )
        except TrassirError as err:
            # Не роняем просмотр: поток уже сведён, и жилец увидит хотя бы то,
            # что отдаёт регистратор по умолчанию. В лог — словами.
            LOGGER.warning("Запись не встала на событие: %s", err)
            return
        clip.started = True
        # `first_frame_ts` — куда курсор встал НА САМОМ ДЕЛЕ. У архива бывают
        # дыры, и «клип начался не с события» это факт регистратора, а не наш
        # промах; отдаём его наружу, чтобы приложение могло сказать правду.
        clip.first_frame = answer.get("first_frame_ts")
        # ⚠ Фоновая задача Home Assistant, а не голая `asyncio.create_task`:
        # неучтённую задачу сборщик мусора вправе выбросить на середине, и
        # токен перестал бы продлеваться посреди просмотра.
        clip.ping = self._gateway.hass.async_create_background_task(
            self._async_ping(clip), f"mega_home_trassir_ping_{clip.token}"
        )

    async def _async_ping(self, clip: Clip) -> None:
        """Держать токен живым, пока смотрят.

        ⚠ Пингует ДОМ, а не браузер: у токена десять секунд без запросов, и
        телефон по Wi-Fi этот срок не выдержит — а он ещё и сворачивается.
        """
        client = self._gateway.client
        if client is None:
            return
        while True:
            await asyncio.sleep(TRASSIR_PING_INTERVAL)
            try:
                await client.async_ping(clip.token)
            except (TrassirError, asyncio.CancelledError):
                return
            except Exception as err:  # noqa: BLE001
                LOGGER.debug("Продление токена записи не удалось: %s", err)
                return

    async def _async_drop_stream(self, name: str) -> None:
        try:
            from homeassistant.helpers.aiohttp_client import async_get_clientsession

            from .go2rtc_embed import URL as OWN_URL

            session = async_get_clientsession(self._gateway.hass)
            async with session.delete(f"{OWN_URL}/api/streams?src={name}") as answer:
                await answer.read()
        except Exception as err:  # noqa: BLE001 — уборка не должна ронять закрытие
            LOGGER.debug("Временный поток %s не удалён: %s", name, err)
