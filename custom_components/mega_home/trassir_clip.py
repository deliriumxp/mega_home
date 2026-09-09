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

⚠ Часы архива идут в РЕАЛЬНОМ времени с момента команды `play`, а телефон
показывает первый кадр на секунды позже: переговоры ICE/DTLS, раскрутка чтения
архива с диска и ожидание ключевого кадра. Всё, что прошло между командой и
первым кадром, потеряно навсегда — перемотки назад у живого сеанса нет. Поэтому
первый показанный кадр — это ещё и сигнал: приложение сообщает `seek`, и дом
ОТДАЁТ КОМАНДУ ЗАНОВО с началом окна. К этому моменту тракт уже прогрет
(RTSP открыт, ключевые кадры текут), и повторный старт встаёт почти сразу —
жилец видит запись с начала окна, а не с середины. Без этого «запись события»
стабильно начиналась на 3–4 секунды позже метки (живой факт 2026-09-09).

⚠ Та же команда — это и ПЕРЕМОТКА таймлайна: `seek` с позицией ставит архив на
любой момент окна. Отдельной команды «seek» у сеанса Trassir нет — повторный
`play` с новым стартом и есть перемотка.
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
        # ⚠ Команду архива НЕ ждём: её RTT (сессия + команда, сотни мс) лежал на
        # критическом пути ответа телефону, а архив всё равно переставляется на
        # начало первым кадром (см. заголовок модуля). Тракт при этом греется
        # раньше: к приходу телефона RTSP уже течёт и ключевые кадры есть.
        #
        # ⚠ Времени «с которого клип реально пошёл» в ответе БОЛЬШЕ НЕТ: команда
        # ушла фоном, и к моменту ответа её ещё нет. Его отдаёт `seek` — и
        # приложение подписывает клип уже из него.
        hass.async_create_background_task(
            self._async_start(clip_id, clip), f"mega_home_trassir_start_{clip.token}"
        )
        return answer

    async def async_seek(self, clip_id: str, position_us: int | None = None) -> dict[str, Any]:
        """Поставить архив на позицию: начало окна по умолчанию, иначе — метка.

        Начало окна — это автовозврат по первому кадру телефона (`seek` без
        позиции). Метка — жест жильца по таймлайну. Команда одна и та же
        (`play` с новым стартом): у сеанса Trassir нет отдельной перемотки.

        Ошибку НЕ глотаем (в отличие от `_async_start`): там команда была
        «лучше, чем ничего», а здесь жилец уже смотрит — молчание прочиталось
        бы как «запись идёт с начала», и приложение обязано узнать, что это
        не так.
        """
        from .ops import OpError

        clip = self._clips.get(clip_id)
        if clip is None:
            raise OpError("Запись уже закрыта, откройте событие заново", HTTPStatus.NOT_FOUND)
        try:
            position = clip.start_us if position_us is None else int(position_us)
        except (TypeError, ValueError) as err:
            raise OpError("Позиция — микросекунды числом", HTTPStatus.BAD_REQUEST) from err
        # ⚠ Кламп, а не отказ: палец на таймлайне не обязан попадать в окно
        # микросекунда в микросекунду, а ронять жест из-за края — хамство.
        position = min(max(position, clip.start_us), clip.stop_us)
        client = self._gateway.client
        if client is None:
            raise TrassirError("Видеонаблюдение объекта не настроено")
        answer = await client.async_archive_command(
            clip.token,
            command="play",
            start=position,
            stop=clip.stop_us,
            speed=1,
        )
        clip.first_frame = answer.get("first_frame_ts") or clip.first_frame
        return {"positionUs": position, "firstFrameTs": clip.first_frame}

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

    async def _async_start(self, clip_id: str, clip: Clip) -> None:
        """Отдать `archive_command` уже открытому потоку и держать токен живым.

        ⚠ Фоновая: зовётся из `async_offer` без ожидания (см. там). Поэтому
        первая проверка — жив ли клип: шторку могли закрыть раньше, чем команда
        дошла, и пинг мёртвому токену — это утечка задачи навсегда.
        """
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
        if self._clips.get(clip_id) is not clip:
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
