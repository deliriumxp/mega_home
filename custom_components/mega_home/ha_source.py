"""Источник состояний поверх Home Assistant: `source.StateSource` его средствами."""

from __future__ import annotations

from collections.abc import Callable
from http import HTTPStatus
from time import monotonic
from typing import Any

import voluptuous as vol
from homeassistant.core import Event, EventStateChangedData, HomeAssistant, State, callback
from homeassistant.exceptions import HomeAssistantError, ServiceNotFound
from homeassistant.helpers.event import async_track_state_change_event

from .core.const import LOGGER
from .core.ops_base import OpError
from .core.source import CommandRejected, CommandUnknown

# Предел кадра. Больше — отказ, а не обрезанная картинка: кадр едет кадром
# переноса до менеджера, и переросший его закрыл бы канал в дом целиком.
MAX_SNAPSHOT_BYTES = 400_000
# Ширина кадра — разрешение камеры здесь не нужно, нужен узнаваемый кадр.
SNAPSHOT_WIDTH = 640
# Как часто греем кадр ФОНОМ, пока приложение открыто. Опрос состояний идёт
# раз в 3 с, и грей мы по тому же порогу — на доме с пятью камерами это был бы
# вечный ffmpeg по кругу ради кадра, на который никто, возможно, не посмотрит.
WARM_INTERVAL = 60.0
# Кадр моложе этого отдаём как есть и камеру не тревожим при открытии.
SNAPSHOT_FRESH = 20.0
# Старше этого не показываем: кадр должен быть похож на то, что во дворе
# сейчас, а не на то, что было полчаса назад.
SNAPSHOT_USABLE = 300.0
# Камера не отдала адрес потока (недоступна, нет потока) — спросить снова не
# раньше этого. ⚠ Без срока ответ «нет» не запоминался, и опрос состояний раз
# в 3 с спрашивал HA о потоке каждой такой камеры каждые 3 с.
SOURCE_RETRY = 300.0


class HaCameras:
    """Камеры `camera.*` этого HA: кадр плитки, его фоновый прогрев и адрес потока.

    ⚠ Видео камер ведёт бандл сам (`connect` к go2rtc); отсюда нужны две вещи
    одной камеры: КАДР — его надо отдать и снаружи (`api/camera-frame/<плитка>`:
    `/api/camera_proxy/...` самого HA вне дома недостижим), и АДРЕС ПОТОКА для
    своего go2rtc (`ops_camera._camera_urls`, поле `source`).

    ⚠ Кэши — поля экземпляра, а не модуля: у модуля (`webrtc.py` до 0.5.6) они
    переживали перезагрузку записи.
    """

    def __init__(self, hass: HomeAssistant) -> None:
        self.hass = hass
        # entity_id → (когда снят, тип, байты).
        self._frames: dict[str, tuple[float, str, bytes]] = {}
        # entity_id → (когда спросили, адрес потока; `None` — камера его не отдала).
        self._sources: dict[str, tuple[float, str | None]] = {}
        # Что снимается или спрашивается прямо сейчас: без этого опрос состояний
        # раз в 3 с заводил бы по граббер на каждый заход.
        self._busy: set[str] = set()

    def warm(self, entity_id: str, max_age: float = WARM_INTERVAL) -> None:
        """Кадр — фоном, если он старше `max_age`; адрес потока — если не спрашивали."""
        cached = self._frames.get(entity_id)
        if f"frame:{entity_id}" not in self._busy and (cached is None or monotonic() - cached[0] > max_age):
            self.hass.async_create_task(self._warm_frame(entity_id))
        asked = self._sources.get(entity_id)
        stale = asked is None or (asked[1] is None and monotonic() - asked[0] > SOURCE_RETRY)
        if stale and f"source:{entity_id}" not in self._busy:
            # Адрес потока камеры на лету не меняется — читаем его раз на запуск
            # дома, а не на каждый опрос состояний.
            self.hass.async_create_task(self._warm_source(entity_id))

    def cached_source(self, entity_id: str) -> str | None:
        """То, что успел прочитать фоновый прогрев — без сети, для `entity_view`."""
        asked = self._sources.get(entity_id)
        return asked[1] if asked else None

    async def snapshot(self, entity_id: str) -> tuple[str, bytes]:
        """Один кадр камеры для плитки, снаружи — единственный способ его увидеть.

        ⚠ Свежий кадр отдаётся ИЗ ПАМЯТИ, не дожидаясь камеры, а обновляется фоном.
        Иначе открытие плитки стоило бы ровно тех секунд, которые уходят у Home
        Assistant на ffmpeg.
        """
        cached = self._frames.get(entity_id)
        if cached is not None and monotonic() - cached[0] <= SNAPSHOT_USABLE:
            # На эту камеру СЕЙЧАС смотрят: обновляем сразу, а не по общему сроку.
            self.warm(entity_id, SNAPSHOT_FRESH)
            return cached[1], cached[2]
        frame = await self._grab(entity_id)
        return frame[1], frame[2]

    async def stream_source(self, entity_id: str) -> str | None:
        """Адрес живого потока — публичной функцией компонента камеры.

        ⚠ `camera.async_get_stream_source`, а не внутренний хелпер ядра: функция —
        то, чем компонент отдаёт поток наружу (менеджер: `docs/ha-official-api.md`).
        Нет потока или камера недоступна — `None`, а не отказ.
        """
        from homeassistant.components.camera import async_get_stream_source

        try:
            return await async_get_stream_source(self.hass, entity_id)
        except HomeAssistantError as err:
            LOGGER.debug("Stream source of %s unavailable: %s", entity_id, err)
            return None

    async def _warm_source(self, entity_id: str) -> None:
        self._busy.add(f"source:{entity_id}")
        try:
            self._sources[entity_id] = (monotonic(), await self.stream_source(entity_id))
        finally:
            self._busy.discard(f"source:{entity_id}")

    async def _warm_frame(self, entity_id: str) -> None:
        try:
            await self._grab(entity_id)
        except OpError as err:
            LOGGER.debug("Warming %s failed: %s", entity_id, err.message)

    async def _grab(self, entity_id: str) -> tuple[float, str, bytes]:
        """Один настоящий кадр с камеры — и в память."""
        from homeassistant.components.camera import async_get_image

        self._busy.add(f"frame:{entity_id}")
        try:
            image = await async_get_image(self.hass, entity_id, width=SNAPSHOT_WIDTH)
        except HomeAssistantError as err:
            LOGGER.debug("Snapshot of %s failed: %s", entity_id, err)
            raise OpError("Камера не отдала кадр", HTTPStatus.BAD_GATEWAY) from err
        finally:
            self._busy.discard(f"frame:{entity_id}")
        if len(image.content) > MAX_SNAPSHOT_BYTES:
            LOGGER.warning("Snapshot of %s is %d bytes — too large to send", entity_id, len(image.content))
            raise OpError("Кадр камеры слишком большой", HTTPStatus.REQUEST_ENTITY_TOO_LARGE)
        frame = (monotonic(), image.content_type, image.content)
        self._frames[entity_id] = frame
        return frame


class HaSource:
    """`source.StateSource` на машине состояний и службах Home Assistant."""

    def __init__(self, hass: HomeAssistant) -> None:
        self.hass = hass
        self.cameras = HaCameras(hass)

    def get(self, entity_id: str) -> State | None:
        return self.hass.states.get(entity_id)

    async def call(
        self, domain: str, service: str, data: dict[str, Any], response: bool = False
    ) -> Any:
        try:
            # blocking=True: ждём ВЫПОЛНЕНИЯ службы, чтобы её отказ дошёл до
            # жильца ответом. Нового состояния это не обещает — его несёт
            # `state_changed` (подписка `subscribe` ниже).
            # ⚠ `return_response` только по просьбе: служба без ответа на него
            # падает (dev-api-websocket.md, «return_response»: «Must be included
            # for service actions that return response data»).
            return await self.hass.services.async_call(
                domain, service, data, blocking=True, return_response=response
            )
        except ServiceNotFound as err:
            raise CommandUnknown(f"{domain}.{service}") from err
        except (vol.Invalid, HomeAssistantError) as err:
            # `HomeAssistantError` — отказ самого прибора или службы (режим не из
            # `fan_modes`, служба без ответа), а не поломка дома: жильцу — «HA
            # отклонил команду», а не пятисотка.
            raise CommandRejected(str(err)) from err

    def subscribe(
        self,
        entity_ids: list[str],
        on_change: Callable[[str, State | None], None],
    ) -> Callable[[], None]:
        @callback
        def _on_state(event: Event[EventStateChangedData]) -> None:
            on_change(event.data.get("entity_id") or "", event.data.get("new_state"))

        return async_track_state_change_event(self.hass, entity_ids, _on_state)
