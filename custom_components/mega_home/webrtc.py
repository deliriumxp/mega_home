"""Кадр камеры Home Assistant для плитки (часть B, источник состояний).

⚠ Переговоров WebRTC здесь больше нет (`docs/plan-thin-gateway.md`): видео
объекта бандл ведёт сам через `connect` к службе «go2rtc». Остаются два дела
одной и той же камеры: фоновый прогрев, пока приложение открыто (`warm`), и
сам кадр (`snapshot`) — ЕГО НАДО ОТДАТЬ СНАРУЖИ. Дома плитка ходит за
картинкой напрямую в `/api/camera_proxy/...` Home Assistant
(`ops_camera._camera_urls`), но этот адрес — самого HA, и вне дома он
недостижим: у приложения снаружи нет ни одного адреса Home Assistant. Кадр
поэтому едет тем же переносом, что и остальной API
(`api/camera-frame/<tileId>`, `core/ops_camera.camera_frame`).
"""

from __future__ import annotations

from http import HTTPStatus
from time import monotonic

from homeassistant.core import HomeAssistant

from .core.const import LOGGER
from .core.ops_base import OpError

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

# entity_id → (когда снят, тип, байты).
_frames: dict[str, tuple[float, str, bytes]] = {}
# Какие кадры сейчас снимаются: без этого опрос состояний раз в 3 с завёл бы
# по граббер на каждый заход.
_grabbing: set[str] = set()


def _camera(hass: HomeAssistant, entity_id: str):  # noqa: ANN201 - HA camera entity
    """Сущность камеры или отказ, понятный жильцу (`HomeAssistantError` от HA)."""
    from homeassistant.components.camera.helper import get_camera_from_entity_id
    from homeassistant.exceptions import HomeAssistantError

    try:
        return get_camera_from_entity_id(hass, entity_id)
    except HomeAssistantError as err:
        LOGGER.debug("Camera %s is not available: %s", entity_id, err)
        raise OpError("Камера недоступна в Home Assistant", HTTPStatus.NOT_FOUND) from err


async def stream_source(hass: HomeAssistant, entity_id: str) -> str | None:
    """Адрес живого потока этой камеры (`source.Cameras.stream_source`).

    ⚠ Живого видео у камер HA в доме больше нет (`docs/plan-thin-gateway.md`,
    часть B): переговоры с go2rtc теперь ведёт бандл, и ему неоткуда взять
    адрес потока камеры, кроме как у самого HA. Нет потока или камера сейчас
    недоступна — `None`, а не отказ: вызывающая сторона (`ha_source.HaCameras`,
    фоновый прогрев) не читает у пользователя, ей нечего показать в ответ.
    """
    try:
        camera = _camera(hass, entity_id)
        return await camera.stream_source()
    except OpError as err:
        LOGGER.debug("Stream source of %s unavailable: %s", entity_id, err.message)
        return None


async def snapshot(hass: HomeAssistant, entity_id: str) -> tuple[str, bytes]:
    """Один кадр камеры для плитки, снаружи — единственный способ его увидеть.

    ⚠ Свежий кадр отдаётся ИЗ ПАМЯТИ, не дожидаясь камеры, а обновляется фоном
    (см. `SNAPSHOT_FRESH`, `warm`). Иначе открытие плитки стоило бы ровно тех
    секунд, которые уходят у Home Assistant на ffmpeg.
    """
    cached = _frames.get(entity_id)
    if cached is not None and monotonic() - cached[0] <= SNAPSHOT_USABLE:
        # На эту камеру СЕЙЧАС смотрят: обновляем сразу, а не по общему сроку.
        warm(hass, entity_id, SNAPSHOT_FRESH)
        return cached[1], cached[2]
    frame = await _grab(hass, entity_id)
    return frame[1], frame[2]


def warm(hass: HomeAssistant, entity_id: str, max_age: float = WARM_INTERVAL) -> None:
    """Снять кадр ЗАРАНЕЕ, фоном, если он старше `max_age`."""
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
