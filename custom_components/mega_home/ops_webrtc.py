"""Живой просмотр: переговоры WebRTC для камеры и для записи.

⚠ Один источник на оба показа — живую камеру и архив: второго способа
показывать видео в приложении нет и не заводится.
"""

from __future__ import annotations

from http import HTTPStatus
from typing import Any

from homeassistant.core import HomeAssistant

from . import go2rtc_session
from .const import LOGGER
from .coordinator import MegaHomeCoordinator
from .ops_base import OpError
from .ops_camera import camera_entity
from .ops_video import _trassir_guid, trassir


async def webrtc_offer(
    hass: HomeAssistant,
    coordinator: MegaHomeCoordinator,
    payload: dict[str, Any],
    remote: bool = False,
) -> dict[str, Any]:
    """Свести телефон жильца, который СНАРУЖИ, с камерой этого дома напрямую.

    Через менеджер проходит только этот обмен (килобайты SDP), видео идёт мимо
    него — ради этого всё и затевалось (remote-access.md у менеджера).

    ⚠ Импорт `webrtc` локальный: модуль камер Home Assistant не грузится в
    домах, где камер нет вовсе. Своя go2rtc (`go2rtc_session`) от него не зависит.
    """
    from . import webrtc
    from .trassir_clip import CLIP_PREFIX

    sdp = payload.get("offer")
    if not isinstance(sdp, str) or not sdp:
        raise OpError("Предложение WebRTC не передано")
    # ⚠ Trickle просит ПРИЛОЖЕНИЕ (новое умеет), а не дом по версии: старый
    # бандл поля не шлёт и получает прежний одноразовый ответ с кандидатами.
    trickle = payload.get("trickle") is True
    # ⚠ Запись события идёт ТОЙ ЖЕ операцией, что и живая камера, и это не
    # экономия строк: своя операция под архив означала бы второй сеанс со своими
    # сроками, своим закрытием и своей диагностикой — то есть вторую трубу
    # (docs/trassir-integration-plan.md §3 у менеджера). Отличается только
    # источник, и решает это приставка id.
    tile = payload.get("id")
    if isinstance(tile, str) and tile.startswith(CLIP_PREFIX):
        return await trassir(coordinator).clips.async_offer(
            tile, sdp, remote, trickle
        )
    guid = _trassir_guid(coordinator, tile)
    if guid:
        # Живая камера регистратора: ссылка постоянная, сеанса и токена нет —
        # но путь тот же самый, что у камеры Home Assistant.
        #
        # ⚠ Качество приезжает В ПРЕДЛОЖЕНИИ, а не отдельной операцией: у живой
        # камеры смена качества — это смена ИСТОЧНИКА, то есть ровно те же
        # переговоры заново. Своя операция здесь означала бы состояние сеанса
        # там, где его нет вовсе.
        quality = payload.get("quality")
        gateway = trassir(coordinator)
        from .go2rtc_embed import is_running

        if not is_running():
            raise OpError(
                "Дом не может отдать камеру: не поднят его go2rtc",
                HTTPStatus.SERVICE_UNAVAILABLE,
            )
        # ⚠ Два пути внутри: постоянный адрес канала и — если его у канала нет —
        # документированный токен. Решает это сам сеанс, потому что там же живут
        # пинг и уборка, которые запасному пути нужны (`async_live_offer`).
        return await gateway.clips.async_live_offer(
            guid, sdp, "sub" if quality == "sub" else "main", remote, trickle
        )
    return await webrtc.negotiate(
        hass, camera_entity(coordinator, payload), sdp, remote, trickle
    )

def webrtc_close(
    hass: HomeAssistant, coordinator: MegaHomeCoordinator, payload: dict[str, Any]
) -> dict[str, Any]:
    """Жилец закрыл просмотр — отпустить камеру, не дожидаясь развала связи."""
    from . import webrtc
    from .trassir_clip import CLIP_PREFIX

    session_id = payload.get("sessionId")
    if not isinstance(session_id, str) or not session_id:
        raise OpError("Сессия не указана")
    tile = payload.get("id")
    gateway = getattr(coordinator, "trassir", None)
    clip_id = tile if isinstance(tile, str) and tile.startswith(CLIP_PREFIX) else None
    if clip_id is None and gateway is not None:
        # Приложение могло закрыть просмотр, не назвав клип: сессия — тот же
        # ключ, и потерять уборку из-за отсутствующего поля нельзя.
        clip_id = gateway.clips.clip_of_session(session_id)
    if clip_id is not None and gateway is not None:
        coordinator.env.spawn(
            gateway.clips.async_close(clip_id, session_id), "mega_home clip close"
        )
        return {"closed": True}
    # ⚠ СНАЧАЛА своя сессия, и только потом сущность камеры. `webrtc.close`
    # пробует `close_own` первой строкой — но `camera_entity(...)` вычислялся
    # РАНЬШЕ, как аргумент вызова, и до этой попытки дело не доходило.
    #
    # Живой камере регистратора, открытой БЫСТРЫМ путём (постоянный адрес
    # канала), сеанс не заводится вовсе — закрывать по клипу нечего, а сущности
    # Home Assistant у неё нет и не будет. Замер объекта 2026-09-13: переход на
    # вкладку «Архив» слал `webrtc/close`, получал `409 «Это камера
    # видеонаблюдения»`, и поток к регистратору оставался висеть до своих
    # таймаутов. У регистратора соединения на IP считаны, и течь им нельзя.
    if go2rtc_session.close_own(coordinator.env, session_id):
        return {"closed": True}
    return webrtc.close(hass, camera_entity(coordinator, payload), session_id)

async def webrtc_candidates(payload: dict[str, Any]) -> dict[str, Any]:
    """Trickle: кандидаты телефона — туда, накопленные домом — оттуда.

    ⚠ Сессию не ищем по камере: кандидаты — часть ТОГО ЖЕ соединения, что уже
    поднято `webrtc`, и живут по его `sessionId`. Закрылась — ответ `done`, и
    приложение перестаёт спрашивать.
    """
    session_id = payload.get("sessionId")
    if not isinstance(session_id, str) or not session_id:
        raise OpError("Сессия не указана")
    incoming = payload.get("candidates")
    lines = (
        [item for item in incoming if isinstance(item, str)]
        if isinstance(incoming, list)
        else []
    )
    return await go2rtc_session.async_candidates(session_id, lines)
