"""Описание выходов общей музыкальной сессии."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any

from .core.ops import command_spec
from .core.ops_base import OpError, arguments
from .providers.music_flow import FLOW_CHANNELS, FLOW_RATE, FLOW_WIDTH, FlowSession, FlowStreams
from .providers.player_output import Output


def output_profile(profile: Mapping[str, Any]) -> dict:
    return {key: value for key, value in profile.items() if key != "delayMs"}


def flow_address(profile: Mapping[str, Any], address: str) -> str:
    """Адрес сессии В ВИДЕ, который примет ЭТОТ плеер: схема из описания плитки.

    ⚠ Без схемы Sonos отклоняет адрес — HA отдаёт отказ службы, и жилец видит «Home Assistant
    отклонил команду» (живой разбор 2026-11-23: «сонос+сонос не работает»). Sonos нужен
    `x-rincon-mp3radio://`, и `https` остаётся ВНУТРИ схемы: без неё он идёт по http, а Яндекс-CDN
    отдаёт только https. То же правило в приложении — `streamAddress` (`core/radio-play.ts`).
    Адрес не по http(s) не трогаем: чужую схему переписать нельзя, и HA отдаст его плееру как есть.
    """
    scheme = profile.get("scheme")
    if not isinstance(scheme, str) or not scheme:
        return address
    if address.startswith("https://"):
        return f"{scheme}://{address}"
    return address.replace("http://", f"{scheme}://", 1)


def flow_output(
    flow: FlowStreams, session: FlowSession, profile: Mapping[str, Any], opened: Callable[[], None],
    player: str, *, aac: bool = False,
) -> Output:
    """Выход потока-сессии: адрес один на сессию, содержимое меняет дом (`providers/music_flow.py`).

    ⚠ Звук отдаёт дом, но адрес всё равно приводится к схеме ЭТОГО плеера (`flow_address`): у Sonos
    он без схемы не принимается. Подтверждение запуска — начало чтения потока (`on_read` →
    `Attempt.opened`): у плеера, который о себе не рассказывает, другого свидетельства не бывает.
    """
    address = flow.address(session, player, aac=aac)
    if address is None:
        raise OpError("Дом не знает своего адреса для потока", 503)
    session.on_read = opened
    session.on_read_player = player
    return Output(
        address if aac else flow_address(profile, address), "audio/aac" if aac else flow_content_type(profile),
        opaque=profile.get("opaqueTrackId") is True, flow=True, native_stream=aac,
    )


def flow_content_type(profile: Mapping[str, Any]) -> str:
    """Тип `media_content_type` адреса сессии. ⚠ Поток дома — MP3 (см. `providers/music_flow.py`)."""
    return str(profile.get("type") or "music").replace("{mime}", "audio/mpeg").replace("{ext}", "mp3")


def member_accepts(tile: dict) -> list[str] | None:
    """Какие типы потока участник объявил (`options` его команды `play_stream`); `None` — не объявлял.

    ⚠ Свои типы объявляет Яндекс Станция (`stream.{ext}` → `stream.aac`/`stream.mp3`); наш поток —
    MP3, и в её списке он есть. ESPHome, Sonos, HEOS типов не объявляют — им можно что угодно.
    """
    spec = command_spec(tile, "play_stream") or {}
    options = ((spec.get("fields") or {}).get("media_content_type") or {}).get("options")
    return options if isinstance(options, list) else None


def fade_bytes(profile: Mapping[str, Any]) -> int:
    """Окно фейда на стыке элементов, байты PCM: `fadeMs` описания плитки, не больше 3 с.

    ⚠ Фейд делает ДОМ (`providers/music_flow.py`, `_Fader`), и только у потока: у плеера, который
    берёт ссылку на элемент, смешивать нечего — там стык как был. Нет `fadeMs` — стык как был.
    """
    value = profile.get("fadeMs")
    if not isinstance(value, (int, float)) or isinstance(value, bool) or value <= 0:
        return 0
    return min(int(value), 3000) * FLOW_CHANNELS * FLOW_WIDTH * FLOW_RATE // 1000


def stream_fields(tile: dict, entity: str, value: Any, source: Any) -> dict[str, str]:
    """Поля `play_media` станции — по описанию КОМАНДЫ из конфига: браузеру жильца верить нельзя.

    ⚠ Описание команды читается тем же правилом, что у `ops.command` (`core/ops.py`,
    `command_spec`), и разбирается тем же `arguments`: второго места «как звать play_stream» быть
    не должно — разъедется с менеджером молча.
    """
    spec = command_spec(tile, "play_stream")
    if spec is None:
        raise OpError("Плеер не играет поток по ссылке", 404)
    state = source.get(entity)
    try:
        data = arguments(spec, value, state.attributes if state else {})
    except ValueError as err:
        raise OpError(str(err)) from err
    return {
        "media_content_id": str(data.get("media_content_id") or ""),
        "media_content_type": str(data.get("media_content_type") or ""),
    }
