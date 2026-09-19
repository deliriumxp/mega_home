"""Медиа доступа: любой источник go2rtc по шаблону из описания.

Зачем. Камера, панель домофона, второй регистратор — у всех видео приходит
адресом, который go2rtc умеет открыть (RTSP, HTTP-MJPEG, ONVIF и прочие его
схемы). Дом не знает, чей это адрес: шаблон с `{host}`, `{camera}` и полями
учётки (`{secret.<поле>|url}`) живёт в описании доступа (`media`), его пишет
менеджер (`docs/home-gateway.md`).

Один путь показа видео на все источники — `go2rtc_session.negotiate_source`,
тот же, что у камер и записей: второй способ показывать видео не заводится.
"""

from __future__ import annotations

import hashlib
from http import HTTPStatus
from typing import Any

from . import go2rtc_session
from .gateway import AccessDenied, AccessUnreachable
from .ops_base import OpError

MAX_FRAME_BYTES = 4 * 1024 * 1024
FRAME_TIMEOUT = 15


def stream_name(url: str) -> str:
    """Имя потока в go2rtc: от адреса, но БЕЗ учётки в имени.

    ⚠ Имя видно в API go2rtc и в его журнале; адрес с паролем туда класть нельзя.
    """
    return "media_" + hashlib.sha256(url.encode("utf-8")).hexdigest()[:16]


def _require_go2rtc() -> str:
    from .go2rtc_embed import URL, is_running

    if not is_running():
        raise OpError("Дом не может отдать видео: не поднят его go2rtc", HTTPStatus.SERVICE_UNAVAILABLE)
    return URL


async def resolve(coordinator: Any, payload: dict[str, Any]) -> str:
    """Адрес источника по `access` + `media` (+ `values` для шаблона)."""
    door = getattr(coordinator, "accesses", None)
    if door is None:
        raise OpError("У объекта нет ни одного доступа", HTTPStatus.NOT_FOUND)
    values = payload.get("values")
    try:
        return await door.media_url(
            payload.get("access"), str(payload.get("media")), values if isinstance(values, dict) else None
        )
    except AccessUnreachable as err:
        raise OpError(str(err), HTTPStatus.BAD_GATEWAY) from err
    except AccessDenied as err:
        raise OpError(str(err), HTTPStatus.FORBIDDEN) from err


async def live_offer(
    coordinator: Any, payload: dict[str, Any], sdp: str, remote: bool, trickle: bool
) -> dict[str, Any]:
    """WebRTC с источника медиа доступа — тот же сеанс, что у любой камеры дома."""
    url = await resolve(coordinator, payload)
    base = _require_go2rtc()
    return await go2rtc_session.negotiate_source(
        coordinator.env, base, stream_name(url), url, sdp, "с этого устройства", remote, False, trickle
    )


async def frame(env: Any, url: str) -> bytes:
    """Кадр JPEG с источника: поток заводится в go2rtc, кадр — его API `frame.jpeg`."""
    import aiohttp
    from go2rtc_client import Go2RtcRestClient

    base = _require_go2rtc()
    name = stream_name(url)
    session = env.session()
    added = False
    try:
        rest = Go2RtcRestClient(session, base)
        streams = await rest.streams.list()
        if name not in streams:
            await rest.streams.add(name, [url])
            added = True
        async with session.get(
            f"{base}/api/frame.jpeg", params={"src": name}, timeout=aiohttp.ClientTimeout(total=FRAME_TIMEOUT)
        ) as response:
            if response.status != 200:
                raise AccessUnreachable(f"Источник не отдал кадр (HTTP {response.status})")
            data = b""
            async for chunk in response.content.iter_chunked(64 * 1024):
                data += chunk
                if len(data) > MAX_FRAME_BYTES:
                    break
    except AccessUnreachable:
        raise
    except Exception as err:  # noqa: BLE001 — go2rtc и источник падают по-разному
        # ⚠ Текст исключения go2rtc не отдаём: в нём URL источника вместе с
        # учёткой (`src=rtsp://user:pass@…`), а ответ видит локальный контур.
        raise AccessUnreachable(f"Источник не отдал кадр ({type(err).__name__})") from err
    finally:
        # Поток под кадр — разовый: иначе таблица go2rtc росла бы с каждым
        # новым набором значений шаблона.
        if added:
            try:
                await session.delete(f"{base}/api/streams", params={"src": name})
            except Exception:  # noqa: BLE001 — уборка не роняет ответ
                pass
    if len(data) > MAX_FRAME_BYTES or not data.startswith(b"\xff\xd8"):
        raise AccessUnreachable("Источник отдал не кадр JPEG")
    return data
