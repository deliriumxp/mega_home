"""Универсальная дверь наружу как операция: `api/gateway/call` и кадр менеджера.

⚠ Дом не знает ни одного вендора и не разбирает ни одного ответа: он
подставляет учётку и сессию, выполняет вызов у доступа из конфига объекта и
отдаёт ответ КАК ЕСТЬ (`gateway.py`; закрытый список возможностей —
`docs/home-gateway.md` в менеджере).

⚠ Дверь берётся у КООРДИНАТОРА, а не у драйвера видеонаблюдения: она несёт
вызовы к любой описанной системе (`docs/plan-video-rework.md`, «Сквозной принцип»).

Форма вызова — одна на все виды доступа:
  * `http`: `method`, `path`, `params`, `body`, `headers`;
    `envelope: true` — ответ всегда конвертом с заголовками, `binary` — телом;
  * `tcp` / `udp` / `mqtt`: `call` — глаголы вида (`access_raw.py`, `mqtt.py`);
  * `media` + `frame: true` — кадр JPEG с источника медиа доступа.
"""

from __future__ import annotations

import base64
import json
from http import HTTPStatus
from typing import Any

from .gateway import SCOPE_MANAGER, SCOPE_RESIDENT, AccessDenied, AccessUnreachable
from .ops_base import OpError


async def gateway_call(
    coordinator: Any, payload: dict[str, Any], scope: str = SCOPE_RESIDENT
) -> Any:
    """Исполнить ОПИСАННЫЙ вызов. `scope` — уровень вызывающего (`gateway.py`)."""
    door = getattr(coordinator, "accesses", None)
    if door is None:
        raise OpError("У объекта нет ни одного доступа", HTTPStatus.NOT_FOUND)
    # ⚠ Описания ещё не приехали — доступа ЭТОГО НЕТ, и это 404, а не отказ: по
    # отказу бандл решил бы, что ему нельзя (живой отчёт 2026-09-12).
    descriptor = door.descriptor(payload.get("access"))
    if descriptor is None:
        raise OpError("У объекта нет такого доступа", HTTPStatus.NOT_FOUND)
    scope = SCOPE_MANAGER if scope == SCOPE_MANAGER else SCOPE_RESIDENT
    try:
        if payload.get("media"):
            return await _media_frame(coordinator, door, payload)
        if descriptor.kind != "http":
            call = payload.get("call")
            return await door.exchange(descriptor.id, call if isinstance(call, dict) else {}, scope)
        return await _http(coordinator, door, payload, scope)
    except AccessUnreachable as err:
        # ⚠ 502, а НЕ 403: отказ политики — наша ошибка в описании вызова, а
        # недоступность той системы — беда объекта, и жилец читает её словами
        # (ревизия 2026-09-13).
        raise OpError(str(err), HTTPStatus.BAD_GATEWAY) from err
    except AccessDenied as err:
        raise OpError(str(err), HTTPStatus.FORBIDDEN) from err


async def _http(coordinator: Any, door: Any, payload: dict[str, Any], scope: str) -> Any:
    session: dict[str, str] = {}
    clip_id = payload.get("clip")
    path = str(payload.get("path") or "")
    gateway = getattr(coordinator, "trassir", None)
    if clip_id and gateway is not None:
        token = gateway.clips.token_of(str(clip_id))
        if token:
            session["token"] = token
            # ⚠ Соединение, по которому бандл КОМАНДУЕТ архивом, дом считает
            # начатым — иначе он отдаст следом СВОЙ `play`, а второй `play` по
            # играющему потоку останавливает данные. Пометка стоит ДО вызова:
            # команда архива отвечает 0,8–1,4 с (замер стенда), и сторож
            # просыпается ровно в этом окне.
            gateway.clips.note_gateway_call(str(clip_id), path)

    body = payload.get("body")
    # ⚠ Тело строкой — это base64 (им же носит файлы реле); объект — это JSON.
    if isinstance(body, str) and body:
        try:
            raw = base64.b64decode(body, validate=True)
        except ValueError as err:
            raise AccessDenied("Тело вызова строкой — это base64") from err
    elif isinstance(body, (dict, list)):
        raw = json.dumps(body).encode("utf-8")
    else:
        raw = None
    headers = payload.get("headers")
    status, content_type, answer, shown = await door.call_full(
        payload.get("access"),
        str(payload.get("method") or "GET"),
        path,
        payload.get("params"),
        raw,
        session,
        headers if isinstance(headers, dict) else None,
        scope,
    )
    envelope = payload.get("envelope") is True
    if "json" in (content_type or "") and not payload.get("binary") and not envelope:
        try:
            return json.loads(answer.decode("utf-8", "ignore"))
        except ValueError:
            # Пустой 204 или HTML-ошибка с типом JSON — отдаём конвертом, а не 500.
            envelope = True
    out: dict[str, Any] = {
        "status": status,
        "contentType": content_type or "",
        "body": base64.b64encode(answer).decode("ascii"),
    }
    if envelope:
        out["headers"] = shown
    return out


async def _media_frame(coordinator: Any, door: Any, payload: dict[str, Any]) -> dict[str, Any]:
    """Кадр JPEG с источника медиа доступа — тем же go2rtc, что живое видео.

    ⚠ Нужен там, где у устройства нет своего снимка по HTTP, и менеджеру для
    уведомления о вызове: облако панель не достаёт, кадр берёт дом.
    """
    from .media import frame

    if payload.get("frame") is not True:
        raise AccessDenied("Медиа дверью отдаётся только кадром; поток — через WebRTC")
    values = payload.get("values")
    url = await door.media_url(
        payload.get("access"), str(payload.get("media")), values if isinstance(values, dict) else None
    )
    jpeg = await frame(coordinator.env, url)
    return {"status": 200, "contentType": "image/jpeg", "body": base64.b64encode(jpeg).decode("ascii")}
