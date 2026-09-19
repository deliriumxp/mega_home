"""Глаголы доступа вида `mqtt` (`docs/home-gateway.md`).

  * `publish` (умолчание): `topic`, `payload` (объект — JSON, строка) или
    `payloadBase64` (байты), `qos` 0/1, `retain`;
  * `get`: `topic`, `timeout` — ТЕКУЩЕЕ значение: подписка, первое сообщение
    (retained у брокера) и отписка. Нужно бандлу, открывшему экран климата или
    реле на MQTT-шине (Wirenboard), — дождаться изменения он не может.
"""

from __future__ import annotations

import asyncio
import base64
from typing import Any

from .access import AccessDescriptor
from .access_http import AccessDenied, AccessUnreachable

GET_TIMEOUT = 3.0
MAX_GET_TIMEOUT = 15.0


async def mqtt_call(door: Any, descriptor: AccessDescriptor, call: dict[str, Any], scope: str) -> dict[str, Any]:
    from .gateway import deny_target
    from .mqtt import MqttError, as_bytes

    topic = str(call.get("topic") or "")
    if not topic:
        raise AccessDenied("Топик не указан")
    verb = str(call.get("verb") or "publish")
    deny_target(descriptor, topic, scope)
    if verb == "get":
        if any(ch in topic for ch in "#+"):
            raise AccessDenied("Текущее значение читается по точному топику, без масок")
        return await _get(door, descriptor, topic, call.get("timeout"))
    if verb != "publish":
        raise AccessDenied(f"Глагол «{verb}» у MQTT не поддержан")
    try:
        data = as_bytes(call.get("payload"), call.get("payloadBase64"))
    except ValueError as err:
        raise AccessDenied("payloadBase64: ожидается base64") from err
    client = await door.mqtt_client(descriptor)
    try:
        await client.publish(topic, data, 1 if call.get("qos") == 1 else 0, call.get("retain") is True)
    except MqttError as err:
        raise AccessUnreachable(f"Брокер: {err}") from err
    return {"published": True}


async def _get(door: Any, descriptor: AccessDescriptor, topic: str, timeout: Any) -> dict[str, Any]:
    from .mqtt import MqttError

    try:
        window = min(max(float(timeout), 0.2), MAX_GET_TIMEOUT) if timeout is not None else GET_TIMEOUT
    except (TypeError, ValueError):
        window = GET_TIMEOUT
    got: asyncio.Future[bytes] = asyncio.get_running_loop().create_future()

    def on_message(name: str, data: bytes) -> None:
        if name == topic and not got.done():
            got.set_result(data)

    client = await door.mqtt_client(descriptor, on_message)
    try:
        await client.subscribe([topic])
        data = await asyncio.wait_for(got, window)
    except asyncio.TimeoutError:
        return {"topic": topic, "found": False}
    except MqttError as err:
        raise AccessUnreachable(f"Брокер: {err}") from err
    finally:
        await client.close()
    try:
        return {"topic": topic, "found": True, "text": data.decode("utf-8")}
    except UnicodeDecodeError:
        return {"topic": topic, "found": True, "base64": base64.b64encode(data).decode("ascii")}
