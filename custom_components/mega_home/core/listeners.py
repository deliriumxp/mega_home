"""Источники событий устройств: то, что дом слушает сам, без жильца у экрана.

Виды — данные описания доступа (`events[]`, `docs/home-gateway.md`):
  * `webhook` — устройство само зовёт дом по HTTP (Action URL домофона,
    вебхук). Свой порт интеграции, а не HTTP-сервер Home Assistant: слушатель
    переедет в ядро без HA без переделки (`docs/plan-core-without-ha.md`);
  * `poll`   — подписка «держи запрос, пока есть что сказать» через дверь.
    ⚠ Только если у вендора нет push-подписки (`docs/vendor-integrations.md`);
  * `mqtt`   — подписка на топики брокера доступа;
  * `tcp`    — постоянное соединение, события — куски между разделителями.

Дом ничего не толкует: событие уходит в концентратор (`device_events.py`) как
есть, с именем доступа и источника. Что оно значит, решают бандл и менеджер.
"""

from __future__ import annotations

import asyncio
import base64
import ipaddress
import json
from typing import Any

from aiohttp import web

from .access import AccessDescriptor
from .const import LOGGER
from .device_events import EventHub

# Порт входящих вызовов устройств. ⚠ Меняется только вместе с менеджером: адрес
# этого порта менеджер прописывает в устройства при их настройке.
HOOK_PORT = 8189
MAX_HOOK_BODY = 64 * 1024
RETRY_FIRST_S = 2.0
RETRY_MAX_S = 60.0
POLL_PAUSE_S = 1.0


def _body(raw: bytes) -> dict[str, Any]:
    try:
        return {"text": raw.decode("utf-8")}
    except UnicodeDecodeError:
        return {"base64": base64.b64encode(raw).decode("ascii")}


class Listeners:
    """Все источники событий объекта; пересобираются по смене описаний."""

    def __init__(self, env: Any, door: Any, hub: EventHub) -> None:
        self._env = env
        self._door = door
        self._hub = hub
        self._signature = ""
        self._tasks: list[asyncio.Task[None]] = []
        self._hooks: dict[tuple[str, str], AccessDescriptor] = {}
        self._runner: web.AppRunner | None = None
        self.why = ""

    def apply(self, descriptors: list[AccessDescriptor]) -> None:
        signature = json.dumps(
            [[d.id, d.host, d.port, d.secret, d.events] for d in descriptors], sort_keys=True, default=str
        )
        if signature == self._signature:
            return
        self._signature = signature
        self._env.spawn(self._restart(descriptors), "mega_home listeners")

    async def _restart(self, descriptors: list[AccessDescriptor]) -> None:
        await self._stop_sources()
        self._hooks = {}
        for descriptor in descriptors:
            for spec in descriptor.events:
                kind, source = str(spec.get("type") or ""), str(spec.get("id") or spec.get("type") or "")
                if kind == "webhook":
                    self._hooks[(descriptor.id, source)] = descriptor
                elif kind == "poll":
                    self._run(self._poll(descriptor, source, spec), f"poll {descriptor.id}/{source}")
                elif kind == "mqtt":
                    self._run(self._mqtt(descriptor, source, spec), f"mqtt {descriptor.id}/{source}")
                elif kind == "tcp":
                    self._run(self._tcp(descriptor, source, spec), f"tcp {descriptor.id}/{source}")
                else:
                    LOGGER.warning("Источник событий «%s» дом пока не умеет", kind)
        if self._hooks:
            await self._start_hooks()
        else:
            await self._stop_hooks()

    def _run(self, coro: Any, name: str) -> None:
        self._tasks.append(self._env.spawn(coro, f"mega_home events {name}"))

    async def stop(self) -> None:
        await self._stop_sources()
        await self._stop_hooks()

    async def _stop_sources(self) -> None:
        for task in self._tasks:
            task.cancel()
        self._tasks = []

    def state(self) -> dict[str, Any]:
        return {
            "hooks": [f"/hook/{a}/{s}" for a, s in self._hooks],
            "hook_port": HOOK_PORT if self._runner else None,
            "sources": len(self._tasks),
            "why": self.why,
        }

    # --- входящий HTTP -----------------------------------------------------

    async def _start_hooks(self) -> None:
        if self._runner is not None:
            return
        app = web.Application(client_max_size=MAX_HOOK_BODY)
        app.router.add_route("*", "/hook/{access}/{source}", self._hook)
        runner = web.AppRunner(app, access_log=None)
        await runner.setup()
        try:
            await web.TCPSite(runner, "0.0.0.0", HOOK_PORT).start()
        except OSError as err:
            await runner.cleanup()
            self.why = f"порт {HOOK_PORT} занят — входящие события не слушаем: {err}"
            LOGGER.warning("События устройств: %s", self.why)
            return
        self._runner, self.why = runner, ""

    async def _stop_hooks(self) -> None:
        runner, self._runner = self._runner, None
        if runner is not None:
            await runner.cleanup()

    async def _hook(self, request: web.Request) -> web.Response:
        access, source = request.match_info["access"], request.match_info["source"]
        descriptor = self._hooks.get((access, source))
        # ⚠ Вызов принимается ТОЛЬКО с адреса устройства из конфига: порт смотрит
        # в LAN без аутентификации (панель её не умеет), и без этой проверки
        # «звонок в дверь» мог бы прислать кто угодно из Wi-Fi объекта.
        if descriptor is None or not _same_host(request.remote, descriptor.host):
            return web.Response(status=404)
        raw = await request.read()
        self._hub.publish(
            access,
            source,
            "hook",
            {"method": request.method, "path": request.path, "query": dict(request.query), **_body(raw)},
        )
        return web.Response(text="OK")

    # --- долгий опрос через дверь -------------------------------------------

    async def _poll(self, descriptor: AccessDescriptor, source: str, spec: dict[str, Any]) -> None:
        from .gateway import SCOPE_MANAGER

        delay = RETRY_FIRST_S
        pause = float(spec.get("pause") or POLL_PAUSE_S)
        while True:
            try:
                status, kind, payload, _ = await self._door.call_full(
                    descriptor.id,
                    str(spec.get("method") or "GET"),
                    str(spec.get("path") or "/"),
                    spec.get("params") if isinstance(spec.get("params"), dict) else None,
                    json.dumps(spec["body"]).encode() if isinstance(spec.get("body"), (dict, list)) else None,
                    scope=SCOPE_MANAGER,
                )
                self._hub.publish(descriptor.id, source, "response", {"status": status, "contentType": kind, **_body(payload)})
                delay = RETRY_FIRST_S
                await asyncio.sleep(pause)
            except asyncio.CancelledError:
                raise
            except Exception as err:  # noqa: BLE001 — источник живёт, пока жив конфиг
                LOGGER.debug("Опрос %s/%s: %s", descriptor.id, source, err)
                await asyncio.sleep(delay)
                delay = min(delay * 2, RETRY_MAX_S)

    # --- MQTT ------------------------------------------------------------------

    async def _mqtt(self, descriptor: AccessDescriptor, source: str, spec: dict[str, Any]) -> None:
        topics = [str(t) for t in spec.get("topics") or [] if str(t)]
        delay = RETRY_FIRST_S

        def on_message(topic: str, data: bytes) -> None:
            self._hub.publish(descriptor.id, source, "message", {"topic": topic, **_body(data)})

        while True:
            client = None
            try:
                client = await self._door.mqtt_client(descriptor, on_message)
                await client.subscribe(topics)
                delay = RETRY_FIRST_S
                await client.closed.wait()
            except asyncio.CancelledError:
                if client is not None:
                    await client.close()
                raise
            except Exception as err:  # noqa: BLE001
                LOGGER.debug("MQTT %s/%s: %s", descriptor.id, source, err)
            if client is not None:
                await client.close()
            await asyncio.sleep(delay)
            delay = min(delay * 2, RETRY_MAX_S)

    # --- TCP -------------------------------------------------------------------

    async def _tcp(self, descriptor: AccessDescriptor, source: str, spec: dict[str, Any]) -> None:
        delimiter = base64.b64decode(spec["until"]) if spec.get("until") else b"\n"
        greeting = base64.b64decode(spec["send"]) if spec.get("send") else b""
        port = int(spec.get("port") or descriptor.port)
        delay = RETRY_FIRST_S
        while True:
            writer = None
            try:
                reader, writer = await asyncio.wait_for(asyncio.open_connection(descriptor.host, port), 10)
                if greeting:
                    writer.write(greeting)
                    await writer.drain()
                delay = RETRY_FIRST_S
                buffer = b""
                while True:
                    chunk = await reader.read(64 * 1024)
                    if not chunk:
                        break
                    buffer += chunk
                    while delimiter in buffer:
                        item, buffer = buffer.split(delimiter, 1)
                        if item:
                            self._hub.publish(descriptor.id, source, "data", _body(item))
                    if len(buffer) > MAX_HOOK_BODY:
                        self._hub.publish(descriptor.id, source, "data", _body(buffer))
                        buffer = b""
            except asyncio.CancelledError:
                if writer is not None:
                    writer.close()
                raise
            except (OSError, asyncio.TimeoutError) as err:
                LOGGER.debug("TCP %s/%s: %s", descriptor.id, source, err)
            if writer is not None:
                writer.close()
            await asyncio.sleep(delay)
            delay = min(delay * 2, RETRY_MAX_S)


def _same_host(remote: str | None, host: str) -> bool:
    try:
        return ipaddress.ip_address(remote or "") == ipaddress.ip_address(host)
    except ValueError:
        return False
