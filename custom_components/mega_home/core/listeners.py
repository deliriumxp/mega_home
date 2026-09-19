"""Источники событий устройств: то, что дом слушает сам, без жильца у экрана.

Виды — данные описания устройства (`events[]`, `docs/plan-thin-gateway.md`):
  * `webhook`   — устройство само зовёт дом по HTTP (Action URL, вебхук, alarm
    server). Путь `/hook/<доступ>/<источник>` или ЛЮБОЙ путь (`anyPath`) —
    тогда источник опознаётся по адресу устройства. Ответ — `reply` описания;
  * `tcpServer` — устройство само подключается к дому по TCP (сырые события);
  * `udp`       — датаграммы на порт дома, в том числе мультикаст (`group`);
  * исходящие `poll`, `stream`, `ws`, `mqtt`, `tcp` — `listeners_out.py`.
⚠ Опрос и поток — только если у вендора нет push-подписки (`docs/vendor-integrations.md`).

⚠ Входящие — только с адреса устройства из конфига: порты смотрят в LAN без
аутентификации. Порты — свои у интеграции, а не HTTP-сервер HA: слушатели
переедут в ядро без HA без переделки.

Событие уходит в концентратор (`device_events.py`) как есть. В локальный поток
приложения — только с `local: true` в описании источника.
"""

from __future__ import annotations

import asyncio
import ipaddress
import socket
from time import monotonic
from typing import Any

from aiohttp import web

from . import listeners_out as out
from .devices import DeviceDescriptor
from .const import LOGGER
from .device_events import EventHub

# Порт входящих HTTP-вызовов устройств. ⚠ Меняется только вместе с менеджером:
# адрес этого порта менеджер прописывает в устройства при их настройке.
HOOK_PORT = 8189
MAX_CHUNK = out.MAX_CHUNK
RETRY_FIRST_S = 2.0
RETRY_MAX_S = 60.0
# Проработал дольше — значит был здоров: следующий сбой снова с короткой паузы.
HEALTHY_S = 60.0
body_of = out.body_of


def _host_ip(host: str) -> ipaddress.IPv4Address | ipaddress.IPv6Address | None:
    try:
        return ipaddress.ip_address(host)
    except ValueError:
        return None


class Listeners:
    """Все источники событий объекта; пересобираются по смене описаний."""

    def __init__(self, env: Any, registry: Any, hub: EventHub) -> None:
        self._env = env
        self._registry = registry
        self._hub = hub
        self._signature = ""
        self._lock = asyncio.Lock()
        self._restarting: asyncio.Task[None] | None = None
        self._tasks: list[Any] = []
        self._hooks: dict[tuple[str, str], tuple[DeviceDescriptor, dict[str, Any]]] = {}
        self._any_path: dict[str, tuple[DeviceDescriptor, dict[str, Any]]] = {}
        self._runner: web.AppRunner | None = None
        self._servers: list[Any] = []
        self._stopped = False
        self.why = ""

    def apply(self, descriptors: list[DeviceDescriptor]) -> None:
        # Подпись — ВСЁ описание: сроки и авторизация тоже меняют работу
        # источника, а держать устаревшее описание он не должен.
        signature = repr(descriptors)
        if signature == self._signature or self._stopped:
            return
        self._signature = signature
        # Прежний перезапуск, не успевший взять замок, снимается: иначе сначала
        # поднялись бы устаревшие источники, и только потом свежие.
        previous = self._restarting
        if isinstance(previous, asyncio.Future) and not previous.done():
            previous.cancel()
        self._restarting = self._env.spawn(self._restart(descriptors), "mega_home listeners")

    async def _restart(self, descriptors: list[DeviceDescriptor]) -> None:
        async with self._lock:
            await self._stop_all()
            if self._stopped:
                return
            self._hooks, self._any_path, self.why = {}, {}, ""
            for descriptor in descriptors:
                for spec in descriptor.events:
                    self._start_source(descriptor, spec)
            if self._hooks or self._any_path:
                self._run(self._hook_server(), "hooks")

    def _start_source(self, descriptor: DeviceDescriptor, spec: dict[str, Any]) -> None:
        kind = str(spec.get("type") or "")
        source = str(spec.get("id") or kind)
        name = f"{descriptor.id}/{source}"
        if kind == "webhook":
            if _host_ip(descriptor.host) is None:
                self.why = f"{name}: вебхук принимается только с IP-адреса, а в описании имя"
                LOGGER.warning("События устройств: %s", self.why)
            elif spec.get("anyPath") is True:
                self._any_path[str(_host_ip(descriptor.host))] = (descriptor, spec)
            else:
                self._hooks[(descriptor.id, source)] = (descriptor, spec)
            return
        workers = {
            "poll": out.poll, "stream": out.stream, "ws": out.ws, "mqtt": out.mqtt, "tcp": out.tcp,
            "tcpServer": _tcp_server, "udp": _udp,
        }
        worker = workers.get(kind)
        if worker is None:
            LOGGER.warning("Источник событий «%s» дом пока не умеет", kind)
            return
        if kind in ("tcpServer", "udp") and _port(spec) is None:
            # ⚠ Словами в `why`, а не вечный повтор с KeyError на уровне debug:
            # инсталлятор видел «источник есть», а он мёртв (ревью 2026-09-19).
            self.why = f"{name}: у источника «{kind}» не задан порт"
            LOGGER.warning("События устройств: %s", self.why)
            return
        self._run(self._forever(worker, descriptor, source, spec, name), name)

    def _run(self, coro: Any, name: str) -> None:
        self._tasks.append(self._env.spawn(coro, f"mega_home events {name}"))

    def emit(self, descriptor: DeviceDescriptor, source: str, spec: dict[str, Any], event: str, data: Any) -> None:
        self._hub.publish(descriptor.id, source, event, data, local=spec.get("local") is True)

    async def stop(self) -> None:
        """Остановить всё и ДОЖДАТЬСЯ: порт 8189 обязан освободиться до новой записи."""
        self._stopped = True
        restarting = self._restarting
        if isinstance(restarting, asyncio.Future) and not restarting.done():
            restarting.cancel()
            await asyncio.gather(restarting, return_exceptions=True)
        async with self._lock:
            await self._stop_all()

    async def _stop_all(self) -> None:
        tasks, self._tasks = self._tasks, []
        for task in tasks:
            task.cancel()
        await asyncio.gather(*[t for t in tasks if isinstance(t, asyncio.Future)], return_exceptions=True)
        runner, self._runner = self._runner, None
        if runner is not None:
            await runner.cleanup()
        for server in self._servers:
            server.close()
        self._servers = []

    def state(self) -> dict[str, Any]:
        return {
            "hooks": [f"/hook/{a}/{s}" for a, s in self._hooks] + [f"* от {ip}" for ip in self._any_path],
            "hook_port": HOOK_PORT if self._runner else None,
            "sources": len(self._tasks),
            "why": self.why,
        }

    async def _forever(self, worker: Any, descriptor: DeviceDescriptor, source: str, spec: dict[str, Any], name: str) -> None:
        """Источник живёт, пока жив конфиг: отказ — повтор с растущей паузой."""
        delay = RETRY_FIRST_S
        while True:
            started = monotonic()
            try:
                await worker(self, descriptor, source, spec)
            except asyncio.CancelledError:
                raise
            except Exception as err:  # noqa: BLE001 — источник не роняет дом
                LOGGER.debug("Источник событий %s: %s", name, type(err).__name__)
            if monotonic() - started > HEALTHY_S:
                delay = RETRY_FIRST_S
            await asyncio.sleep(delay)
            delay = min(delay * 2, RETRY_MAX_S)

    # --- входящий HTTP -----------------------------------------------------

    async def _hook_server(self) -> None:
        app = web.Application(client_max_size=MAX_CHUNK)
        app.router.add_route("*", "/hook/{access}/{source}", self._hook)
        app.router.add_route("*", "/{tail:.*}", self._hook_any)
        delay = RETRY_FIRST_S
        while True:
            runner = web.AppRunner(app, access_log=None)
            await runner.setup()
            try:
                await web.TCPSite(runner, "0.0.0.0", HOOK_PORT).start()
            except OSError as err:
                await runner.cleanup()
                # ⚠ Порт мог ещё держать прошлый запуск записи — повторяем, а не
                # сдаёмся: иначе Action URL домофона молча не принимался бы.
                self.why = f"порт {HOOK_PORT} занят — повторю через {delay:.0f} с: {err.strerror}"
                LOGGER.warning("События устройств: %s", self.why)
                await asyncio.sleep(delay)
                delay = min(delay * 2, RETRY_MAX_S)
                continue
            except BaseException:
                # Отмена посреди старта — порт не должен остаться занятым.
                await runner.cleanup()
                raise
            self._runner, self.why = runner, ""
            return

    async def _hook(self, request: web.Request) -> web.Response:
        found = self._hooks.get((request.match_info["access"], request.match_info["source"]))
        if found is None or not _same_host(request.remote, found[0].host):
            return await self._hook_any(request)
        return await self._accept(request, found, request.match_info["source"])

    async def _hook_any(self, request: web.Request) -> web.Response:
        remote = _host_ip(request.remote or "")
        found = self._any_path.get(str(remote)) if remote else None
        if found is None:
            return web.Response(status=404)
        return await self._accept(request, found, str(found[1].get("id") or "webhook"))

    async def _accept(self, request: web.Request, found: tuple[DeviceDescriptor, dict[str, Any]], source: str) -> web.Response:
        raw = await request.read()
        data = {"method": request.method, "path": request.path, "query": dict(request.query), **body_of(raw)}
        self.emit(found[0], source, found[1], "hook", data)
        # Ответ — данные описания: часть устройств ждёт своего тела или кода и
        # иначе повторяет вызов (дубли событий).
        reply = found[1].get("reply") if isinstance(found[1].get("reply"), dict) else {}
        try:
            status = int(reply.get("status") or 200)
        except (TypeError, ValueError):
            status = 200
        return web.Response(
            status=status, text=str(reply.get("body") if reply.get("body") is not None else "OK"),
            content_type=str(reply.get("contentType") or "text/plain"),
        )



# --- входящий TCP и UDP -------------------------------------------------------
# Та же сигнатура, что у исходящих источников (`listeners_out.py`): таблица
# `workers` в `_start_source` — единственное место, где виды различаются.


def _port(spec: dict[str, Any]) -> int | None:
    try:
        port = int(spec.get("port") or 0)
    except (TypeError, ValueError):
        return None
    return port if 0 < port < 65536 else None


async def _tcp_server(ctx: Listeners, descriptor: DeviceDescriptor, source: str, spec: dict[str, Any]) -> None:
    sep = out.delimiter(spec, b"\n")
    allowed = _host_ip(descriptor.host)
    idle = out.seconds(spec.get("idle"), out.IDLE_S, 5.0)

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        peer = writer.get_extra_info("peername")
        if allowed is None or not peer or _host_ip(peer[0]) != allowed:
            writer.close()
            return
        try:
            await out.split(reader, sep, lambda item: ctx.emit(descriptor, source, spec, "data", body_of(item)), idle)
        except (ConnectionError, OSError):
            pass
        finally:
            writer.close()

    server = await asyncio.start_server(handle, "0.0.0.0", _port(spec))
    ctx._servers.append(server)  # noqa: SLF001 — сервер закрывает владелец при остановке
    async with server:
        await server.serve_forever()


async def _udp(ctx: Listeners, descriptor: DeviceDescriptor, source: str, spec: dict[str, Any]) -> None:
    group = str(spec.get("group") or "")
    allowed = _host_ip(descriptor.host)
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("0.0.0.0", _port(spec)))
        if group:
            membership = socket.inet_aton(group) + socket.inet_aton("0.0.0.0")
            sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, membership)
    except OSError:
        sock.close()
        raise

    class Receiver(asyncio.DatagramProtocol):
        def datagram_received(self, data: bytes, addr: Any) -> None:
            sender = _host_ip(addr[0])
            # Мультикаст шлют многие — тогда адрес доступа и есть группа, и
            # принимается любой отправитель из частной сети.
            if group and allowed is not None and allowed.is_multicast:
                if sender is None or not sender.is_private:
                    return
            elif sender != allowed:
                return
            ctx.emit(descriptor, source, spec, "datagram", {"from": addr[0], **body_of(data)})

    transport, _ = await asyncio.get_running_loop().create_datagram_endpoint(Receiver, sock=sock)
    try:
        await asyncio.Event().wait()
    finally:
        transport.close()


def _same_host(remote: str | None, host: str) -> bool:
    left, right = _host_ip(remote or ""), _host_ip(host)
    return left is not None and left == right
