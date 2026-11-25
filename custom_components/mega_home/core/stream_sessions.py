"""Виды сессий потока (`stream.py`): TCP, WebSocket, HTTP и UDP до устройства и общие их пределы.

Реестр сессий сокета, потолки и разбор кадров канала — `stream.py`; здесь — что делает ОДНА сессия
своего вида: качает байты в обе стороны, держит свои сроки и потолок трафика и сообщает владельцу
о конце. Новый вид транспорта — новый класс здесь и строка выбора в `Streams`.
"""

from __future__ import annotations

import asyncio
import struct
from typing import TYPE_CHECKING, Any

import aiohttp

from . import connect as connect_mod
from .const import LOGGER
from .ops_base import OpError

if TYPE_CHECKING:
    from .stream import Streams

# Жизнь сессии. Час — столько же, сколько даёт Domotz, и по той же причине:
# забытая открытой вкладка не должна держать дверь в чужую квартиру сутками.
MAX_LIFETIME_S = 3600.0
# Молчание в обе стороны. Вкладку закрыли — поток обязан умереть сам, не дожидаясь
# часа: браузер про закрытие сообщает не всегда.
IDLE_TIMEOUT_S = 120.0
# Потолок трафика на сессию: прошивку залить хватит, выкачать через дом жильца
# содержимое NAS — уже нет.
MAX_BYTES = 256 * 1024 * 1024
# Кусок чтения. 64 КБ — обычный размер, на котором TCP не мельчит кадрами.
CHUNK = 64 * 1024
# Очередь в сторону устройства. Полная очередь ПРИТОРМАЖИВАЕТ чтение канала —
# это и есть обратное давление: без него быстрый менеджер и медленное устройство
# копили бы память дома, пока Home Assistant не убьют по OOM.
QUEUE_DEPTH = 64

HEADER = struct.Struct(">I")


def frame(stream_id: int, payload: bytes) -> bytes:
    """Бинарный кадр данных: номер потока + байты."""
    return HEADER.pack(stream_id) + payload


class SessionBase:
    """Общее у всех видов сессии: срок жизни и конец с уведомлением владельца.

    Наследник задаёт `id`, `_owner`, `_tasks` и `_release()` — что закрыть у
    своего вида (сокет, сессию HTTP, транспорт UDP); `_run()` или свой `start()`.
    """

    # Неизменяемые умолчания класса: присваивание в экземпляре их не трогает.
    _bytes = 0
    _stopping = False

    def start(self) -> None:
        self._tasks = [asyncio.ensure_future(self._run()), asyncio.ensure_future(self._deadline())]

    async def _relay(self, data: bytes | str) -> bool:
        """Кадр менеджеру под потолком трафика (текст — кадром `stream.data`). `False` — потолок
        превышен и сессия закрыта. Потолок — на ОБА вида кадров: текстом выкачивается так же."""
        self._bytes += len(data)
        if self._bytes > MAX_BYTES:
            await self._done("превышен потолок трафика сессии")
            return False
        if isinstance(data, str):
            await self._owner._send_json({"t": "stream.data", "id": self.id, "text": data})
        else:
            await self._owner._send_bytes(frame(self.id, data))
        return True

    async def stop(self, error: str | None) -> None:
        if self._stopping:
            return
        self._stopping = True
        for task in self._tasks:
            # ⚠ Не свою: закрытие по потолку или по концу приходит из задачи,
            # живущей в этом же списке, и отменить себя значило бы не дойти до
            # конца — `_release` оборвался бы на первом же `await`.
            if task is not asyncio.current_task():
                task.cancel()
        await self._release()
        if error:
            LOGGER.info("Сессия %s закрыта: %s", self.id, error)

    async def _deadline(self) -> None:
        await asyncio.sleep(MAX_LIFETIME_S)
        await self._done("истёк срок сессии")

    async def _done(self, error: str | None) -> None:
        # ⚠ Сообщаем ВЛАДЕЛЬЦУ, а не закрываемся сами: реестр знает только он, и
        # забытая в нём сессия занимала бы место до конца связи.
        await self._owner._finished(self.id, error)
        await self.stop(error)


class TcpSession(SessionBase):
    """Одно TCP-соединение и две его качалки."""

    def __init__(
        self,
        stream_id: int,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        owner: Streams,
        idle: float = IDLE_TIMEOUT_S,
    ) -> None:
        self.id = stream_id
        self._reader = reader
        self._writer = writer
        self._owner = owner
        self._idle_s = idle
        self._queue: asyncio.Queue[bytes] = asyncio.Queue(QUEUE_DEPTH)
        self._tasks: list[asyncio.Task[None]] = []

    def start(self) -> None:
        self._tasks = [
            asyncio.ensure_future(self._pump_out()),
            asyncio.ensure_future(self._pump_in()),
            asyncio.ensure_future(self._deadline()),
        ]

    async def to_device(self, chunk: bytes) -> None:
        # ⚠ `await put` — это и есть обратное давление: очередь полна, значит
        # устройство не успевает, и чтение канала притормаживается само.
        if not self._stopping:
            await self._queue.put(chunk)

    async def to_device_text(self, text: str) -> None:
        """Текстовый кадр — понятие WebSocket; у байтового потока его нет.

        ⚠ Молча, а не отказом: вызывающий мог перепутать вид сессии, но рвать
        из-за этого живое соединение к устройству — хуже, чем не доставить
        кадр, которого тут быть не должно.
        """
        await self.to_device(text.encode("utf-8"))

    async def _release(self) -> None:
        self._writer.close()
        try:
            await asyncio.wait_for(self._writer.wait_closed(), 2)
        except (asyncio.TimeoutError, OSError):
            pass

    async def _pump_in(self) -> None:
        """Менеджер → устройство."""
        try:
            while True:
                chunk = await self._queue.get()
                self._writer.write(chunk)
                await self._writer.drain()
        except asyncio.CancelledError:
            raise
        except OSError as err:
            await self._done(describe(err))

    async def _pump_out(self) -> None:
        """Устройство → менеджер."""
        try:
            while True:
                chunk = await asyncio.wait_for(self._reader.read(CHUNK), self._idle_s)
                if not chunk:
                    await self._done(None)
                    return
                if not await self._relay(chunk):
                    return
        except asyncio.CancelledError:
            raise
        except asyncio.TimeoutError:
            await self._done("тишина в сессии")
        except OSError as err:
            await self._done(describe(err))


class WsSession(SessionBase):
    """Сессия WebSocket к устройству: рукопожатие ведёт ДОМ, смысл — вызывающий.

    ⚠ Ради этого вида ось и закрывалась именно удержанием, а не потоковым
    ответом: у WebSocket два направления, и подписка, где на каждый шаг надо
    послать посчитанное вызывающим, иначе не выражается вовсе.
    """

    def __init__(
        self,
        stream_id: int,
        req: dict[str, Any],
        host: str,
        port: int,
        owner: Streams,
        idle: float,
    ) -> None:
        self.id = stream_id
        self._req = req
        self._url = f"{'wss' if req.get('tls') is True else 'ws'}://{host}:{port}{req.get('path') or '/'}"
        self._host, self._port = host, port
        self._owner = owner
        self._idle_s = idle
        self._outgoing: asyncio.Queue[tuple[str, Any]] = asyncio.Queue(QUEUE_DEPTH)
        self._tasks: list[asyncio.Task[None]] = []
        self._session: aiohttp.ClientSession | None = None
        self._socket: Any = None

    async def to_device(self, chunk: bytes) -> None:
        if not self._stopping:
            await self._outgoing.put(("bin", chunk))

    async def to_device_text(self, text: str) -> None:
        if self._stopping:
            return
        # Кадр к службе дома — через её запрет (`services.Service.guard`): учётку службы
        # ставит дом, и закрытое ею не открывается кадром вызывающего.
        refused = connect_mod.refused(self._req, "frame", text)
        if refused:
            await self._done(refused)
            return
        await self._outgoing.put(("text", text))

    async def _release(self) -> None:
        if self._socket is not None:
            await self._socket.close()
        if self._session is not None:
            await self._session.close()

    async def _run(self) -> None:
        headers = self._req.get("headers") if isinstance(self._req.get("headers"), dict) else None
        try:
            connector = await connect_mod.connector_for(self._host, self._port, self._req.get("tls") is True, self._req)
            self._session = aiohttp.ClientSession(connector=connector, trace_configs=connect_mod.no_redirects())
            # Срок — на РУКОПОЖАТИЕ: число в `timeout=` у aiohttp задавало лишь
            # срок закрытия (и устарело), а к мёртвому адресу ждали бы минуты.
            self._socket = await asyncio.wait_for(self._session.ws_connect(self._url, headers=headers), 10)
            await asyncio.wait_for(connect_mod.open_ws(self._req, self._socket), 10)
        except OpError as err:
            await self._done(err.message)
            return
        except (aiohttp.ClientError, asyncio.TimeoutError, OSError) as err:
            await self._done(describe(err))
            return
        sender = asyncio.ensure_future(self._pump_in())
        self._tasks.append(sender)
        try:
            while True:
                message = await asyncio.wait_for(self._socket.receive(), self._idle_s)
                if message.type not in (aiohttp.WSMsgType.TEXT, aiohttp.WSMsgType.BINARY):
                    # Устройство закрыло сокет само — это конец, а не авария.
                    await self._done(None)
                    return
                # Текстовый кадр несёт `str`, двоичный — `bytes`: вид кадра выбирает `_relay`.
                if not await self._relay(message.data):
                    return
        except asyncio.CancelledError:
            raise
        except asyncio.TimeoutError:
            await self._done("тишина в сессии")
        except (aiohttp.ClientError, OSError) as err:
            await self._done(describe(err))

    async def _pump_in(self) -> None:
        while True:
            kind, payload = await self._outgoing.get()
            try:
                if kind == "text":
                    await self._socket.send_str(payload)
                else:
                    await self._socket.send_bytes(payload)
            except (aiohttp.ClientError, OSError) as err:
                await self._done(describe(err))
                return


class HttpSession(SessionBase):
    """HTTP-ответ, который ЧИТАЕТСЯ ПО МЕРЕ ПРИХОДА, а не целиком.

    ⚠ Ради длинного опроса и потоковых ответов вендора: одиночный `connect`
    ждёт ответ целиком и режется сроком, а снаружи его к тому же обрывает
    перенос менеджера. Заголовки уезжают отдельным кадром `stream.head` ДО
    тела — иначе вызывающий не отличит «ответ 401» от «тело ещё едет».

    ⚠ Запрос собирается и шлётся ТЕМ ЖЕ кодом, что одиночный вызов
    (`connect.prepare_http`, `connect.http_send`), включая второй круг Digest.
    """

    def __init__(self, stream_id: int, req: dict[str, Any], owner: Streams, idle: float) -> None:
        self.id = stream_id
        self._req = req
        self._owner = owner
        self._idle_s = idle
        self._tasks: list[asyncio.Task[None]] = []
        self._session: aiohttp.ClientSession | None = None

    async def to_device(self, chunk: bytes) -> None:
        """У ответа нет обратного направления — молча, см. `TcpSession.to_device_text`."""

    async def to_device_text(self, text: str) -> None:
        """То же: тело запроса едет в описании, дослать в ответ нечего."""

    async def _release(self) -> None:
        if self._session is not None:
            await self._session.close()

    async def _run(self) -> None:
        try:
            call = connect_mod.prepare_http(self._req)
        except OpError as err:
            await self._done(err.message)
            return
        try:
            self._session = await connect_mod.client_for(call)
            response = await connect_mod.http_send(
                self._session, call, aiohttp.ClientTimeout(total=None, sock_read=self._idle_s)
            )
            await self._owner._send_json(
                {
                    "t": "stream.head",
                    "id": self.id,
                    "status": response.status,
                    "headers": {str(k): str(v) for k, v in response.headers.items()},
                }
            )
            async for chunk in response.content.iter_any():
                if not await self._relay(chunk):
                    return
            # Тело кончилось — это нормальный конец ответа, а не отказ.
            await self._done(None)
        except asyncio.CancelledError:
            raise
        except asyncio.TimeoutError:
            await self._done("тишина в сессии")
        except OpError as err:
            # Имя хоста не прошло проверку адреса (`connect.pinned`).
            await self._done(err.message)
        except (aiohttp.ClientError, OSError) as err:
            await self._done(describe(err))



class UdpProtocol(asyncio.DatagramProtocol):
    def __init__(self) -> None:
        self.owner: UdpSession | None = None

    def datagram_received(self, data: bytes, addr: Any) -> None:
        if self.owner is not None:
            self.owner.received(data)

    def error_received(self, exc: Exception) -> None:
        if self.owner is not None:
            self.owner.failed(exc)


class UdpSession(SessionBase):
    """UDP-сессия: кадр данных = одна датаграмма. Те же сроки и потолок, что у TCP."""

    def __init__(
        self,
        stream_id: int,
        transport: Any,
        protocol: UdpProtocol,
        owner: Streams,
        idle: float = IDLE_TIMEOUT_S,
    ) -> None:
        self.id = stream_id
        self._transport = transport
        self._owner = owner
        self._idle_s = idle
        self._overflow = False
        self._tasks: list[asyncio.Task[None]] = []
        self._idle: asyncio.TimerHandle | None = None
        # ⚠ Одна очередь и одна качалка к менеджеру, как у TCP: задача на каждую
        # датаграмму копила бы сотни тысяч объектов на потоке RTP и слала бы в
        # один сокет параллельно (ревью 2026-09-19). Полная очередь — датаграмма
        # теряется: для UDP это норма, для памяти HA — защита.
        self._outgoing: asyncio.Queue[bytes] = asyncio.Queue(QUEUE_DEPTH)
        protocol.owner = self

    def start(self) -> None:
        self._tasks = [asyncio.ensure_future(self._deadline()), asyncio.ensure_future(self._pump())]
        self._touch()

    async def to_device(self, chunk: bytes) -> None:
        if not self._stopping:
            self._transport.sendto(chunk)
            self._touch()

    async def to_device_text(self, text: str) -> None:
        """Датаграмма из текста — см. `TcpSession.to_device_text`."""
        await self.to_device(text.encode("utf-8"))

    def received(self, data: bytes) -> None:
        if self._stopping:
            return
        self._bytes += len(data)
        self._touch()
        if self._bytes > MAX_BYTES:
            # Флаг сразу: иначе каждая следующая датаграмма заводила бы ещё `_done`.
            if not self._overflow:
                self._overflow = True
                self._later(self._done("превышен потолок трафика сессии"))
            return
        try:
            self._outgoing.put_nowait(data)
        except asyncio.QueueFull:
            pass

    async def _pump(self) -> None:
        while True:
            data = await self._outgoing.get()
            await self._owner._send_bytes(frame(self.id, data))

    def failed(self, exc: Exception) -> None:
        self._later(self._done(describe(exc)))

    async def _release(self) -> None:
        if self._idle is not None:
            self._idle.cancel()
        self._transport.close()

    def _touch(self) -> None:
        # ⚠ У UDP нет «закрыли соединение»: тишина — единственный признак, что
        # сессия брошена, поэтому срок молчания обязателен и здесь.
        if self._idle is not None:
            self._idle.cancel()
        loop = asyncio.get_running_loop()
        self._idle = loop.call_later(self._idle_s, lambda: self._later(self._done("тишина в сессии")))

    def _later(self, coro: Any) -> None:
        # Только для редких событий (срок, отказ): задача живёт до своего конца.
        task = asyncio.ensure_future(coro)
        self._tasks.append(task)
        task.add_done_callback(lambda done: done in self._tasks and self._tasks.remove(done))

def describe(err: Exception) -> str:
    """Сетевая ошибка по-русски — её читает инженер, а не разработчик."""
    if isinstance(err, ConnectionRefusedError):
        return "соединение отклонено (никто не слушает)"
    if isinstance(err, asyncio.TimeoutError):
        return "таймаут"
    if isinstance(err, OSError) and err.errno in (101, 113):
        return "хост недостижим (нет маршрута)"
    return str(err) or err.__class__.__name__


# Одна из сессий: байтовый поток, датаграммы, WebSocket или HTTP-ответ.
Session = TcpSession | UdpSession | WsSession | HttpSession
