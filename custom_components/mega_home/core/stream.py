"""TCP-поток до устройства объекта по каналу менеджера.

Зачем. Инсталлятору нужно попасть в веб-интерфейс или SSH устройства, стоящего в
локалке клиента, — функция, которая у OvrC (Web Connect) и Domotz спасает объект
чаще всего. Проба (`probe.py`) для этого не годится: она один вопрос и один
ответ, а тут СЕССИЯ — два направления, тысячи кадров, живёт минутами.

⚠ Здесь нет ни HTTP, ни SSH, ни знания о том, что за устройство на том конце:
только байты туда и обратно. Разбор протокола живёт в менеджере и меняется его
деплоем — иначе каждая новая железка на объекте стоила бы релиза HACS с
перезапуском Home Assistant (docs/plan-thin-integration.md в менеджере).

⚠ Кто зовёт: ТОЛЬКО менеджер живым каналом, где объект опознан своим токеном.
Локальной HTTP-двери у этого нет и быть не должно — контур дома без
аутентификации, и такая дверь стала бы проходным двором в LAN объекта для
любого, кто в его Wi-Fi.

Кадры канала:
  управление — JSON: `stream.open` {id, host, port} → `stream.ok` | `stream.error`,
  `stream.close` {id, error?} в обе стороны;
  данные — БИНАРНЫЕ: 4 байта номера потока (big-endian) + байты как есть.
Бинарные, а не base64 в JSON: треть лишнего объёма и лишние проходы по каждому
килобайту веб-морды — это заметно уже на одной странице с картинками.
"""

from __future__ import annotations

import asyncio
import ipaddress
import struct
from typing import Any

from .const import LOGGER

# Сколько сессий разом на объект.
#
# ⚠ Число считано НЕ из «сколько устройств смотрит инженер», а из того, как
# устроен браузер: он держит около шести параллельных соединений на origin, и
# каждое — отдельная сессия к устройству (`session-server.ts` в менеджере).
# Четырёх, стоявших здесь в 0.2.33, не хватило бы даже на одну страницу морды с
# картинками: часть запросов молча вставала бы в очередь до таймаута. Шестнадцать
# — это страница плюс запас на вторую вкладку, но всё ещё не обход сети.
MAX_STREAMS = 16
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


class Streams:
    """Все открытые потоки ОДНОГО подключения к менеджеру.

    ⚠ Живут ровно столько, сколько живёт канал: оборвался — все закрыты. Второй
    жизни у сессии нет намеренно, менеджер откроет заново. Иначе пришлось бы
    восстанавливать состояние TCP-соединения, которого уже нет.
    """

    def __init__(self, socket: Any) -> None:
        self._socket = socket
        self._streams: dict[int, _Stream] = {}

    async def handle(self, payload: dict[str, Any]) -> bool:
        """Управляющий кадр. `True` — кадр наш и обработан."""
        kind = payload.get("t")
        if kind == "stream.open":
            await self._open(payload)
            return True
        if kind == "stream.close":
            await self._close(payload.get("id"))
            return True
        return False

    async def on_binary(self, data: bytes) -> None:
        """Данные от менеджера в сторону устройства."""
        if len(data) < HEADER.size:
            return
        (stream_id,) = HEADER.unpack_from(data)
        stream = self._streams.get(stream_id)
        if stream is None:
            # Поток уже закрыт, а кадры ещё летят — норма на разрыве, молчим.
            return
        await stream.to_device(data[HEADER.size :])

    async def close_all(self) -> None:
        for stream in list(self._streams.values()):
            await stream.stop(None)
        self._streams.clear()

    async def _open(self, payload: dict[str, Any]) -> None:
        stream_id = payload.get("id")
        if not isinstance(stream_id, int):
            return
        error = self._refuse(payload)
        if error:
            await self._send_json({"t": "stream.error", "id": stream_id, "error": error})
            return
        host = str(payload.get("host"))
        port = int(payload.get("port"))
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(host, port), 10
            )
        except (asyncio.TimeoutError, OSError) as err:
            await self._send_json(
                {"t": "stream.error", "id": stream_id, "error": _describe(err)}
            )
            return
        stream = _Stream(stream_id, reader, writer, self)
        self._streams[stream_id] = stream
        await self._send_json({"t": "stream.ok", "id": stream_id})
        stream.start()
        LOGGER.info("Сессия %s открыта: %s:%s", stream_id, host, port)

    def _refuse(self, payload: dict[str, Any]) -> str | None:
        """Почему открывать нельзя. `None` — можно.

        ⚠ Только литеральный ПРИВАТНЫЙ адрес. Имя пришлось бы резолвить, а
        резолвер дома смотрит и в интернет — так дверь в локалку объекта стала бы
        заодно анонимным выходом в сеть с его адреса. Устройства менеджер и так
        знает по скану и называет их адресами.
        """
        if len(self._streams) >= MAX_STREAMS:
            return f"на объекте уже {MAX_STREAMS} открытых сессии"
        port = payload.get("port")
        if not isinstance(port, int) or not 1 <= port <= 65535:
            return "порт не указан"
        try:
            address = ipaddress.ip_address(str(payload.get("host")))
        except ValueError:
            return "адрес устройства должен быть IP, а не именем"
        if not address.is_private or address.is_loopback:
            return "адрес вне локальной сети объекта"
        return None

    async def _close(self, stream_id: Any) -> None:
        stream = (
            self._streams.pop(stream_id, None) if isinstance(stream_id, int) else None
        )
        if stream is not None:
            await stream.stop(None)

    async def _send_json(self, payload: dict[str, Any]) -> None:
        try:
            await self._socket.send_json(payload)
        except Exception as err:  # noqa: BLE001 — канал переподключится сам
            LOGGER.debug("Кадр сессии не ушёл: %s", err)

    async def _send_bytes(self, payload: bytes) -> None:
        try:
            await self._socket.send_bytes(payload)
        except Exception as err:  # noqa: BLE001
            LOGGER.debug("Данные сессии не ушли: %s", err)

    async def _finished(self, stream_id: int, error: str | None) -> None:
        """Сессия кончилась со стороны устройства — сказать менеджеру."""
        if self._streams.pop(stream_id, None) is None:
            return
        payload: dict[str, Any] = {"t": "stream.close", "id": stream_id}
        if error:
            payload["error"] = error
        await self._send_json(payload)


class _Stream:
    """Одно TCP-соединение и две его качалки."""

    def __init__(
        self,
        stream_id: int,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        owner: Streams,
    ) -> None:
        self.id = stream_id
        self._reader = reader
        self._writer = writer
        self._owner = owner
        self._queue: asyncio.Queue[bytes] = asyncio.Queue(QUEUE_DEPTH)
        self._tasks: list[asyncio.Task[None]] = []
        self._bytes = 0
        self._stopping = False

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

    async def stop(self, error: str | None) -> None:
        if self._stopping:
            return
        self._stopping = True
        for task in self._tasks:
            task.cancel()
        self._writer.close()
        try:
            await asyncio.wait_for(self._writer.wait_closed(), 2)
        except (asyncio.TimeoutError, OSError):
            pass
        if error:
            LOGGER.info("Сессия %s закрыта: %s", self.id, error)

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
            await self._done(_describe(err))

    async def _pump_out(self) -> None:
        """Устройство → менеджер."""
        try:
            while True:
                chunk = await asyncio.wait_for(self._reader.read(CHUNK), IDLE_TIMEOUT_S)
                if not chunk:
                    await self._done(None)
                    return
                self._bytes += len(chunk)
                if self._bytes > MAX_BYTES:
                    await self._done("превышен потолок трафика сессии")
                    return
                await self._owner._send_bytes(frame(self.id, chunk))
        except asyncio.CancelledError:
            raise
        except asyncio.TimeoutError:
            await self._done("тишина в сессии")
        except OSError as err:
            await self._done(_describe(err))

    async def _deadline(self) -> None:
        await asyncio.sleep(MAX_LIFETIME_S)
        await self._done("истёк срок сессии")

    async def _done(self, error: str | None) -> None:
        # ⚠ Сообщаем ВЛАДЕЛЬЦУ, а не закрываемся сами: реестр знает только он, и
        # забытая в нём сессия занимала бы место до конца связи.
        await self._owner._finished(self.id, error)
        await self.stop(error)


def _describe(err: Exception) -> str:
    """Сетевая ошибка по-русски — её читает инженер, а не разработчик."""
    if isinstance(err, ConnectionRefusedError):
        return "соединение отклонено (никто не слушает)"
    if isinstance(err, asyncio.TimeoutError):
        return "таймаут"
    if isinstance(err, OSError) and err.errno in (101, 113):
        return "хост недостижим (нет маршрута)"
    return str(err) or err.__class__.__name__
