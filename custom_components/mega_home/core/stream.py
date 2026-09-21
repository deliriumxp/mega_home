"""TCP-поток до устройства объекта по каналу менеджера.

Зачем. Инсталлятору нужно попасть в веб-интерфейс или SSH устройства, стоящего в
локалке клиента, — функция, которая у OvrC (Web Connect) и Domotz спасает объект
чаще всего. Проба (`probe.py`) для этого не годится: она один вопрос и один
ответ, а тут СЕССИЯ — два направления, тысячи кадров, живёт минутами.

⚠ Здесь нет ни HTTP, ни SSH, ни знания о том, что за устройство на том конце:
только байты туда и обратно. Разбор протокола живёт в менеджере и меняется его
деплоем — иначе каждая новая железка на объекте стоила бы релиза HACS с
перезапуском Home Assistant (docs/plan-thin-integration.md в менеджере).

⚠ Кто зовёт: менеджер живым каналом (объект опознан своим токеном) И приложение
жильца — формой Upgrade того же `api/connect` (`http.py`). Снаружи она идёт
переносом менеджера и закрыта сессией жильца, внутри дома — без аутентификации,
как и всё в локальном контуре (решение заказчика 2026-09-20).

⚠ Здесь стояло «локальной двери у этого нет и быть не должно — она стала бы
проходным двором в LAN объекта». Довод УСТАРЕЛ с приходом `connect`: локальный
`POST api/connect` уже ходит без аутентификации на любой частный адрес объекта,
то есть досягаемость у локального контура ровно та же. Удержание добавляет
длительность, а не досягаемость, и ограничивают её потолки ниже
(`MAX_STREAMS`/`TOTAL_STREAMS`/`MAX_BYTES`/сроки), а не отсутствие двери.

Кадры канала:
  управление — JSON: `stream.open` {id, req: ConnectRequest} → `stream.ok` |
  `stream.error`, `stream.close` {id, error?} в обе стороны, `stream.data`
  {id, text} — текстовый кадр WebSocket в обе стороны, `stream.head`
  {id, status, headers} — заголовки ответа у вида `http`, один раз до тела;
  данные — БИНАРНЫЕ: 4 байта номера потока (big-endian) + байты как есть.
Бинарные, а не base64 в JSON: треть лишнего объёма и лишние проходы по каждому
килобайту веб-морды — это заметно уже на одной странице с картинками.

⚠ Описание сессии — ТА ЖЕ форма `ConnectRequest`, что у одиночного вызова, и
разбирается ТЕМ ЖЕ кодом (`connect.resolve_address`, `connect.prepare_http`).
Свой разбор здесь разошёлся бы с ним на первой правке: «Digest работает в
вызове и не работает в сессии».
"""

from __future__ import annotations

import asyncio
import ssl
import struct
from typing import Any

import aiohttp

from . import connect as connect_mod, digest
from .const import LOGGER
from .ops_base import OpError

# ⚠ Службы САМОГО дома, до которых менеджер вправе открыть поток по loopback,
# больше не список констант — их держит реестр `services.py` (замок 3 плана
# `docs/plan-thin-gateway.md`): служба слушает только loopback ровно потому,
# что вход в неё — этот канал, и должна быть ПОДНЯТА именно сейчас.

# Сколько сессий разом на объект.
#
# ⚠ Число считано НЕ из «сколько устройств смотрит инженер», а из того, как
# устроен браузер: он держит около шести параллельных соединений на origin, и
# каждое — отдельная сессия к устройству (`session-server.ts` в менеджере).
# Четырёх, стоявших здесь в 0.2.33, не хватило бы даже на одну страницу морды с
# картинками: часть запросов молча вставала бы в очередь до таймаута. Шестнадцать
# — это страница плюс запас на вторую вкладку, но всё ещё не обход сети.
MAX_STREAMS = 16
# Сессий на ВЕСЬ дом, поверх потолка на сокет.
#
# ⚠ Сокетов теперь несколько: канал менеджера и по одному на каждую открытую
# вкладку жильца (`http.py`, форма Upgrade). Потолок «на сокет» их не считает —
# без общего потолка вкладки без счёта превратили бы дом в обход сети.
#
# ⚠⚠ Число — от САМОГО ТЯЖЁЛОГО ДОМА (решение заказчика 2026-09-21): до 5
# телефонов и до 5 настенных мониторов, каждому до 8 сессий (до 5 живых плиток
# — по сессии на поток go2rtc, камера во весь экран, подписка архива и запас) —
# 80, и ещё 16 — канал менеджера (инженер в «Доступе»). Прежние 32 были
# потолком «чтобы был», не считанным против приложения жильца: объект упёрся в
# него, и архив перестал открываться у всех. Обычный дом (1–2 монитора, 1–2
# телефона) занимает 10–30.
TOTAL_STREAMS = 96
# Предел срока молчания, который вправе попросить вызывающий (`req.timeout`).
#
# ⚠ Умолчание (`IDLE_TIMEOUT_S`) короткое, потому что брошенная вкладка молчит
# так же, как живая подписка. Но подписка на события регистратора законно
# молчит минутами, и резать её своим умолчанием значит вернуть частый опрос —
# поэтому срок берётся ИЗ ОПИСАНИЯ, а здесь только потолок.
MAX_IDLE_S = 600.0
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

# ⚠ Сертификат устройства объекта самоподписанный, проверять его нечем — тот же
# довод, что у одиночного вызова (`connect.prepare_http`): адреса пускает только
# частная сеть объекта. ⚠ Не `ssl=False`: у `open_connection` это значит «без
# TLS вовсе», то есть `tls: true` молча ушёл бы открытым текстом.
_TLS = ssl.create_default_context()
_TLS.check_hostname = False
_TLS.verify_mode = ssl.CERT_NONE

# Сколько сессий открыто ВО ВСЁМ доме: сокетов несколько (канал менеджера и
# вкладки жильца), и потолок на сокет их не считает.
_open_total = 0


def _total() -> int:
    return _open_total


def _idle_of(req: dict[str, Any]) -> float:
    """Срок молчания сессии — из описания, с потолком дома (`MAX_IDLE_S`)."""
    try:
        wanted = float(req.get("timeout"))
    except (TypeError, ValueError):
        return IDLE_TIMEOUT_S
    return min(max(wanted, 1.0), MAX_IDLE_S) if wanted > 0 else IDLE_TIMEOUT_S


def frame(stream_id: int, payload: bytes) -> bytes:
    """Бинарный кадр данных: номер потока + байты."""
    return HEADER.pack(stream_id) + payload


async def serve(socket: Any) -> None:
    """Держать сессии одного сокета: читать кадры, пока он жив, и прибрать за ним.

    ⚠ Для ЛОКАЛЬНОЙ двери (`http.py`, форма Upgrade). Канал менеджера крутит
    свой цикл сам (`link.py`): там по тому же сокету ездят ещё и `hello`,
    запросы жильца и события, и общий цикл пришлось бы учить чужим кадрам.
    """
    streams = Streams(socket)
    try:
        async for message in socket:
            if message.type is aiohttp.WSMsgType.TEXT:
                await streams.dispatch(message.json())
            elif message.type is aiohttp.WSMsgType.BINARY:
                await streams.on_binary(message.data)
            elif message.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                break
    finally:
        # Вкладку закрыли (или телефон уснул и сокет порвался по пропущенному
        # heartbeat) — соединения к устройствам закрываются здесь же.
        await streams.close_all()


class Streams:
    """Все открытые потоки ОДНОГО подключения к менеджеру.

    ⚠ Живут ровно столько, сколько живёт канал: оборвался — все закрыты. Второй
    жизни у сессии нет намеренно, менеджер откроет заново. Иначе пришлось бы
    восстанавливать состояние TCP-соединения, которого уже нет.
    """

    def __init__(self, socket: Any) -> None:
        self._socket = socket
        self._streams: dict[int, _Stream] = {}
        # Открытия, которые ещё идут (см. `dispatch`), и номера тех из них,
        # которые вызывающий успел закрыть, не дождавшись соединения.
        self._opening_tasks: set[asyncio.Task[None]] = set()
        self._opening_ids: set[int] = set()
        self._abandoned: set[int] = set()

    async def dispatch(self, payload: dict[str, Any]) -> bool:
        """Управляющий кадр ИЗ ЦИКЛА ЧТЕНИЯ сокета. `True` — кадр наш.

        ⚠ `stream.open` соединяется с устройством, и до адреса за упавшим
        коммутатором TCP молчит все 10 с срока. Ждать это в цикле чтения канала
        значит не читать ни одного кадра: запросы жильца отваливаются по
        трёхсекундному сроку менеджера, и «инженер открыл сеанс к мёртвой
        камере» превращается в «дом не на связи» для всех остальных. Поэтому
        открытие уходит отдельной задачей — тем же приёмом, что ответы на
        запросы (`link.py::_handle`). Остальные кадры быстрые и идут по месту;
        закрытие, обогнавшее открытие, помнит `_close` (см. `_abandoned`).
        """
        if payload.get("t") == "stream.open":
            # Номер помечаем ЗДЕСЬ, синхронно: задача стартует на следующем
            # обороте цикла, а закрытие тем же кадром может прийти раньше.
            stream_id = payload.get("id")
            if isinstance(stream_id, int):
                self._opening_ids.add(stream_id)
            task = asyncio.ensure_future(self._open(payload))
            self._opening_tasks.add(task)
            task.add_done_callback(self._opening_tasks.discard)
            return True
        return await self.handle(payload)

    async def handle(self, payload: dict[str, Any]) -> bool:
        """Управляющий кадр. `True` — кадр наш и обработан.

        Открытие здесь ЖДЁТСЯ — форма для того, кому нужен результат по месту
        (тесты); цикл чтения сокета зовёт `dispatch`.
        """
        kind = payload.get("t")
        if kind == "stream.open":
            await self._open(payload)
            return True
        if kind == "stream.close":
            await self._close(payload.get("id"))
            return True
        if kind == "stream.data":
            # Текстовый кадр WebSocket в сторону устройства. Бинарный ездит
            # бинарным кадром (`on_binary`) — текст в него не заворачиваем:
            # у WebSocket это РАЗНЫЕ типы кадров, и устройство их различает.
            stream = self._streams.get(payload.get("id"))
            text = payload.get("text")
            if stream is not None and isinstance(text, str):
                await stream.to_device_text(text)
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
        global _open_total
        # Сначала — незаконченные открытия: соединение, поднявшееся после
        # уборки, некому было бы закрыть. Между установкой соединения и записью
        # в реестр (`_connect`) нет ни одного `await`, поэтому отмена не
        # оставляет полуоткрытого сокета.
        for task in list(self._opening_tasks):
            task.cancel()
        self._opening_tasks.clear()
        self._opening_ids.clear()
        self._abandoned.clear()
        # ⚠⚠ Счёт — ДО первого `await`, закрытие — под `shield`. Home Assistant
        # отменяет обработчик запроса, когда клиент рвёт соединение
        # (`handler_cancellation=True`, `components/http/server.py`), и при
        # ШТАТНОМ закрытии вкладки отмена прилетает сюда, посреди закрытия
        # первой сессии. Пока вычитание стояло после цикла, отмена его
        # пропускала, и сессии закрытой вкладки навсегда оставались в общем
        # счёте дома: объект 2026-09-21 дошёл так до «дом держит уже 32
        # соединений» при пустом доме, и лечил это только перезапуск HA
        # (воспроизведено: четыре вкладки по две сессии → счёт 8 вместо 0).
        streams = list(self._streams.values())
        self._streams.clear()
        _open_total -= len(streams)
        if streams:
            # Соединения к устройствам закрываем до конца, даже если нас
            # отменили: иначе они висели бы до тишины, держа устройство.
            await asyncio.shield(
                asyncio.gather(*(stream.stop(None) for stream in streams), return_exceptions=True)
            )

    async def _open(self, payload: dict[str, Any]) -> None:
        stream_id = payload.get("id")
        if not isinstance(stream_id, int):
            return
        self._opening_ids.add(stream_id)
        try:
            await self._connect(stream_id, payload)
        finally:
            self._opening_ids.discard(stream_id)
            self._abandoned.discard(stream_id)

    async def _connect(self, stream_id: int, payload: dict[str, Any]) -> None:
        req = payload.get("req")
        if not isinstance(req, dict):
            await self._fail(stream_id, "описание сессии — объект req (ConnectRequest)")
            return
        error = self._refuse()
        if error:
            await self._fail(stream_id, error)
            return
        # ⚠ Адрес разбирает `connect`, а не своя копия правил: частный IPv4 либо
        # имя ПОДНЯТОЙ службы дома, loopback только по имени (замок 3 плана).
        # Копия этих проверок жила здесь до 2026-09-20 и уже начала расходиться
        # — у `connect` она отвергала `0.0.0.0` и зарезервированное, здесь
        # список был свой.
        try:
            host, port = connect_mod.resolve_address(req)
        except OpError as err:
            await self._fail(stream_id, err.message)
            return
        kind = str(req.get("kind") or "tcp")
        idle = _idle_of(req)
        stream: _Session
        try:
            if kind == "udp":
                loop = asyncio.get_running_loop()
                transport, protocol = await loop.create_datagram_endpoint(
                    _DatagramProtocol, remote_addr=(host, port)
                )
                stream = _Datagrams(stream_id, transport, protocol, self, idle)
            elif kind == "ws":
                stream = _WsStream(stream_id, req, host, port, self, idle)
            elif kind == "http":
                stream = _HttpStream(stream_id, req, self, idle)
            elif kind == "tcp":
                reader, writer = await asyncio.wait_for(
                    asyncio.open_connection(
                        host, port, ssl=_TLS if req.get("tls") is True else None
                    ),
                    10,
                )
                stream = _Stream(stream_id, reader, writer, self, idle)
            else:
                await self._fail(stream_id, f"вид «{kind}» connect не умеет")
                return
        except (asyncio.TimeoutError, OSError) as err:
            await self._fail(stream_id, _describe(err))
            return
        if stream_id in self._abandoned:
            # Менеджер закрыл сессию, не дождавшись соединения (браузер ушёл,
            # срок открытия у менеджера вышел). Подтверждать нечего — он её уже
            # забыл; соединение гасим сразу, иначе оно жило бы до срока
            # молчания, занимая место в потолке.
            await stream.stop(None)
            return
        global _open_total
        self._streams[stream_id] = stream
        _open_total += 1
        # ⚠ `stream.ok` уезжает ДО старта качалок: у видов `ws` и `http`
        # соединение поднимает сама сессия, и первый кадр от устройства не
        # должен обогнать подтверждение открытия.
        await self._send_json({"t": "stream.ok", "id": stream_id})
        stream.start()
        LOGGER.info("Сессия %s открыта: %s %s:%s", stream_id, kind, host, port)

    async def _fail(self, stream_id: int, error: str) -> None:
        await self._send_json({"t": "stream.error", "id": stream_id, "error": error})

    def _refuse(self) -> str | None:
        """Почему открывать нельзя. `None` — можно.

        ⚠ Остались только ПОТОЛКИ: адрес проверяет `connect.resolve_address`
        (см. `_open`). Потолков два — на сокет и на дом: сокетов теперь
        несколько (канал менеджера и вкладки жильца), и первый второй не
        заменяет.
        """
        # Идущие открытия — тоже в счёт: они идут задачами (`dispatch`), и без
        # этого шесть одновременных запросов браузера обходили бы потолок.
        if len(self._streams) + len(self._opening_ids) > MAX_STREAMS:
            return f"на объекте уже {MAX_STREAMS} открытых сессии"
        if _total() >= TOTAL_STREAMS:
            return f"дом держит уже {TOTAL_STREAMS} соединений"
        return None

    async def _close(self, stream_id: Any) -> None:
        global _open_total
        stream = (
            self._streams.pop(stream_id, None) if isinstance(stream_id, int) else None
        )
        if stream is not None:
            _open_total -= 1
            await stream.stop(None)
        elif stream_id in self._opening_ids:
            # Закрытие обогнало открытие (оно идёт задачей, см. `dispatch`):
            # соединение ещё поднимается, и погасить его сможет только `_connect`.
            self._abandoned.add(stream_id)

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
        """Сессия кончилась со стороны устройства — сказать вызывающему."""
        global _open_total
        if self._streams.pop(stream_id, None) is None:
            return
        _open_total -= 1
        payload: dict[str, Any] = {"t": "stream.close", "id": stream_id}
        if error:
            payload["error"] = error
        await self._send_json(payload)


class _SessionBase:
    """Общее у всех видов сессии: срок жизни и конец с уведомлением владельца.

    Наследник задаёт `id`, `_owner` и `stop(error)`.
    """

    async def _deadline(self) -> None:
        await asyncio.sleep(MAX_LIFETIME_S)
        await self._done("истёк срок сессии")

    async def _done(self, error: str | None) -> None:
        # ⚠ Сообщаем ВЛАДЕЛЬЦУ, а не закрываемся сами: реестр знает только он, и
        # забытая в нём сессия занимала бы место до конца связи.
        await self._owner._finished(self.id, error)
        await self.stop(error)


class _Stream(_SessionBase):
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

    async def to_device_text(self, text: str) -> None:
        """Текстовый кадр — понятие WebSocket; у байтового потока его нет.

        ⚠ Молча, а не отказом: вызывающий мог перепутать вид сессии, но рвать
        из-за этого живое соединение к устройству — хуже, чем не доставить
        кадр, которого тут быть не должно.
        """
        await self.to_device(text.encode("utf-8"))

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
                chunk = await asyncio.wait_for(self._reader.read(CHUNK), self._idle_s)
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


class _WsStream(_SessionBase):
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
        self._owner = owner
        self._idle_s = idle
        self._outgoing: asyncio.Queue[tuple[str, Any]] = asyncio.Queue(QUEUE_DEPTH)
        self._tasks: list[asyncio.Task[None]] = []
        self._session: aiohttp.ClientSession | None = None
        self._socket: Any = None
        self._bytes = 0
        self._stopping = False

    def start(self) -> None:
        self._tasks = [asyncio.ensure_future(self._run()), asyncio.ensure_future(self._deadline())]

    async def to_device(self, chunk: bytes) -> None:
        if not self._stopping:
            await self._outgoing.put(("bin", chunk))

    async def to_device_text(self, text: str) -> None:
        if not self._stopping:
            await self._outgoing.put(("text", text))

    async def stop(self, error: str | None) -> None:
        if self._stopping:
            return
        self._stopping = True
        for task in self._tasks:
            if task is not asyncio.current_task():
                task.cancel()
        if self._socket is not None:
            await self._socket.close()
        if self._session is not None:
            await self._session.close()
        if error:
            LOGGER.info("Сессия %s закрыта: %s", self.id, error)

    async def _run(self) -> None:
        headers = self._req.get("headers") if isinstance(self._req.get("headers"), dict) else None
        try:
            connector = aiohttp.TCPConnector(ssl=False) if self._req.get("tls") is True else None
            self._session = aiohttp.ClientSession(connector=connector)
            self._socket = await self._session.ws_connect(self._url, headers=headers, timeout=10)
        except (aiohttp.ClientError, asyncio.TimeoutError, OSError) as err:
            await self._done(_describe(err))
            return
        sender = asyncio.ensure_future(self._pump_in())
        self._tasks.append(sender)
        try:
            while True:
                message = await asyncio.wait_for(self._socket.receive(), self._idle_s)
                if message.type is aiohttp.WSMsgType.TEXT:
                    await self._owner._send_json(
                        {"t": "stream.data", "id": self.id, "text": message.data}
                    )
                elif message.type is aiohttp.WSMsgType.BINARY:
                    self._bytes += len(message.data)
                    if self._bytes > MAX_BYTES:
                        await self._done("превышен потолок трафика сессии")
                        return
                    await self._owner._send_bytes(frame(self.id, message.data))
                else:
                    # Устройство закрыло сокет само — это конец, а не авария.
                    await self._done(None)
                    return
        except asyncio.CancelledError:
            raise
        except asyncio.TimeoutError:
            await self._done("тишина в сессии")
        except (aiohttp.ClientError, OSError) as err:
            await self._done(_describe(err))

    async def _pump_in(self) -> None:
        while True:
            kind, payload = await self._outgoing.get()
            try:
                if kind == "text":
                    await self._socket.send_str(payload)
                else:
                    await self._socket.send_bytes(payload)
            except (aiohttp.ClientError, OSError) as err:
                await self._done(_describe(err))
                return


class _HttpStream(_SessionBase):
    """HTTP-ответ, который ЧИТАЕТСЯ ПО МЕРЕ ПРИХОДА, а не целиком.

    ⚠ Ради длинного опроса и потоковых ответов вендора: одиночный `connect`
    ждёт ответ целиком и режется сроком, а снаружи его к тому же обрывает
    перенос менеджера. Заголовки уезжают отдельным кадром `stream.head` ДО
    тела — иначе вызывающий не отличит «ответ 401» от «тело ещё едет».

    ⚠ Запрос собирается ТЕМ ЖЕ `connect.prepare_http`, включая второй круг
    Digest: разбор описания у форм один.
    """

    def __init__(self, stream_id: int, req: dict[str, Any], owner: Streams, idle: float) -> None:
        self.id = stream_id
        self._req = req
        self._owner = owner
        self._idle_s = idle
        self._tasks: list[asyncio.Task[None]] = []
        self._session: aiohttp.ClientSession | None = None
        self._bytes = 0
        self._stopping = False

    def start(self) -> None:
        self._tasks = [asyncio.ensure_future(self._run()), asyncio.ensure_future(self._deadline())]

    async def to_device(self, chunk: bytes) -> None:
        """У ответа нет обратного направления — молча, см. `_Stream.to_device_text`."""

    async def to_device_text(self, text: str) -> None:
        """То же: тело запроса едет в описании, дослать в ответ нечего."""

    async def stop(self, error: str | None) -> None:
        if self._stopping:
            return
        self._stopping = True
        for task in self._tasks:
            if task is not asyncio.current_task():
                task.cancel()
        if self._session is not None:
            await self._session.close()
        if error:
            LOGGER.info("Сессия %s закрыта: %s", self.id, error)

    async def _run(self) -> None:
        try:
            call = connect_mod.prepare_http(self._req)
        except OpError as err:
            await self._done(err.message)
            return
        try:
            self._session = aiohttp.ClientSession(connector=call["connector"])
            response = await self._open_response(call)
            await self._owner._send_json(
                {
                    "t": "stream.head",
                    "id": self.id,
                    "status": response.status,
                    "headers": {str(k): str(v) for k, v in response.headers.items()},
                }
            )
            async for chunk in response.content.iter_any():
                self._bytes += len(chunk)
                if self._bytes > MAX_BYTES:
                    await self._done("превышен потолок трафика сессии")
                    return
                await self._owner._send_bytes(frame(self.id, chunk))
            # Тело кончилось — это нормальный конец ответа, а не отказ.
            await self._done(None)
        except asyncio.CancelledError:
            raise
        except asyncio.TimeoutError:
            await self._done("тишина в сессии")
        except (aiohttp.ClientError, OSError) as err:
            await self._done(_describe(err))

    async def _open_response(self, call: dict[str, Any]) -> Any:
        """Запрос и, если устройство просит Digest, второй круг — как у `_http`."""
        assert self._session is not None
        timeout = aiohttp.ClientTimeout(total=None, sock_read=self._idle_s)
        response = await self._session.request(
            call["method"],
            call["url"],
            data=call["body"] or None,
            headers=call["headers"],
            auth=call["basic"],
            allow_redirects=False,
            timeout=timeout,
        )
        if call["digest"] and response.status == 401:
            asked = next(
                (v for k, v in response.headers.items() if k.lower() == "www-authenticate"), ""
            )
            try:
                params = digest.parse_www_auth(asked)
            except ValueError:
                params = None
            if params:
                response.close()
                signed = dict(call["headers"])
                signed["Authorization"] = digest.authorization(
                    call["method"], call["path"], call["digest"][0], call["digest"][1], params
                )
                response = await self._session.request(
                    call["method"],
                    call["url"],
                    data=call["body"] or None,
                    headers=signed,
                    allow_redirects=False,
                    timeout=timeout,
                )
        return response


class _DatagramProtocol(asyncio.DatagramProtocol):
    def __init__(self) -> None:
        self.owner: _Datagrams | None = None

    def datagram_received(self, data: bytes, addr: Any) -> None:
        if self.owner is not None:
            self.owner.received(data)

    def error_received(self, exc: Exception) -> None:
        if self.owner is not None:
            self.owner.failed(exc)


class _Datagrams(_SessionBase):
    """UDP-сессия: кадр данных = одна датаграмма. Те же сроки и потолок, что у TCP."""

    def __init__(
        self,
        stream_id: int,
        transport: Any,
        protocol: _DatagramProtocol,
        owner: Streams,
        idle: float = IDLE_TIMEOUT_S,
    ) -> None:
        self.id = stream_id
        self._transport = transport
        self._owner = owner
        self._idle_s = idle
        self._bytes = 0
        self._stopping = False
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
        """Датаграмма из текста — см. `_Stream.to_device_text`."""
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
        self._later(self._done(_describe(exc)))

    async def stop(self, error: str | None) -> None:
        if self._stopping:
            return
        self._stopping = True
        for task in self._tasks:
            # ⚠ Не свою: закрытие по потолку приходит из задачи, живущей в этом
            # же списке (`_later`), и отменить себя значило бы не дойти до конца.
            if task is not asyncio.current_task():
                task.cancel()
        if self._idle is not None:
            self._idle.cancel()
        self._transport.close()
        if error:
            LOGGER.info("Сессия %s закрыта: %s", self.id, error)

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

def _describe(err: Exception) -> str:
    """Сетевая ошибка по-русски — её читает инженер, а не разработчик."""
    if isinstance(err, ConnectionRefusedError):
        return "соединение отклонено (никто не слушает)"
    if isinstance(err, asyncio.TimeoutError):
        return "таймаут"
    if isinstance(err, OSError) and err.errno in (101, 113):
        return "хост недостижим (нет маршрута)"
    return str(err) or err.__class__.__name__


# Одна из сессий: байтовый поток, датаграммы, WebSocket или HTTP-ответ.
_Session = _Stream | _Datagrams | _WsStream | _HttpStream
