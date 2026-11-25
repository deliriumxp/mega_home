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
import socket
from typing import Any

import aiohttp

from . import connect as connect_mod
from .const import LOGGER
from .ops_base import OpError
from .stream_sessions import HEADER, IDLE_TIMEOUT_S, HttpSession, Session, TcpSession, UdpProtocol, UdpSession, WsSession, describe

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

# Сколько сессий открыто ВО ВСЁМ доме: сокетов несколько (канал менеджера и
# вкладки жильца), и потолок на сокет их не считает.
_open_total = 0


def open_total() -> int:
    """Сколько сессий открыто во всём доме — для потолка и диагностики."""
    return _open_total


def _idle_of(req: dict[str, Any]) -> float:
    """Срок молчания сессии — из описания, с потолком дома (`MAX_IDLE_S`)."""
    try:
        wanted = float(req.get("timeout"))
    except (TypeError, ValueError):
        return IDLE_TIMEOUT_S
    return min(max(wanted, 1.0), MAX_IDLE_S) if wanted > 0 else IDLE_TIMEOUT_S



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
        self._streams: dict[int, TcpSession] = {}
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
        # Обработка (`reduce.py`) — над ЦЕЛЫМ ответом, а у сессии ответа нет, есть кадры.
        error = self._refuse() or (
            "reduce — у одиночного вызова connect, не у сессии" if req.get("reduce") is not None else None
        )
        if error:
            await self._fail(stream_id, error)
            return
        # ⚠ Адрес разбирает `connect`, а не своя копия правил: частный IPv4 либо
        # имя ПОДНЯТОЙ службы дома, loopback только по имени (замок 3 плана).
        # Копия этих проверок жила здесь до 2026-09-20 и уже начала расходиться
        # — у `connect` она отвергала `0.0.0.0` и зарезервированное, здесь
        # список был свой.
        try:
            # Сессии — дорога бандла (своя дверь или пересылка менеджером): запреты бандла в силе.
            req = connect_mod.with_service(req, bundle=True)
            host, port = connect_mod.resolve_address(req)
            # Голому сокету имя — проверенным адресом (`connect.socket_target`); у ws и http это
            # делает соединитель aiohttp. UDP-сокет сессии — IPv4, как у одиночного вызова.
            if str(req.get("kind") or "tcp") in ("tcp", "udp"):
                udp = req.get("kind") == "udp"
                address, context, sni = await connect_mod.socket_target(
                    host, port, not udp and req.get("tls") is True, socket.AF_INET if udp else socket.AF_UNSPEC, payload=req
                )
        except OpError as err:
            await self._fail(stream_id, err.message)
            return
        kind = str(req.get("kind") or "tcp")
        idle = _idle_of(req)
        stream: Session
        try:
            if kind == "udp":
                loop = asyncio.get_running_loop()
                transport, protocol = await loop.create_datagram_endpoint(
                    UdpProtocol, remote_addr=(address, port)
                )
                stream = UdpSession(stream_id, transport, protocol, self, idle)
            elif kind == "ws":
                stream = WsSession(stream_id, req, host, port, self, idle)
            elif kind == "http":
                stream = HttpSession(stream_id, req, self, idle)
            elif kind == "tcp":
                # ⚠ Не `ssl=False`: у `open_connection` это значит «без TLS
                # вовсе», то есть `tls: true` молча ушёл бы открытым текстом.
                reader, writer = await asyncio.wait_for(
                    asyncio.open_connection(address, port, ssl=context, server_hostname=sni), 10
                )
                if context is not None:
                    try:
                        connect_mod.pin_socket(writer, req)
                    except OpError as err:
                        writer.close()
                        await self._fail(stream_id, err.message)
                        return
                stream = TcpSession(stream_id, reader, writer, self, idle)
            else:
                await self._fail(stream_id, f"вид «{kind}» connect не умеет")
                return
        except (asyncio.TimeoutError, OSError) as err:
            await self._fail(stream_id, describe(err))
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
        if open_total() >= TOTAL_STREAMS:
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
