"""Live link to the manager: "the config changed, come and get it"."""

from __future__ import annotations

import asyncio
from typing import Any

import aiohttp

from homeassistant.core import HomeAssistant

from . import ops
from .const import CONF_MANAGER_URL, CONF_TOKEN, CONF_VERIFY_SSL, LOGGER
from .coordinator import MegaHomeConfigEntry, MegaHomeCoordinator

WS_PATH = "/inbound/home"
# Reconnect backoff. An object may be offline for good, so a dead link is an
# ordinary state: back off to a minute and stay quiet rather than retry-storm.
FIRST_RETRY = 5
MAX_RETRY = 60
# Our own keepalive on top of the server's ping: a NAT that silently drops an
# idle connection is the classic way a link looks alive and delivers nothing.
HEARTBEAT = 30


class ManagerLink:
    """Holds one outgoing WebSocket to the manager and refreshes on its nudge.

    ⚠ The socket is OUTGOING — the object dials the cloud, exactly like the
    MegaDriver link does. That is the whole reason this works on a normal home
    connection: no public IP, no port forwarding, no hole in the perimeter. The
    manager pushing *into* the house would need reachability, which is a
    different (later) problem.

    Two kinds of traffic ride it:

    * the manager's nudge ("the config moved", "a new bundle is out"). The
      config itself is still fetched over the same HTTPS endpoint the poll uses,
      so exactly one code path updates the cache — and the poll stays as the
      safety net for when this link is down;
    * requests from a resident who is AWAY from home. Their phone talks to the
      manager, the manager forwards the question here, and this house answers it
      (remote-access.md in the manager repo). ⚠ The answer is produced by the
      same `ops` module the local HTTP views use — the resident must get the
      same data and the same refusal wording wherever they are.
    """

    def __init__(
        self,
        hass: HomeAssistant,
        entry: MegaHomeConfigEntry,
        coordinator: MegaHomeCoordinator,
    ) -> None:
        self._hass = hass
        self._entry = entry
        self._coordinator = coordinator
        self._connected = False
        self._task: asyncio.Task[None] | None = None
        # Фоновая работа канала: ответы на запросы жильца и скачивание бандла по
        # подсказке (см. `_handle`) — всё, что нельзя делать в цикле чтения
        # сокета. Ссылку держим здесь: задача без ссылки может быть собрана
        # сборщиком мусора на полпути.
        self._answers: set[asyncio.Task[None]] = set()

    @property
    def connected(self) -> bool:
        """Whether the link is up right now."""
        return self._connected

    def start(self) -> None:
        """Run the link for as long as the config entry is loaded."""
        self._task = self._entry.async_create_background_task(
            self._hass, self._run(), "mega_home_link", eager_start=False
        )

    async def async_stop(self) -> None:
        """Stop the link (the background task dies with the entry anyway)."""
        if self._task:
            self._task.cancel()
            self._task = None
        for task in list(self._answers):
            task.cancel()
        self._answers.clear()

    async def _run(self) -> None:
        from homeassistant.helpers.aiohttp_client import async_get_clientsession

        session = async_get_clientsession(
            self._hass, self._entry.data.get(CONF_VERIFY_SSL, True)
        )
        url = _ws_url(self._entry.data[CONF_MANAGER_URL])
        headers = {"Authorization": f"Bearer {self._entry.data[CONF_TOKEN]}"}
        delay = FIRST_RETRY
        while True:
            try:
                async with session.ws_connect(
                    url, headers=headers, heartbeat=HEARTBEAT
                ) as socket:
                    self._connected = True
                    delay = FIRST_RETRY
                    LOGGER.info("Manager link is up")
                    await socket.send_json(await self._hello())
                    async for message in socket:
                        if message.type is aiohttp.WSMsgType.TEXT:
                            await self._handle(message.json(), socket)
                        elif message.type in (
                            aiohttp.WSMsgType.CLOSED,
                            aiohttp.WSMsgType.ERROR,
                        ):
                            break
            except asyncio.CancelledError:
                raise
            except Exception as err:  # noqa: BLE001 - a link must never die
                # One line per state change, not per attempt: an object that is
                # offline for good would otherwise fill the log for years.
                if self._connected:
                    LOGGER.info("Manager link is down: %s", err)
                else:
                    LOGGER.debug("Manager link retry failed: %s", err)
            finally:
                if self._connected:
                    LOGGER.info("Manager link closed")
                self._connected = False
            await asyncio.sleep(delay)
            delay = min(delay * 2, MAX_RETRY)

    async def _answer(self, socket: Any, frame: dict[str, Any]) -> None:
        """Answer one request from the manager (a resident who is away).

        ⚠ A refusal travels as a normal answer with `ok: false`, never as a
        dropped frame: the manager waits with a timeout, and silence would make
        every "device not found" look like "the house is offline" — three
        seconds later, and to a resident who is standing in that house.
        """
        request_id = frame.get("id")
        if socket is None or not isinstance(request_id, str):
            return
        try:
            payload = await ops.run(
                self._hass, self._coordinator, frame.get("op") or "", frame.get("payload")
            )
            reply: dict[str, Any] = {"t": "res", "id": request_id, "ok": True, "payload": payload}
        except ops.OpError as err:
            reply = {
                "t": "res",
                "id": request_id,
                "ok": False,
                "error": err.message,
                "status": err.status,
            }
        except Exception as err:  # noqa: BLE001 - an answer must always come back
            LOGGER.exception("Request from the manager failed: %s", err)
            reply = {
                "t": "res",
                "id": request_id,
                "ok": False,
                "error": "Дом не смог выполнить запрос",
                "status": 500,
            }
        try:
            await socket.send_json(reply)
        except Exception as err:  # noqa: BLE001 - the link reconnects on its own
            LOGGER.debug("Could not send the answer: %s", err)

    async def _sync_bundle(self, version: Any, socket: Any) -> None:
        """Скачать новый интерфейс и СРАЗУ доложить, чем дом теперь раздаётся.

        ⚠ Доклад обязателен: без него менеджер до следующего переподключения
        показывал бы прошлый бандл, а смотрят туда как раз тогда, когда у
        жильца «старый интерфейс». Отказ синхронизации доедет тем же кадром —
        он лежит в `last_error`.
        """
        bundle = self._coordinator.bundle
        if bundle is None:
            return
        await bundle.async_sync(version)
        if socket is None:
            return
        try:
            await socket.send_json(await self._hello())
        except Exception as err:  # noqa: BLE001 - канал переподключится сам
            LOGGER.debug("Could not report the bundle version: %s", err)

    async def _hello(self) -> dict[str, Any]:
        """Кадр представления: ДВЕ версии, и разница между ними осмысленна.

        `version` — код, который сейчас исполняется (константа, попавшая в
        память при загрузке). `disk_version` — то, что лежит на диске, то есть
        что положил HACS. Они расходятся ровно в одном случае: обновление
        скачано, а Home Assistant не перезапускали, — и менеджер тогда может
        сказать инсталлятору, какие дома ждут перезапуска, вместо того чтобы
        объезжать все.

        ⚠ Ни одна из них не должна ничего запрещать: после обновления без
        перезапуска дом в смешанном состоянии (загруженные модули прежние, а
        импортируемые позже читаются с диска новыми), поэтому «умеет ли дом
        операцию» решается только его ОТВЕТОМ.
        """
        bundle = self._coordinator.bundle
        return {
            "t": "hello",
            "version": _integration_version(self._hass),
            "disk_version": await self._hass.async_add_executor_job(_disk_version),
            # ⚠ Какой интерфейс дом РАЗДАЁТ прямо сейчас, и почему не новее.
            # Без этих двух полей вопрос «почему у жильца старые кнопки»
            # разбирался по скриншотам: у менеджера были канал и версия, а что
            # доехало до дома — только со слов человека у телефона. Версия — имя
            # каталога активного бандла, то есть ровно то, что отдаётся
            # браузеру, а не то, что мы намеревались скачать.
            "app_version": bundle.version if bundle else None,
            "app_error": bundle.last_error if bundle else None,
        }

    async def _handle(self, payload: dict[str, Any], socket: Any = None) -> None:
        kind = payload.get("t")
        if kind == "req":
            # ⚠ ОТДЕЛЬНОЙ задачей, а не по месту. Этот метод зовётся из цикла
            # чтения сокета, и пока он не вернётся, ни один следующий кадр не
            # прочитан. Пока операции занимали миллисекунды, это было незаметно;
            # переговоры WebRTC длятся секунды — и на всё это время приложение
            # жильца перестало бы получать состояния, то есть показало бы «дом
            # не на связи» ровно в момент, когда он открыл камеру.
            task = asyncio.ensure_future(self._answer(socket, payload))
            self._answers.add(task)
            task.add_done_callback(self._answers.discard)
            return
        if kind == "app_changed":
            # A new interface was published. Nothing about this home changed, so
            # the config refresh would not notice it on its own.
            #
            # ⚠ ОТДЕЛЬНОЙ задачей, по той же причине, что и ответы выше: тут
            # качается бандл — сотни килобайт по уплинку квартиры, — а пока этот
            # метод не вернётся, ни один следующий кадр из сокета не прочитан.
            # То есть на всё скачивание дом переставал отвечать жильцу, и его
            # запросы отваливались по таймауту менеджера. С 2026-09-08 ожидание
            # стало ещё и длиннее: синхронизация сериализована замком
            # (`bundle.py`), поэтому подсказка может ждать идущий опрос.
            if self._coordinator.bundle:
                task = asyncio.ensure_future(
                    self._sync_bundle(payload.get("version"), socket)
                )
                self._answers.add(task)
                task.add_done_callback(self._answers.discard)
            return
        if kind != "config_changed":
            return
        version = payload.get("version")
        if version and version == self._coordinator.version:
            # Already holding it (the poll got there first) — nothing to do.
            return
        LOGGER.debug("Manager says the config moved to %s", version)
        await self._coordinator.async_request_refresh()


def _ws_url(manager_url: str) -> str:
    base = manager_url.rstrip("/")
    if base.startswith("https://"):
        return f"wss://{base[len('https://'):]}{WS_PATH}"
    if base.startswith("http://"):
        return f"ws://{base[len('http://'):]}{WS_PATH}"
    return f"ws://{base}{WS_PATH}"


def _integration_version(hass: HomeAssistant) -> str:
    """Версия ЗАГРУЖЕННОГО кода — константа, попавшая в память при старте.

    ⚠ Не `async_get_loaded_integration(...).version` и не чтение манифеста: обе
    величины приходят от Home Assistant и с диска, а нам нужно число, про
    которое ТОЧНО известно, откуда оно взялось. Константа лежит в модуле рядом с
    исполняемым кодом и уезжает вместе с ним; что лежит на диске, дом сообщает
    отдельным полем (`_disk_version`). Подробности — `const.py`.
    """
    from .const import INTEGRATION_VERSION

    return INTEGRATION_VERSION


def _disk_version() -> str:
    """Версия из `manifest.json` РЯДОМ с этим файлом — то, что положил HACS.

    ⚠ Блокирующее чтение файла, поэтому зовётся только из executor'а и только
    на подключение канала. Ошибку глотаем: неизвестная версия на диске — это
    минус одна подсказка инсталлятору, а не повод не поднять канал.
    """
    from json import loads
    from pathlib import Path

    try:
        manifest = loads((Path(__file__).parent / "manifest.json").read_text("utf-8"))
        version = manifest.get("version")
        return version if isinstance(version, str) else ""
    except Exception:  # noqa: BLE001 - канал важнее подсказки
        return ""
