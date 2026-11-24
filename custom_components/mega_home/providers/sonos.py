"""Локальные команды Sonos S2 через клиент aiosonos."""

from __future__ import annotations

import asyncio
import re
from collections.abc import Awaitable, Callable
from contextlib import suppress
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from aiohttp import ClientError

from ..core.const import LOGGER
from ..core.ops_base import OpError

if TYPE_CHECKING:
    from aiosonos.client import SonosLocalApiClient
    from aiosonos.group import SonosGroup

_UID = re.compile(r"RINCON_[0-9A-F]{17}", re.IGNORECASE)


def normalize_uid(value: str) -> str | None:
    return value.upper() if _UID.fullmatch(value) else None


@dataclass
class Connection:
    client: SonosLocalApiClient
    listener: asyncio.Task[None]

    async def close(self) -> None:
        try:
            await self.client.disconnect()
        finally:
            self.listener.cancel()
            await asyncio.gather(self.listener, return_exceptions=True)


class Sonos:
    def __init__(
        self, uid_of: Callable[[str], str | None],
        address_of: Callable[[str], Awaitable[str | None]],
        make_client: Callable[[str], SonosLocalApiClient],
    ) -> None:
        self._uid_of = uid_of
        self._address_of = address_of
        self._make_client = make_client
        self._known: set[str] = set()
        self._discovered: dict[str, str] = {}
        self._connections: dict[str, Connection] = {}
        self._lock = asyncio.Lock()

    def uid(self, entity: str) -> str | None:
        return normalize_uid(self._uid_of(entity) or "")

    async def supports(self, entity: str) -> bool:
        uid = self.uid(entity)
        if uid is None:
            return False
        if uid in self._known or uid in self._discovered:
            return True
        if address := await self._address_of(uid):
            self._discovered[uid] = address
            return True
        return False

    async def _connect(self, uid: str) -> SonosLocalApiClient:
        async with self._lock:
            connection = self._connections.get(uid)
            if connection is not None:
                if not connection.listener.done() and connection.client.api.connected:
                    return connection.client
                self._connections.pop(uid)
                await connection.close()
            address = self._discovered.pop(uid, None) or await self._address_of(uid)
            if address is None:
                raise OpError(f"Sonos S2 {uid}: колонка не найдена в локальной сети", 409)
            client = self._make_client(address)
            listener = None
            try:
                async with asyncio.timeout(7):
                    await client.connect()
                    if normalize_uid(client.player_id) != uid:
                        raise OpError(f"Sonos S2 {uid}: колонка по найденному адресу имеет другой ID", 409)
                    initialized = asyncio.Event()
                    listener = asyncio.create_task(client.start_listening(initialized), name=f"sonos:{uid}")
                    ready = asyncio.create_task(initialized.wait())
                    try:
                        await asyncio.wait((listener, ready), return_when=asyncio.FIRST_COMPLETED)
                        if listener.done():
                            listener.result()
                            raise OSError("соединение с колонкой закрыто")
                    finally:
                        ready.cancel()
            except BaseException:
                if getattr(client, "api", None) is not None:
                    with suppress(Exception):
                        await client.disconnect()
                if listener is not None:
                    listener.cancel()
                    await asyncio.gather(listener, return_exceptions=True)
                raise
            self._known.add(uid)
            self._connections[uid] = Connection(client, listener)
            return client

    async def group(self, entity: str) -> SonosGroup:
        from aiosonos.exceptions import SonosException

        uid = self.uid(entity)
        if uid is None:
            raise OpError(f"{entity}: в реестре HA нет идентификатора Sonos", 409)
        try:
            client = await self._connect(uid)
            group = client.player.group
            if group is not None and (coordinator := normalize_uid(group.coordinator_id)) and coordinator != uid:
                client = await self._connect(coordinator)
                group = client.player.group
                if group is None or normalize_uid(group.coordinator_id) != coordinator:
                    raise OpError(f"{entity}: координатор группы Sonos изменился, повторите команду", 409)
            if group is None:
                raise OpError(f"{entity}: Sonos не сообщил группу воспроизведения", 409)
            return group
        except (ClientError, SonosException, OSError, TimeoutError) as error:
            LOGGER.warning("Sonos S2 %s: подключение не удалось (%s)", entity, type(error).__name__)
            raise OpError(f"{entity}: Sonos S2 не ответил", 409) from error

    async def play(self, entity: str, address: str, title: str, image: str) -> None:
        container: dict[str, Any] = {
            "_objectType": "container",
            "name": title,
            "type": "track",
            "id": {"_objectType": "id", "objectId": address},
            "service": {"_objectType": "service", "name": "Mega Home", "id": "mega-home"},
        }
        if image:
            container["imageUrl"] = image
        await self._command(entity, lambda group: group.play_stream_url(address, container))

    async def join(self, entity: str, members: list[str]) -> None:
        ids = [self.uid(member) for member in members]
        if None in ids:
            raise OpError("В реестре HA отсутствует идентификатор одной из колонок Sonos", 409)
        await self._command(entity, lambda group: group.modify_group_members(
            [uid for uid in ids if uid is not None and uid not in group.player_ids], [],
        ) if any(uid not in group.player_ids for uid in ids) else None)

    async def leave(self, entity: str) -> None:
        uid = self.uid(entity)

        async def remove(group: SonosGroup) -> None:
            if uid in group.player_ids and len(group.player_ids) > 1:
                await group.modify_group_members([], [uid])

        await self._command(entity, remove)

    async def transport(self, entity: str, action: str) -> None:
        await self._command(entity, lambda group: {
            "play": group.play, "pause": group.pause, "stop": group.stop,
        }[action]())

    async def _command(
        self, entity: str, invoke: Callable[[SonosGroup], Awaitable[None] | None],
    ) -> None:
        from aiosonos.exceptions import SonosException

        group = await self.group(entity)
        try:
            if command := invoke(group):
                async with asyncio.timeout(8):
                    await command
        except (ClientError, SonosException, OSError, TimeoutError) as error:
            LOGGER.warning("Sonos S2 %s: отказ команды (%s)", entity, type(error).__name__)
            raise OpError(f"{entity}: Sonos S2 не выполнил команду", 409) from error

    async def close(self) -> None:
        async with self._lock:
            connections = tuple(self._connections.values())
            self._connections.clear()
            await asyncio.gather(*(item.close() for item in connections), return_exceptions=True)
