"""Связка физического выхода Sonos S2 с координатором и командами."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from mega_home.core.ops_base import OpError
from mega_home.providers.sonos import Sonos

LEADER = "RINCON_00112233445501400"
MEMBER = "RINCON_00112233445601400"
OTHER = "RINCON_00112233445701400"


class Client:
    def __init__(self, uid, group):
        self.player_id = uid
        self.player = SimpleNamespace(group=group)
        self.api = SimpleNamespace(connected=False)
        self.closed = asyncio.Event()

    async def connect(self):
        self.api.connected = True

    async def disconnect(self):
        self.api.connected = False
        self.closed.set()

    async def start_listening(self, ready):
        ready.set()
        await self.closed.wait()


def test_координатор_получает_радио_и_изменения_состава_после_смены_ip():
    async def run():
        ids = {"media_player.a": LEADER, "media_player.b": MEMBER, "media_player.c": OTHER}
        addresses = {LEADER: "10.0.0.1", MEMBER: "10.0.0.2", OTHER: "10.0.0.3"}
        group = SimpleNamespace(
            coordinator_id=LEADER, player_ids=[LEADER, MEMBER], play_stream_url=AsyncMock(),
            modify_group_members=AsyncMock(), play=AsyncMock(), pause=AsyncMock(), stop=AsyncMock(),
        )
        clients = {address: Client(uid, group) for uid, address in addresses.items()}
        looked_up = []

        async def discover(uid):
            looked_up.append(uid)
            return addresses.get(uid)

        sonos = Sonos(ids.get, discover, clients.__getitem__)
        try:
            address = "http://home.local/mega-home/stream/abcdef.aac"
            assert await sonos.supports("media_player.b")
            await sonos.play("media_player.b", address, "Радио", "http://home.local/cover.jpg")
            url, metadata = group.play_stream_url.await_args.args
            assert url == address
            assert metadata["name"] == "Радио"
            assert metadata["imageUrl"] == "http://home.local/cover.jpg"
            await sonos.join("media_player.a", ["media_player.b", "media_player.c"])
            await sonos.leave("media_player.b")
            assert [call.args for call in group.modify_group_members.await_args_list] == [
                ([OTHER], []), ([], [MEMBER]),
            ]
            assert group.play_stream_url.await_count == 1
            await clients["10.0.0.1"].disconnect()
            addresses[LEADER] = "10.0.0.4"
            clients["10.0.0.4"] = Client(LEADER, group)
            await sonos.transport("media_player.b", "pause")
            group.pause.assert_awaited_once()
            assert looked_up.count(LEADER) == 2
        finally:
            await sonos.close()
        assert all(client.closed.is_set() for address, client in clients.items() if address != "10.0.0.3")

    asyncio.run(run())


def test_ip_другой_колонки_даёт_отказ_до_команды():
    async def run():
        group = SimpleNamespace(play_stream_url=AsyncMock())
        client = Client(OTHER, group)
        sonos = Sonos(lambda _: LEADER, AsyncMock(return_value="10.0.0.1"), lambda _: client)
        with pytest.raises(OpError, match="другой ID"):
            await sonos.play("media_player.a", "http://home.local/radio.aac", "Радио", "")
        group.play_stream_url.assert_not_awaited()
        assert client.closed.is_set()

    asyncio.run(run())
