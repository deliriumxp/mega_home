"""Контракт шлюза приложения со штатным мастером и очередью MA."""

from __future__ import annotations

import asyncio
import base64
from types import SimpleNamespace

import pytest

from mega_home.core.ops_base import OpError
from mega_home.providers.music_assistant_auth import MusicAssistantAuth
from mega_home.music_assistant_bridge import MusicAssistantBridge, entity_registry


class Client:
    def __init__(self):
        self.calls = []
        self.providers = []
        self.callback = None

    def subscribe(self, callback):
        self.callback = callback
        return lambda: setattr(self, "callback", None)

    async def send_command(self, command, **args):
        self.calls.append((command, args))
        if command == "config/providers":
            return self.providers
        if command == "config/providers/setup":
            return {"type": "form", "step_id": "user", "flow_id": "flow"}
        if command == "config/flows/submit":
            svg = ('<svg xmlns="http://www.w3.org/2000/svg">'
                   '<text>Address</text><text>ya.ru/device</text>'
                   '<text>Code</text><text>ABCD</text></svg>')
            image = "data:image/svg+xml;base64," + base64.b64encode(svg.encode()).decode()
            return {"type": "progress", "step_id": "device_login", "image": image, "expires_at": 2_000_000_000}
        if command == "config/flows/get":
            raise KeyError("Unknown (or finished) setup flow: flow")
        if command == "player_queues/get_active_queue":
            return {"active": True, "queue_id": "group"}
        if command == "player_queues/all":
            return []


def test_device_login_stays_in_ma():
    async def scenario():
        mass = Client()
        auth = MusicAssistantAuth()
        with pytest.raises(OpError, match="жилец"):
            await auth.call(mass, False, "start")
        assert not mass.calls
        pending = await auth.call(mass, True, "start")
        assert pending["code"] == "ABCD"
        assert pending["url"] == "https://ya.ru/device"
        assert pending["expiresAt"] == 2_000_000_000_000
        assert mass.calls[-1][1]["values"] == {"method": "device", "remember_session": True}
        assert (await auth.call(mass, False, "status"))["state"] == "disconnected"
        mass.providers = [{"instance_id": "yandex_music--abc", "enabled": True}]
        assert (await auth.call(mass, True, "status"))["state"] == "connected"

    asyncio.run(scenario())


def test_radio_and_controls_use_ma_queue(monkeypatch):
    async def scenario():
        mass = Client()
        hass = SimpleNamespace(config_entries=SimpleNamespace(async_loaded_entries=lambda domain: [
            SimpleNamespace(runtime_data=SimpleNamespace(mass=mass)),
        ]))
        player = SimpleNamespace(platform="music_assistant", unique_id="ma_player")
        monkeypatch.setattr(
            entity_registry, "async_get", lambda hass: SimpleNamespace(async_get=lambda entity: player),
            raising=False,
        )
        tile = {"id": "room-player", "entityId": "media_player.room", "stream": {"externalQueue": True}}
        revisions = []
        bridge = MusicAssistantBridge(hass, lambda: [tile], lambda: revisions.append(True))
        url = "https://radiorecord.hostingradio.ru/rr_main96.aacp"
        assert await bridge.call(False, {
            "action": "play_station", "player": "room-player", "station": {"id": "record", "url": url},
        }) == {"accepted": True}
        assert mass.calls[-1] == (
            "player_queues/play_media", {"queue_id": "ma_player", "media": url, "option": "replace"},
        )
        assert await bridge.call(False, {"action": "queue_control", "player": "room-player", "command": "stop"})
        assert mass.calls[-1] == ("player_queues/stop", {"queue_id": "group"})
        with pytest.raises(OpError, match="адрес"):
            await bridge.call(False, {
                "action": "play_station", "player": "room-player",
                "station": {"id": "bad", "url": "http://127.0.0.1:8123"},
            })
        mass.callback(SimpleNamespace(event="queue_updated"))
        assert revisions == [True]
        bridge.close()
        assert mass.callback is None

    asyncio.run(scenario())
