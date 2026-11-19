"""Службы Яндекс Музыки для сценариев: тот же каталог и поток, но из HA."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from homeassistant.exceptions import HomeAssistantError

from mega_home.core.const import DOMAIN, SERVICE_MUSIC, SERVICE_MUSIC_PLAY
from mega_home.music_services import async_register_music_services


class Services:
    def __init__(self) -> None:
        self.handlers: dict[tuple[str, str], tuple[object, dict]] = {}
        self.calls: list[tuple] = []

    def async_register(self, domain: str, name: str, handler: object, **kwargs: object) -> None:
        self.handlers[(domain, name)] = (handler, kwargs)

    async def async_call(self, domain: str, service: str, data=None, blocking=False, target=None, **_kwargs) -> None:
        self.calls.append((domain, service, data, target, blocking))


class Entries:
    def __init__(self, entry: object) -> None:
        self.entry = entry

    def async_loaded_entries(self, _domain: str) -> list[object]:
        return [self.entry]


class Music:
    def __init__(self) -> None:
        self.calls: list[tuple[bool, dict]] = []

    async def __call__(self, can_manage: bool, body: str) -> dict:
        request = body
        self.calls.append((can_manage, request))
        if request["action"] == "stream":
            return {"infos": [{"url": "https://cdn.yandex/track.mp3", "codec": "mp3", "bitrate": 320}]}
        return {"items": [], "more": False}


def make():
    music = Music()
    entry = SimpleNamespace(entry_id="home", runtime_data=SimpleNamespace(resident_services={"yandex-music": music}))
    hass = SimpleNamespace(services=Services(), config_entries=Entries(entry))
    async_register_music_services(hass)
    return hass, music


def test_каталог_сценария_отвечает_тем_же_контрактом_что_приложению():
    hass, music = make()
    handler, options = hass.services.handlers[(DOMAIN, SERVICE_MUSIC)]
    assert options["supports_response"] == "only"

    async def run():
        answer = await handler(SimpleNamespace(data={"request": {"action": "liked"}}, hass=hass))
        assert answer == {"items": [], "more": False}
        assert music.calls == [(False, {"action": "liked"})]

    asyncio.run(run())


def test_служба_не_управляет_привязкой():
    hass, _music = make()
    handler, _options = hass.services.handlers[(DOMAIN, SERVICE_MUSIC)]

    async def run():
        with pytest.raises(HomeAssistantError):
            await handler(SimpleNamespace(data={"request": {"action": "disconnect"}}, hass=hass))

    asyncio.run(run())


def test_трек_разрешается_и_уходит_плееру_без_радио_схемы():
    hass, music = make()
    handler, _options = hass.services.handlers[(DOMAIN, SERVICE_MUSIC_PLAY)]

    async def run():
        await handler(SimpleNamespace(data={"id": "123", "entity_id": "media_player.kitchen", "media_content_type": "{mime}"}, hass=hass))
        assert music.calls[-1] == (False, {"action": "stream", "id": "123"})
        domain, service, data, target, blocking = hass.services.calls[-1]
        assert (domain, service, blocking) == ("media_player", "play_media", True)
        assert data == {"media_content_id": "https://cdn.yandex/track.mp3", "media_content_type": "audio/mpeg"}
        assert target == {"entity_id": "media_player.kitchen"}

    asyncio.run(run())


def test_сценарий_отправляет_трек_настроенному_плееру_через_тот_же_выход():
    hass, music = make()
    handler, _options = hass.services.handlers[(DOMAIN, SERVICE_MUSIC_PLAY)]
    calls = []

    async def configured(entities, track):
        calls.append((entities, track))
        return []

    hass.config_entries.entry.runtime_data.music_playback = SimpleNamespace(play_configured=configured)

    async def run():
        await handler(SimpleNamespace(data={"id": "123", "entity_id": "media_player.station"}, hass=hass))
        assert calls == [(["media_player.station"], "123")]
        assert not music.calls and not hass.services.calls

    asyncio.run(run())
