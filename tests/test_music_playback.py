"""Привязка очереди дома к штату плеера и короткому потоку."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from mega_home.core.host import PlainHost
from mega_home.core.ops_base import OpError
from mega_home.core.source import PlainState
from mega_home.core.topics import Topics
from mega_home.providers.music_flow import FlowStreams
from mega_home.music_playback import MusicPlayback, TOPIC
from mega_home.providers.music_catalog import _cover_uri
from mega_home.providers.music_proxy import MusicStreamLinks


class Source:
    def __init__(self):
        self.states = {entity: PlainState("idle") for entity in ["media_player.local", "media_player.receiver"]}
        self.callbacks = {}
        self.calls = []

    def get(self, entity):
        return self.states.get(entity)

    def subscribe(self, entities, callback):
        entity, = entities
        self.callbacks[entity] = callback
        return lambda: self.callbacks.pop(entity)

    async def call(self, domain, service, data, response, by):
        self.calls.append((domain, service, data))

    def report(self, entity, state, *, content="", pos=None, **attrs):
        data = {"media_content_id": content, **attrs}
        if pos is not None:
            data.update(media_position=pos, media_position_updated_at=datetime.now(timezone.utc))
        sample = self.states[entity] = PlainState(state, data)
        if callback := self.callbacks.get(entity):
            callback(entity, sample)


class Catalog:
    def __init__(self):
        self.actions = []

    async def call(self, _manage, request):
        self.actions.append(request["action"])
        if request["action"] == "track_details":
            return {"items": [{
                "id": track, "title": "Песня " + track, "artist": "Исполнитель",
                "album": "Альбом", "cover": "https://img.example/cover.jpg", "durationMs": 180_000,
            } for track in request["ids"]]}
        if request["action"] == "stream":
            return {"infos": [{"url": "https://cdn.example/track.mp3", "codec": "mp3", "bitrate": 320}]}
        if request["action"] == "liked":
            assert request["page"] == 1
            return {"items": [{"id": "2", "kind": "track", "title": "Следующая страница"}], "more": False}
        if request["action"] == "set_favorite":
            if request["id"] == "2":
                raise OpError("Яндекс Музыка не сохранила отметку", 502)
            return {"id": request["id"], "liked": request["liked"]}
        raise AssertionError(request)


def setup(tmp_path):
    source, catalog = Source(), Catalog()
    tiles = [
        {"id": "station", "domain": "media_player", "entityId": "media_player.local",
         "stream": {"type": "stream.{ext}", "nativeTrackType": "track", "needsKnownState": True}},
        {"id": "receiver", "domain": "media_player", "entityId": "media_player.receiver",
         "stream": {"type": "url", "opaqueTrackId": True}},
    ]
    home = SimpleNamespace(data={"tiles": tiles}, source=source, env=PlainHost(tmp_path), topics=Topics())
    links = MusicStreamLinks("http://home.local:8123")
    # ⚠ Адрес дома для потока обязателен: сессию получает ЛЮБОЙ плеер со `stream` (источник — дом).
    flow = FlowStreams(lambda: "http://home.local:8123")
    return MusicPlayback(home, catalog, catalog, links, flow), home, catalog, links


async def play(service, player, index=0):
    return await service.call(False, {"action": "play_queue", "player": player, "ids": ["1:40", "2", "3"], "index": index})


async def view(service, player):
    return (await service.call(False, {"action": "playback_state"}))["players"].get(player)


def test_станция_играет_каталог_и_продолжает_очередь_после_конца_песни(tmp_path):
    async def run():
        service, home, catalog, _ = setup(tmp_path)
        source = home.source
        assert TOPIC in home.topics.revs()
        assert (await play(service, "station"))["accepted"]
        assert source.calls[-1] == ("media_player", "play_media", {
            "entity_id": "media_player.local", "media_content_type": "track", "media_content_id": "1",
        })
        assert catalog.actions == ["track_details"]
        source.report("media_player.local", "playing", content="1:40", pos=0)
        state = await view(service, "station")
        assert state["started"] and state["next"] and not state["previous"]
        assert state["track"]["image"] == "https://img.example/cover.jpg"
        source.report("media_player.local", "paused", content="1", pos=30)
        assert (await view(service, "station"))["track"]["id"] == "1:40"
        source.report("media_player.local", "playing", content="1", pos=178)
        source.report("media_player.local", "playing", content="444", pos=0)
        await asyncio.sleep(0)
        assert source.calls[-1][2]["media_content_id"] == "2"
        source.report("media_player.local", "playing", content="2", pos=0)
        assert (await view(service, "station"))["previous"]
        source.report("media_player.local", "playing", content="99", pos=0)
        assert await view(service, "station") is None
        service.close()

    asyncio.run(run())


def test_ресивер_с_непрозрачным_id_подтверждает_поток_и_переходит_по_событию_остаётся_с_обложкой(tmp_path):
    async def run():
        service, home, _catalog, links = setup(tmp_path)
        source = home.source
        await play(service, "receiver")
        first = source.calls[-1][2]
        assert first["media_content_id"].startswith("media-source://mega_home/track/")
        assert first["extra"]["metadata"]["albumName"] == "Альбом"
        assert first["extra"]["metadata"]["images"] == [{"url": "https://img.example/cover.jpg"}]
        source.report("media_player.receiver", "playing", content="1", media_title="Url Stream", pos=0)
        assert not (await view(service, "receiver"))["started"]
        links.opened(first["media_content_id"])
        assert (await view(service, "receiver"))["started"]
        source.report("media_player.receiver", "playing", content="1", media_title="Url Stream", pos=178)
        source.report("media_player.receiver", "idle")
        await asyncio.sleep(0)
        second = source.calls[-1][2]
        assert second["media_content_id"] != first["media_content_id"]
        assert second["extra"]["metadata"]["title"] == "Песня 2"
        links.opened(second["media_content_id"])
        source.report("media_player.receiver", "playing", content="1", media_title="Url Stream", pos=0)
        source.report("media_player.receiver", "playing", content="1", media_title="AirPlay", source="AirPlay", pos=0)
        assert await view(service, "receiver") is None
        service.close()

    asyncio.run(run())


def test_очереди_двух_плееров_независимы_состав_ограничивает_команды(tmp_path):
    async def run():
        service, home, _, links = setup(tmp_path)
        source = home.source
        with pytest.raises(OpError):
            await play(service, "media_player.unlisted")
        await play(service, "station")
        await play(service, "receiver", index=1)
        receiver = source.calls[-1][2]["media_content_id"]
        links.opened(receiver)
        source.report("media_player.receiver", "playing", content="1")
        source.report("media_player.local", "playing", content="1")
        assert (await service.call(False, {"action": "queue_control", "player": "station", "command": "next"}))["handled"]
        assert source.calls[-1][2]["media_content_id"] == "2"
        assert (await view(service, "receiver"))["track"]["id"] == "2"
        home.data["tiles"].pop()
        service.config_changed()
        assert "media_player.receiver" not in source.callbacks
        assert await view(service, "receiver") is None
        service.close()

    asyncio.run(run())


def test_обложки_артиста_плейлиста_и_альбома_берутся_из_полей_каталога():
    cover = "avatars.mds.yandex.net/track/%%"
    assert _cover_uri(SimpleNamespace(cover=SimpleNamespace(uri=cover))) == cover
    assert _cover_uri(SimpleNamespace(og_image=cover)) == cover
    assert _cover_uri(SimpleNamespace(albums=[SimpleNamespace(og_image=cover)])) == cover


def test_дом_загружает_следующую_страницу_сам_когда_закончились_треки(tmp_path):
    async def run():
        service, home, catalog, _ = setup(tmp_path)
        await service.call(False, {
            "action": "play_queue", "player": "station", "ids": ["1"], "index": 0,
            "continuation": {"action": "liked", "page": 1},
        })
        assert catalog.actions == ["track_details"]
        home.source.report("media_player.local", "playing", content="1", pos=179)
        assert (await view(service, "station"))["next"]
        home.source.report("media_player.local", "idle")
        await asyncio.sleep(0)
        assert catalog.actions == ["track_details", "liked"]
        assert home.source.calls[-1][2]["media_content_id"] == "2"
        assert not (await view(service, "station"))["next"]
        service.close()

    asyncio.run(run())


def test_дом_публикует_смену_избранного_после_подтверждения_от_яндекса(tmp_path):
    async def run():
        service, home, _, _ = setup(tmp_path)
        with pytest.raises(OpError):
            await service.call(True, {"action": "set_favorite", "id": "2", "liked": True})
        assert "media/favorites" not in home.topics.revs()
        assert (await service.call(True, {"action": "set_favorite", "id": "1", "liked": True}))["liked"]
        assert "media/favorites" in home.topics.revs()
        service.close()

    asyncio.run(run())
