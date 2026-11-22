"""Поток сессии дома: один адрес плееру, элементы сменяет дом.

Живой случай, ради которого это заведено (прошитый Amazon Echo, объект 90-Marksa68,
2026-11-22): плеер не берёт новый `media_url` поверх живого потока, поэтому переключение
трека/станции работало только после «Стоп», а станцию приходилось отгадывать по атрибутам,
которых он не сообщает.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

from mega_home.core.host import PlainHost
from mega_home.core.source import PlainState
from mega_home.core.topics import Topics
from mega_home.music_playback import MusicPlayback
from mega_home.providers.music_flow import FlowSession, FlowStreams, wav_header
from mega_home.providers.music_proxy import MusicStreamLinks


class FakeProcess:
    """Декодер элемента: отдаёт заготовленные куски звука и помнит, что его гасили."""

    def __init__(self, chunks: list[bytes]) -> None:
        self.chunks = list(chunks)
        self.stopped = False

    async def read(self, size: int) -> bytes:
        return self.chunks.pop(0) if self.chunks else b""

    async def stop(self) -> None:
        self.stopped = True


class Source:
    """Плеер дома: состояния и вызовы служб, как их видит очередь."""

    def __init__(self, entity: str) -> None:
        self.states = {entity: PlainState("idle")}
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

    def report(self, entity, state, **attrs):
        sample = self.states[entity] = PlainState(state, attrs)
        if callback := self.callbacks.get(entity):
            callback(entity, sample)


class Catalog:
    """Яндекс: только те действия, что нужны очереди треков."""

    def __init__(self) -> None:
        self.actions = []

    async def call(self, _manage, request):
        self.actions.append(request["action"])
        if request["action"] == "track_details":
            return {"items": [{"id": track, "title": "Песня " + track, "artist": "Исполнитель",
                               "album": "Альбом", "cover": "", "durationMs": 60_000}
                              for track in request["ids"]]}
        if request["action"] == "stream":
            return {"infos": [{"url": f"https://cdn.example/{request['id']}.mp3", "codec": "mp3", "bitrate": 320}]}
        raise AssertionError(request)


def flow_tile(flow: bool = True, entity: str = "media_player.echo") -> dict:
    return {
        "id": "echo", "domain": "media_player", "entityId": entity,
        "stream": {"type": "music", "opaqueTrackId": True, **({"flow": True} if flow else {})},
        "commands": {"play_stream": {
            "domain": "media_player", "service": "play_media", "label": "Радиостанция",
            "fields": {"media_content_id": {"type": "string"}, "media_content_type": {"type": "string"}},
        }},
    }


def setup(tmp_path, *, flow=True):
    source, catalog = Source("media_player.echo"), Catalog()
    home = SimpleNamespace(data={"tiles": [flow_tile(flow)]}, source=source, env=PlainHost(tmp_path), topics=Topics())
    links = MusicStreamLinks("http://home.local:8123")
    streams = FlowStreams(lambda: "http://home.local:8123")
    return MusicPlayback(home, catalog, catalog, links, streams), home, catalog, streams


def test_заголовок_потока_объявляет_wav_неизвестной_длины():
    header = wav_header()
    assert header[:4] == b"RIFF" and header[8:12] == b"WAVE" and b"data" in header
    # 48 кГц, моно, 16 бит: под этот поток дом декодирует элементы (`providers/music_flow.py`).
    assert header[22:24] == (1).to_bytes(2, "little")
    assert header[24:28] == (48_000).to_bytes(4, "little")
    assert header[34:36] == (16).to_bytes(2, "little")


def test_поток_играет_элементы_подряд_одним_ответом():
    async def run():
        session = FlowSession("t")
        items = tuple(SimpleNamespace(source=f"src{i}") for i in range(3))
        session.set_items(items, 0)
        asked: list[str] = []
        written: list[bytes] = []

        async def source_of(item):
            return item.source

        async def decoder(url):
            asked.append(url)
            return FakeProcess([b"AA" * 8, b"BB" * 8])

        async def write(chunk):
            written.append(chunk)
            await asyncio.sleep(0)

        async def on_end():
            # Так это делает очередь дома: элемент доигран — дальше по очереди, без команды плееру.
            if session.index + 1 < len(session.items):
                session.set_items(session.items, session.index + 1)

        session.source_of, session.on_end = source_of, on_end
        await session.play(write, decoder)

        assert asked == ["src0", "src1", "src2"]
        assert written[0] == wav_header()
        assert b"".join(written[1:]) == (b"AA" * 8 + b"BB" * 8) * 3
        # Очередь кончилась — поток закрылся сам, а не остался висеть.
        assert session.reading is False

    asyncio.run(run())


def test_смена_элемента_приходит_в_тот_же_поток():
    async def run():
        session = FlowSession("t")
        session.set_items((SimpleNamespace(source="src0"),), 0)
        asked: list[str] = []
        audio: list[bytes] = []
        made: list[FakeProcess] = []

        async def source_of(item):
            return item.source

        async def decoder(url):
            asked.append(url)
            process = FakeProcess([b"AA" * 8] * 4)
            made.append(process)
            return process

        async def write(chunk):
            await asyncio.sleep(0)
            if chunk != wav_header():
                audio.append(chunk)
                # Первый элемент длинный: посреди него плееру дают следующую станцию.
                if len(asked) == 1 and len(audio) == 2:
                    session.set_items((SimpleNamespace(source="src1"),), 0)

        session.source_of = source_of
        await session.play(write, decoder)

        # Тот же ответ, другой звук: дом сам погасил прежний декодер и взял следующий элемент.
        assert asked == ["src0", "src1"]
        assert made[0].stopped is True
        # Хвост прежнего элемента не доигран (его куски остались у декодера) — переключились сразу.
        assert len(made[0].chunks) == 2
        assert len(audio) == 6

    asyncio.run(run())


def test_станция_на_плеере_потока_идёт_адресом_сессии(tmp_path):
    async def run():
        service, home, _catalog, streams = setup(tmp_path)
        station = {"id": "rock", "title": "Радио Рекорд", "image": "https://rr.example/logo.svg",
                   "url": "https://cdn.example/rock.aac"}
        fields = {"media_content_id": "https://cdn.example/rock.aac", "media_content_type": "audio/aac"}
        accepted = await service.call(False, {"action": "play_station", "player": "echo",
                                              "station": station, "fields": fields})
        assert accepted["accepted"]

        command = home.source.calls[-1][2]
        assert command["entity_id"] == "media_player.echo"
        # Плееру уходит адрес СЕССИИ дома, а не ссылка станции: дальше элементы сменяет дом.
        assert command["media_content_id"] == "http://home.local:8123/mega-home/stream/" + streams.session("echo").token
        assert command["media_content_type"] == "music"

        # Плеер заиграл — но подтверждения ещё нет: у такого плеера свидетельство одно, и это
        # «устройство взяло поток дома» (о себе оно не рассказывает).
        home.source.report("media_player.echo", "playing")
        assert (await view(service, "echo"))["started"] is False
        streams.session("echo").on_read()
        state = await view(service, "echo")
        assert state["started"] is True
        assert state["track"]["id"] == "station:rock"
        assert state["track"]["title"] == "Радио Рекорд"
        assert state["track"]["image"] == "https://rr.example/logo.svg"
        service.close()

    asyncio.run(run())


def test_станция_на_плеере_без_потока_идёт_полями_описания(tmp_path):
    async def run():
        service, home, _catalog, streams = setup(tmp_path, flow=False)
        station = {"id": "rock", "title": "Радио Рекорд", "url": "https://cdn.example/rock.aac"}
        fields = {"media_content_id": "x-rincon-mp3radio://https://cdn.example/rock.aac",
                  "media_content_type": "music"}
        assert (await service.call(False, {"action": "play_station", "player": "echo",
                                          "station": station, "fields": fields}))["accepted"]
        # Без потока плееру уходят ровно те поля, что прислал бандл (проверенные по описанию команды).
        assert home.source.calls[-1][2] == {
            "entity_id": "media_player.echo",
            "media_content_id": "x-rincon-mp3radio://https://cdn.example/rock.aac",
            "media_content_type": "music",
        }
        # Запуск подтверждает владелец: у станции адрес чужой (CDN источника), свидетельства нет.
        assert (await view(service, "echo"))["started"] is True
        # Чужое поле в команде — отказ: браузеру жильца верить нельзя (описание в конфиге).
        try:
            await service.call(False, {"action": "play_station", "player": "echo", "station": station,
                                       "fields": {**fields, "extra": "nope"}})
        except Exception as err:  # noqa: BLE001 — важна сторона отказа, а не класс
            assert "extra" in str(err)
        else:
            raise AssertionError("чужое поле принято")
        service.close()

    asyncio.run(run())


def test_трек_на_плеере_потока_переключается_без_команды_плееру(tmp_path):
    async def run():
        service, home, _catalog, streams = setup(tmp_path)
        await service.call(False, {"action": "play_queue", "player": "echo", "ids": ["1", "2"], "index": 0})
        first = home.source.calls[-1][2]
        session = streams.session("echo")
        assert first["media_content_id"] == "http://home.local:8123/mega-home/stream/" + session.token
        assert session.current.id == "1"
        home.source.report("media_player.echo", "playing")
        # Устройство взяло поток: так это помечает `FlowSession.play` (дома — маршрут потока).
        session.reading = True
        session.on_read()
        assert (await view(service, "echo"))["started"] is True

        calls = len(home.source.calls)
        assert (await service.call(False, {"action": "queue_control", "player": "echo", "command": "next"}))["handled"]
        # Новая команда плееру НЕ ушла: играет тот же поток, а элемент внутри сменился.
        assert len(home.source.calls) == calls
        assert session.current.id == "2"
        assert (await view(service, "echo"))["track"]["id"] == "2"
        service.close()

    asyncio.run(run())


async def view(service, player):
    return (await service.call(False, {"action": "playback_state"}))["players"].get(player)
