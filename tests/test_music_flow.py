"""Поток сессии дома: один адрес плееру, элементы сменяет дом.

Живой случай, ради которого это заведено (прошитый Amazon Echo, объект 90-Marksa68,
2026-11-22): плеер не берёт новый `media_url` поверх живого потока, поэтому переключение
трека/станции работало только после «Стоп», а станцию приходилось отгадывать по атрибутам,
которых он не сообщает.
"""

from __future__ import annotations

import asyncio
import time
from array import array
from types import SimpleNamespace

import pytest

from mega_home.core import ops
from mega_home.core.host import PlainHost
from mega_home.core.ops_base import OpError
from mega_home.core.source import PlainState
from mega_home.core.topics import Topics
from mega_home.music_playback import MusicPlayback
from mega_home.providers.music_flow import FlowSession, FlowStreams
from mega_home.providers.music_proxy import MusicStreamLinks


class FakeProcess:
    """Декодер элемента: труба — отдаёт не больше `piece` байт за чтение и помнит остановку.

    ⚠ Именно труба, а не «весь кусок разом»: дом читает PCM порциями, и фейд считает окно по ним.
    """

    def __init__(self, chunks: list[bytes], piece: int = 8) -> None:
        self.data = b"".join(chunks)
        self.piece = piece
        self.at = 0
        self.stopped = False

    @property
    def left(self) -> int:
        """Сколько звука декодер ещё не отдал (после смены элемента хвост не доигрывается)."""
        return len(self.data) - self.at

    async def read(self, size: int) -> bytes:
        # Уступаем циклу событий, как настоящая труба: у насоса сессии и у читателя свои задачи,
        # и смена элемента приходит ИМЕННО между кусками (иначе весь элемент успевает уйти в очередь).
        await asyncio.sleep(0)
        chunk = self.data[self.at : self.at + min(size, self.piece)]
        self.at += len(chunk)
        return chunk

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


def pcm(value: int, count: int) -> bytes:
    """PCM s16le из одного значения — по нему видно, что фейд делает со стыком."""
    return array("h", [value] * count).tobytes()


def samples(data: bytes) -> list[int]:
    """Отсчёты PCM s16le — для проверки микса."""
    out = array("h")
    out.frombytes(data)
    return list(out)


def flow_tile(flow: bool = True, entity: str = "media_player.echo") -> dict:
    return {
        "id": "echo", "domain": "media_player", "entityId": entity,
        "stream": {"type": "music", "opaqueTrackId": True, **({"flow": True} if flow else {})},
        "commands": {
            "play_stream": {
                "domain": "media_player", "service": "play_media", "label": "Радиостанция",
                "fields": {"media_content_id": {"type": "string"}, "media_content_type": {"type": "string"}},
            },
            # Мультирум: та же пара команд, что у плитки с соседями (таблица команд менеджера).
            "join": {
                "domain": "media_player", "service": "join", "label": "Объединение в группу",
                "fields": {"group_members": {"type": "list", "of": {"type": "string"}}},
            },
            "unjoin": {"domain": "media_player", "service": "unjoin", "label": "Выход из группы"},
        },
    }


def setup(tmp_path, *, flow=True):
    source, catalog = Source("media_player.echo"), Catalog()
    home = SimpleNamespace(data={"tiles": [flow_tile(flow)]}, source=source, env=PlainHost(tmp_path), topics=Topics())
    links = MusicStreamLinks("http://home.local:8123")
    streams = FlowStreams(lambda: "http://home.local:8123")
    return MusicPlayback(home, catalog, catalog, links, streams), home, catalog, streams


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
        await session.serve(write, decoder)

        assert asked == ["src0", "src1", "src2"]
        # Провода-заголовка нет: звук в потоке — MP3 (в спеках — сырой PCM).
        assert b"".join(written) == (b"AA" * 8 + b"BB" * 8) * 3
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
            audio.append(chunk)
            # Первый элемент длинный: посреди него плееру дают следующую станцию.
            if len(asked) == 1 and len(audio) == 2:
                session.set_items((SimpleNamespace(source="src1"),), 0)

        session.source_of = source_of
        await session.serve(write, decoder)

        # Тот же ответ, другой звук: дом сам погасил прежний декодер и взял следующий элемент.
        assert asked == ["src0", "src1"]
        assert made[0].stopped is True
        # Хвост прежнего элемента не доигран (точная граница — по кускам очереди, она плавающая).
        assert made[0].left > 0

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
        assert command["media_content_id"] == streams.address(streams.session("echo"), "media_player.echo")
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
        assert first["media_content_id"] == streams.address(session, "media_player.echo")
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


def test_мультирум_даёт_участнику_тот_же_адрес_сессии(tmp_path):
    """Sonos с HEOS HA не сгруппирует; у нас группа — один поток дома на всех участников."""

    async def run():
        service, home, _catalog, streams = setup(tmp_path)
        source = home.source
        living = flow_tile(entity="media_player.living")
        living["id"] = "living"
        home.data["tiles"].append(living)
        source.states["media_player.living"] = PlainState("idle")
        station = {"id": "rock", "title": "Радио Рекорд", "url": "https://cdn.example/rock.aac"}
        fields = {"media_content_id": station["url"], "media_content_type": "audio/aac"}
        await service.call(False, {"action": "play_station", "player": "echo", "station": station, "fields": fields})
        session = streams.session("echo")
        address = streams.address(session, "media_player.living")

        # Ведущий зовёт соседа: ему уходит адрес ТОЙ ЖЕ сессии (свой токен — задержка на плеера).
        assert (await ops.command(
            SimpleNamespace(data=home.data, source=source, music_playback=service),
            {"id": "echo", "command": "join", "value": {"group_members": ["media_player.living"]}},
        ))["accepted"]
        assert source.calls[-1] == (
            "media_player", "play_media",
            {"entity_id": "media_player.living", "media_content_id": address, "media_content_type": "music"},
        )
        # Состав — тем же полем, что штатная группировка HA: приложение читает ОДНИМ путём.
        line = ["media_player.echo", "media_player.living"]
        assert ops.groups_of(SimpleNamespace(music_playback=service)) == {
            "media_player.echo": line, "media_player.living": line,
        }
        view = ops.entity_view(flow_tile(), None, None, None, ops.groups_of(SimpleNamespace(music_playback=service)))
        assert view["attributes"]["group_members"] == line

        # Сосед выходит: гаснет он, ведущий играет дальше. Служба — по маске прибора: у фикстуры
        # бита STOP нет, значит пауза (`media_stop` HA принял бы только с битом STOP).
        await ops.command(
            SimpleNamespace(data=home.data, source=source, music_playback=service),
            {"id": "living", "command": "unjoin", "value": {}},
        )
        assert source.calls[-1] == ("media_player", "media_pause", {"entity_id": "media_player.living"})
        assert ops.groups_of(SimpleNamespace(music_playback=service)) == {}
        service.close()

    asyncio.run(run())


def test_мультирум_ведущим_может_быть_плеер_без_потока_дома(tmp_path):
    """Источник звука — дом: вести группу может ЛЮБОЙ плеер со `stream` (заказчик 2026-11-22).

    ⚠ Раньше ведущим был только «потоковый» плеер, и такой `join` уходил в HA (`None`). Теперь дом
    переводит ведущего на свой поток и повторяет его же запрос — иначе участникам нечего слушать.
    """

    async def run():
        service, home, _catalog, streams = setup(tmp_path, flow=False)
        tile = home.data["tiles"][0]
        home.data["tiles"].append(station_tile())
        # Музыку на ведущем включили НЕ из приложения (пульт, голос): очередь дома пуста. Такой
        # ведущий группу собрать не может — участник получал бы адрес пустого потока и вечную тишину
        # без единого отказа (живой симптом «отметка колонок ничего не включает»).
        with pytest.raises(OpError, match="Сначала включите музыку"):
            await service.group_command(tile, "join", {"group_members": ["media_player.station"]}, None)
        assert service.groups == {}

        # Включили из приложения — ведущий переведён на поток дома, и участник получает ТОТ ЖЕ адрес.
        fields = {"media_content_id": "https://cdn.example/rock.aac", "media_content_type": "audio/aac"}
        await service.call(False, {"action": "play_station", "player": "echo",
                                   "station": {"id": "rock", "title": "Рок", "url": fields["media_content_id"]},
                                   "fields": fields})
        answer = await service.group_command(tile, "join", {"group_members": ["media_player.station"]}, None)
        assert answer and answer["accepted"] and answer["group"][0] == "media_player.echo"
        addresses = [str(call[2].get("media_content_id", "")) for call in home.source.calls]
        assert any("/mega-home/stream/" in address for address in addresses)
        # Участник без потока дома в группу не годится — отказ, а не тишина.
        with pytest.raises(OpError):
            await service.group_command(tile, "join", {"group_members": ["media_player.living"]}, None)

    asyncio.run(run())


def test_мультирум_гасит_плеер_участника_без_потока_дома(tmp_path):
    async def run():
        service, home, _catalog, streams = setup(tmp_path, flow=False)
        tile = home.data["tiles"][0]
        # Снятие отметки принимается с ЛЮБОГО участника (у станции нет потока дома): её музыка
        # гаснет, а не уходит в HA пустым `unjoin` (живой баг 2026-11-22).
        home.data["tiles"].append(station_tile())
        source = home.source
        source.states["media_player.station"] = PlainState("playing")
        service.groups["media_player.echo"] = ["media_player.station"]
        assert (await ops.command(
            SimpleNamespace(data=home.data, source=source, music_playback=service),
            {"id": "station", "command": "unjoin", "value": {}},
        ))["accepted"]
        assert source.calls[-1] == ("media_player", "media_pause", {"entity_id": "media_player.station"})
        service.close()

    asyncio.run(run())


def vendor_tile(entity: str, uid: str, sync: str) -> dict:
    """Колонка системы со СВОИМ мультирумом: одна система — группирует штатный `join` HA."""
    return {
        "id": uid, "domain": "media_player", "entityId": entity,
        "stream": {"type": "music", "sync": sync},
        "commands": {
            "play_stream": {
                "domain": "media_player", "service": "play_media", "label": "Радиостанция",
                "fields": {"media_content_id": {"type": "string"}, "media_content_type": {"type": "string"}},
            },
            "join": {"domain": "media_player", "service": "join",
                     "fields": {"group_members": {"type": "list", "of": {"type": "string"}}}},
            "unjoin": {"domain": "media_player", "service": "unjoin"},
        },
    }


def test_одна_система_группируется_вендором_через_ha(tmp_path):
    """Sonos+Sonos и HEOS+HEOS сводит ВЕНДОР, а не мы (заказчик, с самого начала).

    ⚠ Живой отказ: дом перехватывал `join` у любой плитки со `stream`, и до HA команда не доходила —
    «сонос+сонос не работает». `None` здесь означает «команда не наша»: `core/ops.py` позовёт
    штатную службу HA, и сработает вендорская синхронность.
    """

    async def run():
        service, home, _catalog, streams = setup(tmp_path)
        sonos = vendor_tile("media_player.living", "sonos", "sync:sonos")
        sonos_2 = vendor_tile("media_player.hall", "sonos2", "sync:sonos")
        home.data["tiles"] += [sonos, sonos_2]
        home.source.states["media_player.living"] = PlainState("playing")
        # Все участники — ОДНА система: команда не наша, её исполняет HA.
        answer = await service.group_command(
            sonos, "join", {"group_members": ["media_player.hall"]}, None,
        )
        assert answer is None
        assert service.groups == {}
        assert home.source.calls == []

    asyncio.run(run())


def test_разные_системы_группирует_наш_поток(tmp_path):
    """Sonos+HEOS — ЗАКОННАЯ группа, и собираем её мы (заказчик, с самого начала).

    ⚠ «Колонки одного вендора группируются средствами вендора, колонки разных вендоров — нашим
    мини-MA с выравниванием задержек». HA вне одной интеграции не группирует вовсе
    (`entity-media-player.md`), поэтому смешанный `join` в HA отклонился бы — жилец видел бы
    «Home Assistant отклонил команду». Группу ведёт дом: участнику уходит адрес потока ведущего.
    """

    async def run():
        service, home, _catalog, streams = setup(tmp_path)
        sonos = vendor_tile("media_player.living", "sonos", "sync:sonos")
        heos = vendor_tile("media_player.hall", "heos", "sync:heos")
        home.data["tiles"] += [sonos, heos]
        home.source.states["media_player.living"] = PlainState("idle")
        home.source.states["media_player.hall"] = PlainState("idle")
        fields = {"media_content_id": "https://cdn.example/rock.aac", "media_content_type": "audio/aac"}
        # Ведущий — ПЕРВЫЙ, кого отметил жилец: музыку включили на нём.
        await service.call(False, {"action": "play_station", "player": "sonos",
                                   "station": {"id": "rock", "title": "Рок", "url": fields["media_content_id"]},
                                   "fields": fields})
        answer = await service.group_command(
            sonos, "join", {"group_members": ["media_player.hall"]}, None,
        )
        assert answer and answer["accepted"]
        assert service.groups == {"media_player.living": ["media_player.hall"]}
        # Ведущий HEOS получил ТОТ ЖЕ поток дома — синхронность между системами держим мы.
        assert home.source.calls[-1] == (
            "media_player", "play_media",
            {
                "entity_id": "media_player.hall",
                "media_content_id": streams.address(streams.session("sonos"), "media_player.hall"),
                "media_content_type": "music",
            },
        )

    asyncio.run(run())


def test_колонки_второй_системы_сводит_вендор_вокруг_её_ведущего(tmp_path):
    """Заказчик 2026-11-23: «ведущий одной системы — с ведущим другой; остальные колонки системы
    раздать, если пользователь их отметил».

    ⚠ Наш поток получает ТОЛЬКО первый отмеченный колонки системы: остальные её колонки вендор
    сводит вокруг него сам, штатным `join` HA. Иначе мы забирали бы у вендора его работу, и его
    внутренняя синхронность ломалась бы.
    """

    async def run():
        service, home, _catalog, streams = setup(tmp_path)
        sonos = vendor_tile("media_player.sonos_head", "s1", "sync:sonos")
        sonos_2 = vendor_tile("media_player.sonos_2", "s2", "sync:sonos")
        heos = vendor_tile("media_player.heos", "h1", "sync:heos")
        home.data["tiles"] += [sonos, sonos_2, heos]
        for entity in ("media_player.sonos_head", "media_player.sonos_2", "media_player.heos"):
            home.source.states[entity] = PlainState("idle")
        fields = {"media_content_id": "https://cdn.example/rock.aac", "media_content_type": "audio/aac"}
        await service.call(False, {"action": "play_station", "player": "s1",
                                   "station": {"id": "rock", "title": "Рок", "url": fields["media_content_id"]},
                                   "fields": fields})
        await service.group_command(
            sonos, "join",
            {"group_members": ["media_player.sonos_2", "media_player.heos"]}, None,
        )
        # Наш поток — ведущему HEOS (вторая система)…
        assert ("media_player", "play_media", {
            "entity_id": "media_player.heos",
            "media_content_id": streams.address(streams.session("s1"), "media_player.heos"),
            "media_content_type": "music",
        }) in home.source.calls
        # …а вторая колонка Sonos присоединена ВЕНДОРОМ к ведущему своей системы, а не потоком дома.
        assert ("media_player", "join", {
            "entity_id": "media_player.sonos_head", "group_members": ["media_player.sonos_2"],
        }) in home.source.calls
        stream_to_sonos_2 = [
            call for call in home.source.calls
            if call[1] == "play_media" and call[2].get("entity_id") == "media_player.sonos_2"
        ]
        assert stream_to_sonos_2 == []

    asyncio.run(run())


def test_яндекс_станция_своей_группой_идёт_нашим_путём(tmp_path):
    """У станции своего мультирума НЕТ — там работает только наша реализация (заказчик 2026-11-23)."""

    async def run():
        service, home, _catalog, streams = setup(tmp_path)
        home.data["tiles"].append(station_tile())
        home.source.states["media_player.station"] = PlainState("idle")
        fields = {"media_content_id": "https://cdn.example/rock.aac", "media_content_type": "audio/aac"}
        await service.call(False, {"action": "play_station", "player": "echo",
                                   "station": {"id": "rock", "title": "Рок", "url": fields["media_content_id"]},
                                   "fields": fields})
        answer = await service.group_command(
            home.data["tiles"][0], "join", {"group_members": ["media_player.station"]}, None,
        )
        assert answer and answer["accepted"]
        assert service.groups == {"media_player.echo": ["media_player.station"]}

    asyncio.run(run())


def test_вторая_отметка_добавляется_к_группе_а_не_стирает_первую(tmp_path):
    """`join` ДОБАВЛЯЕТ к группе, как служба HA (живой симптом 2026-11-23).

    ⚠ Раньше состав перезаписывался целиком: жилец ставил вторую галку — и первая колонка
    выпадала из группы молча. Симптом был ровно «отметка колонок ничего не включает».
    """

    async def run():
        service, home, _catalog, streams = setup(tmp_path)
        sonos = vendor_tile("media_player.sonos", "sonos", "sync:sonos")
        heos = vendor_tile("media_player.heos", "heos", "sync:heos")
        station = station_tile("media_player.station")
        home.data["tiles"] += [sonos, heos, station]
        for entity in ("media_player.sonos", "media_player.heos", "media_player.station"):
            home.source.states[entity] = PlainState("idle")
        fields = {"media_content_id": "https://cdn.example/rock.aac", "media_content_type": "audio/aac"}
        await service.call(False, {"action": "play_station", "player": "sonos",
                                   "station": {"id": "rock", "title": "Рок", "url": fields["media_content_id"]},
                                   "fields": fields})
        await service.group_command(sonos, "join", {"group_members": ["media_player.heos"]}, None)
        await service.group_command(sonos, "join", {"group_members": ["media_player.station"]}, None)
        assert service.groups == {"media_player.sonos": ["media_player.heos", "media_player.station"]}
        # Повторная отметка того же участника ничего не задваивает и не перезапускает его.
        await service.group_command(sonos, "join", {"group_members": ["media_player.heos"]}, None)
        assert service.groups == {"media_player.sonos": ["media_player.heos", "media_player.station"]}

    asyncio.run(run())


def test_участник_перестаёт_играть_свой_источник(tmp_path):
    """Отмеченная колонка играет НАШ поток, а не свой (живой отказ 2026-11-23).

    ⚠ Дом отдавал участнику адрес сессии, но его собственная очередь оставалась жива: она видела
    «играет» и перезапускала ЕГО источник. Плееры пели разное.
    """

    async def run():
        service, home, _catalog, streams = setup(tmp_path)
        sonos = vendor_tile("media_player.sonos", "s1", "sync:sonos")
        heos = vendor_tile("media_player.heos", "h1", "sync:heos")
        home.data["tiles"] += [sonos, heos]
        for entity in ("media_player.sonos", "media_player.heos"):
            home.source.states[entity] = PlainState("idle")
        fields = {"media_content_id": "https://cdn.example/rock.aac", "media_content_type": "audio/aac"}
        # HEOS играл СВОЁ до отметки: у него заведена своя очередь.
        await service.call(False, {"action": "play_station", "player": "h1",
                                   "station": {"id": "jazz", "title": "Джаз", "url": "https://cdn.example/jazz.aac"},
                                   "fields": fields})
        assert service._players.get("media_player.heos") is not None
        # Ведущий — Sonos: музыку включили на нём.
        await service.call(False, {"action": "play_station", "player": "s1",
                                   "station": {"id": "rock", "title": "Рок", "url": fields["media_content_id"]},
                                   "fields": fields})
        await service.group_command(sonos, "join", {"group_members": ["media_player.heos"]}, None)
        # Своей очереди у участника больше нет — он играет поток ведущего и только его.
        assert service._players.get("media_player.heos") is None
        assert service._served["media_player.heos"] == streams.session("s1").token

    asyncio.run(run())


def test_снятие_отметки_гасит_и_вендорскую_колонку(tmp_path):
    """`unjoin` вендорского участника гасит звук, а не уходит в HA пустым (живой отказ 2026-11-23).

    ⚠ Колонка, сведённая вендором вокруг ведущего, вендорскую группу паузой не покидает: звук
    возвращался на первом же воспроизведении. Её вывод — `unjoin` у неё самой.
    """

    async def run():
        service, home, _catalog, streams = setup(tmp_path)
        sonos = vendor_tile("media_player.sonos_head", "s1", "sync:sonos")
        sonos_2 = vendor_tile("media_player.sonos_2", "s2", "sync:sonos")
        heos = vendor_tile("media_player.heos", "h1", "sync:heos")
        home.data["tiles"] += [sonos, sonos_2, heos]
        for entity in ("media_player.sonos_head", "media_player.sonos_2", "media_player.heos"):
            home.source.states[entity] = PlainState("idle")
        fields = {"media_content_id": "https://cdn.example/rock.aac", "media_content_type": "audio/aac"}
        await service.call(False, {"action": "play_station", "player": "s1",
                                   "station": {"id": "rock", "title": "Рок", "url": fields["media_content_id"]},
                                   "fields": fields})
        await service.group_command(
            sonos, "join", {"group_members": ["media_player.sonos_2", "media_player.heos"]}, None,
        )
        assert service._vendor == {"media_player.sonos_2": "media_player.sonos_head"}
        # Отметку снимают со ВТОРОЙ колонки Sonos — она в группе, хоть и не ведущая.
        answer = await service.group_command(
            sonos_2, "unjoin", {}, None,
        )
        assert answer and answer["accepted"]
        assert ("media_player", "unjoin", {"entity_id": "media_player.sonos_2"}) in home.source.calls
        assert service._vendor == {}

    asyncio.run(run())


def test_вендорский_участник_виден_в_составе_группы(tmp_path):
    """Галка обязана стоять на отмеченной колонке (живой отказ 2026-11-23).

    ⚠ Колонки, сведённые вендором, не попадали в `group_members`: приложение показывало группу из
    одного ведущего, и снять отметку с такой колонки было нечем.
    """

    async def run():
        service, home, _catalog, streams = setup(tmp_path)
        sonos = vendor_tile("media_player.sonos_head", "s1", "sync:sonos")
        sonos_2 = vendor_tile("media_player.sonos_2", "s2", "sync:sonos")
        heos = vendor_tile("media_player.heos", "h1", "sync:heos")
        home.data["tiles"] += [sonos, sonos_2, heos]
        for entity in ("media_player.sonos_head", "media_player.sonos_2", "media_player.heos"):
            home.source.states[entity] = PlainState("idle")
        fields = {"media_content_id": "https://cdn.example/rock.aac", "media_content_type": "audio/aac"}
        await service.call(False, {"action": "play_station", "player": "s1",
                                   "station": {"id": "rock", "title": "Рок", "url": fields["media_content_id"]},
                                   "fields": fields})
        await service.group_command(
            sonos, "join", {"group_members": ["media_player.sonos_2", "media_player.heos"]}, None,
        )
        # Порядок — как жилец отметил: ведущий, затем отмеченные колонки по порядку.
        line = ["media_player.sonos_head", "media_player.sonos_2", "media_player.heos"]
        assert service.state_groups()["media_player.sonos_2"] == line
        assert service.state_groups()["media_player.heos"] == line

    asyncio.run(run())


def test_смена_трека_не_переотдаёт_поток_участнику(tmp_path):
    """Адрес один на сессию: элементы сменяются ВНУТРИ него (живой отказ 2026-11-23).

    ⚠ Повторная `play_media` на каждой смене трека перезапускала плееру поток: колонки стартовали
    вразнобой и расходились — «плееры играют разные источники».
    """

    async def run():
        service, home, _catalog, streams = setup(tmp_path)
        sonos = vendor_tile("media_player.sonos", "s1", "sync:sonos")
        heos = vendor_tile("media_player.heos", "h1", "sync:heos")
        home.data["tiles"] += [sonos, heos]
        for entity in ("media_player.sonos", "media_player.heos"):
            home.source.states[entity] = PlainState("idle")
        fields = {"media_content_id": "https://cdn.example/rock.aac", "media_content_type": "audio/aac"}
        await service.call(False, {"action": "play_station", "player": "s1",
                                   "station": {"id": "rock", "title": "Рок", "url": fields["media_content_id"]},
                                   "fields": fields})
        await service.group_command(sonos, "join", {"group_members": ["media_player.heos"]}, None)
        given = [c for c in home.source.calls if c[1] == "play_media" and c[2].get("entity_id") == "media_player.heos"]
        assert len(given) == 1
        # Поток читается (так его видит дом) — новый трек на ведущем не должен слать участнику команду.
        streams.session("s1").reading = True
        await service.call(False, {"action": "play_station", "player": "s1",
                                   "station": {"id": "pop", "title": "Поп", "url": fields["media_content_id"]},
                                   "fields": fields})
        after = [c for c in home.source.calls if c[1] == "play_media" and c[2].get("entity_id") == "media_player.heos"]
        assert len(after) == 1

    asyncio.run(run())


def station_tile(entity: str = "media_player.station") -> dict:
    """Яндекс Станция: объявляет СВОИ типы потока (`stream.mp3`/`stream.aac`), WAV ей не отдать."""
    return {
        "id": "station", "domain": "media_player", "entityId": entity,
        "stream": {"type": "stream.{ext}", "needsKnownState": True, "nativeTrackType": "track"},
        "commands": {
            "play_stream": {
                "domain": "media_player", "service": "play_media", "label": "Радиостанция",
                "fields": {
                    "media_content_id": {"type": "string"},
                    "media_content_type": {"type": "string", "options": ["stream.aac", "stream.mp3"]},
                },
            },
            "unjoin": {"domain": "media_player", "service": "unjoin", "label": "Выход из группы"},
        },
    }


def test_яндекс_станция_встаёт_в_группу_проводом_mp3(tmp_path):
    """Станция в группе: участник — любой плеер со `stream`, а провод группы — MP3 (её типы)."""

    async def run():
        service, home, _catalog, streams = setup(tmp_path)
        source = home.source
        home.data["tiles"].append(station_tile())
        source.states["media_player.station"] = PlainState("idle")
        station = {"id": "rock", "title": "Рок", "url": "https://cdn.example/rock.aac"}
        await service.call(False, {"action": "play_station", "player": "echo", "station": station,
                                   "fields": {"media_content_id": station["url"], "media_content_type": "audio/aac"}})
        session = streams.session("echo")
        address = streams.address(session, "media_player.station")
        assert (await service.group_command(
            home.data["tiles"][0], "join", {"group_members": ["media_player.station"]}, None,
        ))["accepted"]
        # Тип — из списка САМОЙ станции, а не наш WAV: иначе она поток не возьмёт.
        assert source.calls[-1] == ("media_player", "play_media", {
            "entity_id": "media_player.station", "media_content_id": address, "media_content_type": "stream.mp3",
        })
        # В облачном режиме (`assumed_state`) станция поток не примет — честный отказ, а не тишина.
        source.states["media_player.station"] = PlainState("playing", {"assumed_state": True})
        with pytest.raises(OpError):
            await service.group_command(home.data["tiles"][0], "join", {"group_members": ["media_player.station"]}, None)
        service.close()

    asyncio.run(run())


def test_поток_в_mp3_идёт_через_кодировщик():
    async def run():
        session = FlowSession("t")
        session.codec = "mp3"
        session.set_items((SimpleNamespace(source="a"),), 0)
        written: list[bytes] = []
        fed: list[bytes] = []

        async def write(chunk):
            written.append(chunk)

        async def source_of(item):
            return item.source

        async def decoder(url):
            return FakeProcess([pcm(1000, 8)])

        class Encoder:
            async def write(self, chunk):
                fed.append(chunk)
                await write(b"MP3")

            async def stop(self):
                await write(b"END")

            async def read(self, size):
                return b""

        async def encoder(_write):
            return Encoder()

        session.source_of = source_of
        await session.serve(write, decoder, encoder)
        # PCM ушёл кодировщику, плееру — его выход (никакого WAV-заголовка).
        assert fed and b"".join(written) != b""
        assert fed and written[-1] == b"END"

    asyncio.run(run())


def test_группа_читает_один_поток_и_очередь_идёт_на_шаг():
    """Двое читателей одного адреса (группа): декодирование ОДНО, очередь двигается ОДИН раз."""

    async def run():
        session = FlowSession("t")
        session.set_items((SimpleNamespace(source="a"), SimpleNamespace(source="b")), 0)
        asked: list[str] = []
        ends = 0

        async def source_of(item):
            return item.source

        async def decoder(url):
            asked.append(url)
            return FakeProcess([pcm(1000, 8)])

        async def on_end():
            nonlocal ends
            ends += 1
            if session.index + 1 < len(session.items):
                session.set_items(session.items, session.index + 1)

        session.source_of, session.on_end = source_of, on_end

        async def read(target: list[bytes]) -> None:
            async def write(chunk):
                target.append(chunk)
                await asyncio.sleep(0)

            await session.serve(write, decoder)

        first, second = [], []
        await asyncio.gather(read(first), read(second))
        # ⚠ Ради этого и сделан насос: иначе каждый участник декодировал бы сам и двигал очередь.
        assert asked == ["a", "b"]
        assert ends == 2
        assert b"".join(first) == b"".join(second)

    asyncio.run(run())


async def view(service, player):
    return (await service.call(False, {"action": "playback_state"}))["players"].get(player)


def test_поток_фейдит_стык_и_на_смене_станции(tmp_path):
    """Фейд — на ЛЮБОМ стыке элементов: и трек доигран, и станцию сменили на ходу."""

    async def run():
        service, home, _catalog, streams = setup(tmp_path)
        session = streams.session("echo")
        session.fade_bytes = 8  # 4 отсчёта PCM — короткое окно, чтобы посчитать ожидаемое
        first = pcm(1000, 8)
        second = pcm(2000, 8)
        session.set_items((SimpleNamespace(id="station:a", source="a"),), 0)
        asked: list[str] = []

        async def source_of(item):
            return item.source

        async def decoder(url):
            asked.append(url)
            return FakeProcess([first if url == "a" else second])

        session.source_of = source_of
        written: list[bytes] = []

        async def write(chunk):
            written.append(chunk)
            await asyncio.sleep(0)
            # Первую станцию слушают недолго: её сменяют, не доиграв (как станцию в каталоге).
            if chunk == first[: len(first) - session.fade_bytes]:
                session.set_items((SimpleNamespace(id="station:b", source="b"),), 0)

        await session.serve(write, decoder)
        await asyncio.sleep(0)

        audio = b"".join(written)
        # Бананов не теряем: 16 + 16 байт минус окно фейда (8) = 24.
        assert len(audio) == 24
        # Первая станция гаснет, вторая нарастает: 1000 → 1000, 1250, 1500, 1750.
        assert samples(audio[8:16]) == [1000, 1250, 1500, 1750]
        assert samples(audio[16:]) == [2000] * 4
        service.close()

    asyncio.run(run())


def test_задержка_плеера_сдвигает_его_чтение_и_меняется_на_ходу():
    """Задержка — на ПЛЕЕРА и живая: инсталлятор крутит её в составе, музыка играет."""

    async def run():
        session = FlowSession("t")
        session.set_items((SimpleNamespace(source="a"),), 0)
        asked: list[str] = []
        seen: list[str] = []
        value = [40.0]
        written: list[bytes] = []

        async def source_of(item):
            return item.source

        async def decoder(url):
            asked.append(url)
            return FakeProcess([b"AA" * 8] * 4, piece=8)

        def delay_of(player: str) -> float:
            seen.append(player)
            return value[0]

        async def write(chunk):
            written.append(chunk)
            await asyncio.sleep(0)
            value[0] = 0.0  # задержку уменьшили на ходу — лишнее выбрасываем

        session.source_of, session.delay_of = source_of, delay_of
        started = time.monotonic()
        await session.serve(write, decoder, player="media_player.station")
        elapsed = time.monotonic() - started
        # Задержку спросили у ИМЕННО этого плеера и реально подождали её.
        assert seen and all(player == "media_player.station" for player in seen)
        assert elapsed >= 0.03
        # Сброшенная задержка выбросила хвост: байт записано меньше, чем отдал декодер.
        assert len(b"".join(written)) < 32

    asyncio.run(run())
