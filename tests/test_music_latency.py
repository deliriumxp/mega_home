"""Задержка системы в потоке дома: предзапас читателей, сдвиг группы и замер щелчками."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from mega_home.core.ops_base import OpError
from mega_home.core.source import PlainState
from mega_home.providers import music_chirp
from mega_home.providers.music_flow import FlowSession
from mega_home.providers.music_ffmpeg import FLOW_RATE

from test_music_flow import FakeProcess, setup, vendor_tile


def test_предзапас_получает_и_второй_читатель():
    async def run():
        session = FlowSession("t")
        session.set_items((SimpleNamespace(source="a"),), 0)
        data = b"".join(bytes([index]) * 5 for index in range(200))

        async def source_of(item):
            return item.source

        async def decoder(_url, _start=0.0):
            return FakeProcess([data], piece=5)

        async def encoder(write):
            async def stop():
                pass
            return SimpleNamespace(write=write, stop=stop)

        first: list[bytes] = []
        second: list[bytes] = []

        async def write_first(chunk):
            first.append(chunk)

        async def write_second(chunk):
            second.append(chunk)
            if len(second) >= 6:
                session.stop()

        session.source_of = source_of
        one = asyncio.create_task(session.serve(write_first, decoder, encoder, "one"))
        while len(first) < 5:
            await asyncio.sleep(0.01)
        seen = len(first)
        await asyncio.wait_for(session.serve(write_second, decoder, encoder, "two"), timeout=2)
        await asyncio.wait_for(one, timeout=2)
        # Второй читатель начал не с головы эфира, а с хвоста уже отданного первому.
        assert second[0] in first[:seen]
        assert sum(chunk in first[:seen] for chunk in second[:5]) >= 3

    asyncio.run(run())


def test_задержка_группы_из_двух_систем_выравнивается_по_замерам(tmp_path):
    service, home, _, _ = setup(tmp_path)
    echo, sonos, heos = "media_player.echo", "media_player.sonos", "media_player.heos"
    home.data["tiles"][0]["stream"]["latencyMs"] = 100
    slow = vendor_tile(sonos, "s1", "sync:sonos")
    slow["stream"]["latencyMs"] = 340
    follower = vendor_tile(heos, "h1", "sync:sonos")
    follower["stream"]["latencyMs"] = 5000
    home.data["tiles"] += [slow, follower]
    service.multiroom.groups[echo] = [sonos, heos]
    service.multiroom._vendor[heos] = sonos
    assert service._delay(echo) == 240
    assert service._delay(sonos) == 0
    # Вендорского участника сводит вендор: его замер в расчёт не идёт и сдвига у него нет.
    assert service._delay(heos) == 0
    service.multiroom.groups.clear()
    assert service._delay(echo) == 0


def test_калибровка_пинг_отвечает_числом(tmp_path):
    async def run():
        service, *_ = setup(tmp_path)
        answer = await service.call(False, {"action": "calibration_ping"})
        assert isinstance(answer["now"], float)

    asyncio.run(run())


def test_щелчки_идут_с_шагом_секунды_и_без_лишнего_звука():
    offsets = music_chirp.click_offsets()
    assert len(offsets) == 5
    assert {b - a for a, b in zip(offsets, offsets[1:])} == {FLOW_RATE * 4}
    pcm, _ = music_chirp.calibration_pcm()
    assert not any(pcm[: offsets[0]])
    assert any(pcm[offsets[0] : offsets[0] + 9600])


def test_замер_отдаёт_времена_пяти_щелчков_и_гасит_плеер(tmp_path, monkeypatch):
    monkeypatch.setattr(music_chirp, "LEAD_S", 1.5)  # длиннее предзапаса: щелчки — уже в темпе эфира
    monkeypatch.setattr(music_chirp, "STEP_S", 0.2)
    monkeypatch.setattr(music_chirp, "TAIL_S", 0.1)

    async def run():
        service, home, _, streams = setup(tmp_path)
        source_call = home.source.call
        readers = []

        async def encoder(write):
            async def stop():
                pass
            return SimpleNamespace(write=write, stop=stop)

        async def unused(*_):
            pytest.fail("Звук калибровки делает дом, декодер не нужен")

        async def sink(_chunk):
            return None

        async def connect(domain, name, data, response, by):
            await source_call(domain, name, data, response, by)
            if name == "play_media":
                session, player = streams.find(data["media_content_id"].rsplit("/", 1)[-1])
                readers.append(asyncio.create_task(session.serve(sink, unused, encoder, player=player)))

        home.source.call = connect
        with pytest.raises(OpError) as unknown:
            await service.call(False, {"action": "calibration_result", "token": "нет"})
        assert unknown.value.status == 404
        token = (await service.call(False, {"action": "calibration_measure", "player": "echo"}))["token"]
        assert (await service.call(False, {"action": "calibration_result", "token": token})) == {"done": False}
        for _ in range(100):
            await asyncio.sleep(0.05)
            answer = await service.call(False, {"action": "calibration_result", "token": token})
            if answer["done"]:
                break
        assert answer["chirp"] == {"fromHz": 2000, "toHz": 6000, "ms": 50}
        sent = answer["sent"]
        assert len(sent) == 5 and sent == sorted(sent)
        assert all(190 <= later - earlier <= 260 for earlier, later in zip(sent, sent[1:]))
        assert any(name in ("media_stop", "media_pause") for _, name, _ in home.source.calls)
        assert await service.call(False, {"action": "calibration_result", "token": token}) == answer
        await asyncio.gather(*readers)

    asyncio.run(run())


def test_колонка_не_подключилась_за_пятнадцать_секунд(tmp_path, monkeypatch):
    monkeypatch.setattr("mega_home.music_latency.CONNECT_TIMEOUT_S", 0)

    async def run():
        service, *_ = setup(tmp_path)
        token = (await service.call(False, {"action": "calibration_measure", "player": "echo"}))["token"]
        answer = await service.call(False, {"action": "calibration_result", "token": token})
        assert answer == {"done": True, "error": "Колонка не начала играть поток дома"}

    asyncio.run(run())


def test_первый_читатель_получает_предзапас_сразу():
    """Первому читателю — тот же секундный предзапас, что позднему из хвоста: сразу, без темпа."""

    async def run():
        from mega_home.providers.music_flow import PREROLL_FRAMES

        session = FlowSession("t")
        session.set_items((SimpleNamespace(source="a"),), 0)
        data = b"".join(bytes([index % 256]) * 5 for index in range(400))

        async def source_of(item):
            return item.source

        async def decoder(_url, _start=0.0):
            return FakeProcess([data], piece=5)

        async def encoder(write):
            async def stop():
                pass
            return SimpleNamespace(write=write, stop=stop)

        got: list[bytes] = []

        async def write(chunk):
            got.append(chunk)

        session.source_of = source_of
        task = asyncio.create_task(session.serve(write, decoder, encoder, "one"))
        # В темпе эфира 42 кадра — это 1 с; предзапас обязан прийти заметно быстрее.
        await asyncio.sleep(0.3)
        assert len(got) >= PREROLL_FRAMES
        assert len(got) < PREROLL_FRAMES + 20
        session.stop()
        await asyncio.wait_for(task, timeout=2)

    asyncio.run(run())
