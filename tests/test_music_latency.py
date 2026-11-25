"""Задержка системы в потоке дома: предзапас читателей, сдвиг группы и замер щелчками."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from mega_home.core.ops_base import OpError
from mega_home.core.source import PlainState
from mega_home.providers import music_melody
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


def test_мелодия_идёт_без_конца_и_отмечает_каждую_ноту():
    async def run():
        process = music_melody.MelodyProcess()
        frame = 4
        at = 0
        marks: list[tuple[int, int]] = []
        heard = None
        loop_s = music_melody.loop_samples() / FLOW_RATE
        # Два круга с лишним: поток не кончается, ноты идут по партитуре и после конца круга.
        while at < (music_melody.LEAD_S + 2 * loop_s + 1) * FLOW_RATE * frame:
            chunk = await process.read(65536)
            assert chunk
            if process.mark is not None:
                marks.append((at, process.mark))
                heard = heard or chunk
            at += len(chunk)
        onsets = music_melody.score_onsets()
        lead = music_melody.LEAD_S * FLOW_RATE * frame
        expected = [(lead + (cycle * music_melody.loop_samples() + start) * frame, note) for cycle in range(3) for start, note in onsets]
        assert marks == [mark for mark in expected if mark[0] < at]
        assert any(heard)
        await process.stop()
        assert await process.read(65536) == b""

    asyncio.run(run())


def test_круг_мелодии_длиннее_предела_задержки_и_без_повтора_фразы():
    # Сдвиг на повтор фразы набрал бы голоса наравне с верной задержкой (`estimateLatency` приложения).
    loop_ms = music_melody.loop_samples() * 1000 / FLOW_RATE
    assert loop_ms > 8000
    onsets = [(start * 1000 // FLOW_RATE, note) for start, note in music_melody.score_onsets()]
    for shift in range(1, int(loop_ms)):
        same = sum(
            1 for at, note in onsets for other, other_note in onsets
            if note == other_note and (other - at) % int(loop_ms) == shift
        )
        assert same * 2 < len(onsets), shift


def test_ноты_описаны_для_телефона():
    notes = music_melody.melody_description()
    assert len(notes) == len(music_melody.NOTES_HZ)
    assert notes[0] == {"hz": 523.25, "ms": 200, "harmonics": [1.0, 0.6, 0.4, 0.3, 0.2, 0.15], "attackMs": 2, "decayMs": 60}


def _measure_env(tmp_path):
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
    return service, home, readers


def test_замер_отдаёт_ноты_по_ходу_и_гасит_колонку_по_команде(tmp_path, monkeypatch):
    monkeypatch.setattr(music_melody, "LEAD_S", 1.5)  # длиннее предзапаса: ноты — уже в темпе эфира
    monkeypatch.setattr(music_melody, "BEAT_MS", 100)

    async def run():
        service, home, readers = _measure_env(tmp_path)
        with pytest.raises(OpError) as unknown:
            await service.call(False, {"action": "calibration_result", "token": "нет"})
        assert unknown.value.status == 404
        token = (await service.call(False, {"action": "calibration_measure", "player": "echo"}))["token"]
        answer = await service.call(False, {"action": "calibration_result", "token": token})
        assert answer["done"] is False
        for _ in range(100):
            await asyncio.sleep(0.05)
            answer = await service.call(False, {"action": "calibration_result", "token": token})
            if len(answer["sent"]) >= 6:
                break
        assert answer["done"] is False
        assert answer["notes"] == music_melody.melody_description()
        sent = answer["sent"]
        expected = music_melody.score_onsets()
        assert [item["note"] for item in sent] == [note for _, note in expected[: len(sent)]]
        times = [item["at"] for item in sent]
        gaps = [(b - a) * 1000 / FLOW_RATE for (a, _), (b, _) in zip(expected, expected[1:])]
        assert all(abs(later - earlier - gap) < 40 for earlier, later, gap in zip(times, times[1:], gaps))
        assert not any(name in ("media_stop", "media_pause") for _, name, _ in home.source.calls)
        assert await service.call(False, {"action": "calibration_stop", "token": token}) == {"stopped": True}
        assert any(name in ("media_stop", "media_pause") for _, name, _ in home.source.calls)
        assert await service.call(False, {"action": "calibration_result", "token": token}) == {"done": True, "stopped": True}
        assert await service.call(False, {"action": "calibration_stop", "token": token}) == {"stopped": True}
        await asyncio.wait_for(asyncio.gather(*readers), timeout=2)

    asyncio.run(run())


def test_брошенный_замер_гасит_сторож(tmp_path, monkeypatch):
    monkeypatch.setattr("mega_home.music_latency.IDLE_S", 0)

    async def run():
        service, home, readers = _measure_env(tmp_path)
        token = (await service.call(False, {"action": "calibration_measure", "player": "echo"}))["token"]
        await asyncio.sleep(1.3)
        answer = await service.call(False, {"action": "calibration_result", "token": token})
        assert answer == {"done": True, "error": "Замер остановлен: приложение перестало его спрашивать"}
        assert any(name in ("media_stop", "media_pause") for _, name, _ in home.source.calls)
        await asyncio.wait_for(asyncio.gather(*readers), timeout=2)

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


def test_отметка_ноты_когда_кадр_ушёл_плееру_а_не_когда_pcm_ушёл_в_кодировщик(monkeypatch):
    """Кодировщик держит в себе запас кадров (трубы и очереди ffmpeg): отметка не должна убегать вперёд."""
    monkeypatch.setattr(music_melody, "LEAD_S", 1.5)
    monkeypatch.setattr(music_melody, "BEAT_MS", 200)
    from collections import deque

    from mega_home.providers.music_flow import MP3_FRAME_SAMPLES

    hold = 10  # насос держит запас не больше 0,3 с (12 кадров): заглушка — в его пределах
    frame_bytes = MP3_FRAME_SAMPLES * 4

    async def run():
        loop = asyncio.get_running_loop()
        session = FlowSession("t")
        session.set_items((SimpleNamespace(id="calibration", source=None),), 0)

        async def source_of(_item):
            return "calibration"

        async def unused(*_):
            pytest.fail("декодер не нужен")

        async def encoder(write):
            buffer = bytearray()
            frames: deque[bytes] = deque()

            async def push(chunk):
                buffer.extend(chunk)
                while len(buffer) >= frame_bytes:
                    frames.append(bytes(buffer[:frame_bytes]))
                    del buffer[:frame_bytes]
                while len(frames) > hold:
                    await write(frames.popleft())

            async def stop():
                pass

            return SimpleNamespace(write=push, stop=stop)

        received: list[tuple[float, bytes]] = []

        async def sink(chunk):
            received.append((loop.time() * 1000, chunk))
            if len(session.probe_sent) >= 3:
                session.stop()

        session.source_of = source_of
        session.generated = lambda _item: music_melody.MelodyProcess()
        await asyncio.wait_for(session.serve(sink, unused, encoder, "one"), timeout=10)
        # Кадр, где начинается первая нота, — первый ненулевой кадр провода (заглушка не кодирует).
        first = next(at for at, chunk in received if any(chunk))
        assert abs(session.probe_sent[0]["at"] - first) < 30
        onsets = music_melody.score_onsets()
        for (a, _), (b, _), earlier, later in zip(onsets, onsets[1:], session.probe_sent, session.probe_sent[1:]):
            assert abs(later["at"] - earlier["at"] - (b - a) * 1000 / FLOW_RATE) < 30

    asyncio.run(run())


def test_проверка_играет_мелодию_на_колонках_со_сдвигом_по_задержкам(tmp_path):
    async def run():
        service, home, readers = _measure_env(tmp_path)
        home.data["tiles"].append(vendor_tile("media_player.sonos", "s1", "sync:sonos"))
        with pytest.raises(OpError):
            await service.call(False, {"action": "calibration_check", "players": [{"player": "echo", "latencyMs": 100}]})
        with pytest.raises(OpError):
            await service.call(False, {"action": "calibration_check", "players": [
                {"player": "echo", "latencyMs": 100}, {"player": "s1", "latencyMs": 9000}]})
        token = (await service.call(False, {"action": "calibration_check", "players": [
            {"player": "echo", "latencyMs": 100}, {"player": "s1", "latencyMs": 340}]}))["token"]
        played = [data["entity_id"] for _, name, data in home.source.calls if name == "play_media"]
        assert played == ["media_player.echo", "media_player.sonos"]
        measure = service._latency._measures[token]
        assert measure.session.delay_of("media_player.echo") == 240
        assert measure.session.delay_of("media_player.sonos") == 0
        assert await service.call(False, {"action": "calibration_stop", "token": token}) == {"stopped": True}
        stopped = [data["entity_id"] for _, name, data in home.source.calls if name in ("media_stop", "media_pause")]
        assert set(stopped) == {"media_player.echo", "media_player.sonos"}
        await asyncio.wait_for(asyncio.gather(*readers), timeout=2)

    asyncio.run(run())


def test_сдвиг_колонки_в_группе_кадрами_тишины_а_не_паузой(monkeypatch):
    """Пауза записи с пачкой накопленного давала колонке тот же запас в то же время — сдвиг пропадал."""
    from mega_home.providers import music_ffmpeg

    monkeypatch.setattr(music_ffmpeg, "_silent_frame", b"S" * 960)

    async def run():
        session = FlowSession("t")
        session.set_items((SimpleNamespace(id="a", source="a"),), 0)
        session.delay_of = lambda player: 48 if player == "late" else 0

        async def source_of(item):
            return item.source

        async def decoder(_url, _start=0.0):
            return FakeProcess([b"\x01" * 4608 * 4], piece=4608)

        async def encoder(write):
            async def push(chunk):
                await write(b"F" * 960)

            async def stop():
                pass

            return SimpleNamespace(write=push, stop=stop)

        got: list[bytes] = []

        async def sink(chunk):
            got.append(chunk)
            if len(got) >= 3:
                session.stop()

        session.source_of = source_of
        await asyncio.wait_for(session.serve(sink, decoder, encoder, "late"), timeout=2)
        # 48 мс = два кадра тишины ДО звука, сразу, без паузы.
        assert got[:3] == [b"S" * 960, b"S" * 960, b"F" * 960]

    asyncio.run(run())


def test_переподключение_колонки_не_обрыв(tmp_path, monkeypatch):
    async def run():
        service, home, readers = _measure_env(tmp_path)
        token = (await service.call(False, {"action": "calibration_measure", "player": "echo"}))["token"]
        for _ in range(50):
            await asyncio.sleep(0.02)
            if service._latency._measures[token].session.reading:
                break
        session = service._latency._measures[token].session
        session.disconnect("media_player.echo")
        # Миг без чтения — замер идёт дальше.
        assert (await service.call(False, {"action": "calibration_result", "token": token}))["done"] is False
        monkeypatch.setattr("mega_home.music_latency.RECONNECT_GRACE_S", 0)
        answer = await service.call(False, {"action": "calibration_result", "token": token})
        assert answer == {"done": True, "error": "Колонка прервала поток дома"}
        await asyncio.wait_for(asyncio.gather(*readers, return_exceptions=True), timeout=2)

    asyncio.run(run())
