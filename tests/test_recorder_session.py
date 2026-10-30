"""Сессии просмотра записи (`core/recorder_session.py`): непрерывный MPEG-TS своему go2rtc, перемотка — команда.

⚠ ffmpeg и go2rtc здесь не запускаются: процесс сессии подменён (его stdout наполняет тест), go2rtc — сессией
HTTP, которая запоминает `PUT`/`DELETE`. Что go2rtc держит один поток через перезапуски ffmpeg с продолжением
времени и снимает его, когда уходит последний зритель, проверено пробой 2026-10-26 (спека переделки движка).
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

import pytest
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from fake_host import FakeHost
from mega_home.core import recorder as rec
from mega_home.core import recorder_session as rs
from mega_home.core import storage
from mega_home.core.recorder_index import segment_name

BASE = 1_793_000_000.0
SECTION = {"cameras": [{"id": "onvif-aa", "stream": "onvif_aa", "source": "rtsp://x", "mode": "continuous"}]}


@pytest.fixture(autouse=True)
def _one_disk(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(storage, "mounted", lambda: {"/": ("/dev/sda1", "ext4"), "/proc": ("proc", "proc")})


class _Answer:
    status = 200

    async def __aenter__(self) -> _Answer:
        return self

    async def __aexit__(self, *_args: object) -> None:
        return None


class _Go2rtc:
    def __init__(self) -> None:
        self.puts: list[tuple[str, str]] = []
        self.deletes: list[str] = []

    def put(self, url: str) -> _Answer:
        query = parse_qs(urlparse(url).query)
        self.puts.append((query["name"][0], query["src"][0]))
        return _Answer()

    def delete(self, url: str) -> _Answer:
        self.deletes.append(parse_qs(urlparse(url).query)["src"][0])
        return _Answer()


class _Proc:
    def __init__(self, argv: tuple[str, ...]) -> None:
        self.argv = list(argv)
        self.stdout = asyncio.StreamReader()
        self.stderr = asyncio.StreamReader()
        self.returncode: int | None = None
        self.pid = 5151

    def kill(self) -> None:
        if self.returncode is None:
            self.returncode = -9
            self.stdout.feed_eof()
            self.stderr.feed_eof()

    def terminate(self) -> None:
        self.kill()

    async def wait(self) -> int:
        # Настоящий процесс выходит не мгновенно: ожидание отдаёт цикл событий другим задачам.
        await asyncio.sleep(0.02)
        while self.returncode is None:
            await asyncio.sleep(0.01)
        return self.returncode


class _World:
    """Запись камеры (куски в индексе), подменённые ffmpeg, go2rtc и часы."""

    def __init__(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, gaps: bool = False, audio: bool = True, video_at: float = 0.0,
    ) -> None:
        self.go2rtc = _Go2rtc()
        self.recorder = rec.Recorder(FakeHost(tmp_path, self.go2rtc), lambda _listener: (lambda: None))
        self.recorder.storage = tmp_path
        self.recorder.cameras = {"onvif-aa": rec._Camera(self.recorder, rec.camera_specs(SECTION)[0], tmp_path / ".cache")}  # noqa: SLF001
        index = self.recorder.cameras["onvif-aa"].index
        index.loaded = True
        for begin in [BASE + n * 10 for n in range(30)] + ([BASE + 3600 + n * 10 for n in range(30)] if gaps else []):
            # Файлы кусков — на диске: пропавший кусок сессия узнаёт по диску.
            path = index.add(begin, 10.0)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.touch()
        self.procs: list[_Proc] = []
        self.clock = [100.0]
        monkeypatch.setattr(rs, "clock", lambda: self.clock[0])

        async def keyframe(_recorder: Any, _path: Any, offset: float) -> float:
            return max(offset - 1.5, 0.0)

        async def spawn(*argv: str, **_kwargs: Any) -> _Proc:
            self.procs.append(_Proc(argv))
            return self.procs[-1]

        async def has_audio(_recorder: Any, _path: Any) -> bool:
            return audio

        async def start(_recorder: Any, path: Path) -> float:
            # Видео куска «на часы позже» начала файла — у каждого куска своё (застывшие метки звука).
            return video_at + (int(path.stem.split("-")[0]) % 1000 if video_at else 0)

        self.sessions = rs.PlaybackSessions(
            self.recorder, lambda: 4321, spawn=spawn, keyframe=keyframe, audio=has_audio, start=start,
        )


def _value(argv: list[str], flag: str) -> str:
    return argv[argv.index(flag) + 1]


async def _serve(world: _World, sid: str) -> tuple[TestClient, Any]:
    app = web.Application()

    async def handler(request: web.Request) -> web.StreamResponse:
        return await world.sessions.serve(request.match_info["sid"], request)

    app.router.add_get("/sessions/{sid}.ts", handler)
    client = TestClient(TestServer(app))
    await client.start_server()
    response = await client.get(f"/sessions/{sid}.ts")
    return client, response


async def _until(check: Any) -> None:
    for _ in range(300):
        if check():
            return
        await asyncio.sleep(0.01)
    raise AssertionError("не дождались")


def test_открытие_заводит_поток_go2rtc_и_встаёт_на_ключевой_кадр(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    world = _World(tmp_path, monkeypatch)
    sid, stream, start = asyncio.run(world.sessions.open("onvif-aa", BASE + 25))
    assert stream == f"play_{sid}"
    assert world.go2rtc.puts == [(stream, f"http://127.0.0.1:4321/sessions/{sid}.ts")]
    assert start == pytest.approx(BASE + 23.5)  # ключевой кадр за 1,5 с до метки (подменён)
    # До зрителя ffmpeg не запускается: время `-re` пошло бы в пустоту.
    assert world.procs == []


def test_список_идёт_через_пустоты(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    world = _World(tmp_path, monkeypatch, gaps=True)
    found = rs.playlist_from(world.recorder.cameras["onvif-aa"].index, BASE + 285)
    assert found is not None
    run, inpoint = found
    assert run[0][0] == BASE + 280 and inpoint == pytest.approx(5.0)
    assert any(piece[0] == BASE + 3600 for piece in run)  # следующий участок после часа пустоты — в том же списке


def test_звук_всегда_в_opus_и_дорожка_необязательна() -> None:
    argv = rs.session_command("ffmpeg", Path("/tmp/l.ffconcat"), 0.0)
    assert _value(argv, "-c:a") == "libopus" and "0:a:0?" in argv and _value(argv, "-c:v") == "copy"
    assert argv[argv.index("-f", argv.index("-c:a")) + 1] == "mpegts" and argv[-1] == "pipe:1"
    assert argv.index("-re") < argv.index("-i")
    # Дорожка звука объявлена, а пакетов в ней нет (EYEPLUS): ffmpeg ждал бы звук до первого байта ~30 с.
    silent = rs.session_command("ffmpeg", Path("/tmp/l.ffconcat"), 0.0, audio=False)
    assert "-an" in silent and "0:a:0?" not in silent and "libopus" not in silent


def test_запись_с_пустым_звуком_играется_без_звука(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    world = _World(tmp_path, monkeypatch, audio=False)

    async def scenario() -> list[str]:
        sid, _stream, _start = await world.sessions.open("onvif-aa", BASE + 25)
        client, _response = await _serve(world, sid)
        try:
            await _until(lambda: world.procs)
            return world.procs[0].argv
        finally:
            await world.sessions.close_all()
            await client.close()

    argv = asyncio.run(scenario())
    assert "-an" in argv and "0:a:0?" not in argv


def test_видео_не_с_начала_файла_каждому_куску_своё_начало_и_длина(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Застывшие метки звука (EYEPLUS «Гостиная» .90.250, 2026-10-30): видео куска на 40 000 с позже начала файла, а
    длина файла — по последней метке. Без начала видео и длины по индексу у каждого куска запись вставала на часы."""
    world = _World(tmp_path, monkeypatch, audio=False, video_at=40000.0)

    async def scenario() -> list[str]:
        sid, _stream, _start = await world.sessions.open("onvif-aa", BASE + 25)
        client, _response = await _serve(world, sid)
        try:
            await _until(lambda: world.procs)
            return Path(_value(world.procs[0].argv, "-i")).read_text().splitlines()
        finally:
            await world.sessions.close_all()
            await client.close()

    lines = asyncio.run(scenario())
    files = [line for line in lines if line.startswith("file")]
    assert len(files) == min(rs.OFFSET_PIECES, 28)
    first, second = lines[2:4], lines[5:7]
    start = 40000 + int(Path(files[0][6:-1]).stem.split("-")[0]) % 1000
    # Первый — с ключевого кадра (23,5 из 20…30), дальше — с начала видео куска и на всю его длину.
    assert first == [f"inpoint {start + 3.5:.3f}", "duration 6.500"]
    assert second[0].startswith("inpoint 40") and second[1] == "duration 10.000"


def test_поток_зрителю_идёт_с_правкой_sps(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """SPS с `nal_ref_idc = 2` go2rtc выбрасывает (`ts_sps.py`): зритель получает его с `nal_ref_idc = 3`."""
    world = _World(tmp_path, monkeypatch)
    pes = b"\x00\x00\x01\xe0\x00\x00\x80\x80\x05\x21\x00\x01\x00\x01" + b"\x00\x00\x00\x01\x47\x4d\x00\x29"
    packet = bytes([0x47, 0x41, 0x00, 0x10]) + pes.ljust(184, b"\xff")

    async def scenario() -> bytes:
        sid, _stream, _start = await world.sessions.open("onvif-aa", BASE + 25)
        client, response = await _serve(world, sid)
        try:
            await _until(lambda: world.procs)
            world.procs[0].stdout.feed_data(packet[:100])
            world.procs[0].stdout.feed_data(packet[100:])
            return await response.content.readexactly(188)
        finally:
            await world.sessions.close_all()
            await client.close()

    got = asyncio.run(scenario())
    assert got == packet.replace(b"\x00\x00\x00\x01\x47", b"\x00\x00\x00\x01\x67")


def test_ffmpeg_без_потока_называет_причину_в_журнале(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    world = _World(tmp_path, monkeypatch)
    monkeypatch.setattr(rs, "RETRY_S", 0.3)

    async def scenario() -> None:
        sid, _stream, _start = await world.sessions.open("onvif-aa", BASE + 25)
        client, _response = await _serve(world, sid)
        try:
            await _until(lambda: world.procs)
            # Вывод объекта 2026-10-27: причина — ПЕРВАЯ строка, остальные — её следствия.
            world.procs[0].stderr.feed_data(
                b"[concat @ 0x1] Impossible to open '/media/nvr/cam/033037-8026.mkv'\n"
                b"[in#0 @ 0x2] Error opening input: No such file or directory\n"
                b"Error opening input file /tmp/mega_home_play/x-1.ffconcat.\n"
                b"Error opening input files: No such file or directory\n"
            )
            world.procs[0].kill()
            await asyncio.sleep(0.1)
        finally:
            await world.sessions.close_all()
            await client.close()

    asyncio.run(scenario())
    assert any("Impossible to open '/media/nvr/cam/033037-8026.mkv'" in record.getMessage() for record in caplog.records)


def _after_failure(world: _World) -> list[str]:
    """Сессия с метки BASE+25 (кусок BASE+20 первым), первый ffmpeg падает без байта — список второго."""

    async def scenario() -> list[str]:
        sid, _stream, _start = await world.sessions.open("onvif-aa", BASE + 25)
        client, _response = await _serve(world, sid)
        try:
            await _until(lambda: world.procs)
            world.procs[0].kill()
            await _until(lambda: len(world.procs) > 1)
            return [line for line in Path(_value(world.procs[1].argv, "-i")).read_text().splitlines() if line.startswith("file")]
        finally:
            await world.sessions.close_all()
            await client.close()

    return asyncio.run(scenario())


def test_пропавший_с_диска_кусок_пропускается_и_уходит_из_индекса(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Иначе сессия повторяет отказ ffmpeg на том же куске без конца — у жильца крутилка."""
    world = _World(tmp_path, monkeypatch)
    index = world.recorder.cameras["onvif-aa"].index
    gone, after = index.first_after(BASE + 25, BASE + 100), index.first_after(BASE + 35, BASE + 100)
    assert gone is not None and after is not None and gone[0] == BASE + 20
    gone[2].unlink()
    files = _after_failure(world)
    assert files[0] == f"file '{after[2]}'"
    assert [b for b, _n, _p in index.between(BASE + 15, BASE + 35)] == [BASE + 10, BASE + 30]


def test_пропала_папка_дня_уходит_день_целиком(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Сутки, удалённые руками: пропуск по куску стоил бы отказа ffmpeg на каждый из 8 640 кусков."""
    import shutil

    world = _World(tmp_path, monkeypatch)
    index = world.recorder.cameras["onvif-aa"].index
    first = index.first_after(BASE + 25, BASE + 100)
    assert first is not None
    shutil.rmtree(first[2].parent)

    async def scenario() -> None:
        sid, _stream, _start = await world.sessions.open("onvif-aa", BASE + 25)
        client, _response = await _serve(world, sid)
        try:
            await _until(lambda: world.procs)
            world.procs[0].kill()
            await _until(lambda: not index.days())
        finally:
            await world.sessions.close_all()
            await client.close()

    asyncio.run(scenario())


def test_отвалилось_хранилище_индекс_не_трогается(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Нет каталога камеры — не смонтирован диск: куски не пропали, сессия ждёт их, как прежде."""
    import shutil

    world = _World(tmp_path, monkeypatch)
    index = world.recorder.cameras["onvif-aa"].index
    first = index.first_after(BASE + 25, BASE + 100)
    assert first is not None
    shutil.rmtree(index.root)
    assert _after_failure(world)[0] == f"file '{first[2]}'"
    assert len(index.between(BASE, BASE + 300)) == 30


def test_до_первого_байта_смещение_ноль_а_перемотка_продолжает_время(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    world = _World(tmp_path, monkeypatch)

    async def scenario() -> tuple[str, str, bytes]:
        sid, _stream, _start = await world.sessions.open("onvif-aa", BASE + 25)
        client, response = await _serve(world, sid)
        try:
            await _until(lambda: world.procs)
            first = _value(world.procs[0].argv, "-output_ts_offset")
            world.procs[0].stdout.feed_data(b"A" * 188)
            chunk = await response.content.readexactly(188)
            world.clock[0] += 7.5
            seeking = asyncio.ensure_future(world.sessions.seek(sid, BASE + 120))
            await _until(lambda: len(world.procs) == 2)
            world.procs[1].stdout.feed_data(b"B" * 188)
            await seeking
            return first, _value(world.procs[1].argv, "-output_ts_offset"), chunk + await response.content.readexactly(188)
        finally:
            await world.sessions.close_all()
            await client.close()

    first, second, data = asyncio.run(scenario())
    assert first == "0.000"
    assert second == "7.500"
    assert data == b"A" * 188 + b"B" * 188
    assert world.procs[0].returncode is not None  # прежний ffmpeg снят


def test_пауза_снимает_процесс_и_не_рвёт_сессию(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    world = _World(tmp_path, monkeypatch)

    async def scenario() -> tuple[int | None, bool, int]:
        sid, _stream, _start = await world.sessions.open("onvif-aa", BASE + 25)
        client, response = await _serve(world, sid)
        try:
            await _until(lambda: world.procs)
            world.procs[0].stdout.feed_data(b"A" * 188)
            await response.content.readexactly(188)
            await world.sessions.pause(sid)
            paused_code = world.procs[0].returncode
            alive = sid in world.sessions.sessions
            seeking = asyncio.ensure_future(world.sessions.seek(sid, BASE + 40))
            await _until(lambda: len(world.procs) == 2)
            world.procs[1].stdout.feed_data(b"C" * 188)
            await seeking
            return paused_code, alive, len(world.procs)
        finally:
            await world.sessions.close_all()
            await client.close()

    code, alive, count = asyncio.run(scenario())
    assert code is not None and alive and count == 2


def test_ушедший_читатель_закрывает_сессию_через_грейс(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    world = _World(tmp_path, monkeypatch)

    async def scenario() -> tuple[bool, bool, list[str]]:
        sid, stream, _start = await world.sessions.open("onvif-aa", BASE + 25)
        client, response = await _serve(world, sid)
        await _until(lambda: world.procs)
        world.procs[0].stdout.feed_data(b"A" * 188)
        await response.content.readexactly(188)
        response.close()
        await client.close()
        await _until(lambda: world.procs[0].returncode is not None)
        world.clock[0] += rs.GRACE_S - 1
        await world.sessions.sweep()
        kept = sid in world.sessions.sessions
        world.clock[0] += 2
        await world.sessions.sweep()
        return kept, sid in world.sessions.sessions, world.go2rtc.deletes

    kept, still, deletes = asyncio.run(scenario())
    assert kept is True and still is False
    assert deletes and deletes[-1].startswith("play_")


def test_сессия_без_читателя_закрывается_по_сроку(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    world = _World(tmp_path, monkeypatch)

    async def scenario() -> tuple[bool, bool]:
        sid, _stream, _start = await world.sessions.open("onvif-aa", BASE + 25)
        world.clock[0] += rs.IDLE_S - 1
        await world.sessions.sweep()
        kept = sid in world.sessions.sessions
        world.clock[0] += 2
        await world.sessions.sweep()
        return kept, sid in world.sessions.sessions

    assert asyncio.run(scenario()) == (True, False)


def test_предел_сессий_снимает_самую_старую_той_же_камеры(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    world = _World(tmp_path, monkeypatch)

    async def scenario() -> list[str]:
        ids = []
        for n in range(rs.MAX_PER_CAMERA + 1):
            world.clock[0] += 1
            sid, _stream, _start = await world.sessions.open("onvif-aa", BASE + 25 + n)
            ids.append(sid)
        return ids

    ids = asyncio.run(scenario())
    assert ids[0] not in world.sessions.sessions and all(sid in world.sessions.sessions for sid in ids[1:])


def test_смотрящую_сессию_не_вытесняет_новое_открытие(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Ревью 2026-10-27: четвёртое открытие той же камеры снимало сессию, которую СМОТРЯТ (самая давняя перемотка), —
    тот зритель вставал, его перемотка вытесняла следующего: каскад. Вытесняется только брошенная; все смотрятся —
    новое открытие получает отказ."""
    world = _World(tmp_path, monkeypatch)

    async def scenario() -> tuple[str, list[str]]:
        ids = []
        for n in range(rs.MAX_PER_CAMERA):
            world.clock[0] += 1
            ids.append((await world.sessions.open("onvif-aa", BASE + 25 + n))[0])
        for sid in ids[:-1]:
            world.sessions.sessions[sid].reader = object()  # type: ignore[assignment]
        fourth = (await world.sessions.open("onvif-aa", BASE + 40))[0]
        world.sessions.sessions[fourth].reader = object()  # type: ignore[assignment]
        with pytest.raises(rs.SessionsBusy):
            await world.sessions.open("onvif-aa", BASE + 45)
        return fourth, ids

    fourth, ids = asyncio.run(scenario())
    assert set(world.sessions.sessions) == {*ids[:-1], fourth}


def test_обрыв_на_одном_месте_дважды_пропускает_кусок(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Ревью 2026-10-27: ошибка чтения посреди куска — ffmpeg отдал байты и упал, место не сдвинулось, перепланирование
    с того же ключевого кадра — жилец видел одни и те же секунды по кругу, журнал молчал. Дважды на том же месте — кусок
    пропускается, причина в журнале."""
    world = _World(tmp_path, monkeypatch)

    async def scenario() -> list[str]:
        sid, _stream, _start = await world.sessions.open("onvif-aa", BASE + 25)
        client, response = await _serve(world, sid)
        try:
            for n in range(rs.STUCK_TRIES):
                await _until(lambda: len(world.procs) > n)
                world.procs[n].stdout.feed_data(b"A" * 188)
                await response.content.readexactly(188)
                world.procs[n].kill()  # байты пошли, и ffmpeg упал на битом месте
            await _until(lambda: len(world.procs) > rs.STUCK_TRIES)
            return Path(_value(world.procs[rs.STUCK_TRIES].argv, "-i")).read_text().splitlines()
        finally:
            await world.sessions.close_all()
            await client.close()

    listed = asyncio.run(scenario())
    index = world.recorder.cameras["onvif-aa"].index
    after = index.first_after(BASE + 20, BASE + 100, started=True)  # кусок после битого [20, 30)
    assert after is not None and after[0] == BASE + 30
    assert listed[1] == f"file '{after[2]}'" and "inpoint" not in listed[2]


def test_конец_списка_у_живого_края_ждёт_новых_кусков(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    world = _World(tmp_path, monkeypatch)
    monkeypatch.setattr(rs, "RETRY_S", 0.05)

    async def scenario() -> tuple[int, str]:
        sid, _stream, _start = await world.sessions.open("onvif-aa", BASE + 285)
        client, response = await _serve(world, sid)
        try:
            await _until(lambda: world.procs)
            world.procs[0].stdout.feed_data(b"A" * 188)
            await response.content.readexactly(188)
            world.clock[0] += 20  # список (16,5 с с ключевого кадра) доигран в реальном времени
            world.procs[0].kill()  # ffmpeg дошёл до конца списка: новых кусков ещё нет
            await asyncio.sleep(0.2)
            waiting = len(world.procs)
            world.recorder.cameras["onvif-aa"].index.add(BASE + 300, 10.0)
            await _until(lambda: len(world.procs) == 2)
            listed = Path(_value(world.procs[1].argv, "-i")).read_text()
            return waiting, listed
        finally:
            await world.sessions.close_all()
            await client.close()

    waiting, listed = asyncio.run(scenario())
    assert waiting == 1  # пока новых кусков нет, ffmpeg не перезапускается вхолостую
    fresh = world.recorder.cameras["onvif-aa"].index.root / rec.day_of(BASE + 300) / segment_name(BASE + 300, 10000)
    assert listed.splitlines()[1] == f"file '{fresh}'"  # продолжение — с нового куска
    assert "inpoint" not in listed  # и с его начала


def test_падающий_ffmpeg_не_крутится_без_паузы(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    world = _World(tmp_path, monkeypatch)
    monkeypatch.setattr(rs, "RETRY_S", 0.3)

    async def scenario() -> int:
        sid, _stream, _start = await world.sessions.open("onvif-aa", BASE + 25)
        client, _response = await _serve(world, sid)
        try:
            for _ in range(3):
                await _until(lambda: world.procs and world.procs[-1].returncode is None)
                world.procs[-1].kill()  # хранилище отвалилось: ни байта
                await asyncio.sleep(0.02)
            await asyncio.sleep(0.1)
            return len(world.procs)
        finally:
            await world.sessions.close_all()
            await client.close()

    assert asyncio.run(scenario()) <= 3


def test_второй_читатель_отвергается(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    world = _World(tmp_path, monkeypatch)

    async def scenario() -> int:
        sid, _stream, _start = await world.sessions.open("onvif-aa", BASE + 25)
        client, _response = await _serve(world, sid)
        try:
            await _until(lambda: world.procs)
            second = await client.get(f"/sessions/{sid}.ts")
            return second.status
        finally:
            await world.sessions.close_all()
            await client.close()

    assert asyncio.run(scenario()) == 409


def test_закрытие_снимает_поток_go2rtc_и_список(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    world = _World(tmp_path, monkeypatch)

    async def scenario() -> tuple[str, list[str], bool]:
        sid, stream, _start = await world.sessions.open("onvif-aa", BASE + 25)
        listed = world.sessions.sessions[sid].playlist
        await world.sessions.close(sid)
        return stream, world.go2rtc.deletes, listed.exists()

    stream, deletes, exists = asyncio.run(scenario())
    assert deletes == [stream] and exists is False


def test_нет_записи_у_метки(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    world = _World(tmp_path, monkeypatch)
    world.recorder.cameras["onvif-aa"].index._days.clear()  # noqa: SLF001
    with pytest.raises(LookupError):
        asyncio.run(world.sessions.open("onvif-aa", BASE + 25))


def test_перемотка_не_отдаёт_место_прежней_подаче(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Снятый перемоткой ffmpeg — не обрыв посреди списка: прежняя подача не переписывает список старым
    местом и не заводит свой процесс поверх нового (живой прогон: синий пришёл через 3 с вместо 0,5)."""
    world = _World(tmp_path, monkeypatch)

    async def scenario() -> tuple[int, str, float]:
        sid, _stream, _start = await world.sessions.open("onvif-aa", BASE + 25)
        client, response = await _serve(world, sid)
        try:
            await _until(lambda: world.procs)
            world.procs[0].stdout.feed_data(b"A" * 188)
            await response.content.readexactly(188)
            seeking = asyncio.ensure_future(world.sessions.seek(sid, BASE + 125))
            await _until(lambda: len(world.procs) >= 2)
            world.procs[1].stdout.feed_data(b"B" * 188)
            started = asyncio.get_running_loop().time()
            await seeking
            answered = asyncio.get_running_loop().time() - started
            await asyncio.sleep(0.2)
            return len(world.procs), Path(_value(world.procs[1].argv, "-i")).read_text(), answered
        finally:
            await world.sessions.close_all()
            await client.close()

    count, listed, answered = asyncio.run(scenario())
    index = world.recorder.cameras["onvif-aa"].index
    assert count == 2
    assert listed.splitlines()[1] == f"file '{index.root / rec.day_of(BASE + 120) / segment_name(BASE + 120, 10000)}'"
    assert answered < 1.0  # ответ — по первым байтам нового места, а не по сроку `FIRST_BYTES_S`


def test_пока_собирается_новое_место_прежнее_играет(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Поиск ключевого кадра и запись списка — до снятия прежнего ffmpeg: экран не стоит без кадров."""
    world = _World(tmp_path, monkeypatch)
    probing = asyncio.Event()

    async def slow_keyframe(_recorder: Any, _path: Any, offset: float) -> float:
        probing.set()
        await asyncio.sleep(0.3)
        return offset

    async def scenario() -> tuple[int | None, int]:
        sid, _stream, _start = await world.sessions.open("onvif-aa", BASE + 25)
        client, response = await _serve(world, sid)
        try:
            await _until(lambda: world.procs)
            world.procs[0].stdout.feed_data(b"A" * 188)
            await response.content.readexactly(188)
            world.sessions.keyframe = slow_keyframe
            seeking = asyncio.ensure_future(world.sessions.seek(sid, BASE + 125))
            await probing.wait()
            during = world.procs[0].returncode
            await _until(lambda: len(world.procs) == 2)
            world.procs[1].stdout.feed_data(b"B" * 188)
            await seeking
            return during, len(world.procs)
        finally:
            await world.sessions.close_all()
            await client.close()

    during, count = asyncio.run(scenario())
    assert during is None  # прежний ffmpeg жив, пока ищется новое место
    assert count == 2


def test_метка_далеко_после_последней_записи_записи_нет(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Объект .90.250 (2026-10-28): камера перестала писать, сессия на событие через сутки вставала на вчерашний
    последний кусок, и жилец смотрел вчерашнее. Назад годится только кусок у самой метки (живой край)."""
    world = _World(tmp_path, monkeypatch)
    index = world.recorder.cameras["onvif-aa"].index
    end = BASE + 300  # 30 кусков по 10 с
    assert rs.playlist_from(index, end + 2 * 3600) is None
    with pytest.raises(LookupError):
        asyncio.run(world.sessions.open("onvif-aa", end + 2 * 3600))
    near = rs.playlist_from(index, end + 60)  # идущий кусок ещё не в индексе
    assert near is not None and near[0][0][0] == BASE + 290
