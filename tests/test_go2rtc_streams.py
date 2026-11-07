"""Поток своего go2rtc заводится одним путём (`core/go2rtc_streams.py`).

⚠ Случай .90.250 2026-11-07: go2rtc 1.9.15 отдаёт адрес продюсера как `rtsp://***@…`, сравнение «как записано» не
сходилось, каждый перезапуск ffmpeg перезаводил потоки камеры — запись оставалась на старом объекте, а живое HD
открывало к Tapo второе подключение и буферизовало.
"""

from __future__ import annotations

import asyncio
from urllib.parse import parse_qs, urlparse

import pytest

from mega_home.core import go2rtc_streams
from mega_home.core.go2rtc_guard import Producer, orphans

SOURCE = "rtsp://admin:Pa%23ss!@10.0.0.5:554/stream1"


@pytest.fixture(autouse=True)
def _forget() -> None:
    go2rtc_streams.registered.clear()  # noqa: SLF001


class _Answer:
    status = 200

    def __init__(self, body: object = None) -> None:
        self.body = body

    async def __aenter__(self) -> _Answer:
        return self

    async def __aexit__(self, *_args: object) -> None:
        return None

    async def json(self, **_kwargs: object) -> object:
        return self.body


class _Session:
    def __init__(self, listing: dict) -> None:
        self.listing = listing
        self.puts: list[tuple[str, str]] = []

    def get(self, _url: str, **_kwargs: object) -> _Answer:
        return _Answer(self.listing)

    def put(self, url: str, **_kwargs: object) -> _Answer:
        query = parse_qs(urlparse(url).query)
        self.puts.append((query["name"][0], query["src"][0]))
        return _Answer()


def _listing(url: str) -> dict:
    return {"cam": {"producers": [{"url": url}]}}


def test_маска_как_у_go2rtc() -> None:
    assert go2rtc_streams.masked(SOURCE) == "rtsp://***@10.0.0.5:554/stream1"
    assert go2rtc_streams.masked("rtsp://***@10.0.0.5:554/stream1") == "rtsp://***@10.0.0.5:554/stream1"
    assert go2rtc_streams.masked("rtsp://10.0.0.5/stream1") == "rtsp://10.0.0.5/stream1"


@pytest.mark.parametrize("shown", [SOURCE, "rtsp://***@10.0.0.5:554/stream1"], ids=["1.9.14", "1.9.15"])
def test_идущий_поток_не_перезаводится(shown: str) -> None:
    session = _Session(_listing(shown))
    asyncio.run(go2rtc_streams.ensure(session, "http://go2rtc", "cam", SOURCE))
    assert session.puts == []


def test_пропавший_и_сменивший_путь_заводятся() -> None:
    gone = _Session({})
    asyncio.run(go2rtc_streams.ensure(gone, "http://go2rtc", "cam", SOURCE))
    assert gone.puts == [("cam", SOURCE)]
    go2rtc_streams.registered.clear()  # перезагрузка записи интеграции: потока процесс не заводил
    moved = _Session(_listing("rtsp://***@10.0.0.5:554/stream2"))
    asyncio.run(go2rtc_streams.ensure(moved, "http://go2rtc", "cam", SOURCE))
    assert moved.puts == [("cam", SOURCE)]


def test_смена_учётки_заводит_поток_хотя_маской_её_не_видно() -> None:
    session = _Session(_listing("rtsp://***@10.0.0.5:554/stream1"))
    asyncio.run(go2rtc_streams.ensure(session, "http://go2rtc", "cam", SOURCE))
    fresh = SOURCE.replace("Pa%23ss!", "new")
    asyncio.run(go2rtc_streams.ensure(session, "http://go2rtc", "cam", fresh))
    assert session.puts == [("cam", fresh)]


def test_сторож_не_снимает_живой_продюсер_со_скрытой_учёткой() -> None:
    child = Producer(pid=1, start=0.0, input=SOURCE)
    assert orphans([child], {"rtsp://***@10.0.0.5:554/stream1"}, uptime=1000.0) == []
    assert orphans([child], {"rtsp://***@10.0.0.9:554/stream1"}, uptime=1000.0) == [child]


def test_свой_поток_узнаётся_по_памяти_при_любом_виде_адреса() -> None:
    """Будущий go2rtc может скрывать в ответе больше (хост, параметры): поток, заведённый этим процессом, всё равно
    не перезаводится, пока он есть под своим именем."""
    session = _Session({})
    asyncio.run(go2rtc_streams.ensure(session, "http://go2rtc", "cam", SOURCE))
    session.listing = {"cam": {"producers": [{"url": "rtsp://***@***:554/***"}], "consumers": [{"id": 1}]}}
    asyncio.run(go2rtc_streams.ensure(session, "http://go2rtc", "cam", SOURCE))
    assert session.puts == [("cam", SOURCE)]


def test_подмена_потока_с_читателями_пишется_в_лог(caplog: pytest.LogCaptureFixture) -> None:
    session = _Session({"cam": {"producers": [{"url": "rtsp://***@***/***"}], "consumers": [{"id": 1}]}})
    asyncio.run(go2rtc_streams.ensure(session, "http://go2rtc", "cam", SOURCE))
    assert session.puts == [("cam", SOURCE)]
    assert "перезаводится при 1 читателях" in caplog.text


def test_сторож_узнаёт_свой_поток_по_точному_источнику() -> None:
    from mega_home.core.go2rtc_guard import registered_sources

    session = _Session({})
    asyncio.run(go2rtc_streams.ensure(session, "http://go2rtc", "cam", SOURCE))
    sources = registered_sources({"cam": {"producers": [{"url": "rtsp://***@***/***"}]}})
    child = Producer(pid=1, start=0.0, input=SOURCE)
    assert orphans([child], sources, uptime=1000.0) == []
