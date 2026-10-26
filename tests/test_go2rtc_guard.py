"""Сторож продюсеров go2rtc (`core/go2rtc_guard.py`): вход ffmpeg, живые источники, сироты и предел.

⚠ Правила проверяются без `/proc` и go2rtc: это чистая арифметика над тем, что уже прочитано.
Живой объект 2026-10-26: `DELETE /api/streams` снимает регистрацию, но ffmpeg-продюсер архива
остаётся, пока WebRTC-потребитель не отпущен; серия перемоток копит процессы до OOM.
"""

from __future__ import annotations

from mega_home.core.go2rtc_guard import Producer, input_of, orphans, over_cap, registered_sources


def test_вход_ffmpeg_берётся_из_первого_i() -> None:
    args = ["/usr/bin/ffmpeg", "-v", "error", "-re", "-i", "/media/NVR/.cache/play/a.ffconcat", "-c", "copy"]
    assert input_of(args) == "/media/NVR/.cache/play/a.ffconcat"


def test_без_входа_i_поток_не_опознан() -> None:
    assert input_of(["/usr/bin/ffmpeg", "-version"]) == ""


def test_живой_источник_находится_и_в_команде_и_по_имени() -> None:
    streams = {
        "rec_cam_1": {
            "producers": [
                {
                    "source": "exec:ffmpeg -re -i /media/NVR/.cache/play/a.ffconcat -c copy",
                    "url": "rtsp://127.0.0.1:1986/internal",
                }
            ]
        }
    }
    sources = registered_sources(streams)
    assert "rec_cam_1" in sources
    assert "rtsp://127.0.0.1:1986/internal" in sources
    child = Producer(pid=10, start=100.0, input="/media/NVR/.cache/play/a.ffconcat")
    assert orphans([child], sources, uptime=120.0) == []


def test_сирота_это_вход_которого_нет_ни_у_одного_потока() -> None:
    child = Producer(pid=10, start=100.0, input="/media/NVR/.cache/play/a.ffconcat")
    sources = {"exec:ffmpeg -i /media/NVR/.cache/play/b.ffconcat"}
    assert orphans([child], sources, uptime=120.0) == [child]


def test_молодой_продюсер_не_трогается_до_появления_в_списке() -> None:
    child = Producer(pid=10, start=100.0, input="/media/NVR/.cache/play/a.ffconcat")
    assert orphans([child], set(), uptime=110.0, grace=15.0) == []


def test_предел_снимает_самых_старых() -> None:
    children = [
        Producer(pid=1, start=10.0, input="/a"),
        Producer(pid=2, start=20.0, input="/b"),
        Producer(pid=3, start=30.0, input="/c"),
    ]
    assert [child.pid for child in over_cap(children, cap=2)] == [1]
    assert over_cap(children, cap=3) == []
