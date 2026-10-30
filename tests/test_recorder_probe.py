"""Проба потока до записи (`recorder_probe.py`): факты по дорожкам — пакеты и метки, а не объявленное камерой."""

from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path

import pytest

from mega_home.core.recorder_api import _installer_only
from mega_home.core.recorder_probe import input_args, tracks_of


def _facts(path: Path) -> list[dict]:
    out = subprocess.run(
        ["ffprobe", "-v", "error", "-show_entries",
         "stream=index,codec_type,codec_name,width,height,avg_frame_rate,sample_rate,channels:packet=stream_index,pts_time",
         "-of", "json", str(path)],
        check=True, capture_output=True, text=True,
    ).stdout
    return tracks_of(json.loads(out))


def test_факты_дорожек_видно_звук_без_пакетов_и_с_застывшими_метками(tmp_path: Path) -> None:
    """EYEPLUS «Прихожая» — звук объявлен, пакетов нет; «Гостиная» (.90.250) — видео на часы позже первого звука.
    Заголовки у обеих как у исправной камеры: отличают их только пакеты и метки."""
    ffmpeg = shutil.which("ffmpeg")
    if not ffmpeg or not shutil.which("ffprobe"):
        pytest.skip("нужны ffmpeg и ffprobe")
    video = ["-f", "lavfi", "-t", "3", "-i", "testsrc=size=160x90:rate=10"]
    sound = ["-f", "lavfi", "-t", "3", "-i", "anullsrc=r=8000:cl=mono"]
    good, skewed = tmp_path / "good.mkv", tmp_path / "skewed.mkv"
    subprocess.run([ffmpeg, "-v", "error", "-y", *video, *sound, "-c:v", "libx264", "-c:a", "pcm_alaw", str(good)], check=True)
    subprocess.run([ffmpeg, "-v", "error", "-y", "-itsoffset", "1000", *video, *sound, "-map", "0:v", "-map", "1:a",
                    "-c:v", "libx264", "-c:a", "pcm_alaw", str(skewed)], check=True)

    video_track, audio_track = _facts(good)
    assert (video_track["type"], video_track["codec"], video_track["width"], video_track["height"]) == ("video", "h264", 160, 90)
    assert video_track["fps"] == 10 and video_track["packets"] == 30
    assert (audio_track["type"], audio_track["codec"], audio_track["rate"]) == ("audio", "pcm_alaw", 8000)
    assert audio_track["packets"] > 0 and abs(audio_track["first"] - video_track["first"]) < 1

    video_track, audio_track = _facts(skewed)
    assert video_track["first"] == pytest.approx(1000, abs=0.1) and audio_track["first"] == pytest.approx(0, abs=0.1)


def test_дорожка_без_пакетов_видна_нулём() -> None:
    answer = {"streams": [{"index": 0, "codec_type": "video", "codec_name": "h264"}, {"index": 1, "codec_type": "audio", "codec_name": "pcm_alaw"}],
              "packets": [{"stream_index": 0, "pts_time": "0.0"}, {"stream_index": 0, "pts_time": "0.1"}]}
    video, audio = tracks_of(answer)
    assert video["packets"] == 2 and video["last"] == 0.1
    assert audio["packets"] == 0 and audio["first"] is None


def test_аргументы_входа_списком_строк_без_второго_входа() -> None:
    assert input_args(["-use_wallclock_as_timestamps", 1]) == ("-use_wallclock_as_timestamps", "1")
    assert input_args("-fflags +genpts") == ()
    assert input_args(["-i", "rtsp://x"]) == ()
    assert input_args(["-x"] * 40) == ()


def test_проба_только_менеджеру() -> None:
    assert _installer_only("path", "/probe") is not None
    assert _installer_only("path", "/probe/" + "a" * 32 + "/audio.m4a") is not None


def test_кадр_отрывок_и_уровень_звука(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Инсталлятор слушает звук пробы и видит его уровень: тишину видно и без наушников."""
    import asyncio

    from mega_home.core import recorder_probe

    ffmpeg = shutil.which("ffmpeg")
    if not ffmpeg or not shutil.which("ffprobe"):
        pytest.skip("нужны ffmpeg и ffprobe")
    monkeypatch.setattr(recorder_probe, "PROBE_DIR", tmp_path / "probe")
    sample = tmp_path / "sample.mkv"
    subprocess.run([ffmpeg, "-v", "error", "-y", "-f", "lavfi", "-t", "2", "-i", "testsrc=size=160x90:rate=10",
                    "-f", "lavfi", "-t", "2", "-i", "sine=frequency=440:sample_rate=8000", "-c:v", "libx264", "-c:a", "pcm_alaw",
                    str(sample)], check=True)
    tracks = _facts(sample)

    class _Env:
        async def run(self, func, *args):
            return func(*args)

    class _Rec:
        env = _Env()

    pid = asyncio.run(recorder_probe._keep(_Rec(), ffmpeg, sample, tracks))
    folder = tmp_path / "probe" / pid
    assert (folder / "frame.jpg").stat().st_size > 0 and (folder / "audio.m4a").stat().st_size > 0
    sound = next(t for t in tracks if t["type"] == "audio")
    assert sound["meanDb"] is not None and -30 < sound["meanDb"] < 0 and sound["peakDb"] >= sound["meanDb"]
