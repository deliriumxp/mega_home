"""Правка заголовка SPS в MPEG-TS сессии просмотра (`core/ts_sps.py`).

⚠ Зачем: go2rtc (1.9.14 и `master` на 2026-10-27, `pkg/h264/annexb.EncodeToAVCC`) выбрасывает NAL-блок, чей
первый байт после маски `0x7E` равен H.265 AUD (`35<<1`), — это SPS H.264 с `nal_ref_idc = 2` (`0x47`). Без SPS
кодек потока становится заглушкой Baseline 128×96, и браузер запись не показывает (камеры EYEPLUS, замер на
объекте 2026-10-27). Живое видео не страдает: там кодек приходит описанием RTSP.
"""

from __future__ import annotations

from mega_home.core.ts_sps import SpsFix, TsTrack, closing_packet

SPS = bytes.fromhex("474d002999b01e0089f96100000303e800003e8084")
PPS = bytes.fromhex("48ea43c8")


def _packet(pid: int, payload: bytes, pusi: bool) -> bytes:
    head = bytes([0x47, (0x40 if pusi else 0) | (pid >> 8), pid & 0xFF, 0x10])
    return head + payload.ljust(184, b"\xff")


def _video_pes(*nals: bytes) -> bytes:
    pes = b"\x00\x00\x01\xe0\x00\x00\x80\x80\x05" + b"\x21\x00\x01\x00\x01"
    return pes + b"".join(b"\x00\x00\x00\x01" + nal for nal in nals)


def _run(fix: SpsFix, data: bytes, sizes: list[int]) -> bytes:
    out, at = [], 0
    for size in sizes:
        out.append(fix.feed(data[at : at + size]))
        at += size
    out.append(fix.feed(data[at:]))
    out.append(fix.flush())
    return b"".join(out)


def test_sps_с_nal_ref_idc_2_получает_3_остальное_байт_в_байт() -> None:
    first = _packet(0x100, _video_pes(b"\x09\xf0", SPS, PPS, b"\x65\x88\x84"), pusi=True)
    out = SpsFix().feed(first)
    at = first.index(b"\x00\x00\x00\x01" + SPS) + 4
    assert out[at] == 0x67
    assert out[:at] + out[at + 1 :] == first[:at] + first[at + 1 :]


def test_звук_и_продолжение_pes_не_трогаются() -> None:
    audio = _packet(0x101, b"\x00\x00\x01\xc0\x00\x00\x80\x80\x05\x21\x00\x01\x00\x01" + b"\x00\x00\x01\x47" * 4, pusi=True)
    tail = _packet(0x100, b"\x00\x00\x01\x47\x01\x02", pusi=False)
    data = audio + tail
    assert SpsFix().feed(data) == data


def test_нарезка_потока_на_куски_не_меняет_результат() -> None:
    data = b"".join(_packet(0x100, _video_pes(b"\x09\xf0", SPS, PPS), pusi=True) for _ in range(5))
    whole = SpsFix().feed(data)
    assert _run(SpsFix(), data, [7, 300, 1, 188, 50]) == whole
    assert whole.count(b"\x00\x00\x00\x01\x67") == 5


def test_поток_без_пакетов_ts_отдаётся_как_есть() -> None:
    junk = b"\x00\x00\x01\x47 not a transport stream" * 20
    assert _run(SpsFix(), junk, [5, 9]) == junk


def _pts_bytes(pts: int) -> bytes:
    return bytes([
        0x21 | ((pts >> 29) & 0x0E), (pts >> 22) & 0xFF, ((pts >> 14) & 0xFE) | 1, (pts >> 7) & 0xFF, ((pts << 1) & 0xFE) | 1,
    ])


def _pes_with_pts(seconds: float) -> bytes:
    return b"\x00\x00\x01\xe0\x00\x00\x80\x80\x05" + _pts_bytes(int(seconds * 90_000)) + b"\x00\x00\x00\x01\x09\xf0"


def test_слежение_за_видео_pid_счётчиком_и_меткой() -> None:
    """Перемотка закрывает прежнее место на ТОМ ЖЕ PID со следующим счётчиком и продолжает время от последней метки."""
    track = TsTrack()
    audio = bytes([0x47, 0x41, 0x01, 0x13]) + b"\x00\x00\x01\xc0\x00\x00\x80\x80\x05" + _pts_bytes(99 * 90_000)
    video = bytes([0x47, 0x41, 0x00, 0x17]) + _pes_with_pts(12.5)
    cont = bytes([0x47, 0x01, 0x00, 0x18]) + b"\x00" * 10
    SpsFix(track).feed(audio.ljust(188, b"\xff") + video.ljust(188, b"\xff") + cont.ljust(188, b"\xff"))
    assert track.pid == 0x100 and track.cc == 8
    assert track.last_pts == 12.5  # метка звука не в счёт


def test_закрывающий_пакет_пустой_pes_на_видео_pid() -> None:
    track = TsTrack()
    assert closing_packet(track) is None  # видео ещё не было — закрывать нечего
    track.pid, track.cc = 0x100, 15
    packet = closing_packet(track)
    assert packet is not None and len(packet) == 188
    assert packet[:4] == bytes([0x47, 0x41, 0x00, 0x30])  # PUSI, PID 0x100, адаптация + данные, счётчик 0
    assert packet[4] == 174 and packet[5] == 0x00
    assert packet[-9:] == b"\x00\x00\x01\xe0\x00\x00\x80\x00\x00"
    # Наш же разбор видит в нём начало видео-PES без метки: время не сдвигается.
    again = TsTrack()
    again.scan(packet)
    assert again.pid == 0x100 and again.last_pts is None
