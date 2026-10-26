"""Правка заголовка SPS в MPEG-TS сессии просмотра (`core/ts_sps.py`).

⚠ Зачем: go2rtc (1.9.14 и `master` на 2026-10-27, `pkg/h264/annexb.EncodeToAVCC`) выбрасывает NAL-блок, чей
первый байт после маски `0x7E` равен H.265 AUD (`35<<1`), — это SPS H.264 с `nal_ref_idc = 2` (`0x47`). Без SPS
кодек потока становится заглушкой Baseline 128×96, и браузер запись не показывает (камеры EYEPLUS, замер на
объекте 2026-10-27). Живое видео не страдает: там кодек приходит описанием RTSP.
"""

from __future__ import annotations

from mega_home.core.ts_sps import SpsFix

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
