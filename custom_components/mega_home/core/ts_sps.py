"""Заголовок SPS в MPEG-TS сессии просмотра — в форме, которую go2rtc не выбрасывает (дом 0.6.1+).

⚠ Зачем. go2rtc (1.9.14 и `master` на 2026-10-27) переводит MPEG-TS в AVCC функцией
`pkg/h264/annexb.EncodeToAVCC` и пропускает NAL-блоки, которые считает AUD: `b & 0x1F == 9 || b & 0x7E == 70`.
Вторая половина — проверка AUD H.265, и под неё попадает SPS H.264 с `nal_ref_idc = 2` (байт `0x47`): go2rtc
его теряет, кодек потока становится заглушкой Baseline 128×96, и браузер запись не показывает (камеры EYEPLUS,
замер на объекте 2026-10-27). Живое видео не страдает — там кодек приходит описанием RTSP.

Правка — `nal_ref_idc` SPS с 2 на 3 (`0x47` → `0x67`): для SPS значение любое ненулевое, декодер его не
читает. Только в первом TS-пакете PES видео — туда ffmpeg кладёт AUD, SPS и PPS ключевого кадра; поле
`nal_ref_idc` вне заголовка NAL не трогается. Запись на диске не меняется: правка покрывает и уже записанное.
"""

from __future__ import annotations

PACKET = 188
SYNC = 0x47
START = b"\x00\x00\x01"
# Признак, что в куске может быть SPS с `nal_ref_idc = 2`: без него кусок уходит как есть.
SUSPECT = START + bytes([SYNC])
SPS_REF2, SPS_REF3 = 0x47, 0x67


class SpsFix:
    """Поток ffmpeg → поток зрителю: выравнивание по пакетам TS и правка SPS; не TS — отдаётся как есть."""

    def __init__(self) -> None:
        self._rest = b""
        self._raw = False

    def feed(self, chunk: bytes) -> bytes:
        if self._raw:
            return chunk
        data = self._rest + chunk
        whole = len(data) - len(data) % PACKET
        block, self._rest = data[:whole], data[whole:]
        if not block:
            return b""
        if block[0] != SYNC:
            # Не транспортный поток — править нечего, и выравнивать незачем.
            self._raw = True
            out, self._rest = block + self._rest, b""
            return out
        if SUSPECT not in block:
            return block
        buf = bytearray(block)
        for at in range(0, whole, PACKET):
            if buf[at] != SYNC:
                break
            _patch(buf, at)
        return bytes(buf)

    def flush(self) -> bytes:
        out, self._rest = self._rest, b""
        return out


def _patch(buf: bytearray, at: int) -> None:
    """Первый пакет PES видео: заголовки NAL после заголовка PES."""
    if not buf[at + 1] & 0x40:
        return
    control = (buf[at + 3] >> 4) & 0x3
    if not control & 0x1:
        return
    pos = at + 4 + (1 + buf[at + 4] if control & 0x2 else 0)
    end = at + PACKET
    if pos + 9 > end or buf[pos : pos + 3] != START or not 0xE0 <= buf[pos + 3] <= 0xEF:
        return
    pos += 9 + buf[pos + 8]
    while (found := buf.find(START, pos, end)) >= 0 and found + 3 < end:
        pos = found + 3
        if buf[pos] == SPS_REF2:
            buf[pos] = SPS_REF3
