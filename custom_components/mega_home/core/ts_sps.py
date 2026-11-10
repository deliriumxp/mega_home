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


PTS_HZ = 90_000


class TsTrack:
    """Видео-PID, его счётчик непрерывности и последняя метка PTS (секунды) — через все ffmpeg сессии.

    Нужен перемотке (`recorder_session.py`): закрывающий пакет идёт на ТОТ ЖЕ PID со следующим счётчиком, а время
    нового места продолжается не раньше последней отданной метки. ⚠ Перенос PTS через 2^33 (26,5 ч непрерывной
    сессии) не учитывается: сессия просмотра столько не живёт.
    """

    def __init__(self) -> None:
        self.pid: int | None = None
        self.cc = 0
        self.last_pts: float | None = None

    def scan(self, block: bytes) -> None:
        for at in range(0, len(block) - PACKET + 1, PACKET):
            if block[at] != SYNC:
                return
            pid = ((block[at + 1] & 0x1F) << 8) | block[at + 2]
            control = (block[at + 3] >> 4) & 0x3
            if self.pid is None and block[at + 1] & 0x40 and control & 0x1:
                pes = _pes_at(block, at, control)
                if pes is not None and 0xE0 <= block[pes + 3] <= 0xEF:
                    self.pid = pid
            if pid != self.pid or not control & 0x1:
                continue
            self.cc = block[at + 3] & 0x0F
            if block[at + 1] & 0x40:
                pes = _pes_at(block, at, control)
                if pes is not None and block[pes + 7] & 0x80 and pes + 14 <= at + PACKET:
                    self.last_pts = _pts(block[pes + 9 : pes + 14]) / PTS_HZ


def _pes_at(block: bytes, at: int, control: int) -> int | None:
    """Начало заголовка PES в пакете с PUSI, если он целиком влезает в пакет."""
    pos = at + 4 + (1 + block[at + 4] if control & 0x2 else 0)
    if pos + 9 > at + PACKET or block[pos : pos + 3] != START:
        return None
    return pos


def _pts(raw: bytes) -> int:
    """33 бита PTS из пяти байт заголовка PES (ISO 13818-1, 2.4.3.7)."""
    return ((raw[0] >> 1) & 0x07) << 30 | raw[1] << 22 | (raw[2] >> 1) << 15 | raw[3] << 7 | raw[4] >> 1


def closing_packet(track: TsTrack) -> bytes | None:
    """Пустой PES на видео-PID: go2rtc отдаёт накопленный кадр по следующему PUSI (`mpegts/demuxer.go`, `readPES`).

    ⚠ Зачем. ffmpeg пишет видео PES без длины, и конец кадра go2rtc узнаёт только по началу следующего: последний
    кадр прежнего места застревал при паузе и перемотке и уходил телефону ПЕРЕД новым местом («моргает ночной кадр»,
    2026-10-30). `-omit_video_pes_length 0` не спасает — у кадра больше 65535 байт длина всё равно 0. Этот пакет
    выталкивает застрявший кадр сразу; пустой — уйдёт следующим PUSI и картинки не даёт.
    Пакет: заголовок с PUSI, поле адаптации из набивки, заголовок PES без данных (длина 0, без меток).
    """
    if track.pid is None:
        return None
    track.cc = (track.cc + 1) & 0x0F
    pes = START + bytes([0xE0, 0x00, 0x00, 0x80, 0x00, 0x00])
    stuffing = PACKET - 4 - 1 - len(pes)
    head = bytes([SYNC, 0x40 | (track.pid >> 8), track.pid & 0xFF, 0x30 | track.cc])
    return head + bytes([stuffing]) + b"\x00" + b"\xff" * (stuffing - 1) + pes


class SpsFix:
    """Поток ffmpeg → поток зрителю: выравнивание по пакетам TS и правка SPS; не TS — отдаётся как есть."""

    def __init__(self, track: TsTrack | None = None) -> None:
        self._rest = b""
        self._raw = False
        self._track = track

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
        if self._track is not None:
            self._track.scan(block)
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
