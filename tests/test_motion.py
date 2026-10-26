"""Своя детекция движения (`core/motion.py`): детектор кадров и источник `motion` слушателя.

Детектор проверяется синтетикой с шумом матрицы (тишина, идущее пятно, вспышка), источник —
подменённым ffmpeg: проверяется, что дом говорит «началось» и «кончилось» ровно по разу и что
декодер запущен дешёвым (дополнительный поток своего go2rtc, без деблокинга).
"""

from __future__ import annotations

import asyncio

import numpy as np
import pytest

from mega_home.core import activity, host_load
from mega_home.core import motion as mo
from mega_home.core.devices import DeviceDescriptor

RNG = np.random.default_rng(7)
SCENE = RNG.integers(40, 200, (mo.HEIGHT, mo.WIDTH)).astype(np.float32)


def _frame(spot_x: int | None = None, bright: float = 0.0) -> bytes:
    """Сцена с шумом ±4 и, по желанию, тёмным пятном 12×12 у `spot_x` или общей засветкой."""
    frame = SCENE + RNG.normal(0, 2, SCENE.shape) + bright
    if spot_x is not None:
        frame[40:52, spot_x : spot_x + 12] = 5
    return np.clip(frame, 0, 255).astype(np.uint8).tobytes()


def _run(detector: mo.Detector, frames: list[bytes]) -> list[float]:
    return [detector.detect(f) for f in frames]


def test_тишина_после_калибровки_не_движение() -> None:
    detector = mo.Detector(np)
    shares = _run(detector, [_frame() for _ in range(200)])
    assert not any(shares[20:])
    assert detector.calibrating is False


def test_идущее_пятно_видно_на_каждом_кадре() -> None:
    detector = mo.Detector(np)
    _run(detector, [_frame() for _ in range(60)])
    shares = _run(detector, [_frame(spot_x=10 + i * 2) for i in range(40)])
    # Живость — 2 из 3 кадров: первые два кадра движения ещё подтверждаются (0,4 с).
    assert all(share > 0 for share in shares[2:])


def test_вспышка_на_весь_кадр_калибровка_а_не_движение() -> None:
    """Смена ИК-режима или свет в комнате: доля изменений выше `lightning` — фон заново."""
    detector = mo.Detector(np)
    _run(detector, [_frame() for _ in range(60)])
    flash = _run(detector, [_frame(bright=90.0) for _ in range(5)])
    assert not any(flash)
    assert detector.calibrating is True
    settled = _run(detector, [_frame(bright=90.0) for _ in range(60)])
    assert not any(settled[-20:])


def test_часы_в_кадре_не_движение() -> None:
    """Камера впечатывает время: цифры секунд меняются раз в секунду (раз в 5 кадров)."""
    detector = mo.Detector(np)
    shares = []
    for i in range(300):
        frame = np.frombuffer(_frame(), np.uint8).reshape(mo.HEIGHT, mo.WIDTH).copy()
        digit = (i // 5) % 10
        frame[2:10, 40:60] = 250 if digit % 2 else 10
        frame[2:10, 44 + digit : 48 + digit] = 128
        shares.append(detector.detect(frame.tobytes()))
    assert not any(shares[30:])


def test_повтор_кадра_не_гасит_движение() -> None:
    """Поток камеры 2 кадра в секунду, ffmpeg дублирует до 5: идущее пятно всё равно видно."""
    detector = mo.Detector(np)
    _run(detector, [_frame() for _ in range(60)])
    frames = []
    for i in range(20):
        frames += [_frame(spot_x=10 + i * 6)] * 3
    shares = _run(detector, frames)
    assert any(shares[6:])
    # Повтор — не кадр: порог «два кадра движения подряд» одним настоящим кадром не набрать.
    assert shares[1] is None and shares[2] is None


def test_маска_закрывает_мигающий_блик() -> None:
    """Блик, мигающий на полу (стенд `.88.202`), — движение для любой детекции; маска его закрывает."""
    def blink(i: int) -> bytes:
        frame = np.frombuffer(_frame(), np.uint8).reshape(mo.HEIGHT, mo.WIDTH).copy()
        if i % 4:
            frame[60:76, 60:90] = 255
        return frame.tobytes()

    plain = mo.Detector(np)
    masked = mo.Detector(np, mask=[[[0.3, 0.55], [0.55, 0.55], [0.55, 0.85], [0.3, 0.85]]])
    for detector in (plain, masked):
        _run(detector, [_frame() for _ in range(60)])
    assert any(_run(plain, [blink(i) for i in range(40)]))
    assert not any(_run(masked, [blink(i) for i in range(40)]))
    assert mo.masked_area(np, [[[0, 0], [1, 0]]]) is None  # не многоугольник — маски нет


def test_минимальный_размер_отсекает_далёкое_пятно() -> None:
    """MinSize — площадь бокса в процентах кадра: малое пятно не сработка, крупное — сработка."""
    def spot(spot_x: int, size: int) -> bytes:
        frame = np.frombuffer(_frame(), np.uint8).reshape(mo.HEIGHT, mo.WIDTH).copy()
        frame[45 : 45 + size, spot_x : spot_x + size] = 5
        return frame.tobytes()

    small = mo.Detector(np, min_size=2.0)
    big = mo.Detector(np, min_size=2.0)
    for detector in (small, big):
        _run(detector, [_frame() for _ in range(60)])
    # 12×12: изменившихся пикселей ~1,2 % кадра — ниже порога 2 %.
    assert not any(_run(small, [spot(10 + i * 4, 12) for i in range(30)]))
    # 40×40 даёт больше 2 % — проходит.
    assert any(_run(big, [spot(10 + i * 4, 40) for i in range(30)]))


def test_малое_пятно_шумом_не_считается() -> None:
    detector = mo.Detector(np)
    _run(detector, [_frame() for _ in range(60)])
    frame = np.frombuffer(_frame(), np.uint8).reshape(mo.HEIGHT, mo.WIDTH).copy()
    frame[50:52, 80:82] = 0
    assert detector.detect(frame.tobytes()) == 0.0


class _Ctx:
    def __init__(self) -> None:
        self.events: list[dict] = []

    def emit(self, descriptor, source, spec, kind, payload, file=None) -> None:  # noqa: ANN001
        self.events.append({"kind": kind, **payload, **({"file": file} if file else {})})


class _Proc:
    def __init__(self) -> None:
        self.stdout = asyncio.StreamReader()
        self.stderr = asyncio.StreamReader()
        self.stderr.feed_eof()
        self.returncode: int | None = None
        self.pid = 4242

    def kill(self) -> None:
        self.returncode = -9

    async def wait(self) -> int:
        return self.returncode or 0


def test_источник_говорит_началось_и_кончилось_по_разу(monkeypatch) -> None:
    started: list[tuple] = []
    proc_box: list[_Proc] = []

    async def fake_exec(*command, **_kwargs):  # noqa: ANN002, ANN003
        started.append(command)
        proc_box.append(_Proc())
        return proc_box[-1]

    monkeypatch.setattr(mo.asyncio, "create_subprocess_exec", fake_exec)
    monkeypatch.setattr(mo.shutil, "which", lambda name: "/usr/bin/ffmpeg" if name == "ffmpeg" else None)
    ctx = _Ctx()
    spec = {"id": "motion", "type": "motion", "stream": "cam_sub", "offDelay": 1}

    async def scenario() -> None:
        task = asyncio.ensure_future(mo.motion(ctx, DeviceDescriptor(id="cam", host="192.168.1.9"), "motion", spec))
        while not proc_box:
            await asyncio.sleep(0.01)
        reader = proc_box[0].stdout
        for _ in range(60):
            reader.feed_data(_frame())
        for i in range(15):
            reader.feed_data(_frame(spot_x=10 + i * 3))
        await asyncio.sleep(0.05)
        for _ in range(30):
            reader.feed_data(_frame())
            await asyncio.sleep(0.05)
        # Нагрузка (`host_load`): ffmpeg детекции отмечен ключом камеры, пока жив.
        assert host_load._owners["motion:cam"] == 4242  # noqa: SLF001
        reader.feed_eof()
        with pytest.raises(RuntimeError, match="ffmpeg закончил поток"):
            await task

    host_load._inside.clear()  # noqa: SLF001
    activity._live.clear()  # noqa: SLF001
    asyncio.run(scenario())
    assert [(e["kind"], e["state"]) for e in ctx.events] == [("motion", "start"), ("motion", "stop")]
    # Сила движения окна — в корзины камеры (`activity.py`) для ленты архива.
    levels = activity.pending("cam", 0, 1e12)
    assert levels and all(0 < share <= 1 for _bucket, share in levels)
    activity._live.clear()  # noqa: SLF001
    # Счёт кадра numpy — в процессе дома, и он записан на ту же камеру; ffmpeg снят с учёта.
    assert host_load._inside["motion:cam"] > 0 and "motion:cam" not in host_load._owners  # noqa: SLF001
    host_load._inside.clear()  # noqa: SLF001
    command = started[0]
    assert "rtsp://127.0.0.1:1986/cam_sub" in command and "-skip_loop_filter" in command
    assert any("scale=176:96" in part and "format=gray" in part for part in command)


def test_источник_встал_посреди_движения_окно_закрывается(monkeypatch) -> None:
    proc_box: list[_Proc] = []

    async def fake_exec(*_command, **_kwargs):  # noqa: ANN002, ANN003
        proc_box.append(_Proc())
        return proc_box[-1]

    monkeypatch.setattr(mo.asyncio, "create_subprocess_exec", fake_exec)
    monkeypatch.setattr(mo.shutil, "which", lambda name: "/usr/bin/" + name)
    ctx = _Ctx()

    async def scenario() -> None:
        task = asyncio.ensure_future(mo.motion(ctx, DeviceDescriptor(id="cam", host="192.168.1.9"), "motion", {"stream": "s"}))
        while not proc_box:
            await asyncio.sleep(0.01)
        for _ in range(60):
            proc_box[0].stdout.feed_data(_frame())
        for i in range(5):
            proc_box[0].stdout.feed_data(_frame(spot_x=10 + i * 3))
        await asyncio.sleep(0.05)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    asyncio.run(scenario())
    assert [e["state"] for e in ctx.events] == ["start", "stop"]


def test_без_потока_источник_не_запускается() -> None:
    with pytest.raises(ValueError, match="не задан поток"):
        asyncio.run(mo.motion(_Ctx(), DeviceDescriptor(id="cam", host="192.168.1.9"), "motion", {}))


def test_петля_детекции_зовёт_запись_напрямую() -> None:
    """Конвейер камеры (`video_pipeline.py`) получает смены движения вызовом, а не подстрокой события."""
    changes: list[bool] = []
    shares: list[float] = []
    emitted: list[tuple[str, str]] = []

    async def scenario() -> None:
        loop = mo.MotionLoop(
            np, {"offDelay": 1},
            emit=lambda event, data, _file: emitted.append((event, data["state"])),
            on_motion=lambda on, _at: changes.append(on),
            on_share=lambda _at, share: shares.append(share),
            load_key="cam:x:sub",
        )
        for _ in range(60):
            await loop.frame(_frame())
        for i in range(15):
            await loop.frame(_frame(spot_x=10 + i * 3))
        for _ in range(30):
            await loop.frame(_frame())
            await asyncio.sleep(0.05)
        loop.close()

    host_load._inside.clear()  # noqa: SLF001
    asyncio.run(scenario())
    assert changes == [True, False]
    assert emitted == [("motion", "start"), ("motion", "stop")]
    assert shares and all(0 < share <= 1 for share in shares)
    assert host_load._inside["cam:x:sub"] > 0  # noqa: SLF001
    host_load._inside.clear()  # noqa: SLF001
