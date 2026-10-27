"""Детекция людей (`core/person.py`, источник `motion` с разделом `person`): сеть, вырез, BMP из ffmpeg,
одно событие на окно движения, кадр и рамка ОДНОГО момента.

⚠ Ответы сети сверены с LiteRT на эталонной модели до 10⁻⁸ (2026-10-24, одноразовый прогон: в образе
HA пакета нет, в спеках — тоже); здесь — замок исполнителя на файле весов пакета.
"""

from __future__ import annotations

import asyncio
import os

import numpy as np
import pytest

from mega_home.core import motion as mo
from mega_home.core import person
from mega_home.core.devices import DeviceDescriptor

from test_motion import _Ctx, _frame, _Proc


def test_сеть_из_пакета_грузится_и_отвечает_как_при_сверке() -> None:
    model = person.load(np)
    assert model["size"] == 96 and model["person"] == 1
    # Ровная серая картинка — не человек; числа — прогон исполнителя на весах пакета.
    for value, expected in ((0.0, 0.0202), (0.5, 0.0355), (1.0, 0.0185)):
        out = person.forward(np, model["layers"], np.full((96, 96, 3), value, np.float32))
        assert out.shape == (2,) and out.sum() == pytest.approx(1.0, abs=1e-5)
        assert out[1] == pytest.approx(expected, abs=1e-3)


def test_вырез_квадратный_с_запасом_и_внутри_кадра() -> None:
    # Узкая фигура в рост — квадрат по большей стороне с запасом 20 %.
    assert person.square((100, 100, 140, 300), 1000, 800) == (0, 80, 240, 320)
    # У края — сдвинут внутрь, не обрезан.
    left, top, right, bottom = person.square((900, 700, 1000, 800), 1000, 800)
    assert (right, bottom) == (1000, 800) and right - left == bottom - top == 120
    # Больше кадра не бывает.
    left, top, right, bottom = person.square((0, 0, 1000, 800), 1000, 800)
    assert right - left == 800 and (top, bottom) == (0, 800)


def test_снимок_события_16_9_вокруг_квадрата_сети() -> None:
    """Кадр ленты — 16:9 и `fill`: квадрат там растянулся бы. Высота — по квадрату, сдвиг внутрь кадра."""
    assert person.wide((0, 80, 240, 320), 1000, 800) == (0, 80, 426, 320)
    # Квадрат во всю высоту узкого кадра — снимок не шире кадра.
    left, top, right, bottom = person.wide((0, 0, 400, 400), 400, 400)
    assert (left, right) == (0, 400) and bottom - top == 225


def test_снимок_события_jpeg_не_шире_заданного() -> None:
    from PIL import Image

    frame = np.random.default_rng(1).integers(0, 255, (720, 1280, 3), np.uint8)[::-1]
    body = person.shot(frame, (500.0, 200.0, 700.0, 600.0), 0.2, 320)
    assert body is not None and body[:2] == b"\xff\xd8"
    with Image.open(__import__("io").BytesIO(body)) as image:
        assert image.width == 320 and abs(image.width / image.height - 16 / 9) < 0.02


def test_уменьшение_без_pillow_сохраняет_ровный_цвет() -> None:
    image = np.full((300, 500, 3), (10, 120, 250), np.uint8)
    small = person.resize(np, image, 96)
    assert small.shape == (96, 96, 3)
    assert np.allclose(small, (10, 120, 250))


def _bmp(pixels: np.ndarray) -> bytes:
    """BMP 24 бит, строки снизу вверх с выравниванием до 4 байт — как пишет ffmpeg (`-c:v bmp`)."""
    height, width = pixels.shape[:2]
    stride = (width * 3 + 3) & ~3
    rows = b"".join(pixels[y, :, ::-1].tobytes().ljust(stride, b"\0") for y in range(height - 1, -1, -1))
    info = (40).to_bytes(4, "little") + width.to_bytes(4, "little") + height.to_bytes(4, "little")
    info += (1).to_bytes(2, "little") + (24).to_bytes(2, "little") + bytes(24)
    head = b"BM" + (14 + len(info) + len(rows)).to_bytes(4, "little") + bytes(4) + (14 + len(info)).to_bytes(4, "little")
    return head + info + rows


def test_кадр_bmp_из_трубы_rgb_сверху_вниз() -> None:
    pixels = np.arange(5 * 3 * 3, dtype=np.uint8).reshape(3, 5, 3)

    async def scenario() -> np.ndarray:
        reader = asyncio.StreamReader()
        reader.feed_data(_bmp(pixels) + _bmp(pixels[::-1]))
        first = await person.read_bmp(np, reader)
        second = await person.read_bmp(np, reader)
        assert np.array_equal(second, pixels[::-1])
        return first

    assert np.array_equal(asyncio.run(scenario()), pixels)


def test_одно_событие_на_окно_после_попаданий_подряд() -> None:
    watch = person.PersonWatch(every=1.0, threshold=0.5, frames=2)
    assert watch.due(0.0)
    assert watch.seen(0.9, 0.0) is False
    # Промах сбрасывает счёт: «подряд», а не «всего».
    assert watch.seen(0.2, 1.0) is False
    assert watch.seen(0.9, 2.0) is False
    assert not watch.due(2.5)
    assert watch.seen(0.9, 3.0) is True
    # Человек уже есть — до конца окна больше не проверяем и не говорим.
    assert not watch.due(10.0)
    assert watch.end() is True and watch.end() is False


def test_крупный_кадр_берёт_рамку_своего_момента() -> None:
    """Выходы ffmpeg приходят пачками: крупный № j — момент маленького № j·ratio, а не последнего пришедшего."""
    big = mo.deque([(0, "кадр0"), (1, "кадр1"), (2, "кадр2")])
    boxes = mo.deque([(i, (i, 0, i + 1, 1)) for i in range(0, 12)], maxlen=20)
    assert mo._person_pairs(big, boxes, 5, 7) == [("кадр0", (0, 0, 1, 1)), ("кадр1", (5, 0, 6, 1))]  # noqa: SLF001
    # Двойник крупного № 2 (маленький № 10) ещё не разобран — кадр ждёт.
    assert list(big) == [(2, "кадр2")]


def _person_run(monkeypatch, spec: dict, scores: list[float]) -> tuple[list[dict], tuple]:
    """Источник с подменённым ffmpeg: маленькие кадры в stdout, крупные BMP — в трубу, которую он отдал."""
    started: list[tuple] = []
    procs: list[_Proc] = []
    pipes: list[int] = []

    async def fake_exec(*command, **kwargs):  # noqa: ANN002, ANN003
        started.append(command)
        # Источник закрывает свой конец трубы сразу после запуска — пишем в копию, как ffmpeg.
        pipes.append(os.dup(kwargs["pass_fds"][0]))
        procs.append(_Proc())
        return procs[-1]

    answers = iter(scores)
    monkeypatch.setattr(mo.asyncio, "create_subprocess_exec", fake_exec)
    monkeypatch.setattr(mo.shutil, "which", lambda name: "/usr/bin/ffmpeg" if name == "ffmpeg" else None)
    monkeypatch.setattr(mo, "_person_score", lambda *_args: (next(answers, 0.0), 0.001))
    ctx = _Ctx()
    big = _bmp(np.zeros((36, 64, 3), np.uint8))

    async def scenario() -> None:
        task = asyncio.ensure_future(mo.motion(ctx, DeviceDescriptor(id="cam", host="192.168.1.9"), "motion", spec))
        while not procs:
            await asyncio.sleep(0.01)
        frames = [_frame() for _ in range(60)] + [_frame(spot_x=10 + i * 3) for i in range(30)] + [_frame() for _ in range(40)]
        for i, raw in enumerate(frames):
            if i % 5 == 0:
                os.write(pipes[0], big)
                await asyncio.sleep(0.01)
            procs[0].stdout.feed_data(raw)
            # Идущее пятно — 30 кадров по 0,08 с: проверки не чаще `every` (1 с) успевают дважды.
            await asyncio.sleep(0.08 if 60 <= i < 90 else 0.03 if i >= 90 else 0)
        procs[0].stdout.feed_eof()
        await asyncio.gather(task, return_exceptions=True)
        os.close(pipes[0])

    asyncio.run(scenario())
    return ctx.events, started[0]


def test_источник_с_людьми_говорит_человек_один_раз(monkeypatch) -> None:
    spec = {"stream": "cam_sub", "offDelay": 1, "person": {"every": 1, "threshold": 0.5, "frames": 2}}
    events, command = _person_run(monkeypatch, spec, [0.9, 0.9, 0.9, 0.9])
    kinds = [(e["kind"], e.get("state") or e.get("person")) for e in events]
    assert kinds.count(("person", "start")) == 1 and kinds.count(("person", "stop")) == 1
    assert kinds.index(("motion", "start")) < kinds.index(("person", "start")) < kinds.index(("person", "stop"))
    # К «человек появился» — свой снимок: крупный участок кадра, где сеть его увидела.
    start = next(e for e in events if e.get("person") == "start")
    assert start["file"][0] == "image/jpeg" and start["file"][1][:2] == b"\xff\xd8"
    # Тот же декодер, второй выход — цветной крупный кадр раз в `every`, BMP в отданную трубу.
    graph = command[command.index("-filter_complex") + 1]
    assert "split=2" in graph and "fps=1" in graph and "scale=640:-2" in graph
    assert command[-1].startswith("pipe:") and command[-1] != "pipe:1" and "bmp" in command


def test_кадр_выреза_данными_описания(monkeypatch) -> None:
    """Ширина кадра выреза — из описания (подстройка без релиза), чётная и в пределах."""
    spec = {"stream": "cam_sub", "offDelay": 1, "person": {"every": 1, "frames": 2, "width": 801}}
    _events, command = _person_run(monkeypatch, spec, [])
    assert "scale=800:-2" in command[command.index("-filter_complex") + 1]


def test_без_человека_события_нет(monkeypatch) -> None:
    spec = {"stream": "cam_sub", "offDelay": 1, "person": {"every": 1, "threshold": 0.5, "frames": 2}}
    events, _command = _person_run(monkeypatch, spec, [0.9, 0.2, 0.9, 0.3, 0.1])
    assert not [e for e in events if e["kind"] == "person"]


def test_рамка_меньше_min_size_сети_не_идёт_а_движение_остаётся(monkeypatch) -> None:
    """`person.minSize` — площадь рамки в % кадра (как `min_area` объекта у Frigate): пятно 12×12 — малая рамка,
    человека нет, но окно движения и его событие на месте."""
    spec = {"stream": "cam_sub", "offDelay": 1, "person": {"every": 1, "threshold": 0.5, "frames": 2, "minSize": 50}}
    events, _command = _person_run(monkeypatch, spec, [0.9, 0.9, 0.9, 0.9])
    assert not [e for e in events if e["kind"] == "person"]
    assert ("motion", "start") in [(e["kind"], e.get("state")) for e in events]


def test_площадь_рамки_в_процентах_кадра() -> None:
    assert mo.box_share((0, 0, mo.WIDTH, mo.HEIGHT)) == 100.0
    assert mo.box_share((0, 0, mo.WIDTH // 2, mo.HEIGHT // 2)) == 25.0


def test_только_люди_движение_не_публикуется(monkeypatch) -> None:
    """Движение камеры приходит от неё самой: детекция дома — ради людей, строк движения дважды нет."""
    spec = {"stream": "cam_sub", "offDelay": 1, "motionEvents": False, "person": {"every": 1, "frames": 1}}
    events, _command = _person_run(monkeypatch, spec, [0.9])
    assert [e["kind"] for e in events] == ["person", "person"]
