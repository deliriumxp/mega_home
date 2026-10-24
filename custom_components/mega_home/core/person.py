"""Человек в окне движения: маленькая сеть «есть / нет» на вырезе, где детектор (`motion.py`) видит движение.

Решение заказчика 2026-10-24 (менеджер: `docs/superpowers/specs/2026-10-24-person-detection-design.md`): на
контроллерах объектов (4 слабых ядра ARM без ускорителя) полный детектор на кадр не тянет, а так же
устроено и в камерах — движение, потом маленькая сеть на уменьшенной картинке.

⚠ Сеть — эталон MLPerf Tiny «Visual Wake Words» (MobileNetV1 α = 0,25, вход 96×96 RGB, Apache-2.0), веса —
`person_model.npz` (`scripts/person_model.py`). Исполнитель — голый numpy: `onnxruntime`, `tflite` и OpenCV
в образ HA (Alpine, musl) не ставятся, а объекты бывают офлайн. Сверка с LiteRT — до 10⁻⁸; кадр — ~5 мс
на ядре x86.
⚠ Вырез — КВАДРАТ вокруг пятна движения с запасом: растянутая в квадрат фигура в рост сеть узнаёт хуже
(замер 2026-10-24: человек на остановке — 0,40 растянутым, 1,00 квадратом).
"""

from __future__ import annotations

import asyncio
import io
from pathlib import Path
from typing import Any

MODEL = Path(__file__).with_name("person_model.npz")
# Запас выреза вокруг пятна: рамка движения у́же человека (стоящие части тела не меняются).
MARGIN = 0.2
# BMP: заголовок файла 14 байт, размер файла — в байтах 2..6.
_BMP_HEAD = 14

_cache: dict[str, Any] = {}


def load(np: Any, path: Path = MODEL) -> dict[str, Any]:
    """Слои сети из `.npz` — один раз на процесс (в исполнителе)."""
    key = str(path)
    if key not in _cache:
        with np.load(path) as data:
            layers = []
            for i, kind in enumerate(data["kinds"]):
                layers.append({
                    "kind": str(kind), "stride": int(data["strides"][i]), "same": bool(data["same"][i]),
                    "act": int(data["acts"][i]),
                    "w": data[f"w{i}"].astype(np.float32) if f"w{i}" in data else None,
                    "b": data[f"b{i}"].astype(np.float32) if f"b{i}" in data else None,
                })
            _cache[key] = {"layers": layers, "size": int(data["size"]), "person": int(data["person"])}
    return _cache[key]


def _pad_same(np: Any, x: Any, k: int, s: int) -> Any:
    h, w = x.shape[:2]
    ph = max((-(-h // s) - 1) * s + k - h, 0)
    pw = max((-(-w // s) - 1) * s + k - w, 0)
    return np.pad(x, ((ph // 2, ph - ph // 2), (pw // 2, pw - pw // 2), (0, 0)))


def forward(np: Any, layers: list[dict[str, Any]], x: Any) -> Any:
    """Прямой проход, раскладка NHWC без N (как у TFLite)."""
    for layer in layers:
        kind, w = layer["kind"], layer["w"]
        if kind in ("conv", "dw"):
            k, s = w.shape[1], layer["stride"]
            src = _pad_same(np, x, k, s) if layer["same"] else x
            win = np.lib.stride_tricks.sliding_window_view(src, (k, k), axis=(0, 1))[::s, ::s]
            if kind == "conv":
                rows, cols = win.shape[:2]
                x = (win.transpose(0, 1, 3, 4, 2).reshape(rows * cols, -1) @ w.reshape(w.shape[0], -1).T).reshape(rows, cols, -1)
            else:
                x = np.einsum("hwcij,ijc->hwc", win, w[0], optimize=True)
        elif kind == "pool":
            x = x.mean(axis=(0, 1), keepdims=True)
        elif kind == "fc":
            x = x.reshape(-1) @ w.T
        elif kind == "softmax":
            e = np.exp(x - x.max())
            x = e / e.sum()
        if layer["b"] is not None:
            x = x + layer["b"]
        if layer["act"] == 2:
            x = np.clip(x, 0, 6)
        elif layer["act"] == 1:
            x = np.maximum(x, 0)
        x = np.ascontiguousarray(x, dtype=np.float32)
    return x


def square(box: tuple[float, float, float, float], width: int, height: int, margin: float = MARGIN) -> tuple[int, int, int, int]:
    """Квадрат вокруг рамки с запасом, сдвинутый внутрь кадра (не больше меньшей стороны кадра)."""
    x0, y0, x1, y1 = box
    side = min(max(x1 - x0, y1 - y0) * (1 + margin), width, height)
    left = min(max((x0 + x1 - side) / 2, 0), width - side)
    top = min(max((y0 + y1 - side) / 2, 0), height - side)
    return int(left), int(top), int(left + side), int(top + side)


def wide(box: tuple[int, int, int, int], width: int, height: int, aspect: float = 16 / 9) -> tuple[int, int, int, int]:
    """Рамка снимка «человек» вокруг квадрата сети: пропорции кадра ленты (16:9, `fill` у бандла —
    квадрат там растянулся бы), высота — по квадрату, сдвинута внутрь кадра."""
    x0, y0, x1, y1 = box
    h = min(float(y1 - y0), height, width / aspect)
    w = h * aspect
    left = min(max((x0 + x1 - w) / 2, 0), width - w)
    top = min(max((y0 + y1 - h) / 2, 0), height - h)
    return int(left), int(top), int(left + w), int(top + h)


def shot(frame: Any, box: tuple[float, float, float, float], margin: float, width: int, quality: int = 80) -> bytes | None:
    """JPEG крупного участка, где сеть увидела человека (в исполнителе); Pillow нет — None, и к событию
    приложится снимок камеры правилом вложения."""
    try:
        from PIL import Image  # noqa: PLC0415 — Pillow есть у HA, ядру без HA не обещан
    except ImportError:
        return None
    height_px, width_px = frame.shape[:2]
    left, top, right, bottom = wide(square(box, width_px, height_px, margin), width_px, height_px)
    if right - left < 2 or bottom - top < 2:
        return None
    # Копия — непрерывная: кадр из BMP перевёрнут видом (`read_bmp`).
    image = Image.fromarray(frame[top:bottom, left:right].copy())
    if image.width > width:
        image = image.resize((width, round(width * image.height / image.width)), Image.Resampling.BILINEAR)
    out = io.BytesIO()
    image.save(out, "JPEG", quality=quality)
    return out.getvalue()


def resize(np: Any, image: Any, size: int) -> Any:
    """Билинейное уменьшение HxWx3 до size×size (без Pillow: ядру он не гарантирован)."""
    h, w = image.shape[:2]
    ys = np.clip((np.arange(size) + 0.5) * h / size - 0.5, 0, h - 1)
    xs = np.clip((np.arange(size) + 0.5) * w / size - 0.5, 0, w - 1)
    y0, x0 = ys.astype(int), xs.astype(int)
    y1, x1 = np.minimum(y0 + 1, h - 1), np.minimum(x0 + 1, w - 1)
    fy, fx = (ys - y0)[:, None, None], (xs - x0)[None, :, None]
    img = image.astype(np.float32)
    top = img[y0][:, x0] * (1 - fx) + img[y0][:, x1] * fx
    bottom = img[y1][:, x0] * (1 - fx) + img[y1][:, x1] * fx
    return top * (1 - fy) + bottom * fy


def score(np: Any, model: dict[str, Any], frame: Any, box: tuple[float, float, float, float], margin: float = MARGIN) -> float:
    """Уверенность «человек» в квадрате вокруг `box` (пиксели кадра `frame`, HxWx3 RGB)."""
    height, width = frame.shape[:2]
    left, top, right, bottom = square(box, width, height, margin)
    if right - left < 2 or bottom - top < 2:
        return 0.0
    crop = resize(np, frame[top:bottom, left:right], model["size"]) / 255.0
    return float(forward(np, model["layers"], crop.astype(np.float32))[model["person"]])


async def read_bmp(np: Any, reader: asyncio.StreamReader) -> Any:
    """Следующий кадр BMP (24 бит, ffmpeg `image2pipe -c:v bmp`) из трубы → HxWx3 RGB uint8."""
    head = await reader.readexactly(_BMP_HEAD)
    size = int.from_bytes(head[2:6], "little")
    body = await reader.readexactly(size - _BMP_HEAD)
    offset = int.from_bytes(head[10:14], "little") - _BMP_HEAD
    width = int.from_bytes(body[4:8], "little", signed=True)
    height = int.from_bytes(body[8:12], "little", signed=True)
    stride = (width * 3 + 3) & ~3
    rows = np.frombuffer(body, np.uint8, count=stride * abs(height), offset=offset).reshape(abs(height), stride)
    image = rows[:, : width * 3].reshape(abs(height), width, 3)[:, :, ::-1]
    # Положительная высота — строки снизу вверх (так пишет ffmpeg).
    return image[::-1] if height > 0 else image


class PersonWatch:
    """Человек в одном окне движения: проверки не чаще `every` секунд, «есть» — после `frames` попаданий
    подряд, и одно событие на окно."""

    def __init__(self, every: float = 1.0, threshold: float = 0.5, frames: int = 2) -> None:
        self.every, self.threshold, self.frames = every, threshold, frames
        self.checked = -1e9
        self.hits = 0
        self.present = False

    def due(self, now: float) -> bool:
        return not self.present and now - self.checked >= self.every

    def seen(self, value: float, now: float) -> bool:
        """Ответ сети; True — человек подтвердился сейчас (событие «началось»)."""
        self.checked = now
        self.hits = self.hits + 1 if value >= self.threshold else 0
        if self.hits >= self.frames and not self.present:
            self.present = True
            return True
        return False

    def end(self) -> bool:
        """Окно движения закрылось; True — был человек (событие «кончилось»)."""
        was = self.present
        self.hits, self.present, self.checked = 0, False, -1e9
        return was
