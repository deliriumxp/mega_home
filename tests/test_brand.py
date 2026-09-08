"""Картинки бренда: они есть, и они того размера, которого ждёт Home Assistant.

⚠ Проверяется размер В ПИКСЕЛЯХ, а не «файл на месте»: HA 2026.3+ отдаёт эту
папку как есть (`/api/brands/integration/mega_home/icon.png`), а те же файлы
уходят в PR к `home-assistant/brands`, где 256×256 и 512×512 — жёсткое условие
приёмки. Пересобрал иконку не тем масштабом — узнать об этом на ревью чужого
репозитория дороже, чем здесь.

Размер читается из заголовка PNG руками: Pillow тут не зависимость, и тащить
её ради двух чисел незачем.
"""

from __future__ import annotations

import struct
from pathlib import Path

import pytest

BRAND = Path(__file__).resolve().parents[1] / "custom_components" / "mega_home" / "brand"

PNG_MAGIC = b"\x89PNG\r\n\x1a\n"


def _size(path: Path) -> tuple[int, int]:
    head = path.read_bytes()[:24]
    assert head[:8] == PNG_MAGIC, f"{path.name} is not a PNG"
    assert head[12:16] == b"IHDR", f"{path.name} has no IHDR first"
    return struct.unpack(">II", head[16:24])


@pytest.mark.parametrize(("name", "expected"), [("icon.png", 256), ("icon@2x.png", 512)])
def test_icon_has_the_required_square_size(name: str, expected: int) -> None:
    assert _size(BRAND / name) == (expected, expected)


def test_source_svg_stays_next_to_the_pngs() -> None:
    """Без источника следующая правка иконки начнётся с обводки по пикселям."""
    assert (BRAND / "icon.svg").is_file()
