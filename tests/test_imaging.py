"""Готовые варианты фото (`imaging.py`).

Проверяется то, что бьёт на объекте: вид совпадает с прежним CSS (ч/б по тем же
коэффициентам, затенение как чёрный слой), набор вариантов не растёт от
произвольных чисел в адресе, а смена исходника не оставляет устройства со
старым видом.
"""

from __future__ import annotations

import asyncio
import os
from io import BytesIO
from pathlib import Path

from PIL import Image

from mega_home.imaging import (
    SIDES,
    Look,
    LookStore,
    asset_file,
    look_from_query,
    photo_file,
    render,
)


def jpeg(path: Path, size=(1920, 1080), color=(200, 40, 40)) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    Image.new("RGB", size, color).save(path, "JPEG", quality=95)
    return path


def picture(payload: bytes) -> Image.Image:
    return Image.open(BytesIO(payload))


class _Hass:
    async def async_add_executor_job(self, func, *args):
        return func(*args)


def test_без_вида_отдаётся_исходник():
    assert look_from_query({}) is None
    assert look_from_query({"v": "abc"}) is None


def test_числа_приводятся_к_ступеням_и_пределам():
    # ⚠ HTTP дома без аутентификации: произвольное число в адресе не должно
    # рождать новый файл на диске.
    assert look_from_query({"w": "1000"}).side == 1080
    assert look_from_query({"w": "99999"}).side == SIDES[-1]
    assert look_from_query({"blur": "5000"}).blur == 60
    assert look_from_query({"dim": "-3", "gray": "1"}) == Look(gray=True)
    assert look_from_query({"w": "мусор", "blur": "x"}) is None


def test_размер_по_длинной_стороне_без_увеличения(tmp_path: Path):
    source = jpeg(tmp_path / "a.jpg")
    assert picture(render(source, Look(side=720))).size == (720, 405)
    small = jpeg(tmp_path / "b.jpg", size=(400, 300))
    assert picture(render(small, Look(side=1920))).size == (400, 300)


def test_размытая_картинка_хранится_мелкой(tmp_path: Path):
    # Размытое гладкое — GPU растянет его без потерь, а весит оно в разы меньше.
    source = jpeg(tmp_path / "a.jpg")
    blurred = picture(render(source, Look(side=1920, blur=40)))
    assert max(blurred.size) < 1920
    assert max(blurred.size) >= SIDES[0]


def test_чб_и_затенение_как_у_css(tmp_path: Path):
    source = jpeg(tmp_path / "a.jpg", color=(255, 255, 255))
    gray = picture(render(source, Look(side=360, gray=True, dim=50)))
    assert gray.mode == "L"
    # Белый под чёрным слоем с непрозрачностью 50% — середина шкалы.
    assert abs(gray.getpixel((10, 10)) - 128) <= 3

    red = jpeg(tmp_path / "r.jpg", color=(255, 0, 0))
    value = picture(render(red, Look(side=360, gray=True))).getpixel((10, 10))
    # Коэффициент красного у `filter: grayscale()` — 0.2126.
    assert abs(value - round(255 * 0.2126)) <= 4


def test_вариант_считается_один_раз(tmp_path: Path):
    source = jpeg(tmp_path / "photos" / "abc.jpg")
    store = LookStore(tmp_path / "looks", {"p": source.parent})
    first = asyncio.run(store.async_file(_Hass(), "p", source, {"w": "540"}))
    stamp = first.stat().st_mtime_ns
    second = asyncio.run(store.async_file(_Hass(), "p", source, {"w": "540"}))
    assert first == second and second.stat().st_mtime_ns == stamp


def test_замена_фото_пересчитывает_виды_заранее(tmp_path: Path):
    photos = tmp_path / "photos"
    source = jpeg(photos / "abc.jpg", color=(10, 10, 10))
    store = LookStore(tmp_path / "looks", {"p": photos})
    old = store.ensure("p", source, Look(side=540, blur=14))

    jpeg(photos / "abc.jpg", size=(1600, 900), color=(250, 250, 250))
    os.utime(photos / "abc.jpg", ns=(1, old.stat().st_mtime_ns + 10**9))
    store.refresh()

    names = [path.name for path in (tmp_path / "looks").iterdir()]
    assert old.name not in names
    fresh = store.path("p", source, Look(side=540, blur=14))
    assert names == [fresh.name]


def test_новая_версия_файла_менеджера_наследует_виды(tmp_path: Path):
    # У файла менеджера версия в ИМЕНИ: новая версия — другой файл того же ключа.
    assets = tmp_path / "assets"
    old_source = jpeg(assets / "k1_v1.bin")
    store = LookStore(tmp_path / "looks", {"a": assets})
    old = store.ensure("a", old_source, Look(side=720, gray=True, dim=35))

    old_source.unlink()
    new_source = jpeg(assets / "k1_v2.bin")
    store.refresh()

    names = [path.name for path in (tmp_path / "looks").iterdir()]
    assert names == [store.path("a", new_source, Look(side=720, gray=True, dim=35)).name]
    assert old.name not in names


def test_варианты_снятого_фото_уходят(tmp_path: Path):
    photos = tmp_path / "photos"
    source = jpeg(photos / "abc.jpg")
    store = LookStore(tmp_path / "looks", {"p": photos})
    store.ensure("p", source, Look(side=360))
    source.unlink()
    store.refresh()
    assert list((tmp_path / "looks").iterdir()) == []


class _Assets:
    def __init__(self, directory: Path) -> None:
        self.directory = directory

    def path(self, key: str, version: str) -> Path:
        return self.directory / f"{key.replace('/', '_')}_{version}.bin"


class _Photos:
    def __init__(self, directory: Path) -> None:
        self.directory = directory

    def path(self, key: str) -> Path:
        return self.directory / f"{key}.jpg"


class _Coordinator:
    def __init__(self, tmp: Path) -> None:
        self.photos = _Photos(tmp / "photos")
        self.assets = _Assets(tmp / "assets")
        self.looks = LookStore(tmp / "looks", {"p": tmp / "photos", "a": tmp / "assets"})
        self.data = {"assets": {"photo/room/r1": {"v": "v2", "type": "image/jpeg"}}}


def test_устаревшая_версия_в_адресе_не_отдаёт_старый_файл(tmp_path: Path):
    """⚠ Макет у инсталлятора знает версию от менеджера РАНЬШЕ дома.

    Ответ `immutable`: отдай дом старый файл под новым адресом — браузер
    держал бы старую картинку навсегда.
    """
    coordinator = _Coordinator(tmp_path)
    jpeg(coordinator.assets.path("photo/room/r1", "v2"))
    hass = _Hass()
    assert asyncio.run(asset_file(hass, coordinator, "photo/room/r1", {"v": "v3"})) is None
    target, kind = asyncio.run(
        asset_file(hass, coordinator, "photo/room/r1", {"v": "v2", "w": "360", "blur": "14"})
    )
    assert kind == "image/jpeg" and target.parent == tmp_path / "looks"
    original, _ = asyncio.run(asset_file(hass, coordinator, "photo/room/r1", {"v": "v2"}))
    assert original == coordinator.assets.path("photo/room/r1", "v2")


def test_битая_картинка_отдаётся_как_есть(tmp_path: Path):
    coordinator = _Coordinator(tmp_path)
    broken = coordinator.photos.path("r1")
    broken.parent.mkdir(parents=True)
    broken.write_bytes(b"\xff\xd8\xff not really a jpeg")
    served = asyncio.run(photo_file(_Hass(), coordinator, "r1", {"w": "360"}))
    assert served == broken
