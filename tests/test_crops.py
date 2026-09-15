"""Camera tile crop storage.

Mirrors `test_photos.py`: what is worth a test here is not "does a file get
written" but the two things that bite in the field — a file name that cannot
escape the directory whatever the manager or the resident sends as a tile id,
and a body validator that actually rejects what the editor never sends
(`docs/local-ha-app.md` — the resident's own crop lays on top of the
installer's, and a malformed one would corrupt the app's read of `TileCrop`).
"""

from __future__ import annotations

from pathlib import Path

from mega_home.crops import CropStore, crop_value_valid

CROP = {"x": 0.5, "y": 0.4, "w": 0.3}


def test_имя_файла_не_выходит_из_каталога(tmp_path: Path) -> None:
    store = CropStore(tmp_path)
    hostile = store.path("../../../etc/passwd")
    assert hostile.parent == tmp_path
    assert hostile.suffix == ".json"
    assert store.path("t1") != store.path("t2")
    assert store.path("t1") == store.path("t1")


def test_сохранённый_кадр_читается_обратно(tmp_path: Path) -> None:
    store = CropStore(tmp_path)
    store.save("t1", CROP)
    assert store.all(["t1", "t2"]) == {"t1": CROP}


def test_удаление_идемпотентно_по_результату(tmp_path: Path) -> None:
    store = CropStore(tmp_path)
    store.save("t1", CROP)
    assert store.delete("t1") is True
    assert store.delete("t1") is False
    assert store.all(["t1"]) == {}


def test_каталог_создаётся_при_первом_сохранении(tmp_path: Path) -> None:
    store = CropStore(tmp_path / "нет-такого")
    store.save("t1", CROP)
    assert store.all(["t1"]) == {"t1": CROP}
    # Незавершённых файлов не остаётся: запись идёт через .part с переименованием.
    assert not list((tmp_path / "нет-такого").glob("*.part"))


def test_битый_файл_на_диске_не_роняет_список(tmp_path: Path) -> None:
    store = CropStore(tmp_path)
    store.save("t1", CROP)
    store.path("t2").write_text("не json", encoding="utf-8")
    assert store.all(["t1", "t2"]) == {"t1": CROP}


def test_валидное_тело_ровно_то_что_рисует_редактор() -> None:
    assert crop_value_valid(CROP)
    assert crop_value_valid({"x": 0, "y": 1, "w": 0.999})


def test_невалидное_тело_отвергается() -> None:
    assert not crop_value_valid(None)
    assert not crop_value_valid([0.5, 0.5, 0.3])
    assert not crop_value_valid({"x": 0.5, "y": 0.5})  # не хватает w
    assert not crop_value_valid({"x": 0.5, "y": 0.5, "w": 0.3, "z": 1})  # лишнее поле
    assert not crop_value_valid({"x": 1.5, "y": 0.5, "w": 0.3})  # вне 0..1
    assert not crop_value_valid({"x": "0.5", "y": 0.5, "w": 0.3})  # не число
