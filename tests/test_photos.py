"""Room photo storage.

What is worth a test here is not "does a file get written" but the two things
that bite in the field: a file name that cannot escape the directory whatever
the manager sends as a room id, and a version that really changes when the
photo is replaced (the image URL is served `immutable`, so a repeated version
means the resident keeps seeing the old background).

⚠ The version test earned its keep immediately: the first implementation used
the file's modification time, and two saves in a row produced the same
`st_mtime_ns` — that is a background the resident replaces and never sees
change. The version is a hash of the content now.
"""

from __future__ import annotations

from pathlib import Path

from mega_home.photos import JPEG_MAGIC, PhotoStore, StockPhotoStore

JPEG = JPEG_MAGIC + b"...body..."


def test_имя_файла_не_выходит_из_каталога(tmp_path: Path) -> None:
    store = PhotoStore(tmp_path)
    hostile = store.path("../../../etc/passwd")
    assert hostile.parent == tmp_path
    assert hostile.suffix == ".jpg"
    # Разные комнаты — разные файлы, одна и та же — один и тот же.
    assert store.path("r1") != store.path("r2")
    assert store.path("r1") == store.path("r1")


def test_версия_меняется_при_замене_фото(tmp_path: Path) -> None:
    store = PhotoStore(tmp_path)
    first = store.save("r1", JPEG)
    second = store.save("r1", JPEG + b"other")
    assert first != second
    assert store.path("r1").read_bytes() == JPEG + b"other"


def test_список_версий_только_по_существующим(tmp_path: Path) -> None:
    store = PhotoStore(tmp_path)
    store.save("r1", JPEG)
    versions = store.versions(["r1", "r2"])
    assert set(versions) == {"r1"}
    assert versions["r1"] == store.save("r1", JPEG)  # версия = содержимое


def test_удаление_идемпотентно_по_результату(tmp_path: Path) -> None:
    store = PhotoStore(tmp_path)
    store.save("r1", JPEG)
    assert store.delete("r1") is True
    # Второе удаление ничего не находит — вью превращает это в 404, а не в 500.
    assert store.delete("r1") is False
    assert store.count() == 0


def test_каталог_создаётся_при_первом_сохранении(tmp_path: Path) -> None:
    store = PhotoStore(tmp_path / "нет-такого")
    store.save("r1", JPEG)
    assert store.count() == 1
    # Незавершённых файлов не остаётся: запись идёт через .part с переименованием.
    assert not list(store.directory.glob("*.part"))


# Заготовки инсталлятора — ЗЕРКАЛО менеджера, а не имущество дома: их можно
# выбросить и выкачать заново. Отсюда и разница в тестах: у своих фотографий
# жильца проверяется версия по содержимому, у этих — что имя файла несёт версию
# ИЗ КОНФИГА (иначе «уже держим?» превращается в скачивание ради сравнения) и
# что лишнее действительно вычищается.
def test_заготовка_лежит_под_версией_из_конфига(tmp_path: Path) -> None:
    store = StockPhotoStore(tmp_path)
    assert not store.has("r1", "v1")
    store.save("r1", "v1", JPEG)
    assert store.has("r1", "v1")
    # Инсталлятор заменил фон — это другой файл, а не тот же под новой меткой.
    assert not store.has("r1", "v2")
    assert store.path("r1", "v1") != store.path("r1", "v2")


def test_имя_заготовки_не_выходит_из_каталога(tmp_path: Path) -> None:
    store = StockPhotoStore(tmp_path)
    hostile = store.path("../../../etc/passwd", "../../v1")
    assert hostile.parent == tmp_path
    assert hostile.suffix == ".jpg"


def test_чистка_убирает_и_старые_версии_и_снятые_фоны(tmp_path: Path) -> None:
    store = StockPhotoStore(tmp_path)
    store.save("r1", "v1", JPEG)
    store.save("r1", "v2", JPEG + b"new")
    store.save("r2", "v1", JPEG)

    # Конфиг называет только r1@v2: у r1 сменился фон, у r2 его сняли совсем.
    store.prune({"r1": "v2"})

    assert store.has("r1", "v2")
    assert not store.has("r1", "v1")
    assert not store.has("r2", "v1")
    assert store.count() == 1


def test_чистка_на_пустом_каталоге_молчит(tmp_path: Path) -> None:
    # Дом, где заготовок не было ни одной: синхронизация всё равно зовёт чистку.
    store = StockPhotoStore(tmp_path / "нет-такого")
    store.prune({})
    assert store.count() == 0
