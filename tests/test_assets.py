"""Хранилище файлов, которые дом забирает у менеджера.

Проверяем то же, что и у фотографий: имя файла не выходит из каталога, каким бы
ни был ключ (он приходит из менеджера и содержит `/` и `:`), и версия входит в
имя — значит замена файла это другое имя, а не «инвалидация кэша».
"""

from __future__ import annotations

from pathlib import Path

from mega_home.assets import AssetStore


def test_имя_файла_не_выходит_из_каталога(tmp_path: Path) -> None:
    store = AssetStore(tmp_path)
    hostile = store.path("../../../etc/passwd", "v1")
    assert hostile.parent == tmp_path
    # Ключ со слэшами и двоеточиями — обычное дело: `photo/tile/ha:light.x`.
    assert store.path("photo/tile/ha:light.x", "v1").parent == tmp_path
    assert store.path("a", "v1") != store.path("b", "v1")
    assert store.path("a", "v1") == store.path("a", "v1")


def test_версия_входит_в_имя_файла(tmp_path: Path) -> None:
    store = AssetStore(tmp_path)
    store.save("sound/doorbell", "v1", b"first")
    store.save("sound/doorbell", "v2", b"second")
    assert store.has("sound/doorbell", "v1")
    assert store.has("sound/doorbell", "v2")
    assert store.path("sound/doorbell", "v2").read_bytes() == b"second"


def test_чистка_оставляет_только_названное_манифестом(tmp_path: Path) -> None:
    store = AssetStore(tmp_path)
    store.save("sound/doorbell", "v1", b"first")
    store.save("font/inter", "f1", b"font")

    store.prune({"font/inter": "f1"})

    assert not store.has("sound/doorbell", "v1")
    assert store.has("font/inter", "f1")
    assert store.count() == 1
