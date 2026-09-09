"""Раздача интерфейса: что видит жилец, пока бандла ещё нет.

⚠ Копии интерфейса в релизе НЕТ (2026-09-06, `docs/plan-thin-integration.md`
в репозитории менеджера): первый запуск считаем онлайн. Значит поведение «пока
не скачали» — не крайний случай, а нормальный первый экран, и оно проверяется.
"""

from __future__ import annotations

import asyncio
from typing import Any

from mega_home.http import _serve


class FakeBundle:
    def __init__(self, active_dir: Any = None, version: Any = None) -> None:
        self.active_dir = active_dir
        self.version = version


class FakeCoordinator:
    def __init__(self, active_dir: Any = None, version: Any = None) -> None:
        self.bundle = FakeBundle(active_dir, version)


class FakeEntries:
    def __init__(self, coordinator: Any) -> None:
        self._coordinator = coordinator

    def async_loaded_entries(self, _domain: str) -> list[Any]:
        if self._coordinator is None:
            return []
        entry = type("Entry", (), {"runtime_data": self._coordinator})()
        return [entry]


class FakeRequest:
    def __init__(self, coordinator: Any, query: dict[str, str] | None = None) -> None:
        hass = type("Hass", (), {"config_entries": FakeEntries(coordinator)})()
        self.app = {"hass": hass}
        self.query = query or {}


def _get(coordinator: Any, path: str) -> Any:
    return _serve(FakeRequest(coordinator), path)


def test_страница_до_первой_загрузки_бандла_это_заглушка() -> None:
    response = _get(FakeCoordinator(), "index.html")

    assert response.status == 200
    assert "Подключаюсь к менеджеру" in response.text
    # Сама перезагрузится: бандл приезжает фоном, жильцу нечего нажимать.
    assert "refresh" in response.text
    assert response.headers["Cache-Control"] == "no-cache"


def test_файл_бандла_до_загрузки_это_честный_404() -> None:
    """⚠ Не заглушка: HTML вместо js — ошибка разбора вместо понятного экрана."""
    assert _get(FakeCoordinator(), "main-A.js").status == 404


def test_без_координатора_тоже_заглушка() -> None:
    """Запись ещё не загрузилась — жилец всё равно не должен видеть 404."""
    assert _get(None, "index.html").status == 200


# --- Адрес приложения несёт версию бандла ----------------------------------
#
# ⚠ Разбор 2026-09-08. Дом раздавал новый интерфейс, а жилец видел старый — до
# ЖЁСТКОГО обновления страницы. Обычная перезагрузка поднимала `index.html` из
# кэша, а его достаточно, чтобы остаться на старом коде целиком: имена бандлов
# внутри хешированные, и старый `index.html` честно тянет старый `main-*.js`.
# Ни `no-cache`, ни `no-store` дыру не закрывают: ту же страницу держит service
# worker самого Home Assistant (scope `/`) — маршрутом `StaleWhileRevalidate` с
# `matchOptions: {ignoreSearch: true}`, то есть в кэше ищется ЛЮБАЯ запись с тем
# же ПУТЁМ. Поэтому первая попытка (версия в `?v=`) не сработала вовсе: свежий
# query добавлял запись, которую никто не читает. Версия уехала в ПУТЬ, а свои
# страницы мы забираем у чужого воркера своим — на более узком scope.


def _root(coordinator: Any, query: dict[str, str] | None = None) -> Any:
    from mega_home.http import MegaHomeAppRootView

    return asyncio.run(MegaHomeAppRootView().get(FakeRequest(coordinator, query)))


def test_голый_адрес_уводит_на_путь_с_версией(tmp_path) -> None:
    (tmp_path / "index.html").write_text("<html></html>", encoding="utf-8")

    response = _root(FakeCoordinator(tmp_path, "sha256-новая"))

    assert response.status == 302
    # ⚠ Версия в ПУТИ, а не в `?v=`: воркер Home Assistant обслуживает страницы
    # с `ignoreSearch: true` и на свежий query отдаёт вчерашнюю запись из кэша.
    assert response.location == "/mega-home/v/sha256-новая/"
    # Сам редирект кэшировать нельзя — иначе он сам станет тем, что устарело.
    assert response.headers["Cache-Control"] == "no-store"


def test_путь_с_версией_отдаёт_приложение(tmp_path) -> None:
    """⚠ Каталога `v/<версия>` в бандле нет: префикс срезается, файлы лежат
    там же, где лежали, и приложение просит их от `<base href>` без него."""
    from mega_home.http import _strip_version

    (tmp_path / "index.html").write_text("<html></html>", encoding="utf-8")

    assert _strip_version("v/sha256-новая/") == "index.html"
    assert _strip_version("v/sha256-новая/main-A.js") == "main-A.js"
    assert _strip_version("main-A.js") == "main-A.js"

    response = _get(FakeCoordinator(tmp_path, "sha256-новая"), "index.html")

    assert response.status == 200
    # ⚠ `no-store`, а не `no-cache`: второе разрешает хранить и лишь обязывает
    # переспросить — этого и не хватило.
    assert response.headers["Cache-Control"] == "no-store"


def test_свой_воркер_раздаётся_и_забирает_scope() -> None:
    """⚠ Ради этого он и заведён: браузер выбирает регистрацию с самым длинным
    совпадающим scope, и наши страницы уходят из-под воркера Home Assistant
    (scope `/`), который отдавал их из кэша с `ignoreSearch`."""
    import asyncio as _asyncio

    from mega_home.http import MegaHomeServiceWorkerView

    response = _asyncio.run(MegaHomeServiceWorkerView().get(FakeRequest(None)))

    assert response.status == 200
    assert response.headers["Content-Type"].startswith("text/javascript")
    # Застрявшая копия воркера — это застрявший scope.
    assert response.headers["Cache-Control"] == "no-store"
    assert response.headers["Service-Worker-Allowed"] == "/mega-home/"
    # Ничего не перехватывает: он нужен как ЗАНЯТЫЙ scope, а не как кэш.
    assert "addEventListener('fetch'" not in response.text


def test_без_бандла_редиректа_нет_а_есть_заглушка() -> None:
    """Уводить некуда: версии нет, а жилец должен видеть «подключаюсь»."""
    assert _root(FakeCoordinator()).status == 200


# --- Фоны: ключи комнат И ПЛИТОК ------------------------------------------
#
# ⚠ Плитка получила право на свой фон в 0.1.15: инсталлятор снимает в квартире
# сам прибор и ставит снимок фоном его плитки. Ключ — `tile:<id>`, и обе
# проверки ниже держат ровно тот контракт, который есть у приложения
# (`room-photos.ts`, `tilePhotoKey`): чужой ключ дом не пишет, свой —
# перечисляет, иначе фон лежит на диске и не показывается.

from mega_home.photos import photo_key_known as _photo_key_known, photo_keys as _photo_keys

PHOTO_CONFIG = {
    "rooms": [{"id": "kitchen"}, {"id": "hall"}],
    "tiles": [{"id": "light.kitchen_main"}, {"id": "media_player.tv"}],
}


def test_перечисляются_и_комнаты_и_плитки() -> None:
    assert _photo_keys(PHOTO_CONFIG) == [
        "kitchen",
        "hall",
        "tile:light.kitchen_main",
        "tile:media_player.tv",
    ]


def test_комната_без_id_в_список_не_попадает() -> None:
    assert _photo_keys({"rooms": [{}, {"id": "hall"}], "tiles": [{}]}) == ["hall"]


def test_писать_можно_только_то_что_есть_в_составе() -> None:
    assert _photo_key_known(PHOTO_CONFIG, "kitchen")
    assert _photo_key_known(PHOTO_CONFIG, "tile:media_player.tv")
    # Чужая комната, чужая плитка и плитка, названная как комната.
    assert not _photo_key_known(PHOTO_CONFIG, "bathroom")
    assert not _photo_key_known(PHOTO_CONFIG, "tile:light.unknown")
    assert not _photo_key_known(PHOTO_CONFIG, "light.kitchen_main")
    # Приставка без идентификатора — тоже не ключ.
    assert not _photo_key_known(PHOTO_CONFIG, "tile:")

