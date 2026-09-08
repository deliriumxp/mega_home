"""Перенос обычного HTTP-запроса к API дома по каналу менеджера.

⚠ Смысл этих тестов один: **жилец снаружи должен уметь ровно то же, что дома, и
теми же путями**. Раньше канал переносил четыре именованные операции, поэтому
фотография комнаты, поставленная не из дома, оседала в браузере телефона и не
доезжала никуда — а выглядело это как «в приложении снаружи чего-то не хватает».
Проверяем поэтому не «работает ли фотография», а что перенос отвечает тем же,
чем локальная дверь, и что границы у него на месте.
"""

from __future__ import annotations

import asyncio
import base64
import json
from http import HTTPStatus
from pathlib import Path

import pytest
from homeassistant.core import State

from mega_home import ops
from mega_home.photos import PhotoStore, StockPhotoStore

JPEG = b"\xff\xd8\xff\xe0" + b"0" * 32

CONFIG = {
    "version": "sha256:abc",
    "home": {"name": "Дом"},
    "floors": [],
    "rooms": [{"id": "r1", "name": "Кухня", "floorId": "f1", "photoVersion": "v7"}],
    "tiles": [
        {
            "id": "t1",
            "roomId": "r1",
            "name": "Свет",
            "domain": "light",
            "entityId": "light.kitchen",
            "dimmable": True,
            "commands": {"turn_on": {"domain": "light", "service": "turn_on"}},
        }
    ],
    "scenarios": [],
    "assets": {"photo/tile/t1": {"v": "a1", "type": "image/jpeg"}},
}


class _States:
    def get(self, entity_id: str) -> State | None:
        return State("on", {}) if entity_id == "light.kitchen" else None


class _Services:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, dict]] = []

    async def async_call(self, domain, service, data, blocking=False):
        self.calls.append((domain, service, data))


class _Hass:
    def __init__(self) -> None:
        self.states = _States()
        self.services = _Services()

    async def async_add_executor_job(self, func, *args):
        return func(*args)


class _Assets:
    def __init__(self, directory: Path) -> None:
        self._dir = directory

    def path(self, key: str, version: str) -> Path:
        return self._dir / f"{key.replace('/', '_')}_{version}.bin"


class _Bundle:
    version = "1.4.0"


class _Coordinator:
    version = "sha256:abc"
    bundle = _Bundle()

    def __init__(self, tmp: Path) -> None:
        self.data = CONFIG
        self.photos = PhotoStore(tmp / "own")
        self.stock_photos = StockPhotoStore(tmp / "stock")
        self.assets = _Assets(tmp / "assets")
        self.icons_dir = tmp / "icons"
        for directory in ("own", "stock", "assets", "icons"):
            (tmp / directory).mkdir(parents=True, exist_ok=True)


def call(coordinator, method: str, path: str, body: bytes | None = None):
    payload = {"method": method, "path": path}
    if body is not None:
        payload["body"] = base64.b64encode(body).decode("ascii")
    return asyncio.run(ops.run(_Hass(), coordinator, "http", payload))


def body_of(answer) -> bytes:
    return base64.b64decode(answer["body"])


def json_of(answer):
    return json.loads(body_of(answer).decode("utf-8"))


@pytest.fixture()
def coordinator(tmp_path: Path) -> _Coordinator:
    return _Coordinator(tmp_path)


def test_состав_и_состояния_едут_теми_же_путями(coordinator):
    assert json_of(call(coordinator, "GET", "api/config"))["version"] == "sha256:abc"
    assert json_of(call(coordinator, "GET", "api/states"))["configVersion"] == "sha256:abc"


def test_команда_прибором_идёт_через_тот_же_перенос(coordinator):
    answer = call(coordinator, "POST", "api/command", json.dumps({"id": "t1", "command": "turn_on"}).encode())
    assert json_of(answer)["accepted"] is True


# ⚠ Ровно то, ради чего перенос и делался: фотография, поставленная СНАРУЖИ,
# обязана лечь в дом, а не в браузер телефона.
def test_фото_комнаты_снаружи_ложится_в_дом_и_потом_отдаётся(coordinator):
    posted = call(coordinator, "POST", "api/photo/r1", JPEG)
    assert json_of(posted)["accepted"] is True

    listed = json_of(call(coordinator, "GET", "api/photos"))["photos"]
    assert "r1" in listed

    got = call(coordinator, "GET", "api/photo/r1")
    assert got["contentType"] == "image/jpeg"
    assert body_of(got) == JPEG

    assert json_of(call(coordinator, "DELETE", "api/photo/r1"))["accepted"] is True
    with pytest.raises(ops.OpError) as err:
        call(coordinator, "GET", "api/photo/r1")
    assert err.value.status == HTTPStatus.NOT_FOUND


def test_фон_плитки_снаружи_тоже_ложится_в_дом(coordinator):
    call(coordinator, "POST", "api/photo/tile:t1", JPEG)
    assert "tile:t1" in json_of(call(coordinator, "GET", "api/photos"))["photos"]


# Границы переноса. ⚠ Они те же, что у локальной двери: ключ обязан быть в
# составе, формат — JPEG. Иначе снаружи можно было бы то, чего нельзя дома.
def test_чужой_ключ_и_не_jpeg_отвергаются(coordinator):
    with pytest.raises(ops.OpError) as err:
        call(coordinator, "POST", "api/photo/чужая-комната", JPEG)
    assert err.value.status == HTTPStatus.NOT_FOUND

    with pytest.raises(ops.OpError) as err:
        call(coordinator, "POST", "api/photo/r1", b"PNG?")
    assert err.value.status == HTTPStatus.BAD_REQUEST


def test_заготовка_и_файл_общего_канала_отдаются_тем_же_переносом(coordinator):
    coordinator.stock_photos.save("r1", "v7", JPEG)
    assert body_of(call(coordinator, "GET", "api/stock-photo/r1")) == JPEG

    coordinator.assets.path("photo/tile/t1", "a1").write_bytes(JPEG)
    answer = call(coordinator, "GET", "api/asset/photo/tile/t1")
    assert answer["contentType"] == "image/jpeg"
    assert body_of(answer) == JPEG


def test_иконка_сценария_не_выпускает_за_свой_каталог(coordinator):
    (coordinator.icons_dir / "evening_300.png").write_bytes(b"\x89PNG")
    assert body_of(call(coordinator, "GET", "icons/evening_300.png")) == b"\x89PNG"

    for path in ("icons/../secret", "icons/sub/dir.png"):
        with pytest.raises(ops.OpError) as err:
            call(coordinator, "GET", path)
        assert err.value.status == HTTPStatus.NOT_FOUND


def test_постер_камеры_едет_тем_же_переносом(coordinator, monkeypatch):
    """⚠ Кадр — обработчик ПУТИ, а не новая операция канала.

    Снаружи у приложения нет ни одного адреса Home Assistant, а переговоры
    WebRTC длятся секунды: без постера просмотр открывается чёрным
    прямоугольником. Один кадр на ОТКРЫТИЕ камеры — кадра для плитки снаружи
    нет вовсе, он обновляется по таймеру и был бы потоком через менеджер.
    """
    from mega_home import webrtc

    coordinator.data = {
        **CONFIG,
        "tiles": [
            *CONFIG["tiles"],
            {"id": "cam1", "roomId": "r1", "name": "Калитка", "domain": "camera",
             "entityId": "camera.hall"},
        ],
    }

    async def snapshot(hass, entity_id):
        assert entity_id == "camera.hall"
        return {"contentType": "image/jpeg", "image": base64.b64encode(JPEG).decode("ascii")}

    monkeypatch.setattr(webrtc, "snapshot", snapshot)
    answer = call(coordinator, "GET", "api/camera-frame/cam1")

    assert body_of(answer) == JPEG
    assert answer["contentType"] == "image/jpeg"
    # Кадр живой: закешированный постер показывал бы вчерашний двор.
    assert answer["cacheControl"] == "no-store"


def test_неизвестный_путь_это_отказ_а_не_догадка(coordinator):
    with pytest.raises(ops.OpError) as err:
        call(coordinator, "GET", "api/чего-нибудь")
    assert err.value.status == HTTPStatus.NOT_FOUND


def test_слишком_большой_запрос_отвергается_до_работы(coordinator):
    with pytest.raises(ops.OpError) as err:
        call(coordinator, "POST", "api/photo/r1", JPEG + b"0" * (5 * 1024 * 1024))
    assert err.value.status == HTTPStatus.REQUEST_ENTITY_TOO_LARGE
