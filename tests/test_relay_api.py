"""Перенос обычного HTTP-запроса к API дома по каналу менеджера (`relay_api.py`).

⚠ Смысл этих тестов один: **жилец снаружи должен уметь ровно то же, что дома, и
теми же путями**. Проверяем поэтому не «работает ли фотография», а что перенос
отвечает тем же, чем локальная дверь, и что границы у него на месте.
"""

from __future__ import annotations

import asyncio
import base64
import json
from http import HTTPStatus
from pathlib import Path

import pytest
from homeassistant.core import State

from fake_host import FakeHost, FakeSource
from mega_home.core import ops
from mega_home.core.crops import CropStore
from mega_home.core.imaging import LookStore
from mega_home.core.photos import PhotoStore
from mega_home.core.relay_api import handle

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
        },
        {
            "id": "crop-cam",
            "roomId": "r1",
            "name": "Камера входа",
            "domain": "camera",
            "entityId": None,
        },
    ],
    "scenarios": [],
    "assets": {"photo/tile/t1": {"v": "a1", "type": "image/jpeg"}},
}


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
    accesses = None

    def __init__(self, tmp: Path) -> None:
        self.data = CONFIG
        self.photos = PhotoStore(tmp / "own")
        self.crops = CropStore(tmp / "crops")
        self.assets = _Assets(tmp / "assets")
        self.looks = LookStore(tmp / "looks", {"p": tmp / "own", "a": tmp / "assets"})
        self.icons_dir = tmp / "icons"
        self.env = FakeHost(tmp)
        self.source = FakeSource({"light.kitchen": State("on", {})})
        for directory in ("own", "crops", "assets", "icons"):
            (tmp / directory).mkdir(parents=True, exist_ok=True)


def call(coordinator, method: str, path: str, body: bytes | None = None):
    payload = {"method": method, "path": path}
    if body is not None:
        payload["body"] = base64.b64encode(body).decode("ascii")
    return asyncio.run(handle(coordinator, payload))


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


# Кадр камеры, подправленный СНАРУЖИ, — та же дисциплина, что у фото: ложится в
# дом, а не остаётся у менеджера или в браузере телефона.
def test_кадр_камеры_снаружи_тоже_ложится_в_дом(coordinator):
    crop = {"x": 0.5, "y": 0.4, "w": 0.3}
    posted = call(coordinator, "POST", "api/crop/crop-cam", json.dumps(crop).encode())
    assert json_of(posted) == {"accepted": True, "crop": crop}

    listed = json_of(call(coordinator, "GET", "api/crops"))["crops"]
    assert listed == {"crop-cam": crop}

    assert json_of(call(coordinator, "DELETE", "api/crop/crop-cam"))["accepted"] is True
    assert json_of(call(coordinator, "GET", "api/crops"))["crops"] == {}


def test_чужая_плитка_и_не_камера_и_кривое_тело_отвергаются(coordinator):
    crop = {"x": 0.5, "y": 0.4, "w": 0.3}
    with pytest.raises(ops.OpError) as err:
        call(coordinator, "POST", "api/crop/неизвестная", json.dumps(crop).encode())
    assert err.value.status == HTTPStatus.NOT_FOUND

    with pytest.raises(ops.OpError) as err:
        call(coordinator, "POST", "api/crop/t1", json.dumps(crop).encode())  # свет, не камера
    assert err.value.status == HTTPStatus.NOT_FOUND

    with pytest.raises(ops.OpError) as err:
        call(coordinator, "POST", "api/crop/crop-cam", json.dumps({"x": 2, "y": 0, "w": 0}).encode())
    assert err.value.status == HTTPStatus.BAD_REQUEST


# Границы переноса. ⚠ Они те же, что у локальной двери: ключ обязан быть в
# составе, формат — JPEG. Иначе снаружи можно было бы то, чего нельзя дома.
def test_чужой_ключ_и_не_jpeg_отвергаются(coordinator):
    with pytest.raises(ops.OpError) as err:
        call(coordinator, "POST", "api/photo/чужая-комната", JPEG)
    assert err.value.status == HTTPStatus.NOT_FOUND

    with pytest.raises(ops.OpError) as err:
        call(coordinator, "POST", "api/photo/r1", b"PNG?")
    assert err.value.status == HTTPStatus.BAD_REQUEST


def test_файл_общего_канала_отдаётся_тем_же_переносом(coordinator):
    coordinator.assets.path("photo/tile/t1", "a1").write_bytes(JPEG)
    answer = call(coordinator, "GET", "api/asset/photo/tile/t1")
    assert answer["contentType"] == "image/jpeg"
    assert body_of(answer) == JPEG


def test_готовый_вид_фото_едет_тем_же_переносом(coordinator):
    """⚠ Снаружи и в макете у инсталлятора вид фото считает ТОТ ЖЕ дом.

    Query не теряется по дороге, и приложение узнаёт умение дома ответом
    (`imaging`), а не номером версии.
    """
    from io import BytesIO

    from PIL import Image

    buffer = BytesIO()
    Image.new("RGB", (1920, 1080), (200, 50, 50)).save(buffer, "JPEG")
    coordinator.assets.path("photo/tile/t1", "a1").write_bytes(buffer.getvalue())

    assert json_of(call(coordinator, "GET", "api/photos"))["imaging"] is True
    answer = call(coordinator, "GET", "api/asset/photo%2Ftile%2Ft1?v=a1&w=540&gray=1&dim=35")
    variant = Image.open(BytesIO(body_of(answer)))
    assert answer["contentType"] == "image/jpeg"
    assert variant.mode == "L" and max(variant.size) == 540


def test_иконка_сценария_не_выпускает_за_свой_каталог(coordinator):
    (coordinator.icons_dir / "evening_300.png").write_bytes(b"\x89PNG")
    assert body_of(call(coordinator, "GET", "icons/evening_300.png")) == b"\x89PNG"

    for path in ("icons/../secret", "icons/sub/dir.png"):
        with pytest.raises(ops.OpError) as err:
            call(coordinator, "GET", path)
        assert err.value.status == HTTPStatus.NOT_FOUND


def test_неизвестный_путь_это_отказ_а_не_догадка(coordinator):
    with pytest.raises(ops.OpError) as err:
        call(coordinator, "GET", "api/чего-нибудь")
    assert err.value.status == HTTPStatus.NOT_FOUND


def test_слишком_большой_запрос_отвергается_до_работы(coordinator):
    with pytest.raises(ops.OpError) as err:
        call(coordinator, "POST", "api/photo/r1", JPEG + b"0" * (5 * 1024 * 1024))
    assert err.value.status == HTTPStatus.REQUEST_ENTITY_TOO_LARGE


# ⚠ Обе двери дома обязаны отвечать ОДИНАКОВО, и сверка ключа с составом стоит
# у обеих на ЗАПИСИ, а не на чтении. Иначе комната, скрытая инсталлятором в
# приложении или переименованная, дома показывала бы фон, а снаружи отдавала
# 404 — то самое «дома работает, снаружи нет».
def test_фон_снятой_из_состава_комнаты_читается_как_и_локально(coordinator):
    call(coordinator, "POST", "api/photo/r1", JPEG)
    # Комната ушла из состава (скрыта, переименована, вычеркнута).
    coordinator.data = {**CONFIG, "rooms": []}

    assert body_of(call(coordinator, "GET", "api/photo/r1")) == JPEG
    # А записать под этим ключом уже нельзя: набор ключей ограничен составом.
    with pytest.raises(ops.OpError) as err:
        call(coordinator, "POST", "api/photo/r1", JPEG)
    assert err.value.status == HTTPStatus.NOT_FOUND


def test_connect_доезжает_и_снаружи_тем_же_переносом(coordinator, monkeypatch):
    """⚠ `connect` — единственный контракт транспорта, и он ходит ОБЕИМИ дверями.

    Снаружи бандл делает то же самое, что дома, — по тому же коду
    (`ops.connect`), не по копии правил в переносе.
    """
    from mega_home.core import connect as connect_mod

    calls: list[dict] = []

    async def fake_perform(payload):
        calls.append(payload)
        return {"status": 200, "headers": {}, "body": "ok"}

    monkeypatch.setattr(connect_mod, "perform", fake_perform)

    body = json.dumps({"kind": "http", "host": "192.168.1.9", "port": 80, "path": "/x"}).encode()
    answer = call(coordinator, "POST", "api/connect", body)

    assert calls == [{"kind": "http", "host": "192.168.1.9", "port": 80, "path": "/x"}]
    assert json_of(answer) == {"status": 200, "headers": {}, "body": "ok"}


def test_лента_событий_устройства_доезжает_переносом(coordinator):
    """Часть F: хранилище на диске отдаёт ленту тем же переносом, что и дома."""
    from mega_home.core.device_store import DeviceEventStore

    import time

    at = round(time.time(), 3)
    coordinator.device_events = DeviceEventStore(coordinator.env)
    coordinator.device_events.add(
        {"id": "e1", "at": at, "access": "dev1", "source": "cam", "event": "motion", "data": None}
    )

    answer = call(coordinator, "GET", "api/device-events?access=dev1&limit=5")

    assert json_of(answer) == {
        "events": [{"id": "e1", "at": at, "source": "cam", "event": "motion", "data": None}]
    }
