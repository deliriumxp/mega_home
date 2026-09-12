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
from mega_home.photos import PhotoStore

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
        self.assets = _Assets(tmp / "assets")
        self.icons_dir = tmp / "icons"
        for directory in ("own", "assets", "icons"):
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


def test_файл_общего_канала_отдаётся_тем_же_переносом(coordinator):
    # ⚠ Своего маршрута заготовок (`api/stock-photo/`) БОЛЬШЕ НЕТ (0.2.40): он
    # появился раньше общего канала и делал ровно то же — фоны едут ключами
    # `photo/room/*` и `photo/tile/*` манифеста, приложение просит их оттуда.
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
        # ⚠ Сырые байты, не base64: `webrtc.snapshot` отдаёт кадр как есть
        # (2026-09-08), кодирует его в base64 только `relay_api.handle` —
        # ровно один раз, а не дважды туда-обратно.
        return "image/jpeg", JPEG

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


def test_состояние_архива_доезжает_и_снаружи(coordinator, monkeypatch):
    """⚠ Строка «поиск в архиве…» обязана работать ОБЕИМИ дверями.

    Состояние архива спрашивают инструментом живой сессии (`/command`), а он
    до этой правки был открыт только дома: снаружи приложение получало бы 404 и
    молча показывало иглу, бегущую впереди картинки. Разница «дома/снаружи»
    обязана оставаться только в адресе базы.
    """
    asked: list[tuple] = []

    async def session_command(coordinator, clip, fn, params):
        asked.append((clip, fn, params))
        return [{"token": "tok1", "state": "4", "time": "2026-09-12 11:56:58"}]

    monkeypatch.setattr(ops, "trassir_session_command", session_command)
    payload = json.dumps({"fn": "archive_status", "params": {"type": "state"}}).encode()
    answer = call(coordinator, "POST", "api/trassir/clips/trassir:tok1/command", payload)

    assert asked == [("trassir:tok1", "archive_status", {"type": "state"})]
    assert json_of(answer)[0]["state"] == "4"


def test_архив_по_метке_и_дни_доезжают_снаружи(coordinator, monkeypatch):
    """⚠ Классический просмотр архива обязан работать ОБЕИМИ дверями.

    Открытие записи по метке и выбор дня — не «домашняя» возможность: снаружи
    жилец смотрит архив ровно так же, и разница «дома/снаружи» обязана
    оставаться только в адресе базы.
    """
    opened: list[tuple] = []

    async def clip_at(
        coordinator, guid, timestamp_us, camera_name, quality=None,
        window_start_us=None, window_stop_us=None,
    ):
        # ⚠ Окно присылает ПРИЛОЖЕНИЕ: шкала — его дело, дом хранит присланное.
        opened.append((guid, timestamp_us, camera_name, quality, window_start_us, window_stop_us))
        return {"id": "trassir:tok1", "startUs": window_start_us, "stopUs": window_stop_us}

    asked: list[str] = []

    async def archive_days(coordinator, clip_id):
        asked.append(clip_id)
        return {"days": ["2026-09-08"], "dayStartUs": 0, "segments": []}

    monkeypatch.setattr(ops, "trassir_clip_at", clip_at)
    monkeypatch.setattr(ops, "trassir_archive_days", archive_days)

    body = json.dumps(
        {
            "timestampUs": 1788960000000000,
            "quality": "sub",
            "windowStartUs": 1788900000000000,
            "windowStopUs": 1788986400000000,
        }
    ).encode()
    clip = json_of(call(coordinator, "POST", "api/trassir/channels/cam1/clip", body))
    days = json_of(call(coordinator, "GET", "api/trassir/clips/trassir:tok1/days"))

    assert opened == [
        ("cam1", 1788960000000000, None, "sub", 1788900000000000, 1788986400000000)
    ]
    assert clip["startUs"] == 1788900000000000, "окно уходит дому как есть"
    assert asked == ["trassir:tok1"]
    assert days["days"] == ["2026-09-08"]
