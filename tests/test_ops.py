"""The four operations of the resident app, independent of transport.

They are reached two ways — locally over HTTP and remotely over the manager
link — and the point of `ops.py` is that both get the SAME answers and the same
refusal wording. These tests exercise the module directly, which is also the
only place where that promise can be checked once instead of twice.
"""

from __future__ import annotations

import asyncio
from http import HTTPStatus

import pytest
from homeassistant.core import State
from homeassistant.exceptions import ServiceNotFound

from mega_home import ops


class _States:
    def __init__(self, states: dict[str, State]) -> None:
        self._states = states

    def get(self, entity_id: str) -> State | None:
        return self._states.get(entity_id)


class _Services:
    def __init__(self, raises: Exception | None = None) -> None:
        self.calls: list[tuple[str, str, dict]] = []
        self._raises = raises

    async def async_call(self, domain, service, data, blocking=False):
        if self._raises:
            raise self._raises
        self.calls.append((domain, service, data))


class _Hass:
    def __init__(self, states=None, raises=None) -> None:
        self.states = _States(states or {})
        self.services = _Services(raises)


class _Bundle:
    version = "1.4.0"


class _Coordinator:
    version = "sha256:abc"
    bundle = _Bundle()

    def __init__(self, data=None) -> None:
        self.data = data if data is not None else _CONFIG


_CONFIG = {
    "version": "sha256:abc",
    "home": {"name": "Дом"},
    "floors": [],
    "rooms": [{"id": "r1", "name": "Кухня", "floorId": "f1"}],
    "tiles": [
        {
            "id": "t1",
            "roomId": "r1",
            "name": "Свет",
            "domain": "light",
            "entityId": "light.kitchen",
            "dimmable": True,
        },
        {
            "id": "t2",
            "roomId": "r1",
            "name": "Розетка",
            "domain": "switch",
            "entityId": None,
            "dimmable": False,
        },
    ],
    "scenarios": [{"id": "s1", "roomId": "r1", "name": "Вечер", "icon": "evening", "entityId": "script.evening"}],
}


def run(hass, coordinator, op, payload=None):
    return asyncio.run(ops.run(hass, coordinator, op, payload))


def test_дом_без_конфига_отвечает_понятным_отказом():
    with pytest.raises(ops.OpError) as err:
        run(_Hass(), None, "config")
    assert err.value.status == HTTPStatus.SERVICE_UNAVAILABLE
    with pytest.raises(ops.OpError):
        run(_Hass(), _Coordinator(data={}), "states")


def test_неизвестная_операция_это_отказ_а_не_падение():
    with pytest.raises(ops.OpError) as err:
        run(_Hass(), _Coordinator(), "выключи-всё")
    assert err.value.status == HTTPStatus.NOT_FOUND


def test_состояния_несут_версии_конфига_и_бандла():
    hass = _Hass({"light.kitchen": State("on", {"brightness": 255})})
    answer = run(hass, _Coordinator(), "states")
    assert answer["configVersion"] == "sha256:abc"
    assert answer["appVersion"] == "1.4.0"
    # Плитка без сущности в Home Assistant остаётся в списке, но недоступна:
    # приложение показывает ВЕСЬ состав объекта, а не только отправленное.
    light, socket = answer["entities"]
    assert light["state"]["power"] is True and light["state"]["brightness"] == 100
    assert "brightness" in light["capabilities"]
    assert socket["available"] is False


def test_команда_превращается_в_вызов_службы():
    hass = _Hass({"light.kitchen": State("off")})
    answer = run(hass, _Coordinator(), "command", {"id": "t1", "command": "set_brightness", "value": 40})
    assert answer["accepted"] is True
    assert hass.services.calls == [
        ("light", "turn_on", {"entity_id": "light.kitchen", "brightness_pct": 40.0})
    ]
    # ⚠ Ответ несёт НОВОЕ состояние плитки: иначе приложение либо ждёт снимка,
    # либо рисует угаданное — и то и другое неправильно внутри дома.
    assert answer["entity"]["id"] == "t1"
    assert answer["entity"]["state"]["value"] == "off"


def test_чужая_команда_и_чужое_устройство_отвергаются():
    hass = _Hass()
    # Команды вне таблицы нет — через нас нельзя позвать произвольную службу.
    with pytest.raises(ops.OpError):
        run(hass, _Coordinator(), "command", {"id": "t1", "command": "delete_everything"})
    with pytest.raises(ops.OpError):
        run(hass, _Coordinator(), "command", {"id": "нет-такого", "command": "turn_on"})
    # Элемент есть в составе, но в Home Assistant не отправлен — управлять нечем.
    with pytest.raises(ops.OpError) as err:
        run(hass, _Coordinator(), "command", {"id": "t2", "command": "turn_on"})
    assert "не отправлен" in err.value.message
    assert hass.services.calls == []


def test_значение_вне_диапазона_не_уходит_в_дом():
    hass = _Hass()
    with pytest.raises(ops.OpError) as err:
        run(hass, _Coordinator(), "command", {"id": "t1", "command": "set_brightness", "value": 900})
    assert "от 0 до 100" in err.value.message
    assert hass.services.calls == []


def test_отсутствующая_служба_это_понятный_отказ_а_не_пятисотка():
    hass = _Hass(raises=ServiceNotFound())
    with pytest.raises(ops.OpError) as err:
        run(hass, _Coordinator(), "command", {"id": "t1", "command": "turn_on"})
    assert err.value.status == HTTPStatus.NOT_FOUND


def test_сценарий_запускает_скрипт():
    hass = _Hass()
    assert run(hass, _Coordinator(), "scenario", {"id": "s1"}) == {"accepted": True}
    assert hass.services.calls == [("script", "turn_on", {"entity_id": "script.evening"})]
    with pytest.raises(ops.OpError):
        run(hass, _Coordinator(), "scenario", {"id": "нет-такого"})


# Камера. ⚠ Форма состояния — КОНТРАКТ с менеджером (smart-home-view.util.ts):
# приложение одно и то же, а проекций две, в разных репозиториях. Разъедутся —
# и камера покажет кадр на одном транспорте и пустоту на другом.


def _камера(attributes: dict) -> dict:
    return ops.entity_view(
        {"id": "cam1", "domain": "camera", "entityId": "camera.gate", "name": "Калитка"},
        State("idle", attributes),
    )


def test_кадр_и_поток_строятся_по_entity_id_и_подписанному_токену():
    view = _камера({"access_token": "tok en", "frontend_stream_type": "hls"})

    assert view["state"]["picture"] == "/api/camera_proxy/camera.gate?token=tok%20en"
    assert view["state"]["stream"] == "/api/camera_proxy_stream/camera.gate?token=tok%20en"
    assert view["state"]["streamType"] == "hls"
    assert view["capabilities"] == ["video"]
    assert view["available"] is True


def test_без_токена_адресов_не_обещаем():
    # Токен ротируется; адрес без него отдаст 401, а битая картинка на плитке
    # читается как сломанная камера.
    view = _камера({})

    assert view["state"]["picture"] == ""
    assert view["state"]["stream"] == ""


def test_у_камеры_нет_вкл_выкл():
    # Состояние камеры в HA — idle/recording/streaming. Выдуманный `power`
    # сделал бы плитку выключателем, которым нечего выключать.
    assert "power" not in _камера({"access_token": "t"})["state"]


def test_элемент_без_сущности_адресов_не_получает():
    view = ops.entity_view(
        {"id": "cam1", "domain": "camera", "entityId": None, "name": "Калитка"}, None
    )

    assert view["state"]["picture"] == ""
    assert view["available"] is False

# Медиаплеер. ⚠ Та же оговорка, что у камеры: форма состояния и список
# способностей — КОНТРАКТ с менеджером (smart-home-view.util.ts). Биты взяты у
# самого Home Assistant (`MediaPlayerEntityFeature`), поэтому «что показывать»
# не выдумано ни здесь, ни там.

def _плеер(state: str, attributes: dict) -> dict:
    return ops.entity_view(
        {
            "id": "tv1",
            "domain": "media_player",
            "entityId": "media_player.tv",
            "name": "Телевизор",
        },
        State(state, attributes),
    )

def test_кнопки_плеера_складываются_из_supported_features():
    # PAUSE(1) + PREVIOUS(16) + NEXT(32) + TURN_OFF(256) + PLAY(16384)
    view = _плеер("playing", {"supported_features": 1 + 16 + 32 + 256 + 16384})

    assert set(view["capabilities"]) >= {
        "pause",
        "play",
        "play_pause",
        "previous",
        "next",
        "power",
    }
    assert "volume" not in view["capabilities"]

def test_плеер_без_маски_способностей_не_получает():
    # Обещать кнопку, которой у прибора нет, хуже, чем не показать её.
    assert _плеер("idle", {})["capabilities"] == []

def test_пауза_это_включён_а_standby_нет():
    # Свести пять состояний к одному `power` нельзя: на паузе плеер ВКЛЮЧЁН, и
    # кнопка на плитке в этих случаях разная.
    paused = _плеер("paused", {"media_title": "Сюита №3", "volume_level": 0.42})
    assert paused["state"]["power"] is True
    assert paused["state"]["playing"] is False
    assert paused["state"]["title"] == "Сюита №3"
    assert paused["state"]["volume"] == 42

    assert _плеер("standby", {})["state"]["power"] is False
    assert _плеер("off", {})["state"]["power"] is False

def test_подпись_плеера_берёт_имя_приложения():
    # У телевизора вместо исполнителя осмысленно только оно.
    assert _плеер("playing", {"app_name": "Netflix"})["state"]["subtitle"] == "Netflix"
