"""Служба `ha` транспорта с учёткой дома (`ha_api.py`, `services.Service`).

Решение заказчика 2026-10-17: бандл ходит в API своего Home Assistant обычным
`connect`, а токен подставляет дом и наружу не отдаёт.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any

import pytest

from mega_home.core import connect, services
from mega_home.core.ops_base import OpError
from mega_home.ha_api import HaApi


@pytest.fixture(autouse=True)
def _clean_registry() -> Any:
    yield
    services.REGISTRY.clear()


def _hass(use_ssl: bool = False) -> Any:
    return SimpleNamespace(config=SimpleNamespace(api=SimpleNamespace(port=8123, use_ssl=use_ssl)))


def test_учётка_дома_перебивает_учётку_вызывающего() -> None:
    api = HaApi(_hass(use_ssl=True))
    api.apply({"house": {"ha": {"token": "T"}}})
    call = connect.with_service({"host": "ha", "headers": {"Authorization": "Bearer чужой", "X": "1"}})
    assert call["headers"] == {"Authorization": "Bearer T", "X": "1"}
    assert call["tls"] is True
    assert connect.resolve_address(call) == ("127.0.0.1", 8123)


def test_без_токена_служба_есть_но_без_учётки() -> None:
    HaApi(_hass()).apply({"rooms": []})
    service = services.entry("ha")
    assert service is not None and service.headers == {} and service.ws_open is None
    # Вызов к устройству объекта службой не трогается.
    assert connect.with_service({"host": "192.168.1.5"}) == {"host": "192.168.1.5"}


class _Socket:
    def __init__(self, answers: list[dict[str, Any]]) -> None:
        self.answers = answers
        self.sent: list[dict[str, Any]] = []

    async def receive_json(self, timeout: float) -> dict[str, Any]:
        return self.answers.pop(0)

    async def send_json(self, data: dict[str, Any]) -> None:
        self.sent.append(data)


def test_вход_websocket_до_кадров_вызывающего() -> None:
    api = HaApi(_hass())
    api.apply({"house": {"ha": {"token": "T"}}})
    socket_ = _Socket([{"type": "auth_required"}, {"type": "auth_ok"}])
    asyncio.run(connect.open_ws({"host": "ha"}, socket_))
    assert socket_.sent == [{"type": "auth", "access_token": "T"}]


def test_учётки_и_систему_ha_через_дом_не_меняют() -> None:
    """Выпуск токена и управление учётками и супервизором пережили бы любой отзыв ключа."""
    HaApi(_hass()).apply({"house": {"ha": {"token": "T"}}})
    for call in (
        {"host": "ha", "path": "/auth/token"},
        {"host": "ha", "path": "/api/hassio/addons/core_ssh/start"},
        {"host": "ha", "path": "/api/services/hassio/addon_stdin"},
        {"host": "ha", "kind": "ws", "messages": ['{"id":1,"type":"auth/long_lived_access_token","client_name":"x","lifespan":3650}']},
        {"host": "ha", "kind": "ws", "messages": [{"id": 1, "type": "config/auth/create", "name": "x"}]},
        {"host": "ha", "kind": "ws", "messages": [{"id": 1, "type": "call_service", "domain": "hassio", "service": "addon_stdin"}]},
        {"host": "ha", "path": "/api/config/automation/config/sa_1", "body": '{"actions":[{"action":"hassio.addon_stdin"}]}'},
    ):
        with pytest.raises(OpError, match="не меняют"):
            connect.with_service(call)
    # Ревью 2026-10-24: тот же запрет в любой записи пути — клиент раскрывает и сводит путь до сервера.
    for path in ("/%61uth/token", "/api/../auth/token", "/./auth/token", "//auth/token", "/auth%2Ftoken",
                 "/api/%2e%2e/auth/token", "/api/hass%69o/addons/core_ssh/start", "/API/hassio/x"):
        with pytest.raises(OpError, match="не меняют"):
            connect.with_service({"host": "ha", "path": path})
    # Устройство объекта путь получает как есть: приведение — только у служб дома.
    assert connect.with_service({"host": "192.168.1.5", "path": "/a/../%61"})["path"] == "/a/../%61"
    # Всё прочее API HA — открыто: редактор автоматизаций, проверка, реестры, службы приборов.
    connect.with_service({"host": "ha", "path": "/api/config/automation/config/sa_1", "body": '{"actions":[{"action":"vacuum.start"}]}'})
    connect.with_service({"host": "ha", "kind": "ws", "messages": [{"id": 1, "type": "validate_config"}, {"id": 2, "type": "call_service", "domain": "vacuum"}]})
    assert connect.refused({"host": "ha"}, "frame", '{"type":"auth/current_user"}')
    assert connect.refused({"host": "ha"}, "frame", '{"type":"config/label_registry/list"}') is None


def test_закрытый_сокет_при_входе_отказ_словами() -> None:
    api = HaApi(_hass())
    api.apply({"house": {"ha": {"token": "T"}}})

    class _Closed:
        async def receive_json(self, timeout: float) -> Any:
            raise TypeError("Received message 8:1000 is not WSMsgType.TEXT")

    with pytest.raises(OpError, match="закрыл соединение"):
        asyncio.run(connect.open_ws({"host": "ha"}, _Closed()))


def test_отказ_входа_называется_словами() -> None:
    api = HaApi(_hass())
    api.apply({"house": {"ha": {"token": "T"}}})
    socket_ = _Socket([{"type": "auth_required"}, {"type": "auth_invalid"}])
    with pytest.raises(OpError, match="не принял токен"):
        asyncio.run(connect.open_ws({"host": "ha"}, socket_))
