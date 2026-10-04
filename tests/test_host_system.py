"""Факты о хосте и переключение службы systemd (`host_system.py`).

Настоящей системной шины на машине разработки нет, `dbus-fast` тоже: подменяем
пакет и шину, проверяем ПОРЯДОК вызовов systemd — именно он решает, переживёт ли
выключение перезагрузку и поднимется ли служба обратно.
"""

from __future__ import annotations

import asyncio
import sys
import types
from enum import Enum
from http import HTTPStatus
from pathlib import Path

import pytest


class _MessageType(Enum):
    METHOD_RETURN = 2
    ERROR = 3


class _Message:
    def __init__(self, **kwargs) -> None:
        self.__dict__.update(kwargs)


_fake = types.ModuleType("dbus_fast")
_fake.Message = _Message
_fake.MessageType = _MessageType
_fake.BusType = types.SimpleNamespace(SYSTEM="system")
sys.modules.setdefault("dbus_fast", _fake)

from mega_home import host_system as hs  # noqa: E402
from mega_home.core.ops_base import OpError  # noqa: E402


class _Variant:
    def __init__(self, value: str) -> None:
        self.value = value


class _Bus:
    """Шина, которая записывает вызовы и отвечает как systemd."""

    def __init__(self, fail: str | None = None) -> None:
        self.calls: list[tuple[str, list]] = []
        self.fail = fail
        self.closed = False
        self.props = {"LoadState": "loaded", "ActiveState": "inactive", "UnitFileState": "masked"}

    async def call(self, message: _Message):
        self.calls.append((message.member, message.body))
        if message.member == self.fail:
            return types.SimpleNamespace(
                message_type=_MessageType.ERROR,
                error_name="org.freedesktop.systemd1.UnitMasked",
                body=["Unit bluetooth.service is masked."],
            )
        body: list = []
        if message.member == "LoadUnit":
            body = ["/org/freedesktop/systemd1/unit/bluetooth_2eservice"]
        elif message.member == "Get":
            body = [_Variant(self.props[message.body[1]])]
        return types.SimpleNamespace(message_type=_MessageType.METHOD_RETURN, body=body)

    def disconnect(self) -> None:
        self.closed = True


def _systemd(bus: _Bus) -> hs.Systemd:
    async def factory():
        return bus

    return hs.Systemd(factory)


def test_выключение_останавливает_маскирует_и_перечитывает() -> None:
    bus = _Bus()
    state = asyncio.run(_systemd(bus).switch("bluetooth.service", False))
    assert [c[0] for c in bus.calls][:3] == ["StopUnit", "MaskUnitFiles", "Reload"]
    assert bus.calls[0][1] == ["bluetooth.service", "replace"]
    # Маска постоянная (переживает перезагрузку) и чужой файл юнита не затирает.
    assert bus.calls[1][1] == [["bluetooth.service"], False, False]
    assert state == {"load": "loaded", "active": "inactive", "file": "masked"}
    assert bus.closed


def test_включение_снимает_маску_до_запуска() -> None:
    bus = _Bus()
    asyncio.run(_systemd(bus).switch("bluetooth.service", True))
    assert [c[0] for c in bus.calls][:3] == ["UnmaskUnitFiles", "Reload", "StartUnit"]


def test_отказ_systemd_ответом_операции() -> None:
    bus = _Bus(fail="StartUnit")
    with pytest.raises(OpError) as err:
        asyncio.run(hs.switch_unit("bluetooth.service", True, _systemd(bus)))
    assert err.value.status == HTTPStatus.BAD_GATEWAY
    assert "masked" in err.value.message
    assert bus.closed


def test_имя_службы_только_формой_service() -> None:
    with pytest.raises(hs.HostUnitError):
        asyncio.run(_systemd(_Bus()).switch("../etc/passwd", False))


def test_адаптеры_без_соединений(tmp_path: Path) -> None:
    for name in ("hci0", "hci0:11", "hci1"):
        (tmp_path / name).mkdir()
    assert hs.adapters(tmp_path) == ["hci0", "hci1"]
    assert hs.adapters(tmp_path / "нет") == []


def test_факты_не_падают_без_шины() -> None:
    async def no_bus():
        raise OSError("No such file or directory: /run/dbus/system_bus_socket")

    async def run(func, *args):
        return ["hci0"]

    result = asyncio.run(hs.facts(run, "Home Assistant Supervised", hs.Systemd(no_bus)))
    assert result["installation_type"] == "Home Assistant Supervised"
    assert result["bluetooth_adapters"] == ["hci0"]
    assert result["bluetooth_unit"] is None
    assert "D-Bus" in result["bluetooth_unit_error"]
    assert result["kernel"]


def test_операция_у_дома_без_умения_честно_отказывает() -> None:
    from mega_home.core import ops

    class _Coordinator:
        data = {"version": "x"}

    with pytest.raises(OpError) as err:
        asyncio.run(ops.run(_Coordinator(), "host-unit", {"unit": "bluetooth.service"}))
    assert err.value.status == HTTPStatus.NOT_IMPLEMENTED
