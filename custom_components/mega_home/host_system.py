"""Факты о ХОСТЕ дома и управление его службами systemd — для менеджера.

Зачем. Ошибки ядра в Bluetooth MGMT (CVE-2025-39981, CVE-2026-43059) роняют
поток bluetooth ядра, после чего Home Assistant висит в непрерываемом системном
вызове, сторож Supervisor его не перезапускает, лечит только перезагрузка
хоста (JetHub D1p, Armbian supervised, ядро 6.6.62-current-meson64). Решает,
уязвим ли дом и предлагать ли инсталлятору выключить Bluetooth, МЕНЕДЖЕР —
таблицей версий, которая обновляется без релиза дома
(`backend/src/modules/smart-home/home-host.util.ts` менеджера). Дом только
сообщает факты и исполняет «остановить/запустить службу» — тонкий транспорт
(`docs/home-gateway.md` менеджера).

⚠ Модуль вне `core/`, но без `homeassistant.*`: systemd по D-Bus — свойство
ХОСТА, на котором стоит дом, то есть адаптера среды, а не ядра (демон без HA
под Debian сделал бы то же своим модулем). Тип установки, который знает только
HA, приходит параметром из `link.py`.

⚠ Работает только там, где контейнеру HA виден СИСТЕМНЫЙ D-Bus хоста. Проверено
на одном объекте (Home Assistant Supervised на Debian/Armbian): контейнер
привилегированный, uid 0, сеть хоста, `/run/dbus` смонтирован с хоста,
`uname -r` в контейнере даёт ядро хоста. На Home Assistant OS менеджер эту
функцию не предлагает вовсе (решение заказчика) — там ядро и службы ведёт
Supervisor, и вмешиваться в них мимо него нельзя. Нет шины или `dbus-fast` —
честное «службой не управляем» (`unit: None`), а не исключение в hello.

⚠ Выгрузить модуль ядра (`btbcm`, `hci_uart`) или занести его в blacklist этим
путём НЕЛЬЗЯ: у systemd по D-Bus нет такого метода, это запись в
`/etc/modprobe.d` и перезагрузка. Поэтому «выключить Bluetooth» = остановить и
замаскировать `bluetooth.service` (решение заказчика). ⚠ НЕ ПРОВЕРЕНО, что этого
хватает против падения: команды MGMT ядру шлёт bluetoothd, но интеграция
`bluetooth` самого HA тоже может говорить с адаптером мимо него — проверять на
стенде, где падение воспроизводится.

⚠ `dbus-fast` НЕ объявлен в `requirements` манифеста: объявление заставило бы
HA ставить пакет pip на объекте, а объекты бывают офлайн
(`docs/plan-core-without-ha.md` менеджера, шаг 3). Пакет — зависимость самого
ядра HA (интеграция `bluetooth`), в образах Supervised и OS он есть; импорт
ленивый, его нет — функция честно недоступна.

D-Bus API systemd: https://www.freedesktop.org/software/systemd/man/latest/org.freedesktop.systemd1.html
"""

from __future__ import annotations

import asyncio
import platform
import re
from pathlib import Path
from typing import Any, Awaitable, Callable

# Служба BlueZ: её состояние едет в hello, её выключает кнопка менеджера.
BLUETOOTH_UNIT = "bluetooth.service"
# Адаптеры ядра. ⚠ В том же каталоге лежат и СОЕДИНЕНИЯ (`hci0:11`) — их не считаем.
SYS_BLUETOOTH = Path("/sys/class/bluetooth")
_ADAPTER = re.compile(r"^hci\d+$")
# Имя юнита — только служба, без путей и шаблонов подстановки: менеджер называет её
# сам, дом проверяет только форму.
_UNIT_NAME = re.compile(r"^[A-Za-z0-9@._-]+\.service$")
# Сколько ждать systemd. Шина отвечает за миллисекунды; дольше — что-то висит, и
# hello из-за этого задерживаться не должен.
DBUS_TIMEOUT = 5.0

_SYSTEMD = "org.freedesktop.systemd1"
_MANAGER_PATH = "/org/freedesktop/systemd1"
_MANAGER = "org.freedesktop.systemd1.Manager"
_UNIT = "org.freedesktop.systemd1.Unit"
_PROPS = "org.freedesktop.DBus.Properties"


class HostUnitError(Exception):
    """Systemd отказал или шины нет — текст для инсталлятора."""


def kernel() -> str:
    """Ядро ХОСТА: контейнер делит ядро с хостом, `uname -r` в нём — ядро хоста."""
    return platform.release()


def adapters(root: Path = SYS_BLUETOOTH) -> list[str]:
    """Адаптеры Bluetooth ядра (`hci0`, …). Блокирующее чтение — только из executor."""
    try:
        return sorted(p.name for p in root.iterdir() if _ADAPTER.match(p.name))
    except OSError:
        return []


async def _system_bus() -> Any:
    """Системная шина D-Bus хоста (`/run/dbus/system_bus_socket`)."""
    from dbus_fast import BusType  # noqa: PLC0415 — пакет ядра HA, может отсутствовать
    from dbus_fast.aio import MessageBus  # noqa: PLC0415

    return await MessageBus(bus_type=BusType.SYSTEM).connect()


class Systemd:
    """Вызовы `org.freedesktop.systemd1` по системной шине.

    `bus_factory` — подмена шины в спеках: настоящей шины на машине разработки нет.
    """

    def __init__(self, bus_factory: Callable[[], Awaitable[Any]] = _system_bus) -> None:
        self._bus_factory = bus_factory

    async def _call(self, bus: Any, path: str, interface: str, member: str, signature: str, body: list) -> list:
        from dbus_fast import Message, MessageType  # noqa: PLC0415

        reply = await bus.call(
            Message(
                destination=_SYSTEMD, path=path, interface=interface,
                member=member, signature=signature, body=body,
            )
        )
        if reply.message_type == MessageType.ERROR:
            raise HostUnitError(f"systemd: {reply.error_name}: {(reply.body or [''])[0]}")
        return reply.body

    async def _state(self, bus: Any, unit: str) -> dict[str, str]:
        # `LoadUnit`, а не `GetUnit`: тот отказывает, если юнит не загружен (остановлен и
        # замаскирован), а нам нужен ответ и тогда. Свойства — интерфейс `Unit`.
        (path,) = await self._call(bus, _MANAGER_PATH, _MANAGER, "LoadUnit", "s", [unit])
        state: dict[str, str] = {}
        for prop, key in (("LoadState", "load"), ("ActiveState", "active"), ("UnitFileState", "file")):
            (variant,) = await self._call(bus, path, _PROPS, "Get", "ss", [_UNIT, prop])
            state[key] = str(getattr(variant, "value", variant))
        return state

    async def _with_bus(self, work: Callable[[Any], Awaitable[Any]]) -> Any:
        try:
            bus = await asyncio.wait_for(self._bus_factory(), DBUS_TIMEOUT)
        except ImportError as err:
            raise HostUnitError("в Home Assistant нет пакета dbus-fast") from err
        except (OSError, asyncio.TimeoutError) as err:
            raise HostUnitError(f"системная шина D-Bus хоста недоступна: {err}") from err
        try:
            return await asyncio.wait_for(work(bus), DBUS_TIMEOUT * 2)
        except asyncio.TimeoutError as err:
            raise HostUnitError("systemd не ответил вовремя") from err
        finally:
            bus.disconnect()

    async def state(self, unit: str) -> dict[str, str]:
        """`{load, active, file}` — `LoadState`, `ActiveState`, `UnitFileState` юнита."""
        return await self._with_bus(lambda bus: self._state(bus, unit))

    async def switch(self, unit: str, on: bool) -> dict[str, str]:
        """Выключить: StopUnit + MaskUnitFiles + Reload; включить — обратный порядок.

        ⚠ Маска, а не DisableUnitFiles: выключенную службу может поднять зависимость
        или активация по D-Bus, замаскированную — нет («masking … makes it
        impossible to start them», systemctl(1)). Маска постоянная (`runtime=false`):
        переживает перезагрузку хоста — ради неё и выключаем. `force=false`: свой
        файл юнита в `/etc/systemd/system` инсталлятора не затираем, а отказываем. `Reload` — после правки файлов юнитов, как велит дока systemd.
        Включение: сперва снять маску, иначе `StartUnit` откажет («unit is masked»).
        """
        if not _UNIT_NAME.match(unit):
            raise HostUnitError(f"недопустимое имя службы: {unit}")

        async def work(bus: Any) -> dict[str, str]:
            if on:
                await self._call(bus, _MANAGER_PATH, _MANAGER, "UnmaskUnitFiles", "asb", [[unit], False])
                await self._call(bus, _MANAGER_PATH, _MANAGER, "Reload", "", [])
                await self._call(bus, _MANAGER_PATH, _MANAGER, "StartUnit", "ss", [unit, "replace"])
            else:
                await self._call(bus, _MANAGER_PATH, _MANAGER, "StopUnit", "ss", [unit, "replace"])
                await self._call(bus, _MANAGER_PATH, _MANAGER, "MaskUnitFiles", "asbb", [[unit], False, False])
                await self._call(bus, _MANAGER_PATH, _MANAGER, "Reload", "", [])
            return await self._state(bus, unit)

        return await self._with_bus(work)


async def facts(
    run: Callable[..., Awaitable[Any]],
    installation_type: str | None,
    systemd: Systemd | None = None,
) -> dict[str, Any]:
    """Поле `host` кадра hello: ядро, тип установки, адаптеры и служба Bluetooth.

    ⚠ Никогда не падает: hello важнее подсказки. Шины нет — `bluetooth_unit: None`
    и причина в `bluetooth_unit_error`.
    """
    result: dict[str, Any] = {
        "kernel": kernel(),
        "installation_type": installation_type,
        "bluetooth_adapters": await run(adapters),
        "bluetooth_unit": None,
    }
    try:
        result["bluetooth_unit"] = await (systemd or Systemd()).state(BLUETOOTH_UNIT)
    except HostUnitError as err:
        result["bluetooth_unit_error"] = str(err)
    except Exception as err:  # noqa: BLE001 — hello важнее подсказки
        result["bluetooth_unit_error"] = f"{type(err).__name__}: {err}"
    return result


async def switch_unit(unit: str, on: bool, systemd: Systemd | None = None) -> dict[str, Any]:
    """Операция `host-unit` (`core/ops.py`): отказ systemd — ответом с текстом, не падением."""
    from http import HTTPStatus  # noqa: PLC0415

    from .core.ops_base import OpError  # noqa: PLC0415

    try:
        return {"unit": unit, **await (systemd or Systemd()).switch(unit, on)}
    except HostUnitError as err:
        raise OpError(f"Служба хоста не переключена — {err}", HTTPStatus.BAD_GATEWAY) from err
