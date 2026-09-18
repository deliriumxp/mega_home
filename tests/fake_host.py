"""Поддельный хозяин (`host.Host`) для спек модулей без Home Assistant.

Блокирующий вызов идёт прямо в потоке теста, фоновая задача не запускается,
а запоминается по имени: спеке важно, ЧТО модуль поручил хозяину.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from mega_home.host import PlainHost


class _Pending:
    def done(self) -> bool:
        return False

    def cancel(self) -> None:
        pass


class _MemoryStore:
    def __init__(self) -> None:
        self.data: Any = None

    async def async_load(self) -> Any:
        return self.data

    async def async_save(self, data: Any) -> None:
        self.data = data

    def async_delay_save(self, data_func: Any, delay: float = 0) -> None:
        """Настоящий откладывает запись; спеке важен только её факт."""
        self.data = data_func()


class FakeHost(PlainHost):
    def __init__(self, root: Path | str = "/config/.storage", session: Any = None) -> None:
        super().__init__(Path(root))
        self.fake_session = session
        self.spawned: list[str] = []
        self.timers: list[tuple[Any, Any]] = []
        self.stores: dict[str, _MemoryStore] = {}

    def session(self, verify_ssl: bool = True) -> Any:
        return self.fake_session

    async def run(self, func, *args):  # noqa: ANN001, ANN201
        return func(*args)

    def spawn(self, coro, name: str):  # noqa: ANN001, ANN201
        self.spawned.append(name)
        coro.close()
        return _Pending()

    def store(self, key: str, version: int = 1) -> _MemoryStore:
        return self.stores.setdefault(key, _MemoryStore())

    def every(self, interval, action):  # noqa: ANN001, ANN201
        self.timers.append((action, interval))
        return lambda: None
