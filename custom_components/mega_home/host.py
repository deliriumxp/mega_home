"""Хозяин: пять примитивов среды, которыми модули пользуются вместо `hass`.

Зачем. Home Assistant — один из адаптеров дома, а не его основа
(`docs/plan-core-without-ha.md` в менеджере). Модулю нужны от среды HTTP-сессия,
блокирующий вызов, фоновая задача, каталог и таймер — и ничего больше; HA
даёт их своими средствами (`ha_host.py`), самостоятельный демон — голым
asyncio (`PlainHost` ниже).

⚠ Здесь НЕТ `homeassistant.*` — модуль в замке модулей без HA. Хозяин приходит
параметром; брать `hass` «ещё для одной мелочи» — значит снова привязать модуль.
"""

from __future__ import annotations

import asyncio
import json
import os
from collections.abc import Awaitable, Callable, Coroutine
from datetime import timedelta
from pathlib import Path
from typing import Any, Protocol, TypeVar

import aiohttp

T = TypeVar("T")


class Host(Protocol):
    """Среда, в которой работает модуль."""

    def session(self, verify_ssl: bool = True) -> aiohttp.ClientSession:
        """Общая HTTP-сессия. ⚠ Не закрывать: она не наша."""

    async def run(self, func: Callable[..., T], *args: Any) -> T:
        """Блокирующий вызов вне цикла событий."""

    def spawn(self, coro: Coroutine[Any, Any, T], name: str) -> asyncio.Task[T]:
        """Фоновая задача; хозяин держит ссылку и снимает её на остановке."""

    def path(self, *parts: str) -> Path:
        """Путь в нашем каталоге хранения (в HA — `.storage/`)."""

    def store(self, key: str, version: int = 1) -> KeyStore:
        """JSON-документ по ключу в каталоге хранения."""

    def every(
        self, interval: timedelta, action: Callable[[Any], Awaitable[None] | None]
    ) -> Callable[[], None]:
        """Периодический вызов `action(now)`; возвращает отписку."""


class KeyStore(Protocol):
    """Документ хранилища: подмножество HA `Store`, которым пользуемся."""

    async def async_load(self) -> Any: ...

    async def async_save(self, data: Any) -> None: ...


class JsonStore:
    """`KeyStore` без HA — в ТОМ ЖЕ конверте, что HA `Store`.

    ⚠ Конверт `{version, key, data}` совпадает с HA намеренно: дом, переехавший
    с HA на демон, должен подняться со своим кэшем правил и учёток, а не с нуля.
    """

    def __init__(self, path: Path, key: str, version: int) -> None:
        self._path, self._key, self._version = path, key, version

    async def async_load(self) -> Any:
        return await asyncio.to_thread(self._read)

    async def async_save(self, data: Any) -> None:
        await asyncio.to_thread(self._write, data)

    def _read(self) -> Any:
        try:
            envelope = json.loads(self._path.read_text("utf-8"))
        except FileNotFoundError:
            return None
        return envelope.get("data") if isinstance(envelope, dict) else None

    def _write(self, data: Any) -> None:
        # Атомарно: оборванная запись не должна оставить дом без кэша.
        self._path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self._path.with_name(f"{self._path.name}.tmp")
        envelope = {"version": self._version, "minor_version": 1, "key": self._key, "data": data}
        tmp.write_text(json.dumps(envelope, ensure_ascii=False), "utf-8")
        os.replace(tmp, self._path)


class PlainHost:
    """Хозяин без Home Assistant: голый asyncio и свой каталог."""

    def __init__(self, root: Path, session: aiohttp.ClientSession | None = None) -> None:
        self._root = root
        self._sessions: dict[bool, aiohttp.ClientSession] = {}
        if session is not None:
            self._sessions[True] = session
        self._tasks: set[asyncio.Task[Any]] = set()

    def session(self, verify_ssl: bool = True) -> aiohttp.ClientSession:
        # ⚠ Сессия на хозяина, а не на вызов: сессия на вызов течёт сокетами.
        # Без проверки сертификата — своя, как у HA (`async_get_clientsession`).
        current = self._sessions.get(verify_ssl)
        if current is None or current.closed:
            connector = None if verify_ssl else aiohttp.TCPConnector(ssl=False)
            current = self._sessions[verify_ssl] = aiohttp.ClientSession(connector=connector)
        return current

    async def run(self, func: Callable[..., T], *args: Any) -> T:
        return await asyncio.to_thread(func, *args)

    def spawn(self, coro: Coroutine[Any, Any, T], name: str) -> asyncio.Task[T]:
        task = asyncio.get_running_loop().create_task(coro, name=name)
        # Без ссылки цикл держит задачу слабо — её может собрать сборщик мусора.
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return task

    def path(self, *parts: str) -> Path:
        return self._root.joinpath(*parts)

    def store(self, key: str, version: int = 1) -> KeyStore:
        return JsonStore(self.path(key), key, version)

    def every(
        self, interval: timedelta, action: Callable[[Any], Awaitable[None] | None]
    ) -> Callable[[], None]:
        from datetime import datetime, timezone

        async def tick() -> None:
            while True:
                await asyncio.sleep(interval.total_seconds())
                result = action(datetime.now(timezone.utc))
                if asyncio.iscoroutine(result):
                    await result

        task = self.spawn(tick(), "mega_home timer")
        return task.cancel

    async def close(self) -> None:
        for task in list(self._tasks):
            task.cancel()
        for session in self._sessions.values():
            if not session.closed:
                await session.close()
