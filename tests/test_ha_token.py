"""Токен HA по заданию менеджера (`ha_token.py`) и порт веб-сервера HA в hello.

Образец — `websocket_create_long_lived_access_token` ядра (`homeassistant/components/auth/__init__.py`)
и `homeassistant/auth/__init__.py` (`async_create_refresh_token`: одно имя клиента — один токен).
"""

from __future__ import annotations

import asyncio
import re
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from homeassistant.auth.models import TOKEN_TYPE_LONG_LIVED_ACCESS_TOKEN, TOKEN_TYPE_NORMAL

from fake_host import FakeHost
from mega_home import ha_token
from mega_home.core import ops
from mega_home.core.ops_base import OpError
from mega_home.link import ManagerLink

PKG = Path(__file__).resolve().parent.parent / "custom_components" / "mega_home"


def _user(name: str, *, owner=False, active=True, system=False, admin=False, tokens=()) -> Any:
    return SimpleNamespace(
        name=name, is_owner=owner, is_active=active, system_generated=system, is_admin=admin,
        refresh_tokens={t.id: t for t in tokens},
    )


def _token(id_: str, name: str | None, kind: str = TOKEN_TYPE_LONG_LIVED_ACCESS_TOKEN) -> Any:
    return SimpleNamespace(id=id_, client_name=name, token_type=kind)


class _Auth:
    def __init__(self, users: list[Any], *, fail: bool = False) -> None:
        self.users, self.fail = users, fail
        self.removed: list[str] = []
        self.created: list[dict[str, Any]] = []

    async def async_get_owner(self) -> Any:
        return next((u for u in self.users if u.is_owner), None)

    async def async_get_users(self) -> list[Any]:
        return self.users

    def async_remove_refresh_token(self, token: Any) -> None:
        self.removed.append(token.id)

    async def async_create_refresh_token(self, user: Any, **kwargs: Any) -> Any:
        if self.fail:
            raise ValueError("Mega Manager already exists")
        self.created.append({"user": user.name, **kwargs})
        return SimpleNamespace(id="new")

    def async_create_access_token(self, refresh: Any) -> str:
        return "СЕКРЕТНЫЙ-ТОКЕН"


def _hass(auth: _Auth) -> Any:
    return SimpleNamespace(auth=auth)


def test_выпуск_владельцу_с_прежним_токеном_менеджера_снятым() -> None:
    old = _token("old", "Mega Manager")
    foreign = _token("keep1", "GPS Logger")
    normal = _token("keep2", "Mega Manager", TOKEN_TYPE_NORMAL)
    owner = _user("Хозяин", owner=True, tokens=[old, foreign, normal])
    auth = _Auth([_user("Другой", admin=True), owner])
    answer = asyncio.run(ha_token.issue(_hass(auth)))
    # Снят ровно прежний долгоживущий «Mega Manager»: чужие и обычные токены остаются.
    assert auth.removed == ["old"]
    assert auth.created == [{
        "user": "Хозяин", "client_name": "Mega Manager",
        "token_type": TOKEN_TYPE_LONG_LIVED_ACCESS_TOKEN, "access_token_expiration": timedelta(days=3650),
    }]
    assert answer["token"] == "СЕКРЕТНЫЙ-ТОКЕН" and answer["user"] == "Хозяин"
    assert set(answer) == {"token", "expires_at", "user"}
    expires = datetime.fromisoformat(answer["expires_at"])
    assert expires.utcoffset() == timedelta(0)
    assert abs(expires - datetime.now(expires.tzinfo) - timedelta(days=3650)) < timedelta(minutes=1)


def test_неактивного_владельца_заменяет_первый_активный_администратор() -> None:
    users = [
        _user("Хозяин", owner=True, active=False),
        _user("Система", admin=True, system=True),
        _user("Жилец"),
        _user("Админ", admin=True),
    ]
    assert asyncio.run(ha_token.issue(_hass(_Auth(users))))["user"] == "Админ"


def test_без_подходящего_пользователя_отказ_текстом() -> None:
    auth = _Auth([_user("Хозяин", owner=True, active=False), _user("Жилец")])
    with pytest.raises(OpError) as caught:
        asyncio.run(ha_token.issue(_hass(auth)))
    assert "Home Assistant" in caught.value.message
    assert auth.created == [] and auth.removed == []


def test_отказ_ядра_не_роняет_канал_и_не_несёт_токена(caplog: pytest.LogCaptureFixture) -> None:
    auth = _Auth([_user("Хозяин", owner=True)], fail=True)
    with pytest.raises(OpError) as caught:
        asyncio.run(ha_token.issue(_hass(auth)))
    assert caught.value.message == "Home Assistant не выпустил токен"
    assert "СЕКРЕТНЫЙ" not in caplog.text


def test_токен_не_попадает_в_журнал(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level("DEBUG")
    asyncio.run(ha_token.issue(_hass(_Auth([_user("Хозяин", owner=True)]))))
    assert "СЕКРЕТНЫЙ" not in caplog.text


def test_операция_ядра_зовёт_адаптер_и_без_него_отказывает() -> None:
    class _C:
        env = FakeHost()
        data = {"rooms": []}

    async def _issue() -> dict[str, str]:
        return {"token": "t", "expires_at": "x", "user": "u"}

    coordinator = _C()
    with pytest.raises(OpError) as caught:
        asyncio.run(ops.run(coordinator, "ha-token", None))
    assert caught.value.status == 501
    coordinator.ha_token = _issue  # type: ignore[attr-defined]
    assert asyncio.run(ops.run(coordinator, "ha-token", None))["token"] == "t"


# --- hello: порт и https веб-сервера HA --------------------------------------

def _link(api: Any) -> ManagerLink:
    link = ManagerLink.__new__(ManagerLink)
    link._hass = SimpleNamespace(config=SimpleNamespace(api=api))
    return link


def test_hello_несёт_порт_и_https() -> None:
    assert _link(SimpleNamespace(port=8123, use_ssl=False))._ha() == {"port": 8123, "ssl": False}
    assert _link(SimpleNamespace(port=443, use_ssl=True))._ha() == {"port": 443, "ssl": True}


def test_hello_без_сервера_или_при_сбое_несёт_none() -> None:
    assert _link(None)._ha() is None
    assert _link(SimpleNamespace(port="не число", use_ssl=False))._ha() is None
    assert ManagerLink.__new__(ManagerLink)._ha() is None  # нет `_hass` вовсе


# --- Замок: токен достижим только каналу менеджера ---------------------------

def _source(name: str) -> str:
    return (PKG / name).read_text(encoding="utf-8")


def test_только_канал_менеджера_зовёт_операцию_и_бандл_с_локалью_не_дотянутся() -> None:
    """`ops.run` зовёт одно место — кадр канала менеджера (`link.py`); ни локальная дверь, ни перенос
    запроса жильца (`relay_api`), ни `connect`/служба `ha` до операции и до выпуска токена не доходят."""
    callers = [
        path.relative_to(PKG).as_posix()
        for path in PKG.rglob("*.py")
        if re.search(r"\bops\.run\(|\bops_mod\.run\(|\bfrom \.ops import run\b", path.read_text(encoding="utf-8"))
    ]
    assert callers == ["link.py"]
    # Ни один модуль дверей не знает ни операции, ни выпускающего модуля.
    for name in ("http.py", "http_door.py", "core/relay_api.py", "core/connect.py", "core/services.py", "ha_api.py"):
        text = _source(name)
        assert "ha_token" not in text and "ha-token" not in text, name
    # Единственные ссылки на модуль: проводка операции и подсказка hello.
    users = sorted(
        p.relative_to(PKG).as_posix() for p in PKG.rglob("*.py") if "ha_token" in p.read_text(encoding="utf-8")
    )
    assert users == ["__init__.py", "core/ops.py", "ha_token.py", "link.py"]


def test_служба_ha_по_прежнему_не_выпускает_токены() -> None:
    from mega_home.ha_api import guard

    assert guard("frame", '{"type": "auth/long_lived_access_token", "client_name": "x", "lifespan": 1}')
    assert guard("path", "/auth/token")
