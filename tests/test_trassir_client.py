"""Две двери одного регистратора — и почему их нельзя перепутать.

Проверено на живом Trassir 4.8.2.0: сессия ПОЛЬЗОВАТЕЛЯ отдаёт каналы, видео и
команды архива, но на `/events` отвечает `no session`; сессия по паролю SDK —
ровно наоборот. Ошибка выглядит как протухшая сессия, поэтому здесь заперты и
маршрутизация запросов по дверям, и текст отказа: без него следующий разбор
снова уйдёт в переподключение.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

import pytest

from mega_home import trassir_client as module
from mega_home.trassir_client import TrassirAuthError, TrassirClient, TrassirError


@pytest.fixture(autouse=True)
def _no_login_gap(monkeypatch: pytest.MonkeyPatch) -> None:
    """Пауза между входами настоящая — 5 секунд, и спеки её не ждут.

    Сама пауза проверяется отдельным тестом, который возвращает её на место.
    """
    monkeypatch.setattr(module, "TRASSIR_LOGIN_GAP", 0)


class FakeResponse:
    def __init__(self, payload: Any) -> None:
        self._payload = payload

    async def __aenter__(self) -> FakeResponse:
        return self

    async def __aexit__(self, *args: object) -> bool:
        return False

    async def json(self, content_type: object = None) -> Any:
        return self._payload

    async def read(self) -> bytes:
        if isinstance(self._payload, bytes):
            return self._payload
        return json.dumps(self._payload).encode()


class FakeSession:
    """Отдаёт заготовленные ответы и запоминает, о чём спрашивали."""

    def __init__(self, answers: dict[str, Any]) -> None:
        self.answers = answers
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def get(self, url: str, params: dict[str, Any] | None = None, **kwargs: Any) -> FakeResponse:
        path = url.rsplit("/", 1)[-1].split("?")[0]
        self.calls.append((path, dict(params or {})))
        answer = self.answers.get(path)
        if callable(answer):
            answer = answer(dict(params or {}))
        return FakeResponse(answer)

    def logins(self) -> list[dict[str, Any]]:
        return [params for path, params in self.calls if path == "login"]

    def params_of(self, path: str) -> dict[str, Any]:
        return next(params for name, params in self.calls if name == path)


def client(session: FakeSession) -> TrassirClient:
    return TrassirClient(session, "192.168.1.50", 8080, "megahome", "s3cret", "sdk-s3cret")


def two_doors() -> FakeSession:
    return FakeSession(
        {
            "login": lambda params: {
                "success": 1,
                "sid": "user-sid" if "username" in params else "sdk-sid",
            },
            "events": [{"timestamp": "1", "type": "Motion Start", "origin": "cam"}],
            "channels": {"channels": [{"guid": "cam", "name": "Вход"}]},
        }
    )


def test_events_use_the_sdk_password_and_channels_the_user() -> None:
    session = two_doors()

    async def scenario() -> None:
        api = client(session)
        await api.async_events()
        await api.async_channels()

    asyncio.run(scenario())

    assert session.logins()[0] == {"password": "sdk-s3cret"}, "события — только по паролю SDK"
    assert session.logins()[1] == {"username": "megahome", "password": "s3cret"}
    assert session.params_of("events")["sid"] == "sdk-sid"
    assert session.params_of("channels")["sid"] == "user-sid"


def test_session_is_reused_between_calls() -> None:
    session = FakeSession({"login": {"success": 1, "sid": "s"}, "channels": {"channels": []}})

    async def scenario() -> None:
        api = client(session)
        await api.async_channels()
        await api.async_channels()

    asyncio.run(scenario())

    assert len(session.logins()) == 1


def test_expired_session_is_retried_once() -> None:
    state = {"logins": 0, "asked": 0}

    def login(_params: dict[str, Any]) -> dict[str, Any]:
        state["logins"] += 1
        return {"success": 1, "sid": f"s{state['logins']}"}

    def channels(_params: dict[str, Any]) -> dict[str, Any]:
        state["asked"] += 1
        if state["asked"] == 1:
            return {"error_code": "no session", "success": 0}
        return {"channels": [{"guid": "cam"}]}

    session = FakeSession({"login": login, "channels": channels})

    assert asyncio.run(client(session).async_channels()) == [{"guid": "cam"}]
    assert state["logins"] == 2, "протухшую сессию открываем заново ровно один раз"


def test_persistent_no_session_names_the_sdk_password() -> None:
    session = FakeSession(
        {
            "login": {"success": 1, "sid": "s"},
            "events": {"error_code": "no session", "success": 0},
        }
    )

    with pytest.raises(TrassirAuthError) as err:
        asyncio.run(client(session).async_events())

    # Текст — половина ценности: «no session» на событиях при живой сессии
    # означает НЕ ТУ ДВЕРЬ, и разбор обязан начаться с пароля SDK.
    assert "пароль SDK" in str(err.value)


def test_unset_sdk_password_answers_with_an_empty_body() -> None:
    # Так ведёт себя живой сервер: не отказ, а пустое тело.
    session = FakeSession({"login": b"", "events": []})

    with pytest.raises(TrassirAuthError) as err:
        asyncio.run(client(session).async_events())
    assert "пароль SDK" in str(err.value)


def test_missing_sdk_password_does_not_even_try() -> None:
    session = FakeSession({"login": {"success": 1, "sid": "s"}})
    api = TrassirClient(session, "h", 8080, "u", "p", "")

    with pytest.raises(TrassirAuthError):
        asyncio.run(api.async_events())
    assert not session.calls, "без пароля SDK ходить некуда — и логин не тратим"


def test_logins_are_spaced_apart(monkeypatch: pytest.MonkeyPatch) -> None:
    """⚠ Чаще одного раза в 5 с — бан АДРЕСА, а не отказ одному запросу."""
    slept: list[float] = []
    real_sleep = asyncio.sleep

    async def fake_sleep(seconds: float) -> None:
        slept.append(seconds)
        await real_sleep(0)

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)
    monkeypatch.setattr(module, "TRASSIR_LOGIN_GAP", 5)
    session = two_doors()

    async def scenario() -> None:
        api = client(session)
        await api.async_events()
        await api.async_channels()

    asyncio.run(scenario())

    assert slept and slept[0] > 0, "второй вход подряд обязан подождать"


def test_refusal_carries_the_reason_from_trassir() -> None:
    session = FakeSession(
        {
            "login": {"success": 1, "sid": "s"},
            "get_video": {"success": 0, "error_code": "flv is disabled"},
        }
    )

    with pytest.raises(TrassirError) as err:
        asyncio.run(client(session).async_get_video("cam", container="flv"))
    assert "flv is disabled" in str(err.value)


def test_timeout_gets_human_text_not_empty() -> None:
    """Голый asyncio.TimeoutError — с ПУСТЫМ str! — обязан превращаться в
    человекочитаемый отказ: в журнале объекта 2026-09-12 осталась строка
    «неожиданная ошибка опроса Trassir:» с висящим двоеточием и без причины.
    Та же ловушка, что у RouterOS (CLAUDE.md): запасной текст обязателен."""

    class DeadSession:
        def get(self, url: str, params: dict[str, Any] | None = None, **kwargs: Any):
            raise asyncio.TimeoutError()

    with pytest.raises(TrassirError) as err:
        asyncio.run(client(DeadSession()).async_events())  # type: ignore[arg-type]

    assert "нет ответа за" in str(err.value), "причина должна быть словами"
