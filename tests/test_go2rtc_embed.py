"""Жизненный цикл своего go2rtc.

⚠ Проверяется здесь не «сеть», а ровно то, чего не хватило в 0.2.5–0.2.13, когда
восемь релизов подряд назывались «фикс старта go2rtc»: занятый порт, вычитывание
лога и остановка процесса. Всё три — про то, останется ли объект с работающим
удалённым просмотром через сутки, а не в первую минуту после установки.
"""

from __future__ import annotations

import asyncio
import socket

import pytest

from mega_home import go2rtc_embed as embed


class _Hass:
    async def async_add_executor_job(self, func, *args):  # noqa: ANN001, ANN201, D102
        return func(*args)


class _Stdout:
    def __init__(self, lines: list[bytes]) -> None:
        self._lines = list(lines)

    def __aiter__(self):  # noqa: ANN204
        return self

    async def __anext__(self) -> bytes:
        if not self._lines:
            # Живой go2rtc не закрывает stdout — держим читателя, как в жизни.
            await asyncio.sleep(3600)
        return self._lines.pop(0)


class _Proc:
    def __init__(self, lines: list[bytes] | None = None) -> None:
        self.stdout = _Stdout(lines or [])
        self.returncode: int | None = None
        self.terminated = False

    def terminate(self) -> None:
        self.terminated = True
        self.returncode = 0

    def kill(self) -> None:  # pragma: no cover - в этих тестах не нужен
        self.returncode = -9

    async def wait(self) -> int | None:
        return self.returncode


@pytest.fixture(autouse=True)
def _clean():
    """Модуль держит процесс в глобальных — между тестами их не должно быть."""
    yield
    asyncio.run(embed.async_stop())


def test_занятый_порт_оставляет_переговоры_штатному_пути(monkeypatch):
    """⚠ Чужой go2rtc на :8555 — это, как правило, правильно настроенный аддон.

    Подняться рядом мы всё равно не сможем (слушателя не будет), а `is_running`
    увёл бы переговоры на путь без единого кандидата — при живом штатном пути
    Home Assistant. Симптом при этом выглядит как «не повезло с NAT», и чинят
    не то.
    """
    monkeypatch.setattr(embed.shutil, "which", lambda _name: "/usr/bin/go2rtc")
    monkeypatch.setattr(
        asyncio, "create_subprocess_exec", _не_должен_запускаться
    )

    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as busy:
        busy.bind(("0.0.0.0", embed.WEBRTC_PORT))
        assert asyncio.run(embed.async_start(_Hass())) is False

    assert embed.is_running() is False


def test_лог_вычитывается_иначе_go2rtc_встанет(monkeypatch):
    """⚠ Труба с ненулевым буфером обязана опустошаться: заполнится (64 КБ
    логов) — и go2rtc заблокируется на записи, то есть просмотр умрёт до
    перезапуска Home Assistant."""
    proc = _Proc([b"11:00:00.000 INF go2rtc version 1.9\n", b"11:00:00.001 INF [api] listen\n"])
    _подменить_запуск(monkeypatch, proc)

    async def сценарий() -> None:
        assert await embed.async_start(_Hass()) is True
        assert embed.is_running() is True
        # Даём читателю разобрать то, что уже написано.
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        assert any("[api] listen" in line for line in embed.log_tail())

    asyncio.run(сценарий())


def test_остановка_снимает_процесс_и_читателя(monkeypatch):
    """⚠ Осиротевший go2rtc держит :8555, и следующий запуск слушатель уже не
    поднимет — «работало, потом перестало, лечится ребутом»."""
    proc = _Proc()
    _подменить_запуск(monkeypatch, proc)

    async def сценарий() -> None:
        assert await embed.async_start(_Hass()) is True
        await embed.async_stop()
        assert proc.terminated is True
        assert embed.is_running() is False

    asyncio.run(сценарий())


def test_не_ответивший_api_не_считается_поднятым(monkeypatch):
    """⚠ Живой процесс — это не «готов». go2rtc, у которого не поднялся
    слушатель, продолжает работать как ни в чём не бывало."""
    proc = _Proc()
    _подменить_запуск(monkeypatch, proc, готов=False)

    async def сценарий() -> None:
        assert await embed.async_start(_Hass()) is False
        assert embed.is_running() is False
        assert proc.terminated is True

    asyncio.run(сценарий())


async def _не_должен_запускаться(*_args, **_kwargs):
    raise AssertionError("порт занят — запускать go2rtc нельзя")


def _подменить_запуск(monkeypatch, proc: _Proc, *, готов: bool = True) -> None:
    async def spawn(*_args, **_kwargs):
        return proc

    async def api(_hass) -> bool:  # noqa: ANN001
        return готов

    monkeypatch.setattr(embed.shutil, "which", lambda _name: "/usr/bin/go2rtc")
    monkeypatch.setattr(embed, "_ports_busy", lambda: "")
    monkeypatch.setattr(embed, "_await_api", api)
    monkeypatch.setattr(asyncio, "create_subprocess_exec", spawn)


def test_сирота_усыновляется_а_не_уступает_чужому(monkeypatch):
    """Свой go2rtc прошлого запуска, переживший нечистую остановку HA, держит
    порты — и «порт занят» раньше означал «уступи чужому аддону»: объект
    оставался с «не поднят go2rtc» до ребута (2026-09-12, два обновления
    подряд). Отвечает НАШ API на петле — усыновляем, переговоры идут."""

    async def alive(hass):  # noqa: ANN001, ANN202
        return True

    def _не_должны_стартовать(*_args):
        pytest.fail("усыновление не должно поднимать новый процесс")

    monkeypatch.setattr(embed, "_api_alive", alive)
    monkeypatch.setattr(embed.shutil, "which", _не_должны_стартовать)

    assert asyncio.run(embed.async_start(_Hass())) is True
    assert embed.is_running() is True
