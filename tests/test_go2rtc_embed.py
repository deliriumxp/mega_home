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

from mega_home.core import go2rtc_embed as embed

from fake_host import FakeHost


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
        asyncio, "create_subprocess_exec", _must_not_launch
    )

    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as busy:
        busy.bind(("0.0.0.0", embed.WEBRTC_PORT))
        assert asyncio.run(embed.async_start(FakeHost())) is False

    assert embed.is_running() is False


def test_лог_вычитывается_иначе_go2rtc_встанет(monkeypatch):
    """⚠ Труба с ненулевым буфером обязана опустошаться: заполнится (64 КБ
    логов) — и go2rtc заблокируется на записи, то есть просмотр умрёт до
    перезапуска Home Assistant."""
    proc = _Proc([b"11:00:00.000 INF go2rtc version 1.9\n", b"11:00:00.001 INF [api] listen\n"])
    _fake_launch(monkeypatch, proc)

    async def scenario() -> None:
        assert await embed.async_start(FakeHost()) is True
        assert embed.is_running() is True
        # Даём читателю разобрать то, что уже написано.
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        assert any("[api] listen" in line for line in embed.log_tail())

    asyncio.run(scenario())


def test_остановка_снимает_процесс_и_читателя(monkeypatch):
    """⚠ Осиротевший go2rtc держит :8555, и следующий запуск слушатель уже не
    поднимет — «работало, потом перестало, лечится ребутом»."""
    proc = _Proc()
    _fake_launch(monkeypatch, proc)

    async def scenario() -> None:
        assert await embed.async_start(FakeHost()) is True
        await embed.async_stop()
        assert proc.terminated is True
        assert embed.is_running() is False

    asyncio.run(scenario())


def test_не_ответивший_api_не_считается_поднятым(monkeypatch):
    """⚠ Живой процесс — это не «готов». go2rtc, у которого не поднялся
    слушатель, продолжает работать как ни в чём не бывало."""
    proc = _Proc()
    _fake_launch(monkeypatch, proc, ready=False)

    async def scenario() -> None:
        assert await embed.async_start(FakeHost()) is False
        assert embed.is_running() is False
        assert proc.terminated is True

    asyncio.run(scenario())


async def _must_not_launch(*_args, **_kwargs):
    raise AssertionError("порт занят — запускать go2rtc нельзя")


def _fake_launch(monkeypatch, proc: _Proc, *, ready: bool = True) -> None:
    async def spawn(*_args, **_kwargs):
        return proc

    async def api(_hass) -> bool:  # noqa: ANN001
        return ready

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

    def _must_not_start(*_args):
        pytest.fail("усыновление не должно поднимать новый процесс")

    monkeypatch.setattr(embed, "_api_alive", alive)
    monkeypatch.setattr(embed.shutil, "which", _must_not_start)

    assert asyncio.run(embed.async_start(FakeHost())) is True
    assert embed.is_running() is True


def test_проба_порта_не_спотыкается_о_time_wait():
    """⚠ Живой факт 2026-09-19: после перезапуска HA на 127.0.0.1:1985 минуту висели
    сокеты TIME_WAIT прежнего go2rtc, проба без SO_REUSEADDR считала порт занятым,
    и объект жил без своего go2rtc до ребута. Слушатель, закрывшийся штатно, — не
    занятость; живой слушатель — занятость.
    """
    from mega_home.core import services

    probes = ((socket.SOCK_STREAM, embed.API_PORT, f"TCP {embed.API_PORT}"),)
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind(("0.0.0.0", embed.API_PORT))
        listener.listen(1)
        assert services.port_busy(probes) == f"TCP {embed.API_PORT}"
    # Слушатель закрыт: порт свободен, что бы ни осталось от его соединений.
    assert services.port_busy(probes) == ""


def test_занятый_порт_повторяет_старт_а_не_сдаётся(monkeypatch):
    """Одна попытка на весь аптайм оставила объект без go2rtc (0.4.0): помеха ушла
    через минуту, а отказ жил до ребута. Повтор в фоне поднимает его сам."""
    busy = ["TCP 1985"]
    monkeypatch.setattr(embed.shutil, "which", lambda _name: "/usr/bin/go2rtc")
    monkeypatch.setattr(embed, "_ports_busy", lambda: busy[0])
    monkeypatch.setattr(embed, "_write_config", lambda: "/tmp/x.yaml")
    monkeypatch.setattr(embed, "_start_drain", lambda: None)

    async def launched(*_args, **_kwargs):
        return _Proc()

    async def api_ok(_env):
        return True

    monkeypatch.setattr(asyncio, "create_subprocess_exec", launched)
    monkeypatch.setattr(embed, "_await_api", api_ok)

    async def alive_no(_env):
        return False

    monkeypatch.setattr(embed, "_api_alive", alive_no)
    sleeps: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)
        busy[0] = ""

    monkeypatch.setattr(embed.asyncio, "sleep", fake_sleep)

    async def scenario() -> None:
        env = FakeHost()
        assert await embed.async_start(env) is False
        assert env.spawned == ["mega_home go2rtc: повтор старта"]
        assert await embed._retry_start(env, attempts=3) is True
        assert embed.is_running() is True

    asyncio.run(scenario())
    assert sleeps == [embed.RETRY_EVERY]
