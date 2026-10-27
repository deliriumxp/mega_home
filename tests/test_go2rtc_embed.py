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
from mega_home.core import host_load

from fake_host import FakeHost


class _Stdout:
    def __init__(self, lines: list[bytes]) -> None:
        self._lines = list(lines)
        # Процесс вышел: труба закрыта, чтение кончается.
        self.eof = False

    def __aiter__(self):  # noqa: ANN204
        return self

    async def __anext__(self) -> bytes:
        while not self._lines:
            if self.eof:
                raise StopAsyncIteration
            # Живой go2rtc не закрывает stdout — держим читателя, как в жизни.
            await asyncio.sleep(0)
        return self._lines.pop(0)


class _Proc:
    def __init__(self, lines: list[bytes] | None = None) -> None:
        self.stdout = _Stdout(lines or [])
        self.returncode: int | None = None
        self.terminated = False
        self.pid = 4321

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
        # Нагрузка контроллера видит go2rtc по pid, пока он жив (`host_load`).
        assert host_load._owners["go2rtc"] == 4321  # noqa: SLF001
        await embed.async_stop()
        assert "go2rtc" not in host_load._owners  # noqa: SLF001
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


def test_сирота_снимается_и_поднимается_свой(monkeypatch):
    """⚠ Свой go2rtc прошлого запуска HA пишет лог в трубу, которую читал умерший процесс HA: Go
    выходит по SIGPIPE на первой же строке (объект 2026-10-24: «усыновлён», девять сессий камер —
    и через минуту после старта HA ни одной камеры до следующего перезапуска). Поэтому сирота
    не усыновляется, а снимается по pid (имя нашего конфига в командной строке), и поднимается
    свой процесс с живой трубой."""
    proc = _Proc()
    _fake_launch(monkeypatch, proc)

    async def alive(_env):  # noqa: ANN202
        return True

    monkeypatch.setattr(embed, "_api_alive", alive)
    markers: list[str] = []
    monkeypatch.setattr(host_load, "find", lambda marker: markers.append(marker) or 777)
    signals: list[tuple[int, int]] = []
    monkeypatch.setattr(embed.os, "kill", lambda pid, sig: signals.append((pid, sig)))
    monkeypatch.setattr(embed, "_gone", lambda pid: bool(signals))

    assert asyncio.run(embed.async_start(FakeHost())) is True
    assert markers == [embed.CONFIG_PREFIX]
    assert signals == [(777, embed.signal.SIGTERM)]
    assert embed.is_running() is True and host_load._owners["go2rtc"] == 4321  # noqa: SLF001


def test_сирота_без_процесса_не_даёт_поднять_свой_и_ждёт_повтора(monkeypatch):
    """API отвечает, а процесса с нашим конфигом не видно (чужое пространство pid): снять его
    нечем, и поднимать второй поверх занятых портов бессмысленно — причина в паспорте, повтор."""

    async def alive(_env):  # noqa: ANN202
        return True

    monkeypatch.setattr(embed, "_api_alive", alive)
    monkeypatch.setattr(host_load, "find", lambda _marker: None)
    monkeypatch.setattr(asyncio, "create_subprocess_exec", _must_not_launch)

    env = FakeHost()
    assert asyncio.run(embed.async_start(env)) is False
    assert "прошлого запуска" in embed.state()["why"]
    assert env.spawned == ["mega_home go2rtc: повтор старта"]


def test_смерть_во_время_работы_снимает_службу_и_перезапускает(monkeypatch):
    """⚠ Процесс умер после старта — служба не должна оставаться в реестре: `connect` камер
    упирался в закрытый порт («ClientConnectorError»), и просмотр лежал до перезапуска HA."""
    from mega_home.core import services

    proc = _Proc([b"12:48:35.000 WRN something\n"])
    _fake_launch(monkeypatch, proc)

    async def scenario() -> FakeHost:
        env = FakeHost()
        assert await embed.async_start(env) is True
        assert services.resolve("go2rtc") == embed.API_PORT
        proc.returncode = 2
        proc.stdout.eof = True
        for _ in range(5):
            await asyncio.sleep(0)
        return env

    env = asyncio.run(scenario())
    assert embed.is_running() is False
    assert services.resolve("go2rtc") is None
    assert "go2rtc" not in host_load._owners  # noqa: SLF001
    assert "(2)" in embed.state()["why"] and "WRN something" in embed.state()["why"]
    assert env.spawned == [
        "mega_home go2rtc: сторож продюсеров",
        "mega_home go2rtc: сторож API",
        "mega_home go2rtc: перезапуск после падения",
    ]


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


def test_повторы_старта_не_кончаются(monkeypatch):
    """Ревью 2026-10-27: после 15 неудач (5 минут) повторы кончались — видео не было до перезапуска HA. Теперь они
    идут всегда: сначала часто, потом раз в `RETRY_SLOW`; исключение попытки повторы не обрывает."""
    calls = [0]

    async def start(_env):
        calls[0] += 1
        if calls[0] == 3:
            raise OSError("конфиг не записался")
        return calls[0] > embed.RETRY_ATTEMPTS + 2

    sleeps: list[float] = []
    real_sleep = asyncio.sleep

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)
        await real_sleep(0)

    monkeypatch.setattr(embed, "async_start", start)
    monkeypatch.setattr(embed.asyncio, "sleep", fake_sleep)
    assert asyncio.run(embed._retry_start(FakeHost())) is True
    assert sleeps[0] == embed.RETRY_EVERY and sleeps[-1] == embed.RETRY_SLOW


def test_зависший_api_перезапускает_go2rtc(monkeypatch):
    """Ревью 2026-10-27: сторож следил только за смертью процесса — go2rtc, повисший по API, никто не перезапускал."""
    proc = _Proc()
    _fake_launch(monkeypatch, proc)

    async def api(_env):
        return False  # процесс жив, а API молчит

    monkeypatch.setattr(embed, "_api_alive", api)
    monkeypatch.setattr(embed, "_start_drain", lambda: None)
    real_sleep = asyncio.sleep

    async def fake_sleep(_seconds: float) -> None:
        await real_sleep(0)

    async def scenario() -> FakeHost:
        env = FakeHost()
        assert await embed.async_start(env) is True
        monkeypatch.setattr(embed.asyncio, "sleep", fake_sleep)
        await embed._watch(env)
        return env

    env = asyncio.run(scenario())
    assert embed.is_running() is False and "API" in embed.state()["why"]
    assert env.spawned[-1] == "mega_home go2rtc: повтор старта"
