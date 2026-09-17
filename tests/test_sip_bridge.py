"""SIP-мост домофонии: конфиг и включение по конфигу объекта.

⚠ Настоящий Asterisk здесь не запускается — его проверяет стенд (контейнер Home
Assistant на объекте). Спека держит то, что ломается молча: мост не должен
подниматься без флага, не должен ждать установки пакетов в цикле конфига и не
должен принимать вызов из-за пределов частных сетей.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

from mega_home import sip_bridge as sb


class _Config:
    def path(self, *parts: str) -> str:
        return str(Path("/config", *parts))


class _Hass:
    def __init__(self) -> None:
        self.config = _Config()
        self.tasks: list[object] = []

    async def async_add_executor_job(self, func, *args):  # noqa: ANN001, ANN201, D102
        return func(*args)

    def async_create_background_task(self, coro, name):  # noqa: ANN001, ANN201, D102
        self.tasks.append(name)
        coro.close()
        return _DoneLater()


class _DoneLater:
    def done(self) -> bool:
        return False

    def cancel(self) -> None:
        pass


def test_без_флага_мост_не_поднимается() -> None:
    hass = _Hass()
    bridge = sb.SipBridge(hass)
    bridge.apply({"version": "x"})
    bridge.apply({"intercom": {"sipBridge": "yes"}})
    assert hass.tasks == []
    assert bridge.state()["why"] == "выключен в конфиге объекта"


def test_флаг_запускает_фоновую_задачу_один_раз() -> None:
    hass = _Hass()
    bridge = sb.SipBridge(hass)
    bridge.apply({"intercom": {"sipBridge": True}})
    # Конфиг посреди установки пакетов не должен запускать вторую.
    bridge.apply({"intercom": {"sipBridge": True}})
    assert hass.tasks == ["mega_home sip bridge"]
    assert bridge.state()["enabled"] is True


def test_нет_ни_asterisk_ни_apk(monkeypatch) -> None:  # noqa: ANN001
    monkeypatch.setattr(sb.shutil, "which", lambda _name: None)
    bridge = sb.SipBridge(_Hass())
    assert asyncio.run(bridge._async_binary()) is None
    assert "поставить нечем" in bridge.state()["why"]


def test_конфиг_в_своих_каталогах_и_только_частные_сети() -> None:
    root = Path("/config/.storage/mega_home_sip")
    files = sb.render_config(root)
    assert f"astetcdir => {root}/etc" in files["asterisk.conf"]
    assert f"astrundir => {root}/run" in files["asterisk.conf"]
    pjsip = files["pjsip.conf"]
    assert f"bind=0.0.0.0:{sb.SIP_PORT}" in pjsip
    for net in sb.PRIVATE_NETS:
        assert f"match={net}" in pjsip
    assert "0.0.0.0/0" not in pjsip
    assert f"rtpstart={sb.RTP_START}" in files["rtp.conf"]
    assert "Echo()" in files["extensions.conf"]


def test_конфиг_раскладывается_на_диск(tmp_path: Path) -> None:
    sb.write_config(tmp_path)
    for name in ("asterisk.conf", "pjsip.conf", "extensions.conf", "modules.conf"):
        assert (tmp_path / "etc" / name).is_file()
    assert (tmp_path / "run").is_dir()
