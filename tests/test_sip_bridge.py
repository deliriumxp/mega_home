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
from mega_home import sip_config as sc


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


KEYS = {"ari": "a" * 24, "resident": "r" * 24}


def test_конфиг_в_своих_каталогах_и_только_частные_сети() -> None:
    root = Path("/config/.storage/mega_home_sip")
    files = sc.render_config(root, KEYS)
    assert f"astetcdir => {root}/etc" in files["asterisk.conf"]
    assert f"astrundir => {root}/run" in files["asterisk.conf"]
    pjsip = files["pjsip.conf"]
    assert f"bind=0.0.0.0:{sc.SIP_PORT}" in pjsip
    for net in sc.PRIVATE_NETS:
        assert f"match={net}" in pjsip
    assert "0.0.0.0/0" not in pjsip
    assert f"rtpstart={sc.RTP_START}" in files["rtp.conf"]


def test_телефон_опознаётся_по_имени_раньше_чем_по_адресу() -> None:
    # Телефон приходит каналом дома с ЧАСТНОГО адреса хоста: при порядке
    # по умолчанию (ip первым) он стал бы панелью — без пароля.
    pjsip = sc.render_config(Path("/x"), KEYS)["pjsip.conf"]
    assert "endpoint_identifier_order=username,ip" in pjsip
    assert f"password={KEYS['resident']}" in pjsip
    assert "webrtc=yes" in pjsip
    assert "protocol=ws" in pjsip


def test_ari_только_с_loopback() -> None:
    ari = sc.render_config(Path("/x"), KEYS)["ari.conf"]
    assert "deny = 0.0.0.0/0.0.0.0" in ari
    assert "permit = 127.0.0.1/255.255.255.255" in ari
    assert f"password = {KEYS['ari']}" in ari


def test_панель_не_отвечаем_в_диалплане() -> None:
    # Ответ гасит мониторы: вызов панели уходит в Stasis звонящим.
    dialplan = sc.render_config(Path("/x"), KEYS)["extensions.conf"]
    panel = dialplan.split("[from-panel]")[1].split("[from-resident]")[0]
    assert f"Stasis({sc.ARI_APP},panel)" in panel
    assert "Answer" not in panel


def test_конфиг_раскладывается_на_диск_и_пароли_не_меняются(tmp_path: Path) -> None:
    first = sc.write_config(tmp_path)
    for name in ("asterisk.conf", "pjsip.conf", "extensions.conf", "http.conf", "ari.conf"):
        assert (tmp_path / "etc" / name).is_file()
    assert (tmp_path / "run").is_dir()
    # Сирота усыновляется со старым конфигом — новый пароль ARI запер бы его.
    assert sc.write_config(tmp_path) == first
    assert len(first["ari"]) >= 16 and first["ari"] != first["resident"]
