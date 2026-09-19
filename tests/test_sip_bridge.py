"""SIP-мост домофонии: конфиг и включение по конфигу объекта.

⚠ Настоящий Asterisk здесь не запускается — его проверяет стенд (контейнер Home
Assistant на объекте). Спека держит то, что ломается молча: мост не должен
подниматься без флага, не должен ждать установки пакетов в цикле конфига и не
должен принимать вызов ни от кого, кроме панелей из конфига и телефона с
loopback.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

from mega_home.core import sip_bridge as sb
from mega_home.core import sip_config as sc

from fake_host import FakeHost


def test_без_флага_мост_не_поднимается() -> None:
    host = FakeHost()
    bridge = sb.SipBridge(host)
    bridge.apply({"version": "x"})
    bridge.apply({"intercom": {"sipBridge": "yes"}})
    assert host.spawned == []
    assert bridge.state()["why"] == "выключен в конфиге объекта"


def test_флаг_запускает_фоновую_задачу_один_раз() -> None:
    host = FakeHost()
    bridge = sb.SipBridge(host)
    bridge.apply({"intercom": {"sipBridge": True}})
    # Конфиг посреди установки пакетов не должен запускать вторую.
    bridge.apply({"intercom": {"sipBridge": True}})
    assert host.spawned == ["mega_home sip bridge"]
    assert bridge.state()["enabled"] is True


def test_нет_ни_asterisk_ни_apk(monkeypatch) -> None:  # noqa: ANN001
    monkeypatch.setattr(sb.shutil, "which", lambda _name: None)
    bridge = sb.SipBridge(FakeHost())
    assert asyncio.run(bridge._async_binary()) is None
    assert "поставить нечем" in bridge.state()["why"]


KEYS = {"ari": "a" * 24}
PANELS = ("192.168.88.90", "192.168.88.92")


def test_конфиг_в_своих_каталогах_и_только_панели_из_конфига() -> None:
    root = Path("/config/.storage/mega_home_sip")
    files = sc.render_config(root, KEYS, PANELS)
    assert f"astetcdir => {root}/etc" in files["asterisk.conf"]
    assert f"astrundir => {root}/run" in files["asterisk.conf"]
    pjsip = files["pjsip.conf"]
    assert f"bind=0.0.0.0:{sc.SIP_PORT}" in pjsip
    panel = pjsip.split("endpoint=panel")[1].split("[")[0]
    assert panel.split() == [f"match={address}" for address in PANELS]
    assert "0.0.0.0/0" not in pjsip
    assert f"rtpstart={sc.RTP_START}" in files["rtp.conf"]


def test_без_панелей_вызов_не_принимается_ни_от_кого() -> None:
    pjsip = sc.render_config(Path("/x"), KEYS)["pjsip.conf"]
    assert "endpoint=panel" not in pjsip
    assert "192.168." not in pjsip


def test_адреса_панелей_только_частные_ipv4() -> None:
    block = {"panels": ["192.168.88.92", " 10.0.0.5", "192.168.88.92", "127.0.0.1",
                        "8.8.8.8", "panel.local", "fd00::1", 5]}
    assert sc.panel_addresses(block) == ("10.0.0.5", "192.168.88.92")
    assert sc.panel_addresses({"panels": "192.168.88.90"}) == ()
    assert sc.panel_addresses(None) == ()


def test_телефон_только_с_loopback_и_мост_виден_только_там() -> None:
    # ⚠ Учётки у телефона нет: его пускает только канал менеджера на loopback.
    files = sc.render_config(Path("/x"), KEYS, PANELS)
    pjsip = files["pjsip.conf"]
    assert "endpoint_identifier_order=ip\n" in pjsip
    assert "type=auth" not in pjsip
    resident = pjsip.split(f"endpoint={sc.RESIDENT}")[1]
    assert resident.split() == ["match=127.0.0.1"]
    assert "webrtc=yes" in pjsip
    assert "protocol=ws" in pjsip
    assert "bindaddr=127.0.0.1" in files["http.conf"]


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
    assert len(first["ari"]) >= 16 and set(first) == {"ari"}


def test_смена_панелей_у_работающего_моста_перечитывает_конфиг() -> None:
    host = FakeHost()
    bridge = sb.SipBridge(host)
    bridge._ready, bridge._written = True, ("192.168.88.90",)
    bridge.apply({"intercom": {"sipBridge": True, "panels": ["192.168.88.90"]}})
    assert host.spawned == []
    bridge.apply({"intercom": {"sipBridge": True, "panels": ["192.168.88.92"]}})
    assert host.spawned == ["mega_home sip bridge"]
    assert bridge.state()["panels"] == ["192.168.88.90"]
