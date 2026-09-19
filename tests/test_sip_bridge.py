"""SIP-мост домофонии: включение по конфигу и диалплан файлами менеджера.

⚠ Настоящий Asterisk здесь не запускается — его проверяет стенд. Спека держит
то, что ломается молча: мост не поднимается без флага и без диалплана,
пароль ARI дом генерирует и подставляет сам, инфраструктурные файлы — данные
процесса, не диалплана (`docs/plan-thin-gateway.md`, пункт 4).
"""

from __future__ import annotations

import asyncio
from pathlib import Path

from mega_home.core import sip_bridge as sb
from mega_home.core.assets import AssetStore

from fake_host import FakeHost

ARI_TEMPLATE = "[general]\nenabled = yes\n\n[mega_home]\npassword = {ari_password}\n"
PJSIP = "[global]\ntype=global\n"
EXTENSIONS = "[from-panel]\nexten => _X.,1,NoOp()\n"

DIALPLAN_CONFIG = {
    "intercom": {"sipBridge": True},
    "assets": {
        "asterisk/pjsip.conf": {"v": "1"},
        "asterisk/extensions.conf": {"v": "1"},
        "asterisk/ari.conf": {"v": "1"},
    },
}


def _store_with_dialplan(tmp_path: Path) -> AssetStore:
    store = AssetStore(tmp_path)
    store.save("asterisk/pjsip.conf", "1", PJSIP.encode())
    store.save("asterisk/extensions.conf", "1", EXTENSIONS.encode())
    store.save("asterisk/ari.conf", "1", ARI_TEMPLATE.encode())
    return store


def test_без_флага_мост_не_поднимается(tmp_path: Path) -> None:
    host = FakeHost()
    bridge = sb.SipBridge(host, AssetStore(tmp_path))
    bridge.apply({"version": "x"})
    bridge.apply({"intercom": {"sipBridge": "yes"}})
    assert host.spawned == []
    assert bridge.state()["why"] == "выключен в конфиге объекта"


def test_флаг_запускает_фоновую_задачу_один_раз(tmp_path: Path) -> None:
    host = FakeHost()
    bridge = sb.SipBridge(host, AssetStore(tmp_path))
    bridge.apply(DIALPLAN_CONFIG)
    # Конфиг посреди установки пакетов не должен запускать вторую.
    bridge.apply(DIALPLAN_CONFIG)
    assert host.spawned == ["mega_home sip bridge"]
    assert bridge.state()["enabled"] is True


def test_нет_ни_asterisk_ни_apk(monkeypatch, tmp_path: Path) -> None:  # noqa: ANN001
    monkeypatch.setattr(sb.shutil, "which", lambda _name: None)
    bridge = sb.SipBridge(FakeHost(), AssetStore(tmp_path))
    assert asyncio.run(bridge._async_binary()) is None
    assert "поставить нечем" in bridge.state()["why"]


def test_без_диалплана_от_менеджера_мост_не_пишет_конфиг(tmp_path: Path) -> None:
    host = FakeHost()
    bridge = sb.SipBridge(host, AssetStore(tmp_path))
    bridge._dialplan_versions = sb._dialplan_versions(DIALPLAN_CONFIG)
    assert asyncio.run(bridge._async_write()) is False
    assert "не пришёл файлами" in bridge._why


def test_диалплан_пишется_как_есть_кроме_подстановки_пароля_ari(tmp_path: Path) -> None:
    assets_dir = tmp_path / "assets"
    store = _store_with_dialplan(assets_dir)
    root = tmp_path / "sip"
    host = FakeHost(root=tmp_path)
    bridge = sb.SipBridge(host, store)
    bridge._root = root
    bridge._dialplan_versions = sb._dialplan_versions(DIALPLAN_CONFIG)

    assert asyncio.run(bridge._async_write()) is True

    pjsip = (root / "etc" / "pjsip.conf").read_text()
    extensions = (root / "etc" / "extensions.conf").read_text()
    ari = (root / "etc" / "ari.conf").read_text()
    assert pjsip == PJSIP
    assert extensions == EXTENSIONS
    # ⚠ Единственная подстановка дома: пароль ARI дом сгенерировал сам.
    assert "{ari_password}" not in ari
    assert bridge._keys["ari"] in ari
    assert len(bridge._keys["ari"]) >= 16


def test_инфраструктурные_файлы_не_диалплан(tmp_path: Path) -> None:
    files = sb._infra_config(tmp_path)
    assert set(files) == {"asterisk.conf", "modules.conf", "logger.conf", "http.conf", "rtp.conf"}
    assert f"astetcdir => {tmp_path}/etc" in files["asterisk.conf"]
    assert f"bindport={sb.HTTP_PORT}" in files["http.conf"]
    assert "bindaddr=127.0.0.1" in files["http.conf"]
    assert f"rtpstart={sb.RTP_START}" in files["rtp.conf"]


def test_смена_версии_диалплана_у_работающего_моста_перечитывает_конфиг(tmp_path: Path) -> None:
    host = FakeHost()
    bridge = sb.SipBridge(host, AssetStore(tmp_path))
    bridge._ready, bridge._written = True, {"asterisk/pjsip.conf": "1"}
    bridge.apply(DIALPLAN_CONFIG)
    assert host.spawned == ["mega_home sip bridge"]
