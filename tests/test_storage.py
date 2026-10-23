"""Хранилище записи (`core/storage.py`): суперблоки, диски, обзор папок, параметры монтирования.

Монтирование настоящего диска здесь не проверить (нужны права и устройство) — проверяется то,
что уходит в `mount(2)`, и разбор ответа ядра. Живая проверка на объекте — по списку выпуска.
"""

from __future__ import annotations

import asyncio
import ctypes
import errno
import os
import tempfile
from pathlib import Path

import pytest
from aiohttp.test_utils import TestClient, TestServer

from mega_home.core import recorder as rec
from mega_home.core import storage
from mega_home.core.recorder_api import start_api
from mega_home.core import services

from fake_host import FakeHost


def _image(path: Path, patches: dict[int, bytes], size: int = 0x10400) -> Path:
    data = bytearray(size)
    for offset, raw in patches.items():
        data[offset : offset + len(raw)] = raw
    path.write_bytes(bytes(data))
    return path


def test_суперблоки_распространённых_файловых_систем(tmp_path: Path) -> None:
    uuid = bytes(range(16))
    ext4 = _image(tmp_path / "ext4", {0x438: b"\x53\xef", 0x468: uuid, 0x478: b"VIDEO"})
    assert storage.superblock(ext4) == {"type": "ext4", "uuid": "00010203-0405-0607-0809-0a0b0c0d0e0f", "label": "VIDEO"}
    exfat = _image(tmp_path / "exfat", {3: b"EXFAT   ", 0x64: bytes([0x78, 0x56, 0x34, 0x12])})
    assert storage.superblock(exfat) == {"type": "exfat", "uuid": "1234-5678", "label": ""}
    fat32 = _image(tmp_path / "fat", {0x43: bytes([0xEF, 0xBE, 0xAD, 0xDE]), 0x47: b"CAMERAS    ", 0x52: b"FAT32   "})
    assert storage.superblock(fat32) == {"type": "vfat", "uuid": "DEAD-BEEF", "label": "CAMERAS"}
    ntfs = _image(tmp_path / "ntfs", {3: b"NTFS    ", 0x48: bytes([1, 2, 3, 4, 5, 6, 7, 8])})
    assert storage.superblock(ntfs) == {"type": "ntfs3", "uuid": "0807060504030201", "label": ""}
    xfs = _image(tmp_path / "xfs", {0: b"XFSB", 32: uuid, 108: b"NAS"})
    assert storage.superblock(xfs)["type"] == "xfs"
    btrfs = _image(tmp_path / "btrfs", {0x10040: b"_BHRfS_M", 0x10020: uuid, 0x1012B: b"pool"})
    assert storage.superblock(btrfs) == {"type": "btrfs", "uuid": "00010203-0405-0607-0809-0a0b0c0d0e0f", "label": "pool"}
    assert storage.superblock(_image(tmp_path / "zero", {})) is None
    assert storage.superblock(tmp_path / "missing") is None


def test_диски_разделы_и_целые_без_разделов(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    sys_block, dev = tmp_path / "sys" / "class" / "block", tmp_path / "dev"
    devices = tmp_path / "sys" / "devices"
    for disk, removable, parts in (("sda", "1", ["sda1"]), ("mmcblk0", "0", ["mmcblk0p1"]), ("sdb", "1", [])):
        (devices / disk).mkdir(parents=True)
        (devices / disk / "removable").write_text(removable)
        (devices / disk / "size").write_text("2048")
        sys_block.mkdir(parents=True, exist_ok=True)
        (sys_block / disk).symlink_to(devices / disk)
        for part in parts:
            (devices / disk / part).mkdir()
            (devices / disk / part / "partition").write_text("1")
            (devices / disk / part / "size").write_text("1024")
            (sys_block / part).symlink_to(devices / disk / part)
    (sys_block / "loop0").mkdir()
    dev.mkdir()
    for name in ("sda", "sda1", "mmcblk0", "mmcblk0p1", "sdb", "loop0"):
        _image(dev / name, {0x438: b"\x53\xef"}, size=0x1000)
    monkeypatch.setattr(storage, "mounted", lambda: {"/config": (str(dev / "mmcblk0p1"), "ext4")})
    found = {d["device"]: d for d in storage.disks(sys_block, dev)}
    # Целый диск с разделами и петли не показываются; системный раздел помечен.
    assert sorted(found) == ["mmcblk0p1", "sda1", "sdb"]
    assert found["sda1"]["removable"] and found["sda1"]["size"] == 1024 * 512
    assert found["mmcblk0p1"]["system"] and not found["sda1"]["system"]


@pytest.fixture
def roots(monkeypatch: pytest.MonkeyPatch) -> Path:
    base = Path(tempfile.mkdtemp(prefix="roots-"))
    (base / "media" / "usb" / "video").mkdir(parents=True)
    (base / "media" / ".hidden").mkdir()
    (base / "mnt").mkdir()
    (base / "outside").mkdir()
    (base / "media" / "escape").symlink_to(base / "outside")
    monkeypatch.setattr(storage, "ROOTS", (base / "media", base / "mnt", base / "share"))
    return base


def test_обзор_папок_только_в_каталогах_хранения(roots: Path) -> None:
    listing = storage.folders("", Path("/proc"))
    assert [f["name"] for f in listing["folders"]] == ["media", "mnt"]
    media = storage.folders(str(roots / "media"), roots)
    # Скрытые и ссылки наружу не показываются; диск — тот же, что «настройки», значит системный.
    assert [f["name"] for f in media["folders"]] == ["usb"]
    assert media["system"] is True and media["free"] > 0
    assert storage.folders(str(roots / "media"), Path("/proc"))["system"] is False
    for bad in (str(roots / "outside"), str(roots / "media" / "escape"), "relative", str(roots / "media" / ".." / "outside")):
        with pytest.raises(ValueError):
            storage.folders(bad, roots)


def test_новая_папка_латиницей_и_только_в_хранении(roots: Path) -> None:
    created = storage.make_folder(str(roots / "media" / "usb"), "cams-2026")
    assert Path(created).is_dir()
    for parent, name in ((str(roots / "media"), "видео"), (str(roots / "media"), ".."), (str(roots / "outside"), "x")):
        with pytest.raises(ValueError):
            storage.make_folder(parent, name)


def test_параметры_монтирования(monkeypatch: pytest.MonkeyPatch) -> None:
    device, fstype, data = storage._plan(  # noqa: SLF001
        "cifs", "//192.168.1.5/video/cams", {"user": "nas", "pass": "a,b", "options": "vers=3.0,bad option,uid=0"}
    )
    # Адрес — цифрами (`ip=`): ядро без `mount.cifs` имя не разрешит; запятая пароля удвоена.
    assert (device, fstype) == ("//192.168.1.5/video/cams", "cifs")
    assert data == "username=nas,password=a,,b,ip=192.168.1.5,vers=3.0,uid=0"
    assert storage._plan("nfs", "192.168.1.6:/export/video", {}) == ("192.168.1.6:/export/video", "nfs4", "vers=4.1,addr=192.168.1.6")  # noqa: SLF001
    monkeypatch.setattr(storage, "disks", lambda: [{"device": "sda1", "type": "exfat", "uuid": "1234-5678"}])
    assert storage._plan("usb", "UUID=1234-5678", {}) == ("/dev/sda1", "exfat", "")  # noqa: SLF001
    for kind, source in (("cifs", "nas/video"), ("nfs", "nas"), ("usb", "UUID=0000"), ("ftp", "x")):
        with pytest.raises(ValueError):
            storage._plan(kind, source, {})  # noqa: SLF001


class _FakeLibc:
    def __init__(self, code: int) -> None:
        self.code, self.calls = code, []

    def mount(self, *args: bytes | int) -> int:
        self.calls.append(args)
        if self.code:
            ctypes.set_errno(self.code)
            return -1
        return 0

    def umount2(self, *args: bytes | int) -> int:
        self.calls.append(("umount", *args))
        return 0


def test_монтирование_и_отказ_ядра_словами(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    refused = _FakeLibc(errno.EPERM)
    monkeypatch.setattr(storage, "_libc", lambda: refused)
    monkeypatch.setattr(storage, "mounted", lambda: {})
    problem = storage.mount({"type": "nfs", "source": "10.0.0.2:/v"}, tmp_path / "point")
    assert "features: mount=cifs;nfs" in problem
    assert refused.calls[0][:3] == (b"10.0.0.2:/v", str(tmp_path / "point").encode(), b"nfs4")
    # То же уже смонтировано — второго вызова ядра нет.
    done = _FakeLibc(0)
    monkeypatch.setattr(storage, "_libc", lambda: done)
    monkeypatch.setattr(storage, "mounted", lambda: {str(tmp_path / "point"): ("10.0.0.2:/v", "nfs4")})
    assert storage.mount({"type": "nfs", "source": "10.0.0.2:/v"}, tmp_path / "point") == ""
    assert done.calls == []


def test_служба_отдаёт_папки_и_подключает_для_выбора(roots: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    recorder = rec.Recorder(FakeHost(roots), lambda _listener: (lambda: None))
    mounts: list[tuple[dict, Path]] = []

    def fake_mount(spec: dict, target: Path = storage.MOUNT_POINT) -> str:
        mounts.append((spec, target))
        return "" if spec.get("pass") == "ok" else "не смонтировалось: доступ запрещён"

    monkeypatch.setattr(storage, "mount", fake_mount)
    monkeypatch.setattr(storage, "PREVIEW_POINT", roots / "mnt")

    async def scenario() -> tuple:
        runner = await start_api(recorder)
        try:
            async with TestClient(TestServer(runner.app)) as client:
                top = await (await client.get("/folders")).json()
                made = await (await client.post("/folders", json={"path": str(roots / "media"), "name": "rec"})).json()
                bad = (await client.post("/folders", json={"path": str(roots / "outside"), "name": "x"})).status
                refused = await client.post("/mount", json={"type": "cifs", "source": "//1.2.3.4/v", "user": "u", "pass": "no"})
                ok = await (await client.post("/mount", json={"type": "cifs", "source": "//1.2.3.4/v", "user": "u", "pass": "ok"})).json()
                return top, made, bad, refused.status, await refused.json(), ok
        finally:
            await runner.cleanup()
            services.unregister(rec.SERVICE)

    top, made, bad, refused_status, refused, ok = asyncio.run(scenario())
    assert [f["name"] for f in top["folders"]] == ["media", "mnt"]
    assert made == {"path": str(roots / "media" / "rec")} and bad == 400
    assert refused_status == 400 and "доступ запрещён" in refused["error"]
    # Подключение для выбора — в свою точку, не в точку записи; пароль ушёл телом.
    assert ok["path"] == str(roots / "mnt") and mounts[-1][1] == roots / "mnt"
    assert os.path.isdir(made["path"])
