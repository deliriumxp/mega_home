"""Хранилище записи: монтирование диска или сетевой папки, список дисков, обзор папок.

Зачем в доме. У HA в контейнере (Wirenboard, Docker на ПК, LXC) нет «Хранилища» интерфейса HA —
его делает агент хоста HA OS (решение заказчика 2026-10-23: «делай монтирование у себя»). Что
монтировать и куда писать — данные менеджера; дом монтирует, проверяет и показывает папки.

⚠ Монтирование — системным вызовом `mount(2)` через libc, а не утилитой: в образе HA нет
`cifs-utils`/`nfs-utils`, а ядро умеет SMB и NFS v4 само, если ему дать адрес цифрами (`ip=`,
`addr=`). Нужен привилегированный контейнер (у Wirenboard он такой по инструкции производителя);
нет прав — причина словами, запись стоит.
⚠ Тип и UUID файловой системы USB-диска дом читает из суперблока сам: `blkid` в образе может не
быть, а udev-ссылок `/dev/disk/by-uuid` внутри контейнера нет. Имя `sda1` между перезагрузками
меняется — диск ищется по UUID.
⚠ «Системный диск» — тот, на котором лежит каталог настроек дома (`/config` у HA): привязки
`/media` в контейнере — точки монтирования, но того же флеша, и проверка «это точка
монтирования» пропускала бы запись на флеш контроллера.
"""

from __future__ import annotations

import ctypes
import ctypes.util
import errno
import os
import re
import shutil
import socket
from pathlib import Path
from typing import Any

# Куда дом монтирует диск или сетевую папку записи (свой каталог контейнера) и куда — для обзора
# папок, пока инсталлятор выбирает: обзор не трогает идущую запись, сторож — обзор.
MOUNT_POINT = Path("/mnt/mega-home/storage")
PREVIEW_POINT = Path("/mnt/mega-home/preview")
# Где показывать папки: каталоги хранения, а не система (`/config` — настройки HA на флеше).
ROOTS = (Path("/media"), Path("/mnt"), Path("/share"), Path("/data"), Path("/srv"))
_NAME = re.compile(r"^[A-Za-z0-9_.-]{1,64}$")
# Без исполнения, устройств и времени доступа: видео, а не система.
_FLAGS = 2 | 4 | 8 | 1024  # MS_NOSUID | MS_NODEV | MS_NOEXEC | MS_NOATIME
_SKIP = ("loop", "ram", "zram", "mtdblock", "sr", "fd", "nbd", "dm-")
_ERRORS = {
    # Замер 2026-10-23, привилегированный LXC Proxmox: tmpfs монтируется, cifs и nfs — EPERM, пока
    # в конфиге контейнера нет `features: mount=cifs;nfs` (AppArmor Proxmox).
    errno.EPERM: (
        "контейнеру не разрешено монтировать: Docker-контейнер Home Assistant должен быть "
        "привилегированным, а LXC Proxmox — с разрешением features: mount=cifs;nfs"
    ),
    errno.EACCES: "доступ запрещён: проверьте логин и пароль сетевой папки",
    errno.ENODEV: "ядро контроллера не умеет эту файловую систему",
    errno.ENOENT: "нет такой папки на сервере или устройства",
    errno.EHOSTUNREACH: "сервер недоступен",
    errno.ETIMEDOUT: "сервер не ответил",
    errno.ECONNREFUSED: "сервер отказал в подключении",
    errno.EBUSY: "диск уже занят",
    errno.EINVAL: "сервер или диск не принял параметры",
}


def _libc() -> Any:
    return ctypes.CDLL(ctypes.util.find_library("c") or "libc.so.6", use_errno=True)


def mounted() -> dict[str, tuple[str, str]]:
    """Точки монтирования контейнера → (источник, тип), по `/proc/mounts`."""
    out: dict[str, tuple[str, str]] = {}
    try:
        lines = Path("/proc/mounts").read_text().splitlines()
    except OSError:
        return out
    for line in lines:
        parts = line.split()
        if len(parts) >= 3:
            out[parts[1].replace("\\040", " ")] = (parts[0], parts[2])
    return out


def superblock(device: Path) -> dict[str, str] | None:
    """Тип, UUID и метка файловой системы по суперблоку; незнакомая — None."""
    try:
        with open(device, "rb") as handle:
            head = handle.read(0x10200)
    except OSError:
        return None

    def text(raw: bytes) -> str:
        return raw.split(b"\0", 1)[0].decode("utf-8", "ignore").strip()

    def uuid(raw: bytes) -> str:
        h = raw.hex()
        return f"{h[0:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:32]}"

    if len(head) >= 0x478 and head[0x438:0x43A] == b"\x53\xef":
        return {"type": "ext4", "uuid": uuid(head[0x468:0x478]), "label": text(head[0x478:0x488])}
    if head[3:11] == b"EXFAT   ":
        serial = head[0x64:0x68][::-1].hex().upper()
        return {"type": "exfat", "uuid": f"{serial[:4]}-{serial[4:]}", "label": ""}
    if head[3:11] == b"NTFS    ":
        return {"type": "ntfs3", "uuid": head[0x48:0x50][::-1].hex().upper(), "label": ""}
    for kind, serial_at, label_at in ((b"FAT32   ", 0x43, 0x47), (b"FAT16   ", 0x27, 0x2B), (b"FAT12   ", 0x27, 0x2B)):
        if head[label_at + 11 : label_at + 19] == kind:
            serial = head[serial_at : serial_at + 4][::-1].hex().upper()
            return {"type": "vfat", "uuid": f"{serial[:4]}-{serial[4:]}", "label": text(head[label_at : label_at + 11])}
    if head[0:4] == b"XFSB":
        return {"type": "xfs", "uuid": uuid(head[32:48]), "label": text(head[108:120])}
    if len(head) >= 0x10140 and head[0x10040:0x10048] == b"_BHRfS_M":
        return {"type": "btrfs", "uuid": uuid(head[0x10020:0x10030]), "label": text(head[0x1012B:0x10200])}
    return None


def disks(sys_block: Path = Path("/sys/class/block"), dev: Path = Path("/dev")) -> list[dict[str, Any]]:
    """Разделы с файловой системой: имя, размер, тип, UUID, метка, съёмный ли, занят ли системой."""
    used = {source for point, (source, _kind) in mounted().items() if not point.startswith(str(MOUNT_POINT))}
    out: list[dict[str, Any]] = []
    for entry in sorted(sys_block.iterdir()) if sys_block.is_dir() else []:
        name = entry.name
        partition = (entry / "partition").exists()
        # Целый диск с разделами пропускается — показываются его разделы.
        if name.startswith(_SKIP) or (not partition and any(child.startswith(name) for child in os.listdir(entry))):
            continue
        found = superblock(dev / name)
        if found is None:
            continue
        out.append({
            "device": name, "size": _read_int(entry / "size") * 512, **found,
            "removable": _read_int((entry.resolve().parent if partition else entry.resolve()) / "removable") == 1,
            "system": str(dev / name) in used,
        })
    return out


def _read_int(path: Path) -> int:
    try:
        return int(path.read_text().strip())
    except (OSError, ValueError):
        return 0


def system_device(reference: Path) -> int:
    """Устройство, на котором лежат настройки дома: запись туда — запись на флеш контроллера."""
    return os.stat(reference).st_dev


def allowed(path: Path) -> bool:
    """Папка внутри каталогов хранения (без выхода ссылками и `..`)."""
    real = Path(os.path.realpath(path))
    return any(real == root or root in real.parents for root in ROOTS) and real == path


def folders(path: str, reference: Path) -> dict[str, Any]:
    """Подпапки `path` (пусто — корни хранения) с диском каждой: системный ли, свободно и всего."""
    system = system_device(reference)
    points = mounted()

    def describe(folder: Path) -> dict[str, Any]:
        usage = shutil.disk_usage(folder)
        source, kind = points.get(str(folder), ("", ""))
        return {
            "path": str(folder), "name": folder.name or str(folder), "system": os.stat(folder).st_dev == system,
            "free": usage.free, "total": usage.total, **({"mount": source, "fs": kind} if source else {}),
        }

    if not path:
        return {"path": "", "folders": [describe(root) for root in ROOTS if root.is_dir()]}
    folder = Path(path)
    if not folder.is_absolute() or not allowed(folder) or not folder.is_dir():
        raise ValueError(f"папка {path} вне каталогов хранения или её нет")
    # Свои точки монтирования в обзоре «папок контроллера» не показываем: туда ходят через вид хранилища.
    children = sorted(
        c for c in folder.iterdir()
        if c.is_dir() and not c.name.startswith(".") and allowed(c) and c != MOUNT_POINT.parent
    )
    return {**describe(folder), "folders": [describe(child) for child in children]}


def make_folder(parent: str, name: str) -> str:
    """Новая папка в каталоге хранения; имя — латиница, цифры, `_ . -` (путь едет в ffconcat и go2rtc)."""
    base = Path(parent)
    if not _NAME.match(name) or name in (".", ".."):
        raise ValueError("имя папки — латиница, цифры и знаки _ . - (до 64)")
    if not base.is_absolute() or not allowed(base) or not base.is_dir():
        raise ValueError(f"папка {parent} вне каталогов хранения или её нет")
    target = base / name
    target.mkdir(exist_ok=True)
    return str(target)


def mount(spec: dict[str, Any], target: Path = MOUNT_POINT) -> str:
    """Смонтировать `spec` (`type`: `cifs`/`nfs`/`usb`, `source`, `user`, `pass`, `options`) в
    `target`; уже смонтировано то же — ничего не делать. Пусто — смонтировано, иначе причина."""
    kind, source = str(spec.get("type") or ""), str(spec.get("source") or "").strip()
    try:
        device, fstype, data = _plan(kind, source, spec)
    except (ValueError, OSError) as err:
        return str(err)
    current = mounted().get(str(target))
    if current is not None and current[0] == device:
        return ""
    if current is not None:
        unmount(target)
    target.mkdir(parents=True, exist_ok=True)
    libc = _libc()
    if libc.mount(device.encode(), str(target).encode(), fstype.encode(), _FLAGS, data.encode()) != 0:
        code = ctypes.get_errno()
        return f"не смонтировалось ({source}): {_ERRORS.get(code, os.strerror(code))}"
    return ""


def unmount(target: Path = MOUNT_POINT) -> None:
    """Отмонтировать лениво (MNT_DETACH): открытый ffmpeg не держит отмену записи."""
    if str(target) in mounted():
        _libc().umount2(str(target).encode(), 2)


def _plan(kind: str, source: str, spec: dict[str, Any]) -> tuple[str, str, str]:
    """(устройство или адрес, тип ФС, строка параметров ядра) для `mount(2)`."""
    options = [o for o in str(spec.get("options") or "").split(",") if re.match(r"^[A-Za-z0-9_.=:-]+$", o)]
    if kind == "usb":
        match = next((d for d in disks() if d["uuid"].lower() == source.removeprefix("UUID=").lower()), None)
        if match is None:
            raise ValueError(f"диск {source} не найден — подключите его к контроллеру")
        return f"/dev/{match['device']}", match["type"], ",".join(options)
    if kind == "cifs":
        found = re.match(r"^//([^/\s]+)(/[^\s]+)$", source)
        if not found:
            raise ValueError("сетевая папка SMB — в виде //сервер/папка")
        address = socket.gethostbyname(found.group(1))
        # Запятая — разделитель параметров ядра: в пароле она удваивается (cifs, mount.cifs(8)).
        secret = str(spec.get("pass") or "").replace(",", ",,")
        account = [f"username={spec.get('user') or 'guest'}", f"password={secret}"]
        return f"//{address}{found.group(2)}", "cifs", ",".join([*account, f"ip={address}", *options])
    if kind == "nfs":
        found = re.match(r"^([^:/\s]+):(/[^\s]*)$", source)
        if not found:
            raise ValueError("папка NFS — в виде сервер:/путь")
        address = socket.gethostbyname(found.group(1))
        return f"{address}:{found.group(2)}", "nfs4", ",".join(["vers=4.1", f"addr={address}", *options])
    raise ValueError(f"неизвестный вид хранилища «{kind}»")
