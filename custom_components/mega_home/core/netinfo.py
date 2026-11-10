"""Сеть САМОГО дома и соседи на его проводе — сырые факты для менеджера.

Зачем. Менеджер угадывал подсеть объекта («сеть HA/24», сеть роутера из карточки). Объект с
перенастроенным роутером (сменили LAN) терял сканирование целиком: дом на связи, а где он
стоит — не знал никто (объект 106-Okishev, 2026-11-10). Теперь дом сообщает свои адреса и
маршруты в `hello`, а толкует их менеджер (`docs/home-gateway.md`, «Кто кому шлёт»).

Соседи (`neighbors`) — устройства на ТОМ ЖЕ проводе, в том числе из чужой подсети: на
ARP-запрос узел отвечает независимо от подсети спрашивающего, а широковещательные ARP чужой
сети дом слышит и так. TCP через шлюз этого не даёт: сброшенный роутер с заводским адресом
стоит рядом, но маршрута до него нет.

⚠ Только ЧТЕНИЕ сети хоста и кадры ARP: адреса, маршруты и настройки интерфейсов дом не
меняет. Это контроллер автоматики объекта; ломать его сеть ради сервиса нельзя (требование:
«не ломать сеть на контроллере автоматизации»).
"""

from __future__ import annotations

import ipaddress
import select
import socket
import struct
import time
from pathlib import Path
from typing import Any

ETH_ARP = 0x0806
SIOCGIFADDR, SIOCGIFNETMASK, SIOCGIFHWADDR = 0x8915, 0x891B, 0x8927
PROC_ROUTE = Path("/proc/net/route")
# Флаги `/proc/net/route` (linux/route.h): маршрут поднят, через шлюз.
RTF_UP, RTF_GATEWAY = 0x1, 0x2

# Прослушивание: по умолчанию и потолок, секунды. Дольше канал держать незачем — широковещательные
# ARP роутера идут каждые несколько секунд, а опрос сам вызывает ответы.
LISTEN_DEFAULT_S = 10.0
SWEEP_DEFAULT_S = 4.0
LISTEN_MAX_S = 30.0
# Опрос чужой подсети — не крупнее /22, как у сканирования (`scan.py`): кадры уходят на весь провод.
SWEEP_MAX_ADDRESSES = 1024
# Кадры опроса — пачками с паузой: тысяча широковещательных кадров разом забивает очередь
# дешёвого коммутатора и Wi-Fi-моста.
SWEEP_BATCH = 64
SWEEP_PAUSE_S = 0.02
# Повтор опроса: одиночная потеря кадра не должна прятать устройство.
SWEEP_REPEAT_AT_S = 1.0
# Потолок ответа — провод с тысячей узлов не должен раздуть кадр канала.
SEEN_MAX = 2048


class NetError(Exception):
    """Отказ задания — текст для инсталлятора."""


def interfaces() -> list[dict[str, Any]]:
    """Свои IPv4-интерфейсы: имя, адрес, подсеть, MAC. Не Linux — пусто.

    ⚠ ioctl даёт ОСНОВНОЙ адрес интерфейса; вторые адреса видны маршрутами (`routes`):
    у каждой своей подсети ядро держит маршрут «на линии».
    """
    try:
        import fcntl  # noqa: PLC0415 - есть только на POSIX
    except ImportError:
        return []
    links: list[dict[str, Any]] = []
    probe = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        for _index, name in socket.if_nameindex():
            if name == "lo":
                continue
            packed = struct.pack("256s", name.encode()[:15])
            try:
                ip = socket.inet_ntoa(fcntl.ioctl(probe, SIOCGIFADDR, packed)[20:24])
                mask = socket.inet_ntoa(fcntl.ioctl(probe, SIOCGIFNETMASK, packed)[20:24])
                mac = fcntl.ioctl(probe, SIOCGIFHWADDR, packed)[18:24]
            except OSError:
                continue
            net = ipaddress.ip_network(f"{ip}/{mask}", strict=False)
            # Точка-точка и туннели (/32, /31) — не L2: ARP там не ходит.
            if net.prefixlen < 31:
                links.append({"name": name, "ip": ip, "net": net, "mac": mac})
    finally:
        probe.close()
    return links


def _hex_ip(value: str) -> str:
    """Адрес из `/proc/net/route`: 32 бита шестнадцатеричным числом в порядке хоста (little-endian)."""
    return socket.inet_ntoa(struct.pack("<I", int(value, 16)))


def routes(text: str | None = None) -> list[dict[str, Any]]:
    """Маршруты IPv4: `{net, via, iface}`; `via` — `None` у сети «на линии», `0.0.0.0/0` — основной шлюз."""
    if text is None:
        try:
            text = PROC_ROUTE.read_text("ascii")
        except OSError:
            return []
    found: list[dict[str, Any]] = []
    for line in text.splitlines()[1:]:
        parts = line.split()
        if len(parts) < 8:
            continue
        try:
            flags = int(parts[3], 16)
            dest, gateway, mask = _hex_ip(parts[1]), _hex_ip(parts[2]), _hex_ip(parts[7])
            net = ipaddress.ip_network(f"{dest}/{mask}", strict=False)
        except ValueError:
            continue
        if not flags & RTF_UP or parts[0] == "lo":
            continue
        found.append({
            "net": str(net),
            "via": gateway if flags & RTF_GATEWAY else None,
            "iface": parts[0],
            "metric": int(parts[6]) if parts[6].isdigit() else 0,
        })
    return found


def facts() -> dict[str, Any]:
    """Поле `net` кадра hello: интерфейсы, основной шлюз, маршруты. Блокирующий — в исполнителе.

    Толкования нет: какие подсети сканировать и что из этого роутер, решает менеджер.
    """
    table = routes()
    defaults = sorted((r for r in table if r["net"] == "0.0.0.0/0" and r["via"]), key=lambda r: r["metric"])
    return {
        "interfaces": [
            {"name": link["name"], "address": f"{link['ip']}/{link['net'].prefixlen}", "mac": link["mac"].hex(":")}
            for link in interfaces()
        ],
        "gateway": {"ip": defaults[0]["via"], "iface": defaults[0]["iface"]} if defaults else None,
        "routes": [{k: r[k] for k in ("net", "via", "iface")} for r in table if r["net"] != "0.0.0.0/0"],
    }


def arp_request(mac: bytes, ip: str, target: str) -> bytes:
    """Кадр Ethernet с ARP-запросом «у кого `target`» на широковещательный адрес (RFC 826)."""
    return (
        b"\xff" * 6 + mac + struct.pack("!H", ETH_ARP)
        + struct.pack("!HHBBH", 1, 0x0800, 6, 4, 1)
        + mac + socket.inet_aton(ip) + b"\x00" * 6 + socket.inet_aton(target)
    )


def arp_sender(frame: bytes) -> tuple[str, str, int] | None:
    """`(адрес, MAC, op)` ОТПРАВИТЕЛЯ любого ARP-кадра; проба с адресом 0.0.0.0 — `None`."""
    if len(frame) < 42 or frame[12:14] != b"\x08\x06" or frame[16:18] != b"\x08\x00":
        return None
    ip = socket.inet_ntoa(frame[28:32])
    if ip == "0.0.0.0":
        return None
    return ip, frame[22:28].hex(":"), int.from_bytes(frame[20:22], "big")


def sweep_targets(value: Any) -> list[str]:
    """Адреса опроса: частная подсеть не крупнее /22, иначе отказ с причиной."""
    try:
        network = ipaddress.ip_network(str(value).strip(), strict=False)
    except ValueError as err:
        raise NetError(f"Подсеть «{value}» не разобрать") from err
    if not isinstance(network, ipaddress.IPv4Network) or not network.is_private:
        raise NetError("Опрос — только частная сеть IPv4")
    if network.num_addresses > SWEEP_MAX_ADDRESSES:
        raise NetError("Опрос — подсеть не крупнее /22")
    return [str(ip) for ip in network.hosts()]


def neighbors(seconds: float, sweep: str | None = None) -> dict[str, Any]:
    """Соседи на проводе: слушать ARP `seconds` и, если задана `sweep`, опросить её адреса.

    Блокирующий — в исполнителе. ⚠ Сокет на КАЖДЫЙ свой L2-интерфейс: у контроллера бывает
    несколько портов, и роутер может оказаться за любым.
    """
    targets = sweep_targets(sweep) if sweep else []
    links = interfaces()
    if not links:
        raise NetError("У дома нет интерфейса Ethernet с адресом IPv4")
    sockets: list[tuple[socket.socket, dict[str, Any]]] = []
    try:
        for link in links:
            try:
                sock = socket.socket(socket.AF_PACKET, socket.SOCK_RAW, socket.htons(ETH_ARP))
            except (OSError, AttributeError) as err:
                raise NetError(f"Нет прав на сырой сокет: {err}") from err
            sockets.append((sock, link))
            sock.bind((link["name"], ETH_ARP))
            sock.setblocking(False)
        return _collect(sockets, targets, seconds)
    finally:
        for sock, _link in sockets:
            sock.close()


def _collect(sockets: list[tuple[socket.socket, dict[str, Any]]], targets: list[str], seconds: float) -> dict[str, Any]:
    started = time.monotonic()
    seen: dict[tuple[str, str], dict[str, Any]] = {}
    # Очередь отправки: (момент, сокет, кадр) — две волны опроса со сдвигом.
    queue: list[tuple[float, socket.socket, bytes]] = []
    for wave in (0.0, SWEEP_REPEAT_AT_S):
        for sock, link in sockets:
            for index, target in enumerate(targets):
                at = wave + (index // SWEEP_BATCH) * SWEEP_PAUSE_S
                queue.append((at, sock, arp_request(link["mac"], link["ip"], target)))
    queue.sort(key=lambda item: item[0])
    by_fd = {sock.fileno(): link for sock, link in sockets}
    while (left := seconds - (time.monotonic() - started)) > 0:
        now = time.monotonic() - started
        while queue and queue[0][0] <= now:
            _at, sock, frame = queue.pop(0)
            try:
                sock.send(frame)
            except OSError:
                pass
        wait = min(left, 0.05) if queue else min(left, 0.25)
        ready, _w, _x = select.select([sock for sock, _l in sockets], [], [], wait)
        for sock in ready:
            try:
                frame = sock.recv(2048)
            except OSError:
                continue
            hit = arp_sender(frame)
            if hit is None or len(seen) >= SEEN_MAX:
                continue
            link = by_fd[sock.fileno()]
            ip, mac, op = hit
            if ip == link["ip"] and mac == link["mac"].hex(":"):
                continue  # свой кадр
            key = (ip, link["name"])
            if key not in seen:
                seen[key] = {"ip": ip, "mac": mac, "iface": link["name"], "replied": op == 2}
            elif op == 2:
                seen[key]["replied"] = True
    return {"seconds": round(time.monotonic() - started, 1), "swept": len(targets), "seen": list(seen.values())}


async def run(env: Any, payload: dict[str, Any]) -> dict[str, Any]:
    """Операция `neighbors` (`core/ops.py`): `{seconds?, sweep?}` → `{seconds, swept, seen[]}`."""
    from http import HTTPStatus  # noqa: PLC0415

    from .ops_base import OpError  # noqa: PLC0415

    sweep = payload.get("sweep") or None
    try:
        seconds = float(payload.get("seconds") or (SWEEP_DEFAULT_S if sweep else LISTEN_DEFAULT_S))
    except (TypeError, ValueError):
        seconds = LISTEN_DEFAULT_S
    seconds = max(1.0, min(seconds, LISTEN_MAX_S))
    try:
        return await env.run(neighbors, seconds, sweep)
    except NetError as err:
        raise OpError(str(err), HTTPStatus.BAD_REQUEST) from err
