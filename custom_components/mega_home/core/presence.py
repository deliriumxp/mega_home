"""Присутствие узла в сети объекта: жив ли адрес, БЕЗ знания, что за ним стоит.

Зачем (менеджер, `docs/plan-device-presence.md`). «В сети без перерыва» считается по
пульсу, как у Domotz: агент на объекте раз в полминуты спрашивает каждый адрес. HTTP и
TCP (`connect.py`) этого не дают: телефон, ноутбук, железка без служб не слушают ни одного
порта, и их видело только часовое сканирование.

Чем спрашиваем — по порядку надёжности:
- своя подсеть — ARP-запрос: на L2 обязан отвечать ЛЮБОЙ узел, файрвол Windows тоже, иначе
  ему не доставить ни одного пакета;
- за маршрутом — ICMP echo (ARP там спрашивает шлюз, а не цель);
- нет прав на сырые сокеты — TCP-соединение: отказ (RST) отвечает живой стек, соединение тоже.

⚠ Чем мерили, едет в ответе (`method`): толкует менеджер. Ответ TCP слабее — молчащий
файрвол там неотличим от пропавшего узла.
"""

from __future__ import annotations

import asyncio
import errno
import ipaddress
import os
import select
import socket
import struct
import time
from typing import Any

from .const import LOGGER
from .host import Host

# Сколько ждём ответов пачки. Запрос уходит дважды (второй — через `RESEND_S`): одиночная
# потеря кадра в Wi-Fi не должна стоить «пропуска пульса».
WAIT_S = 1.5
RESEND_S = 0.5
TCP_PORT = 9
TCP_TIMEOUT_S = 2.0
ETH_ARP = 0x0806
SIOCGIFADDR, SIOCGIFNETMASK, SIOCGIFHWADDR = 0x8915, 0x891B, 0x8927

# Есть ли права на сырой сокет — узнаём ОДИН раз за жизнь процесса: права контейнера HA
# не меняются, а пробовать сокет на каждый пульс — это ошибка ядра в цикле.
_raw: dict[str, bool] = {}


async def check(env: Host, hosts: list[str]) -> dict[str, dict[str, Any]]:
    """`адрес → {ok, method, mac?, ms?}` для каждого адреса пачки."""
    links = await env.run(_interfaces)
    arp: dict[str, list[str]] = {}
    icmp: list[str] = []
    for host in hosts:
        link = next((item for item in links if ipaddress.ip_address(host) in item["net"]), None)
        if link and host == link["ip"]:
            continue
        if link:
            arp.setdefault(link["name"], []).append(host)
        else:
            icmp.append(host)
    answers: dict[str, dict[str, Any]] = {}
    for link in links:
        if host_list := arp.get(link["name"]):
            found = await env.run(_arp_batch, link, host_list) if _raw.get("arp", True) else None
            if found is None:
                icmp.extend(host_list)
            else:
                answers.update(found)
    if icmp:
        found = await env.run(_icmp_batch, icmp) if _raw.get("icmp", True) else None
        answers.update(found if found is not None else await _tcp_batch(icmp))
    for link in links:
        if link["ip"] in hosts:
            answers[link["ip"]] = {"ok": True, "method": "self"}
    return answers


def _interfaces() -> list[dict[str, Any]]:
    """Свои IPv4-интерфейсы: имя, адрес, подсеть, MAC. Не Linux — пусто (тогда ICMP/TCP)."""
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


def arp_request(mac: bytes, ip: str, target: str) -> bytes:
    """Кадр Ethernet с ARP-запросом «у кого `target`» на широковещательный адрес."""
    return (
        b"\xff" * 6 + mac + struct.pack("!H", ETH_ARP)
        + struct.pack("!HHBBH", 1, 0x0800, 6, 4, 1)
        + mac + socket.inet_aton(ip) + b"\x00" * 6 + socket.inet_aton(target)
    )


def arp_reply(frame: bytes) -> tuple[str, str] | None:
    """`(адрес, MAC)` отвечающего, если кадр — ARP-ответ."""
    if len(frame) < 42 or frame[12:14] != b"\x08\x06" or frame[20:22] != b"\x00\x02":
        return None
    return socket.inet_ntoa(frame[28:32]), frame[22:28].hex(":")


def _arp_batch(link: dict[str, Any], hosts: list[str]) -> dict[str, dict[str, Any]] | None:
    """ARP по пачке через сырой сокет. `None` — прав нет (запасной путь у вызывающего)."""
    try:
        sock = socket.socket(socket.AF_PACKET, socket.SOCK_RAW, socket.htons(ETH_ARP))
    except (OSError, AttributeError) as err:
        _raw["arp"] = False
        # Один раз за жизнь процесса (`_raw`): дальше молча идём запасным путём.
        LOGGER.warning("Присутствие в своей подсети — без ARP, нет прав на сырой сокет: %s", err)
        return None
    frames = [arp_request(link["mac"], link["ip"], host) for host in hosts]
    try:
        sock.bind((link["name"], ETH_ARP))
        return _collect(sock, frames, set(hosts), "arp", arp_reply)
    finally:
        sock.close()


def icmp_echo(ident: int, seq: int) -> bytes:
    """ICMP echo request с контрольной суммой (RFC 792)."""
    head = struct.pack("!BBHHH", 8, 0, 0, ident, seq) + b"mega"
    total = sum(struct.unpack(f"!{len(head) // 2}H", head))
    total = (total >> 16) + (total & 0xFFFF)
    total += total >> 16
    return head[:2] + struct.pack("!H", ~total & 0xFFFF) + head[4:]


def _icmp_batch(hosts: list[str]) -> dict[str, dict[str, Any]] | None:
    """ICMP по пачке: сырой сокет, иначе непривилегированный (`ping_group_range`)."""
    for kind in (socket.SOCK_RAW, socket.SOCK_DGRAM):
        try:
            sock = socket.socket(socket.AF_INET, kind, socket.IPPROTO_ICMP)
            break
        except OSError:
            continue
    else:
        _raw["icmp"] = False
        LOGGER.warning("Присутствие — запасным путём TCP: нет прав на ICMP-сокет")
        return None
    raw = kind == socket.SOCK_RAW

    def reply(packet: bytes, source: Any) -> tuple[str, str] | None:
        body = packet[(packet[0] & 0x0F) * 4 :] if raw else packet
        return (source[0], "") if body[:1] == b"\x00" else None

    try:
        packets = [icmp_echo(os.getpid() & 0xFFFF, index) for index in range(len(hosts))]
        return _collect(sock, packets, set(hosts), "icmp", reply, hosts)
    finally:
        sock.close()


def _collect(
    sock: socket.socket, packets: list[bytes], wanted: set[str], method: str, parse: Any,
    to: list[str] | None = None,
) -> dict[str, dict[str, Any]]:
    """Разослать пачку дважды и собирать ответы до срока. Блокирующий — в исполнителе."""
    started = time.monotonic()
    found: dict[str, dict[str, Any]] = {}
    sends = [0.0, RESEND_S]
    sock.setblocking(False)
    while (left := WAIT_S - (time.monotonic() - started)) > 0:
        if sends and time.monotonic() - started >= sends[0]:
            sends.pop(0)
            for index, packet in enumerate(packets):
                if (to[index] if to else None) in found:
                    continue
                try:
                    sock.sendto(packet, (to[index], 0)) if to else sock.send(packet)
                except OSError:
                    pass
        ready, _w, _x = select.select([sock], [], [], min(left, RESEND_S / 2))
        if not ready:
            continue
        try:
            packet, source = sock.recvfrom(2048)
        except OSError:
            continue
        hit = parse(packet) if method == "arp" else parse(packet, source)
        if hit and hit[0] in wanted and hit[0] not in found:
            found[hit[0]] = {"ok": True, "method": method, "ms": int((time.monotonic() - started) * 1000)}
            if hit[1]:
                found[hit[0]]["mac"] = hit[1]
    for host in wanted - found.keys():
        found[host] = {"ok": False, "method": method}
    return found


async def _tcp_batch(hosts: list[str]) -> dict[str, dict[str, Any]]:
    """Запасной путь: TCP на закрытый порт. Отказ (RST) — жив, тишина и «нет пути» — нет."""

    async def one(host: str) -> dict[str, Any]:
        started = time.monotonic()
        try:
            _reader, writer = await asyncio.wait_for(asyncio.open_connection(host, TCP_PORT), TCP_TIMEOUT_S)
            writer.close()
        except ConnectionRefusedError:
            pass
        except (asyncio.TimeoutError, OSError) as err:
            # «Host is unreachable» — ядро не дождалось ARP: узла нет. Тишина — нет ответа.
            reason = "unreachable" if getattr(err, "errno", None) == errno.EHOSTUNREACH else "timeout"
            return {"ok": False, "method": "tcp", "reason": reason}
        return {"ok": True, "method": "tcp", "ms": int((time.monotonic() - started) * 1000)}

    results = await asyncio.gather(*(one(host) for host in hosts))
    return dict(zip(hosts, results))
