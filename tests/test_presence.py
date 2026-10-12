"""Присутствие узла (`core/presence.py`): кадры, раскладка по способам, запасной путь."""

from __future__ import annotations

import asyncio
import ipaddress
import socket
import struct

from fake_host import FakeHost
from mega_home.core import presence

MAC = bytes.fromhex("020000000001")
LINK = {"name": "eth0", "ip": "192.168.1.10", "net": ipaddress.ip_network("192.168.1.0/24"), "mac": MAC}


def test_arp_запрос_и_ответ_по_rfc_826() -> None:
    frame = presence.arp_request(MAC, "192.168.1.10", "192.168.1.20")
    assert frame[:6] == b"\xff" * 6 and frame[12:14] == b"\x08\x06"
    assert frame[20:22] == b"\x00\x01"
    assert socket.inet_ntoa(frame[38:42]) == "192.168.1.20"
    # Ответ — тот же кадр с op=2 и адресом отвечающего в поле отправителя.
    reply = frame[:20] + b"\x00\x02" + bytes.fromhex("aabbccddeeff") + socket.inet_aton("192.168.1.20") + frame[32:]
    assert presence.arp_reply(reply) == ("192.168.1.20", "aa:bb:cc:dd:ee:ff")
    assert presence.arp_reply(frame) is None


def test_icmp_контрольная_сумма_сходится() -> None:
    packet = presence.icmp_echo(0x1234, 7)
    words = struct.unpack(f"!{len(packet) // 2}H", packet)
    total = sum(words)
    total = (total >> 16) + (total & 0xFFFF)
    assert total + (total >> 16) & 0xFFFF == 0xFFFF


def test_своя_подсеть_arp_чужая_icmp_сам_себя_не_спрашивает(monkeypatch) -> None:
    calls: dict[str, list[str]] = {}
    monkeypatch.setattr(presence, "_raw", {})
    monkeypatch.setattr(presence, "_interfaces", lambda: [LINK])
    monkeypatch.setattr(presence, "_arp_batch", lambda link, hosts: calls.setdefault("arp", hosts) and {h: {"ok": True, "method": "arp"} for h in hosts})
    monkeypatch.setattr(presence, "_icmp_batch", lambda hosts: calls.setdefault("icmp", hosts) and {h: {"ok": False, "method": "icmp"} for h in hosts})
    answers = asyncio.run(presence.check(FakeHost(), ["192.168.1.20", "10.0.5.3", "192.168.1.10"]))
    assert calls == {"arp": ["192.168.1.20"], "icmp": ["10.0.5.3"]}
    assert answers["192.168.1.10"] == {"ok": True, "method": "self"}
    assert answers["192.168.1.20"]["method"] == "arp"


def test_без_прав_на_сырые_сокеты_уходит_в_tcp(monkeypatch) -> None:
    monkeypatch.setattr(presence, "_raw", {})
    monkeypatch.setattr(presence, "_interfaces", lambda: [LINK])

    def no_rights(*_args):
        return None

    monkeypatch.setattr(presence, "_arp_batch", no_rights)
    monkeypatch.setattr(presence, "_icmp_batch", no_rights)

    async def tcp(hosts):
        return {h: {"ok": True, "method": "tcp"} for h in hosts}

    monkeypatch.setattr(presence, "_tcp_batch", tcp)
    answers = asyncio.run(presence.check(FakeHost(), ["192.168.1.20"]))
    assert answers["192.168.1.20"]["method"] == "tcp"


def test_tcp_отказ_значит_жив(monkeypatch) -> None:
    async def refuse(host, port):
        raise ConnectionRefusedError

    async def unreachable(host, port):
        raise OSError(113, "Host is unreachable")

    monkeypatch.setattr(asyncio, "open_connection", refuse)
    assert asyncio.run(presence._tcp_batch(["192.168.1.20"]))["192.168.1.20"]["ok"] is True
    monkeypatch.setattr(asyncio, "open_connection", unreachable)
    answer = asyncio.run(presence._tcp_batch(["192.168.1.20"]))["192.168.1.20"]
    assert answer == {"ok": False, "method": "tcp", "reason": "unreachable"}
