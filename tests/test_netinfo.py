"""Сеть дома и соседи на проводе (`core/netinfo.py`): разбор маршрутов, форма hello, кадры ARP, пределы опроса."""

from __future__ import annotations

import asyncio
import ipaddress
import json
import socket
from pathlib import Path

import pytest

from fake_host import FakeHost
from mega_home.core import netinfo
from mega_home.core.ops_base import OpError

CONTRACT = json.loads(
    (Path(__file__).parents[1] / "custom_components/mega_home/contracts/net.json").read_text("utf-8")
)
MAC = bytes.fromhex("020000000001")
LINK = {"name": "end0", "ip": "192.168.0.23", "net": ipaddress.ip_network("192.168.0.0/24"), "mac": MAC}

# `/proc/net/route` хоста: основной шлюз, сеть на линии, сеть за вторым маршрутизатором, опущенный маршрут.
ROUTE = """Iface\tDestination\tGateway \tFlags\tRefCnt\tUse\tMetric\tMask\t\tMTU\tWindow\tIRTT
end0\t00000000\t0100A8C0\t0003\t0\t0\t100\t00000000\t0\t0\t0
end0\t0000A8C0\t00000000\t0001\t0\t0\t100\t00FFFFFF\t0\t0\t0
end0\t0000140A\tFE00A8C0\t0003\t0\t0\t0\t0000FFFF\t0\t0\t0
wlan0\t0001A8C0\t00000000\t0000\t0\t0\t0\t00FFFFFF\t0\t0\t0
"""


def test_маршруты_из_proc_net_route() -> None:
    table = netinfo.routes(ROUTE)
    assert [(r["net"], r["via"], r["iface"]) for r in table] == [
        ("0.0.0.0/0", "192.168.0.1", "end0"),
        ("192.168.0.0/24", None, "end0"),
        ("10.20.0.0/16", "192.168.0.254", "end0"),
    ]


def test_факты_hello_той_же_формы_что_образец(monkeypatch) -> None:
    monkeypatch.setattr(netinfo, "interfaces", lambda: [LINK])
    table = netinfo.routes(ROUTE)
    monkeypatch.setattr(netinfo, "routes", lambda: table)
    facts = netinfo.facts()
    sample = CONTRACT["hello"]
    assert facts.keys() == sample.keys()
    assert facts["interfaces"] == [{"name": "end0", "address": "192.168.0.23/24", "mac": "02:00:00:00:00:01"}]
    assert facts["interfaces"][0].keys() == sample["interfaces"][0].keys()
    assert facts["gateway"] == {"ip": "192.168.0.1", "iface": "end0"}
    # Основной шлюз — отдельным полем, в маршрутах его нет.
    assert all(r["net"] != "0.0.0.0/0" for r in facts["routes"])
    assert all(r.keys() == sample["routes"][0].keys() for r in facts["routes"])


def test_без_шлюза_поле_пустое(monkeypatch) -> None:
    monkeypatch.setattr(netinfo, "interfaces", lambda: [LINK])
    table = [r for r in netinfo.routes(ROUTE) if r["net"] != "0.0.0.0/0"]
    monkeypatch.setattr(netinfo, "routes", lambda: table)
    assert netinfo.facts()["gateway"] is None


def _arp(op: int, sender_ip: str, sender_mac: bytes = bytes.fromhex("f41e57a049fa")) -> bytes:
    frame = netinfo.arp_request(sender_mac, sender_ip, "192.168.0.23")
    return frame[:20] + op.to_bytes(2, "big") + frame[22:]


def test_отправитель_любого_arp_кадра() -> None:
    assert netinfo.arp_sender(_arp(2, "192.168.88.1")) == ("192.168.88.1", "f4:1e:57:a0:49:fa", 2)
    assert netinfo.arp_sender(_arp(1, "192.168.88.1"))[2] == 1
    # Проба занятости адреса (RFC 5227) — отправитель 0.0.0.0, соседом не считается.
    assert netinfo.arp_sender(_arp(1, "0.0.0.0")) is None
    assert netinfo.arp_sender(b"\x00" * 60) is None
    frame = netinfo.arp_request(MAC, "192.168.0.23", "192.168.88.1")
    assert socket.inet_ntoa(frame[38:42]) == "192.168.88.1"


def test_опрос_только_частной_сети_не_крупнее_22() -> None:
    assert len(netinfo.sweep_targets("192.168.88.0/24")) == 254
    assert len(netinfo.sweep_targets("10.0.0.0/22")) == 1022
    for bad in ("10.0.0.0/21", "8.8.8.0/24", "сеть", "fd00::/120"):
        with pytest.raises(netinfo.NetError):
            netinfo.sweep_targets(bad)


def test_задание_держит_срок_в_пределах_и_отказывает_словами(monkeypatch) -> None:
    calls: list[tuple[float, str | None]] = []

    def fake(seconds: float, sweep: str | None = None) -> dict:
        calls.append((seconds, sweep))
        return CONTRACT["neighbors"]

    monkeypatch.setattr(netinfo, "neighbors", fake)
    env = FakeHost()
    answer = asyncio.run(netinfo.run(env, {"sweep": "192.168.88.0/24"}))
    assert answer.keys() == CONTRACT["neighbors"].keys()
    asyncio.run(netinfo.run(env, {"seconds": 600}))
    asyncio.run(netinfo.run(env, {"seconds": "кривое"}))
    assert calls == [
        (netinfo.SWEEP_DEFAULT_S, "192.168.88.0/24"),
        (netinfo.LISTEN_MAX_S, None),
        (netinfo.LISTEN_DEFAULT_S, None),
    ]

    def refuse(seconds: float, sweep: str | None = None) -> dict:
        raise netinfo.NetError("Опрос — подсеть не крупнее /22")

    monkeypatch.setattr(netinfo, "neighbors", refuse)
    with pytest.raises(OpError, match="/22"):
        asyncio.run(netinfo.run(env, {"sweep": "10.0.0.0/8"}))


class _FakeSock:
    """Сырой сокет: отдаёт заготовленные кадры, запоминает отправленные."""

    def __init__(self, frames: list[bytes]) -> None:
        self.frames, self.sent = list(frames), []
        self._r, self._w = socket.socketpair()
        for _ in frames:
            self._w.send(b"x")

    def fileno(self) -> int:
        return self._r.fileno()

    def send(self, frame: bytes) -> int:
        self.sent.append(frame)
        return len(frame)

    def recv(self, _size: int) -> bytes:
        self._r.recv(1)
        return self.frames.pop(0)


def test_соседи_по_проводу_без_своих_кадров_и_с_отметкой_ответа() -> None:
    own = netinfo.arp_request(MAC, "192.168.0.23", "192.168.88.1")
    sock = _FakeSock([_arp(1, "192.168.88.1"), _arp(2, "192.168.88.1"), own, _arp(1, "0.0.0.0")])
    answer = netinfo._collect([(sock, LINK)], ["192.168.88.1", "192.168.88.2"], netinfo.SWEEP_REPEAT_AT_S + 0.2)
    assert answer["swept"] == 2
    assert answer["seen"] == [{"ip": "192.168.88.1", "mac": "f4:1e:57:a0:49:fa", "iface": "end0", "replied": True}]
    # Две волны опроса по два адреса.
    assert len(sock.sent) == 4
