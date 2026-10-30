"""Оси `connect`, закрытые заранее (ревью заказчика 2026-10-30): чтобы первая встреча не стоила релиза.

- личность: доверие, отпечаток, клиентский сертификат — полями описания (`connect_tls.py`);
- вид транспорта: мультикаст у `udp` и сбор нескольких ответов (обнаружение устройств);
- конец ответа TCP выражением (`readUntil.pattern`) — то, чем проба менеджера ждала `sysmand`.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import ssl

import pytest

from mega_home.core import connect, connect_sockets, connect_tls
from mega_home.core.ops_base import OpError


@pytest.fixture
def loopback_is_lan(monkeypatch):
    monkeypatch.setattr(connect_sockets, "resolve_address", lambda payload: ("127.0.0.1", int(payload["port"])))


def test_опции_tls_по_форме() -> None:
    pin = hashlib.sha256(b"cert").hexdigest()
    opts = connect_tls.options({"tlsInsecure": True, "tlsFingerprint": ":".join(pin[i : i + 2] for i in range(0, 64, 2)).upper()})
    assert opts == {"insecure": True, "pin": pin}
    with pytest.raises(OpError):
        connect_tls.options({"tlsFingerprint": "abc"})
    with pytest.raises(OpError):
        connect_tls.options({"tlsClient": {"cert": "не PEM", "key": "-----BEGIN KEY"}})
    # Описание без опций — прежнее поведение: устройство LAN без проверки, имя — с проверкой.
    assert connect_tls.trusts_any(False, {}) and not connect_tls.trusts_any(True, {})
    assert connect_tls.trusts_any(True, {"pin": pin})


def test_отпечаток_сверяется_до_запроса() -> None:
    class Peer:
        def __init__(self, der: bytes) -> None:
            self.der = der

        def getpeercert(self, binary_form: bool = False) -> bytes:
            return self.der

    pin = {"pin": hashlib.sha256(b"right").hexdigest()}
    connect_tls.check_pin(Peer(b"right"), pin)
    with pytest.raises(OpError):
        connect_tls.check_pin(Peer(b"wrong"), pin)


def test_http_с_отпечатком_и_клиентом_разом_отказ_словами() -> None:
    opts = {"pin": "0" * 64, "client": ("-----BEGIN C", "-----BEGIN K")}
    with pytest.raises(OpError, match="только у tcp и mqtt"):
        asyncio.run(connect_tls.aiohttp_ssl(True, opts))
    fingerprint = asyncio.run(connect_tls.aiohttp_ssl(True, {"pin": "0" * 64}))
    assert fingerprint.__class__.__name__ == "Fingerprint"
    assert asyncio.run(connect_tls.aiohttp_ssl(False, {})) is False
    assert asyncio.run(connect_tls.aiohttp_ssl(True, {"insecure": True})) is False


def test_разбор_http_несёт_опции_tls() -> None:
    call = connect.prepare_http({"kind": "http", "host": "192.168.1.5", "port": 443, "tls": True, "tlsInsecure": True})
    assert call["tlsPayload"] == {"tlsInsecure": True}
    with pytest.raises(OpError):
        connect.prepare_http({"kind": "http", "host": "192.168.1.5", "port": 443, "tlsFingerprint": "zz"})


def test_tcp_конец_ответа_выражением(loopback_is_lan) -> None:
    async def handler(reader, writer):  # noqa: ANN001
        await reader.read(100)
        writer.write(b"director enabled\r\nOK\r\n")
        await writer.drain()
        await asyncio.sleep(5)  # соединение не закрываем — как sysmand

    async def scenario():
        server = await asyncio.start_server(handler, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        try:
            return await connect.perform({
                "kind": "tcp", "host": "127.0.0.1", "port": port, "sendBase64": base64.b64encode(b"status\n").decode(),
                "readUntil": {"pattern": "(^|\\n)OK\\r?\\n?$", "deadline": 3},
            })
        finally:
            server.close()

    answer = asyncio.run(scenario())
    assert answer["matched"] is True and "director enabled" in answer["body"]
    with pytest.raises(OpError):
        connect_sockets._pattern("x" * 300)  # noqa: SLF001


def test_udp_собирает_несколько_ответов(loopback_is_lan) -> None:
    class Echo(asyncio.DatagramProtocol):
        def connection_made(self, transport):  # noqa: ANN001
            self.transport = transport

        def datagram_received(self, data, addr):  # noqa: ANN001
            for n in range(3):
                self.transport.sendto(data + str(n).encode(), addr)

    async def scenario():
        loop = asyncio.get_running_loop()
        transport, _ = await loop.create_datagram_endpoint(Echo, local_addr=("127.0.0.1", 0))
        port = transport.get_extra_info("sockname")[1]
        try:
            many = await connect.perform({"kind": "udp", "host": "127.0.0.1", "port": port, "sendBase64": base64.b64encode(b"hi").decode(), "readUntil": {"count": 3, "deadline": 2}})
            one = await connect.perform({"kind": "udp", "host": "127.0.0.1", "port": port, "sendBase64": base64.b64encode(b"hi").decode(), "readUntil": {"deadline": 2}})
        finally:
            transport.close()
        return many, one

    many, one = asyncio.run(scenario())
    assert [a["body"] for a in many["answers"]] == ["hi0", "hi1", "hi2"]
    assert many["answers"][0]["from"] == "127.0.0.1"
    assert one == {"body": "hi0"}


def test_мультикаст_только_групп_локальной_сети() -> None:
    assert connect_sockets._multicast_group({"host": "239.255.255.250", "multicast": {}}) == "239.255.255.250"  # noqa: SLF001
    assert connect_sockets._multicast_group({"host": "224.0.0.251", "multicast": {"ttl": 1}}) == "224.0.0.251"  # noqa: SLF001
    assert connect_sockets._multicast_group({"host": "192.168.1.255"}) is None  # noqa: SLF001
    with pytest.raises(OpError):
        connect_sockets._multicast_group({"host": "232.1.1.1", "multicast": {}})  # noqa: SLF001
    with pytest.raises(OpError):
        connect_sockets._multicast_group({"host": "192.168.1.5", "multicast": {}})  # noqa: SLF001


def test_клиентский_сертификат_грузится_в_контекст() -> None:
    """PEM из описания — в контекст (через временные файлы, которые тут же стираются); битый — отказ."""
    x509 = pytest.importorskip("cryptography.x509")
    from datetime import datetime, timedelta, timezone

    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import ec
    from cryptography.x509.oid import NameOID

    key = ec.generate_private_key(ec.SECP256R1())
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "mega-home-client")])
    now = datetime.now(timezone.utc)
    cert = (
        x509.CertificateBuilder().subject_name(name).issuer_name(name).public_key(key.public_key())
        .serial_number(1).not_valid_before(now).not_valid_after(now + timedelta(days=1)).sign(key, hashes.SHA256())
    )
    cert_pem = cert.public_bytes(serialization.Encoding.PEM).decode()
    key_pem = key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()).decode()
    context = asyncio.run(connect_tls.context_for(False, connect_tls.options({"tlsClient": {"cert": cert_pem, "key": key_pem}})))
    assert isinstance(context, ssl.SSLContext) and context.verify_mode == ssl.CERT_NONE
    with pytest.raises(OpError, match="tlsClient не принят"):
        asyncio.run(connect_tls.context_for(False, {"client": (cert_pem, cert_pem)}))
