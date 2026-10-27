"""Вид `connect` `mqtt` (`core/mqtt.exchange`) против поддельного брокера по сокету.

Брокер отвечает байтами стандарта OASIS «MQTT Version 3.1.1»: проверяется порядок
пакетов в сокете (подписка подтверждена ДО публикации) и условия конца сбора.
"""

from __future__ import annotations

import asyncio
import struct

import pytest

from mega_home.core import connect, mqtt
from mega_home.core.ops_base import OpError


def _publish(topic: str, data: bytes, retain: bool = False) -> bytes:
    return mqtt.publish_packet(topic, data, 0, retain, 0)


async def _run(broker, payload: dict, monkeypatch) -> dict:  # noqa: ANN001
    server = await asyncio.start_server(broker, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    # Брокер теста на петле: правило адресата (частный IPv4 объекта) проверяет `test_connect_public.py`.
    monkeypatch.setattr(mqtt, "resolve_address", lambda _payload: ("127.0.0.1", port))
    async with server:
        return await connect.perform({"kind": "mqtt", "host": "192.168.1.10", "port": port, **payload})


async def _handshake(reader, writer) -> bytes:  # noqa: ANN001
    _, body = await mqtt.read_packet(reader)
    writer.write(mqtt.packet(mqtt.CONNACK, b"\x00\x00"))
    await writer.drain()
    return body


def test_rpc_подписка_до_публикации_и_конец_по_ответу(monkeypatch) -> None:
    seen: list[int] = []

    async def broker(reader, writer):  # noqa: ANN001
        await _handshake(reader, writer)
        head, body = await mqtt.read_packet(reader)
        seen.append(head & 0xF0)
        (packet_id,) = struct.unpack_from(">H", body)
        writer.write(mqtt.packet(mqtt.SUBACK, struct.pack(">H", packet_id) + b"\x01"))
        await writer.drain()
        head, body = await mqtt.read_packet(reader)
        seen.append(head & 0xF0)
        topic, data, _, _ = mqtt.parse_publish(head & 0x0F, body)
        assert (topic, data) == ("/rpc/v1/wbrules/Editor/List/c1", b'{"id":"1","params":{}}')
        writer.write(_publish("/rpc/v1/wbrules/Editor/List/c1/reply", b'{"id":"1","result":[]}'))
        writer.write(_publish("/rpc/v1/wbrules/Editor/List/c1/reply", b"second"))
        await writer.drain()
        await asyncio.sleep(1)

    answer = asyncio.run(_run(broker, {
        "subscribe": ["/rpc/v1/wbrules/Editor/List/c1/reply"],
        "publish": [{"topic": "/rpc/v1/wbrules/Editor/List/c1", "payload": '{"id":"1","params":{}}'}],
        "collect": {"until": "/rpc/v1/wbrules/Editor/List/+/reply", "deadline": 5},
    }, monkeypatch))
    assert seen == [mqtt.SUBSCRIBE & 0xF0, mqtt.PUBLISH]
    assert answer["messages"] == [
        {"topic": "/rpc/v1/wbrules/Editor/List/c1/reply", "body": '{"id":"1","result":[]}', "retained": False}
    ]


def test_снимок_кончается_тишиной_и_помечает_сохранённые(monkeypatch) -> None:
    async def broker(reader, writer):  # noqa: ANN001
        await _handshake(reader, writer)
        _, body = await mqtt.read_packet(reader)
        (packet_id,) = struct.unpack_from(">H", body)
        writer.write(mqtt.packet(mqtt.SUBACK, struct.pack(">H", packet_id) + b"\x01"))
        writer.write(_publish("/devices/ac/controls/mode", b"2", retain=True))
        writer.write(_publish("/devices/ac/controls/mode/meta", b'{"type":"value"}', retain=True))
        await writer.drain()
        await asyncio.sleep(3)

    answer = asyncio.run(_run(broker, {"subscribe": ["/devices/#"], "collect": {"quiet": 0.2, "deadline": 10}}, monkeypatch))
    assert [m["topic"] for m in answer["messages"]] == ["/devices/ac/controls/mode", "/devices/ac/controls/mode/meta"]
    assert all(m["retained"] for m in answer["messages"])
    assert answer["truncated"] is False


def test_публикация_qos1_с_сохранением_ждёт_puback(monkeypatch) -> None:
    got: dict = {}

    async def broker(reader, writer):  # noqa: ANN001
        await _handshake(reader, writer)
        head, body = await mqtt.read_packet(reader)
        got["flags"] = head & 0x0F
        topic, data, qos, packet_id = mqtt.parse_publish(head & 0x0F, body)
        got["message"] = (topic, data, qos)
        writer.write(mqtt.packet(mqtt.PUBACK, struct.pack(">H", packet_id)))
        await writer.drain()
        await asyncio.sleep(1)

    answer = asyncio.run(_run(broker, {
        "publish": [{"topic": "homeassistant/climate/x/config", "payloadBase64": "e30=", "qos": 1, "retain": True}],
    }, monkeypatch))
    assert got == {"flags": 0x03, "message": ("homeassistant/climate/x/config", b"{}", 1)}
    assert answer["messages"] == []


def test_отказ_брокера_словами(monkeypatch) -> None:
    async def broker(reader, writer):  # noqa: ANN001
        await mqtt.read_packet(reader)
        writer.write(mqtt.packet(mqtt.CONNACK, b"\x00\x05"))
        await writer.drain()

    with pytest.raises(OpError) as caught:
        asyncio.run(_run(broker, {"subscribe": ["a"], "auth": {"user": "u", "pass": "p"}}, monkeypatch))
    assert caught.value.message == "Брокер: брокер не разрешил подключение"
    assert caught.value.status == 502


@pytest.mark.parametrize(
    ("item", "reason"),
    [
        ({"topic": "/devices/+/on", "payload": "1"}, "publish[].topic — тема без подстановочных знаков"),
        ({"topic": "a", "payload": "1", "qos": 2}, "publish[].qos — 0 или 1"),
    ],
)
def test_публикация_проверяется_до_соединения(item, reason) -> None:  # noqa: ANN001
    with pytest.raises(OpError) as caught:
        asyncio.run(connect.perform({"kind": "mqtt", "host": "192.168.1.10", "port": 1883, "publish": [item]}))
    assert caught.value.message == reason


@pytest.mark.parametrize(
    ("pattern", "topic", "hit"),
    [
        ("/devices/+/meta", "/devices/ac/meta", True),
        ("/devices/#", "/devices/ac/controls/mode/meta", True),
        ("/devices/+/meta", "/devices/ac/controls/mode", False),
        ("a/b", "a/b/c", False),
    ],
)
def test_фильтр_тем(pattern: str, topic: str, hit: bool) -> None:
    assert mqtt.matches(pattern, topic) is hit
