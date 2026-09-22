"""Клиент MQTT 3.1.1 (`core/mqtt.py`) против поддельного брокера по сокету.

⚠ До 0.5.6 у клиента не было ни одной спеки, хотя на нём держится подписка
слушателей (`listeners_out.mqtt`) — звонок в дверь по MQTT без жильца у экрана.
Брокер здесь настоящий по проводу (TCP на петле) и отвечает байтами стандарта
OASIS «MQTT Version 3.1.1», разделы 2–3: проверяется то, что уходит в сокет.
"""

from __future__ import annotations

import asyncio
import struct

import pytest

from mega_home.core import mqtt


async def _broker(on_client):  # noqa: ANN001, ANN202
    server = await asyncio.start_server(on_client, "127.0.0.1", 0)
    return server, server.sockets[0].getsockname()[1]


def _publish(topic: str, data: bytes, packet_id: int) -> bytes:
    body = mqtt._string(topic) + struct.pack(">H", packet_id) + data
    return mqtt.packet(mqtt.PUBLISH | 0x02, body)  # QoS 1


def test_подписка_принимает_сообщение_и_подтверждает_qos1() -> None:
    async def scenario() -> tuple[list[tuple[str, bytes]], bytes, bytes]:
        got: list[tuple[str, bytes]] = []
        seen: dict[str, bytes] = {}

        async def client(reader, writer):  # noqa: ANN001
            head, body = await mqtt.read_packet(reader)
            seen["connect"] = body
            writer.write(mqtt.packet(mqtt.CONNACK, b"\x00\x00"))
            head, body = await mqtt.read_packet(reader)
            (packet_id,) = struct.unpack_from(">H", body)
            writer.write(mqtt.packet(mqtt.SUBACK, struct.pack(">H", packet_id) + b"\x01"))
            writer.write(_publish("door/ring", b"1", 7))
            await writer.drain()
            head, body = await mqtt.read_packet(reader)
            seen["puback"] = bytes([head]) + body

        server, port = await _broker(client)
        async with server:
            client_ = mqtt.MqttClient(
                "127.0.0.1", port, "user", "pass", on_message=lambda t, d: got.append((t, d))
            )
            await client_.connect()
            await client_.subscribe(["door/#"])
            for _ in range(50):
                if got and "puback" in seen:
                    break
                await asyncio.sleep(0.01)
            await client_.close()
        return got, seen["connect"], seen["puback"]

    got, connect_body, puback = asyncio.run(scenario())
    assert got == [("door/ring", b"1")]
    # Учётка ушла флагами CONNECT (3.1.2.8–9), а сессия чистая (3.1.2.4).
    assert connect_body[7] == 0x80 | 0x40 | 0x02
    # QoS 1 обязан получить PUBACK с тем же номером пакета (3.3.4).
    assert puback == bytes([mqtt.PUBACK]) + struct.pack(">H", 7)


def test_отказ_брокера_словами_и_без_висящего_сокета() -> None:
    async def scenario() -> str:
        async def client(reader, writer):  # noqa: ANN001
            await mqtt.read_packet(reader)
            writer.write(mqtt.packet(mqtt.CONNACK, b"\x00\x04"))
            await writer.drain()

        server, port = await _broker(client)
        async with server:
            client_ = mqtt.MqttClient("127.0.0.1", port, "user", "bad")
            with pytest.raises(mqtt.MqttError) as caught:
                await client_.connect()
            assert client_.closed.is_set()
            return str(caught.value)

    assert asyncio.run(scenario()) == "неверный логин или пароль брокера"


def test_подписка_без_подтверждения_не_оседает_в_ожиданиях(monkeypatch) -> None:
    """Долг `docs/home-gateway.md`: не дождавшееся брокера ожидание жило в
    `_acks` до закрытия подключения."""

    async def scenario() -> dict:
        async def client(reader, writer):  # noqa: ANN001
            await mqtt.read_packet(reader)
            writer.write(mqtt.packet(mqtt.CONNACK, b"\x00\x00"))
            await writer.drain()
            await asyncio.sleep(1)  # на SUBSCRIBE молчит

        real_wait_for = asyncio.wait_for

        async def fast_wait_for(awaitable, timeout):  # noqa: ANN001, ANN202
            return await real_wait_for(awaitable, min(timeout, 0.05))

        server, port = await _broker(client)
        async with server:
            client_ = mqtt.MqttClient("127.0.0.1", port)
            await client_.connect()
            monkeypatch.setattr(mqtt.asyncio, "wait_for", fast_wait_for)
            with pytest.raises(mqtt.MqttError):
                await client_.subscribe(["a"])
            acks = dict(client_._acks)  # noqa: SLF001
            await client_.close()
        return acks

    assert asyncio.run(scenario()) == {}
