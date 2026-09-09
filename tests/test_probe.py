"""Проба сети по заданию менеджера (`probe.py`).

⚠ Предмет этих тестов — ГРАНИЦЫ и ЧЕСТНОСТЬ примитивов, а не «работает ли
мониторинг». Смысл проверок живёт в менеджере и меняется его деплоем; сюда он
не приезжает никогда, и первый же тест вида «директор здоров» означал бы, что
толкование снова уехало в Python, за который платят релизом HACS и перезапуском
Home Assistant на каждом объекте.

⚠ TCP-примитив проверяется НАСТОЯЩИМ сокетом на localhost: он существует ради
одного собеседника — `sysmand` контроллера Control4, который не закрывает
соединение никогда. Мок вернул бы то, что мы сами придумали, а вопрос ровно в
том, отпустит ли проба такого собеседника.
"""

from __future__ import annotations

import asyncio
from http import HTTPStatus

import pytest

from mega_home import probe
from mega_home.ops import OpError


def run(coro):
    return asyncio.run(coro)


async def serve(handler, host: str = "127.0.0.1"):
    """Поднять TCP-сервер на свободном порту и вернуть (порт, сервер)."""
    server = await asyncio.start_server(handler, host, 0)
    return server.sockets[0].getsockname()[1], server


def probe_tcp(port: int, **extra):
    return {"kind": "tcp", "host": "127.0.0.1", "port": port, **extra}


def test_empty_task_is_refused():
    # Пустое задание — ошибка ВЫЗЫВАЮЩЕГО, а не «нет результатов»: молчаливый
    # пустой ответ читался бы менеджером как «проб не было», и мониторинг тихо
    # перестал бы существовать.
    with pytest.raises(OpError) as err:
        run(probe.run(None, {"probes": []}))
    assert err.value.status == HTTPStatus.BAD_REQUEST


def test_too_many_probes_refused():
    task = {"probes": [{"kind": "http", "url": "http://x/"}] * (probe.MAX_PROBES + 1)}
    with pytest.raises(OpError):
        run(probe.run(None, task))


def test_unknown_kind_is_a_result_not_a_crash():
    # ⚠ Отказ ОДНОЙ пробы не имеет права ронять пачку: менеджер шлёт задания
    # вместе, и «одно новое, дом старый» не должно стоить остальных.
    answer = run(probe.run(None, {"probes": [{"kind": "smtp"}]}))
    assert answer["results"][0]["ok"] is False
    assert "неизвестный вид" in answer["results"][0]["error"]


def test_tcp_reads_until_marker():
    async def handler(reader, writer):
        await reader.read(100)
        writer.write(b"director enabled\r\nOK\r\n")
        await writer.drain()
        # Соединение НЕ закрываем — ровно как sysmand.
        await asyncio.sleep(30)

    async def scenario():
        port, server = await serve(handler)
        try:
            answer = await probe.run(
                None,
                {
                    "probes": [
                        probe_tcp(
                            port,
                            send="status\n",
                            readUntil="(^|\\n)OK\\r?\\n?$",
                            timeoutMs=3000,
                        )
                    ]
                },
            )
        finally:
            server.close()
        return answer["results"][0]

    result = run(scenario())
    assert result["ok"] is True
    assert "director enabled" in result["body"]


def test_tcp_without_marker_gives_partial_text_and_reason():
    """Собеседник ответил, но признака конца не прислал.

    ⚠ Текст всё равно отдаём: по нему видно, на чём контроллер замолчал, и это
    полезнее пустоты. Но `ok` — false, иначе оборванный вывод пошёл бы в замер
    как полный.
    """

    async def handler(reader, writer):
        await reader.read(100)
        writer.write(b"director enabled\n")
        await writer.drain()
        await asyncio.sleep(30)

    async def scenario():
        port, server = await serve(handler)
        try:
            answer = await probe.run(
                None,
                {"probes": [probe_tcp(port, send="status\n", readUntil="OK$", timeoutMs=300)]},
            )
        finally:
            server.close()
        return answer["results"][0]

    result = run(scenario())
    assert result["ok"] is False
    assert result["body"] == "director enabled\n"
    assert "не завершился признаком" in result["error"]


def test_tcp_refused_says_so_in_russian():
    async def scenario():
        # Порт, который точно никто не слушает: занимаем и сразу отпускаем.
        port, server = await serve(lambda r, w: None)
        server.close()
        await server.wait_closed()
        answer = await probe.run(None, {"probes": [probe_tcp(port, timeoutMs=1000)]})
        return answer["results"][0]

    result = run(scenario())
    assert result["ok"] is False
    assert "отклонено" in result["error"]


def test_tcp_needs_address():
    answer = run(probe.run(None, {"probes": [{"kind": "tcp", "port": 5810}]}))
    assert answer["results"][0]["ok"] is False


def test_http_url_must_be_http():
    # Без этого «проба» открыла бы файл на диске дома (`file://`).
    answer = run(probe.run(None, {"probes": [{"kind": "http", "url": "file:///etc/passwd"}]}))
    assert answer["results"][0]["ok"] is False
    assert "http(s)" in answer["results"][0]["error"]


def test_timeout_is_capped():
    # Задание не может занять дом дольше потолка: зависшая проба держит его
    # соединение и пул, а мониторинг всё равно спросит снова через минуту.
    assert probe._timeout({"timeoutMs": 10 ** 9}) == probe.MAX_TIMEOUT_S
    assert probe._timeout({"timeoutMs": 2500}) == 2.5
    assert probe._timeout({}) == probe.MAX_TIMEOUT_S
