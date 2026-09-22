"""Клиент менеджера (`core/api.py`): любой отказ дороги — `ManagerError`.

⚠ До 0.5.6 таймаут (`TimeoutError` у aiohttp, а не `ClientError`) проходил мимо:
опрос конфига падал «неожиданной ошибкой» без отступа, синхронизация бандла
бросала, сторож ронял весь цикл. Сервер здесь настоящий, по петле.
"""

from __future__ import annotations

import asyncio

import aiohttp
import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer

from mega_home.core.api import ManagerAuthError, ManagerClient, ManagerError


def _manager() -> web.Application:
    async def slow(_request: web.Request) -> web.Response:
        await asyncio.sleep(1)
        return web.json_response({})

    async def denied(_request: web.Request) -> web.Response:
        return web.Response(status=401)

    async def relay(_request: web.Request) -> web.Response:
        return web.json_response({"answer": "нет"}, status=404)

    async def proxy_page(_request: web.Request) -> web.Response:
        return web.Response(text="<html>captive portal</html>", content_type="text/html")

    app = web.Application()
    app.router.add_get("/slow", slow)
    app.router.add_get("/denied", denied)
    app.router.add_post("/inbound/home-config/relay", relay)
    app.router.add_get("/portal", proxy_page)
    return app


def _run(scenario):  # noqa: ANN001, ANN202
    async def go():  # noqa: ANN202
        async with TestServer(_manager()) as server, aiohttp.ClientSession() as session:
            return await scenario(ManagerClient(session, str(server.make_url("/")), "t"))

    return asyncio.run(go())


def test_таймаут_это_manager_error() -> None:
    async def scenario(client: ManagerClient) -> None:
        with pytest.raises(ManagerError):
            await client._request("GET", "/slow", timeout=0.1)  # noqa: SLF001

    _run(scenario)


def test_отказ_токена_отдельным_классом() -> None:
    async def scenario(client: ManagerClient) -> None:
        with pytest.raises(ManagerAuthError):
            await client._get_json("/denied")  # noqa: SLF001

    _run(scenario)


def test_не_json_от_прокси_это_manager_error() -> None:
    async def scenario(client: ManagerClient) -> None:
        with pytest.raises(ManagerError):
            await client._get_json("/portal")  # noqa: SLF001

    _run(scenario)


def test_перенос_отдаёт_статус_менеджера_как_есть() -> None:
    async def scenario(client: ManagerClient) -> tuple[int, object]:
        return await client.async_relay({"q": 1})

    assert _run(scenario) == (404, {"answer": "нет"})
