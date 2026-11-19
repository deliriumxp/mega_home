"""Передача сессии через штатный мастер Home Assistant."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, call

import pytest
from homeassistant.data_entry_flow import UnknownHandler

from mega_home.core.ops_base import OpError
from mega_home.station_auth import STATION, StationAuth


def _station(entries=()):
    credentials = SimpleNamespace(x_token=SimpleNamespace(get_secret=lambda: "passport-token"), display_login="family")

    async def with_passport(use):
        return await use(credentials)

    music = SimpleNamespace(with_passport=with_passport)
    flow = SimpleNamespace(
        async_init=AsyncMock(return_value={"type": "form", "step_id": "user", "flow_id": "flow"}),
        async_configure=AsyncMock(),
        async_abort=Mock(),
    )
    store = SimpleNamespace(
        flow=flow,
        async_entries=Mock(return_value=list(entries)),
        async_loaded_entries=Mock(return_value=[]),
    )
    return StationAuth(SimpleNamespace(config_entries=store), music), store, flow


def test_станция_берёт_токен_дома_через_мастер():
    station, store, flow = _station()
    entry = SimpleNamespace(unique_id="family", data={"x_token": "passport-token"})

    async def configure(_flow, data):
        if data == {"method": "token"}:
            return {"type": "form", "step_id": "token"}
        store.async_entries.return_value = [entry]
        store.async_loaded_entries.return_value = [entry]
        return {"type": "create_entry", "data": entry.data}

    flow.async_configure.side_effect = configure

    async def run():
        assert await station.call(True, {"action": "station.status"}) == {"state": "ready"}
        assert await station.call(True, {"action": "station.connect", "token": "external-token"}) == {"state": "connected"}
        assert await station.call(True, {"action": "station.connect"}) == {"state": "connected"}

    asyncio.run(run())
    flow.async_init.assert_awaited_once_with(STATION, context={"source": "user"})
    assert flow.async_configure.await_args_list == [call("flow", {"method": "token"}), call("flow", {"token": "passport-token"})]
    flow.async_abort.assert_not_called()


def test_гость_и_другая_учётная_запись_не_запускают_мастер():
    station, _, flow = _station([SimpleNamespace(unique_id="existing", data={})])

    async def run():
        with pytest.raises(OpError) as denied:
            await station.call(False, {"action": "station.connect", "canManage": True})
        assert denied.value.status == 403
        assert await station.call(True, {"action": "station.connect"}) == {"state": "other-account"}

    asyncio.run(run())
    flow.async_init.assert_not_awaited()


def test_отсутствующая_станция_и_незавершённый_мастер():
    station, _, flow = _station()

    async def run():
        flow.async_init.side_effect = UnknownHandler
        assert await station.call(True, {"action": "station.connect"}) == {"state": "not-installed"}
        flow.async_init.side_effect = None
        flow.async_configure.side_effect = [
            {"type": "form", "step_id": "token"},
            {"type": "form", "step_id": "token", "errors": {"base": "unauthorised"}},
        ]
        assert await station.call(True, {"action": "station.connect"}) == {"state": "reconnect-music"}

    asyncio.run(run())
    flow.async_abort.assert_called_once_with("flow")
