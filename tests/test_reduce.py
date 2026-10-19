"""Обработка данных в доме — `reduce` у `connect` (`core/reduce.py`).

Смысл свёрток — контракт: после релиза его меняет только следующий релиз (решения
описания `docs/superpowers/specs/2026-10-19-home-reduce-design.md` менеджера). Сценарии
с историей климата — те же, что в спеках графика бандла (`core/climate-trace.spec.ts`):
дом их не знает, но обязан посчитать так же.
"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path

import pytest
from aiohttp import web

from mega_home.core import connect, services
from mega_home.core import reduce as reduce_mod
from mega_home.core.ops_base import OpError

M = 60_000
STEP = 15 * M
T0 = 1_760_000_400_000  # кратно 15 минутам


def iso(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat()


def run(series: dict, rows: list, start: int = T0, end: int = T0 + 4 * STEP, **extra) -> dict:
    spec = {"time": {"path": "at", "format": "ms"}, "from": start, "to": end, "step": STEP, "series": series, **extra}
    out = reduce_mod.apply(reduce_mod.parse(spec), json.dumps(rows).encode())
    assert out is not None
    return out


def one(fold: str, rows: list, path: str = "v", **extra) -> list:
    return run({"x": {"path": path, "fold": fold, **extra}}, rows)["groups"][""]["x"]


def test_запись_действует_до_следующей_доля_по_времени() -> None:
    # Импульс в полминуты — 1/30 отрезка, а не «был» (тот же случай у `shares` бандла).
    rows = [{"at": T0 - M, "v": "idle"}, {"at": T0 + 5 * M, "v": "heat"}, {"at": T0 + 5 * M + 30_000, "v": "idle"}]
    assert one("share", rows, **{"in": ["heat"]}) == [pytest.approx(1 / 30, abs=1e-6), 0, 0, 0]


def test_до_первой_записи_значения_нет() -> None:
    rows = [{"at": T0 + STEP + 5 * M, "v": 20}]
    assert one("start", rows) == [None, None, 20, 20]
    assert one("mean", rows) == [None, 20, 20, 20]
    assert one("share", rows, notIn=["off"]) == [None, pytest.approx(2 / 3), 1, 1]


def test_значение_до_окна_переносится_в_начало() -> None:
    assert one("end", [{"at": T0 - 10 * STEP, "v": 21.5}]) == [21.5] * 4


def test_start_и_end_включают_запись_ровно_на_границе() -> None:
    rows = [{"at": T0, "v": 1}, {"at": T0 + STEP, "v": 2}]
    assert one("start", rows) == [1, 2, 2, 2]
    assert one("end", rows) == [2, 2, 2, 2]
    # А в счёт запись на границе идёт ОДНОМУ отрезку — тому, что с неё начинается.
    assert one("count", rows) == [1, 1, 0, 0]


def test_неполный_последний_отрезок_считается_по_прошедшему_времени() -> None:
    rows = [{"at": T0 - M, "v": "heat"}]
    out = run({"x": {"path": "v", "fold": "share", "in": ["heat"]}}, rows, end=T0 + STEP + 5 * M)
    assert out["n"] == 2
    assert out["groups"][""]["x"] == [1, 1]


def test_среднее_по_времени_мин_макс_прирост_сумма() -> None:
    rows = [{"at": T0, "v": 10}, {"at": T0 + 10 * M, "v": "40"}, {"at": T0 + STEP, "v": 40}]
    assert one("mean", rows)[0] == pytest.approx(20)
    assert one("min", rows)[0] == 10
    assert one("max", rows)[0] == 40
    assert one("delta", rows) == [30, 0, 0, 0]
    assert one("sum", rows) == [50, 40, 0, 0]


def test_нет_значения_и_null_одно_и_то_же() -> None:
    rows = [{"at": T0, "v": "on"}, {"at": T0 + 5 * M, "v": None}, {"at": T0 + 10 * M}, {"at": T0 + 12 * M, "v": "on"}]
    # null и отсутствующий ключ не совпадают ни с in, ни с notIn.
    assert one("share", rows, notIn=["off"])[0] == pytest.approx(8 / 15)
    assert one("count", rows)[0] == 2
    # A → null → A — не смена.
    assert one("changes", rows)[0] == 0
    assert one("changes", rows + [{"at": T0 + 13 * M, "v": "off"}])[0] == 1


def test_смена_считается_и_от_значения_до_отрезка() -> None:
    rows = [{"at": T0 - M, "v": "a"}, {"at": T0 + M, "v": "b"}, {"at": T0 + STEP + M, "v": "b"}]
    assert one("changes", rows) == [1, 0, 0, 0]


def test_строгое_равенство_json() -> None:
    rows = [{"at": T0, "v": True}]
    assert one("share", rows, **{"in": [1]}) == [0, 0, 0, 0]
    assert one("share", rows, **{"in": [True]}) == [1, 1, 1, 1]


def test_история_ha_массив_массивов_группы_и_iso() -> None:
    """Форма ответа `GET /api/history/period` (dev-api-rest.md): массив на сущность."""
    answer = [
        [
            {"entity_id": "a.one", "state": "heat", "attributes": {"t": 21}, "last_updated": iso(T0 - M)},
            {"entity_id": "a.one", "state": "off", "attributes": {"t": 20}, "last_updated": iso(T0 + 2 * STEP)},
        ],
        [{"entity_id": "a.two", "state": "cool", "attributes": {"t": 25}, "last_updated": iso(T0)}],
    ]
    spec = {
        "rows": "",
        "time": {"path": "last_updated", "format": "iso"},
        "groupBy": "entity_id",
        "from": T0,
        "to": T0 + 4 * STEP,
        "step": STEP,
        "series": {"t": {"path": "attributes.t", "fold": "end"}, "on": {"path": "state", "fold": "share", "notIn": ["off"]}},
    }
    out = reduce_mod.apply(reduce_mod.parse(spec), json.dumps(answer).encode())
    assert out["rows"] == 3 and out["skipped"] == 0
    assert out["groups"]["a.one"] == {"t": [21, 20, 20, 20], "on": [1, 1, 0, 0]}
    assert out["groups"]["a.two"]["t"] == [25] * 4


def test_неразборчивая_метка_пропускается() -> None:
    out = run({"x": {"path": "v", "fold": "count"}}, [{"at": "вчера", "v": 1}, {"v": 2}, {"at": T0, "v": 3}])
    assert out["skipped"] == 2
    assert run(
        {"x": {"path": "v", "fold": "count"}}, [{"at": "2026-10-19T10:00:00", "v": 1}], time={"path": "at", "format": "iso"}
    )["skipped"] == 1


def test_не_json_отдаётся_как_есть() -> None:
    plan = reduce_mod.parse({"time": {"path": "at"}, "from": T0, "to": T0 + STEP, "step": STEP, "series": {"x": {"path": "v", "fold": "end"}}})
    assert reduce_mod.apply(plan, b"<html>") is None


@pytest.mark.parametrize(
    "spec",
    [
        {"fold": "median"},
        {"fold": "share"},
        {"fold": "share", "in": ["a"], "notIn": ["b"]},
        {"step": 0},
        {"to": T0},
        {"to": T0 + (reduce_mod.MAX_BUCKETS + 1) * STEP},
        {"time": {"path": "at", "format": "день"}},
    ],
)
def test_ошибка_описания_отказ_до_вызова(spec: dict) -> None:
    base = {"time": {"path": "at"}, "from": T0, "to": T0 + STEP, "step": STEP}
    series = {"x": {"path": "v", "fold": spec.pop("fold", "end"), **{k: spec.pop(k) for k in ("in", "notIn") if k in spec}}}
    with pytest.raises(OpError):
        reduce_mod.parse({**base, "series": series, **spec})


def test_нет_массива_по_пути_rows_отказ() -> None:
    plan = reduce_mod.parse({"rows": "data", "time": {"path": "at"}, "from": T0, "to": T0 + STEP, "step": STEP, "series": {"x": {"path": "v", "fold": "end"}}})
    with pytest.raises(OpError):
        reduce_mod.apply(plan, b'{"data": 1}')


def test_в_обработке_нет_предметных_слов() -> None:
    """Дом не знает, что сворачивает: смысл ряда — у вызывающего (`docs/plan-thin-integration.md`)."""
    text = Path(reduce_mod.__file__).read_text("utf-8").lower()
    assert [w for w in ("climate", "hvac", "temperature", "thermostat", "heating", "cooling") if w in text] == []


def _serve(body: bytes, status: int = 200):
    async def handler(_request: web.Request) -> web.Response:
        return web.Response(body=body, status=status, content_type="application/json")

    async def start():
        app = web.Application()
        app.router.add_get("/history", handler)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        services.register("source", site._server.sockets[0].getsockname()[1])
        return runner

    return start


REQ = {
    "kind": "http",
    "host": "source",
    "path": "/history",
    "reduce": {"time": {"path": "at", "format": "ms"}, "from": T0, "to": T0 + 2 * STEP, "step": STEP, "series": {"x": {"path": "v", "fold": "end"}}},
}


def _call(body: bytes, req: dict, status: int = 200) -> dict:
    async def scenario():
        runner = await _serve(body, status)()
        try:
            return await connect.perform(req)
        finally:
            services.unregister("source")
            await runner.cleanup()

    return asyncio.run(scenario())


def test_connect_отдаёт_обработанное_вместо_тела() -> None:
    answer = _call(json.dumps([{"at": T0, "v": 7}]).encode(), REQ)
    assert answer["status"] == 200
    assert "body" not in answer
    assert answer["reduced"]["groups"][""]["x"] == [7, 7]


def test_отказ_адресата_уходит_как_есть() -> None:
    answer = _call(b'{"message": "no"}', REQ, status=500)
    assert answer["status"] == 500
    assert json.loads(answer["body"]) == {"message": "no"}


def test_reduce_только_у_http_и_не_у_ресурса() -> None:
    with pytest.raises(OpError):
        asyncio.run(connect.perform({**REQ, "kind": "tcp", "port": 1}))
    with pytest.raises(OpError):
        asyncio.run(connect.resource(REQ))
