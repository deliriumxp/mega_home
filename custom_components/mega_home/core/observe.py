"""Наблюдатель: дом сам ведёт интервалы «жив / не жив» по целям от менеджера.

Зачем (менеджер, `docs/plan-device-presence.md`). «В сети без перерыва» обязан считать тот,
кто стоит в сети объекта и не уходит вместе со связью: менеджер, туннель и WAN объекта
отваливаются чаще, чем устройства в нём. Как агент Domotz: пульс по каждой цели, счёт здесь,
менеджер его только читает.

⚠ Здесь НЕТ знания, что за целью стоит: задание — данные (`id`, запрос `connect`, период,
выдержка), толкует менеджер. Тот же договор, что у сторожа (`agent.py`).

⚠ Счёт ПЕРЕЖИВАЕТ перезапуск дома и менеджера (требование заказчика 2026-10-12): интервалы
лежат в хранилище, а простой дома — это провал наблюдения (`gapMs`), а не обрыв устройства.

⚠ Всё хранимое ограничено (требование того же дня): целей `MAX_TARGETS`, журнал смен
`MAX_LOG` записей и `LOG_TTL_S`, запись на диск — отложенная. Менеджер недоступен неделями —
журнал вытесняет старое, а не растёт.
"""

from __future__ import annotations

import ipaddress
import time
from datetime import timedelta
from typing import Any

from . import connect as connect_mod
from . import presence
from .const import LOGGER
from .host import Host, PeriodicSave
from .ops_base import OpError

STORE_KEY = "mega_home_observe"
STORE_VERSION = 1
TICK = timedelta(seconds=10)
MAX_TARGETS = 512
MAX_LOG = 2000
LOG_TTL_S = 7 * 86400
# Сколько смен отдаём за один опрос: остальное — следующим, по курсору.
MAX_LOG_PER_READ = 500
# Счёт живёт в памяти, на диск — раз в период и на остановке (`host.PeriodicSave`). Сбой
# питания теряет не больше периода: это провал наблюдения, а не обрыв (`gapMs`).
MIN_EVERY_S = 10
# Опоздание проверки больше стольких периодов — дом не смотрел (лежал, перезапускался).
GAP_PERIODS = 3


class Observer:
    """Исполнитель задания наблюдения. Ничего не знает о том, ЧТО он наблюдает."""

    def __init__(self, env: Host) -> None:
        self._env = env
        self._store = env.store(STORE_KEY, STORE_VERSION)
        self._version: str | None = None
        self._targets: list[dict[str, Any]] = []
        self._spans: dict[str, dict[str, Any]] = {}
        self._log: list[dict[str, Any]] = []
        self._seq = 0
        self._busy = False
        self._saver = PeriodicSave(self._store, self._snapshot)
        self._unsub: Any = None

    async def async_start(self) -> None:
        """Поднять задание и счёт из хранилища ДО связи с менеджером: объект без
        интернета наблюдает себя сразу после перезапуска HA."""
        cached = await self._store.async_load() or {}
        self._version = cached.get("version")
        self._targets = [t for t in cached.get("targets", []) if _valid(t)][:MAX_TARGETS]
        self._spans = cached.get("spans", {})
        self._log = cached.get("log", [])
        self._seq = int(cached.get("seq", 0))
        self._unsub = self._env.every(TICK, self._async_tick)

    async def async_stop(self) -> None:
        """Выгрузка записи (обновление, перезагрузка интеграции): несохранённое — сейчас.
        Остановку самого HA дописывает HA `Store` (`host.PeriodicSave`)."""
        if self._unsub:
            self._unsub()
            self._unsub = None
        await self._saver.flush()

    def read(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Операция канала `observe`: принять задание и подтверждение, отдать счёт.

        Задание едет, только когда дом сказал `needTargets` (версия не та): полсотни целей
        в каждом минутном опросе — это байты ради ничего.
        """
        targets = payload.get("targets")
        if isinstance(targets, list):
            self._set(str(payload.get("version") or ""), targets)
        ack = payload.get("ack")
        if isinstance(ack, int):
            self._log = [entry for entry in self._log if entry["seq"] > ack]
        self._saver.touch()
        return {
            "version": self._version,
            "needTargets": payload.get("version") != self._version,
            "now": _ms(time.time()),
            "spans": self._spans,
            "log": self._log[:MAX_LOG_PER_READ],
        }

    def _set(self, version: str, targets: list[Any]) -> None:
        # ⚠ Больше предела — отказ ЦЕЛИКОМ, а не обрезка: молча наблюдать половину объекта
        # хуже, чем громко не наблюдать ничего.
        if len(targets) > MAX_TARGETS:
            raise OpError(f"Целей наблюдения больше {MAX_TARGETS}")
        self._targets = [t for t in targets if _valid(t)]
        self._version = version
        # Счёт снятых целей уносим с ними; у оставшихся он продолжается.
        alive = {t["id"] for t in self._targets}
        self._spans = {k: v for k, v in self._spans.items() if k in alive}
        LOGGER.info("Задание наблюдения обновлено: целей %d", len(self._targets))

    async def _async_tick(self, _now: Any = None) -> None:
        if self._busy or not self._targets:
            return
        self._busy = True
        try:
            now = time.time()
            due = [t for t in self._targets if now - self._spans.get(t["id"], {}).get("checkAt", 0) / 1000 >= t["everySec"]]
            if due:
                for target, ok, answer in await self._async_check(due):
                    self._apply(target, ok, answer, _ms(time.time()))
                self._trim(now)
                self._saver.touch()
        except Exception as err:  # noqa: BLE001
            # Тик не должен умирать: наблюдение нужно целиком. Debug, а не warning: ошибка
            # повторилась бы каждые 10 с неделями — это и есть «забить журнал».
            LOGGER.debug("Тик наблюдения не прошёл: %s", err)
        finally:
            self._busy = False

    async def _async_check(self, due: list[dict[str, Any]]) -> list[tuple[dict[str, Any], bool, dict[str, Any]]]:
        """Пинги — ОДНОЙ пачкой (сотня целей — один сокет), прочее — по одному `connect`."""
        pings = [t for t in due if t["request"].get("kind") == "ping"]
        found = await presence.check(self._env, [t["request"]["host"] for t in pings]) if pings else {}
        results = [(t, bool(found.get(t["request"]["host"], {}).get("ok")), found.get(t["request"]["host"], {})) for t in pings]
        for target in due:
            if target["request"].get("kind") == "ping":
                continue
            try:
                answer = await connect_mod.perform(target["request"])
                results.append((target, True, {"method": target["request"].get("kind")}))
            except Exception as err:  # noqa: BLE001
                results.append((target, False, {"method": target["request"].get("kind"), "reason": str(err)[:200]}))
        return results

    def _apply(self, target: dict[str, Any], ok: bool, answer: dict[str, Any], now: int) -> None:
        """Один пульс цели → интервал.

        ⚠ «Не в сети» — с ПОСЛЕДНЕГО подтверждения, а не с момента, когда истекла выдержка:
        настоящий обрыв лежит между ними, и ранняя граница — единственная, которая не врёт
        «в сети» за минуты, когда цели уже не было (`plan-monitoring-observations.md`).
        """
        span = self._spans.setdefault(target["id"], {"observedSince": now, "gapMs": 0})
        every = target["everySec"] * 1000
        last = span.get("checkAt")
        if last and now - last > every * GAP_PERIODS:
            span["gapMs"] = span.get("gapMs", 0) + now - last - every
        span["checkAt"] = now
        span["method"] = answer.get("method")
        if answer.get("mac"):
            span["mac"] = answer["mac"]
        if ok:
            span["okAt"] = now
            if span.get("state") != "up":
                self._change(target["id"], span, "up", now)
            return
        quiet_from = span.get("okAt") or span["observedSince"]
        if span.get("state") != "down" and now - quiet_from >= target["downAfterSec"] * 1000:
            self._change(target["id"], span, "down", quiet_from)

    def _change(self, target_id: str, span: dict[str, Any], state: str, since: int) -> None:
        span.update(state=state, since=since, gapMs=0)
        self._seq += 1
        self._log.append({"seq": self._seq, "id": target_id, "state": state, "since": since})

    def _trim(self, now: float) -> None:
        """Журнал смен — не больше `MAX_LOG` и не старше `LOG_TTL_S`, старое вытесняется."""
        cutoff = _ms(now - LOG_TTL_S)
        self._log = [entry for entry in self._log if entry["since"] >= cutoff][-MAX_LOG:]

    def _snapshot(self) -> dict[str, Any]:
        return {
            "version": self._version, "targets": self._targets, "spans": self._spans,
            "log": self._log, "seq": self._seq,
        }

    @property
    def summary(self) -> dict[str, Any]:
        """Для диагностики интеграции."""
        return {"version": self._version, "targets": len(self._targets), "log": len(self._log)}


def _valid(target: Any) -> bool:
    """Цель пригодна: форма, а не смысл. Битая цель уронила бы тик целиком."""
    try:
        request = target["request"]
        if not isinstance(request, dict):
            return False
        if request.get("kind") == "ping":
            address = ipaddress.ip_address(str(request.get("host")))
            if address.version != 4 or not address.is_private:
                return False
        every, down = target["everySec"], target["downAfterSec"]
        return (
            isinstance(target["id"], str) and 0 < len(target["id"]) <= 64
            and isinstance(request, dict)
            and all(isinstance(v, (int, float)) and not isinstance(v, bool) for v in (every, down))
            and MIN_EVERY_S <= every <= down
        )
    except (TypeError, ValueError, KeyError, AttributeError):
        return False


def _ms(seconds: float) -> int:
    return int(seconds * 1000)
