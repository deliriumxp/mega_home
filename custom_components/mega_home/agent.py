"""Сторож объекта: правила менеджера, которые дом крутит САМ.

Зачем это есть. Проба (`probe.py`) убрала из мониторинга WG-туннель, но
инициатива осталась у менеджера: дом идёт к устройству только тогда, когда его
попросили. Значит объект, потерявший связь с менеджером, не сторожит себя вовсе
— а это ровно тот случай, когда чинить его некому: WAN объекта, офисный роутер и
наш сервер отваливаются вместе с ним чаще, чем поодиночке.

⚠ И ровно поэтому здесь НЕТ ни одной проверки: ни «здоров ли директор Control4»,
ни «что означает этот ответ». Правило — это данные от менеджера: «периодически
выполняй вот эти задания, проверь вот эти условия, и если они нарушены дольше N
— выполни вот эти задания, не чаще T и не больше K раз в сутки». Что означают
ответы, знает менеджер: перезагрузка зависшего директора собрана у него ОДНОЙ
функцией (`c4-health/director-watchdog.util.ts`), и следующий сторож — ещё одна
такая функция и НИ СТРОКИ здесь. Это и есть цена вопроса: каждая строка Python,
без которой функция не работает, стоит релиза HACS и перезапуска Home Assistant
на КАЖДОМ объекте (docs/plan-thin-integration.md).

⚠ Правила кэшируются на диске рядом с конфигом, и это не оптимизация, а весь
смысл: дом обязан сторожить объект, когда менеджера нет неделю.

⚠ Правила НЕ едут в теле конфига. Тело конфига дом отдаёт браузеру жильца как
есть, а локальный контур пока без аутентификации; в правиле же стоят адрес
контроллера и команда `reboot` — в конфиге это была бы кнопка «выключить дом»
для любого, кто в Wi-Fi объекта. Поэтому у правил своя дверь с токеном объекта
(`api.async_agent`), и приложению они не показываются никогда.
"""

from __future__ import annotations

import time
from datetime import timedelta
from typing import Any

from homeassistant.core import HomeAssistant
from homeassistant.helpers.event import async_track_time_interval
from homeassistant.helpers.storage import Store

from .api import ManagerClient, ManagerError
from .const import LOGGER

# Тик сторожа. Само правило говорит, как часто ему выполняться (`everySec`);
# тик только будит проверку «кому пора». Мельче незачем: минута — самый частый
# период правила, а батарейка тут ни при чём — это сервер, а не телефон.
TICK = timedelta(seconds=30)

STORE_VERSION = 1
STORE_KEY = "mega_home_agent"

# Потолок очереди отчётов. Дом может быть офлайн неделями, и весь этот журнал
# всё равно никому не нужен: важны последние события, а не каждый тик.
MAX_PENDING_REPORTS = 100
# Сколько заданий готовы выполнить по одному правилу. Тот же потолок, что у
# пробы: больше — это уже не проверка, а обход сети.
MAX_PROBES = 8


class AgentRunner:
    """Исполнитель правил. Ничего не знает о том, ЧТО он сторожит."""

    def __init__(self, hass: HomeAssistant, client: ManagerClient) -> None:
        self._hass = hass
        self._client = client
        self._store = Store[dict[str, Any]](hass, STORE_VERSION, STORE_KEY)
        self._version: str | None = None
        self._rules: list[dict[str, Any]] = []
        # Состояние по правилу: когда началась непрерывная серия нарушений,
        # когда последний раз срабатывало действие и сколько раз за сутки.
        self._state: dict[str, dict[str, Any]] = {}
        self._pending: list[dict[str, Any]] = []
        self._unsub: Any = None
        self._busy = False

    # --- жизненный цикл ---

    async def async_start(self) -> None:
        """Поднять сторожа из кэша и запустить тик.

        ⚠ Кэш читается ДО первого визита к менеджеру: объект без интернета
        обязан продолжать сторожить себя сразу после перезапуска Home Assistant,
        а не с того момента, когда менеджер снова станет доступен.
        """
        cached = await self._store.async_load() or {}
        self._version = cached.get("version")
        self._rules = [r for r in cached.get("rules", []) if _valid(r)]
        self._state = cached.get("state", {})
        self._pending = cached.get("pending", [])
        if self._rules:
            LOGGER.info("Сторож объекта поднят из кэша: правил %d", len(self._rules))
        self._unsub = async_track_time_interval(self._hass, self._async_tick, TICK)

    async def async_stop(self) -> None:
        if self._unsub:
            self._unsub()
            self._unsub = None
        await self._async_save()

    # --- обмен с менеджером ---

    async def async_sync(self) -> None:
        """Отдать отчёты и забрать правила. Недоступный менеджер — не авария.

        ⚠ Отчёты уходят ПЕРВЫМ делом и вычёркиваются только после успеха:
        «контроллер перезагрузился ночью» обязано доехать до журнала объекта
        даже с недельным опозданием, иначе жилец узнает об этом раньше нас.
        """
        pending = list(self._pending)
        try:
            answer = await self._client.async_agent(self._version, pending)
        except ManagerError as err:
            LOGGER.debug("Правила сторожа не обновлены (менеджер недоступен): %s", err)
            return
        # Отчёты приняты — очередь чистим ровно на ту длину, что отправляли:
        # за время запроса сторож мог дописать в неё новое событие.
        self._pending = self._pending[len(pending) :]
        if not answer.get("unchanged"):
            rules = answer.get("rules")
            if isinstance(rules, list):
                self._rules = [r for r in rules if _valid(r)]
                self._version = answer.get("version")
                # Состояние правил, которых больше нет, уносим вместе с ними:
                # иначе снятая и через месяц включённая галка сработала бы по
                # старой серии нарушений мгновенно.
                alive = {r["id"] for r in self._rules}
                self._state = {k: v for k, v in self._state.items() if k in alive}
                LOGGER.info("Правила сторожа обновлены: %d", len(self._rules))
        await self._async_save()

    # --- собственно сторож ---

    async def _async_tick(self, _now: Any = None) -> None:
        # Тики не наслаиваются: правило с восемью заданиями по 20 с живёт
        # дольше тика, и второй проход читал бы то же состояние.
        if self._busy or not self._rules:
            return
        self._busy = True
        try:
            for rule in self._rules:
                try:
                    await self._async_run_rule(rule)
                except Exception as err:  # noqa: BLE001
                    # Одно испорченное правило не должно останавливать
                    # остальные: сторож нужен целиком, а не до первой ошибки.
                    LOGGER.warning("Правило «%s» не выполнено: %s", rule.get("id"), err)
                    self._report(rule.get("id", "?"), "error", str(err)[:200])
            await self._async_save()
        finally:
            self._busy = False

    async def _async_run_rule(self, rule: dict[str, Any]) -> None:
        from .probe import run as run_probe

        now = time.time()
        rule_id = rule["id"]
        state = self._state.setdefault(rule_id, {})
        if now - float(state.get("ranAt", 0)) < float(rule["everySec"]):
            return
        state["ranAt"] = now

        answer = await run_probe(self._hass, {"probes": rule["probes"][:MAX_PROBES]})
        results = answer.get("results", [])
        healthy = all(_check(results, c) for c in rule["healthy"])

        if healthy:
            if state.get("failingSince"):
                if state.get("reported"):
                    self._report(rule_id, "recovered")
                state.pop("failingSince", None)
                state.pop("reported", None)
            return

        since = state.get("failingSince") or now
        state["failingSince"] = since
        if now - since < float(rule["failForSec"]):
            return
        if not state.get("reported"):
            state["reported"] = True
            self._report(rule_id, "fail", rule.get("note"))
        await self._async_act(rule, state, now)

    async def _async_act(self, rule: dict[str, Any], state: dict[str, Any], now: float) -> None:
        from .probe import run as run_probe

        action = rule["action"]
        # Пауза: устройство после действия минуты недоступно, и без неё сторож
        # бил бы по нему на каждом тике — то есть не давал бы подняться.
        if now - float(state.get("actedAt", 0)) < float(action["cooldownSec"]):
            return
        # Потолок в сутки. Сутки скользящие, от первого срабатывания: календарные
        # дали бы в полночь сразу новую пачку перезагрузок.
        day_started = float(state.get("dayStartedAt", 0))
        if now - day_started >= 86400:
            state["dayStartedAt"] = now
            state["actedToday"] = 0
            state.pop("blockedReported", None)
        if int(state.get("actedToday", 0)) >= int(action["maxPerDay"]):
            # Упёрлись в потолок — это сигнал инженеру «сюда надо ехать», и он
            # обязан прозвучать РОВНО ОДИН раз за сутки, а не каждый тик.
            if not state.get("blockedReported"):
                state["blockedReported"] = True
                self._report(
                    rule["id"],
                    "blocked",
                    f"потолок {action['maxPerDay']} за сутки исчерпан, действие не выполняется",
                )
            return

        state["actedAt"] = now
        state["actedToday"] = int(state.get("actedToday", 0)) + 1
        answer = await run_probe(self._hass, {"probes": action["probes"][:MAX_PROBES]})
        # ⚠ Отказ задания действия НЕ считаем провалом: контроллер, которому
        # послали `reboot`, уходит в перезагрузку прямо в этой сессии и ответить
        # уже не может. В отчёт кладём то, что видели, и решает человек.
        note = rule.get("note") or ""
        detail = "; ".join(
            str(r.get("error") or r.get("status") or "ok") for r in answer.get("results", [])
        )
        self._report(rule["id"], "acted", f"{note} → {detail}"[:400])
        LOGGER.warning("Сторож объекта выполнил действие правила «%s»: %s", rule["id"], detail)

    # --- отчёты ---

    def _report(self, rule_id: str, event: str, note: str | None = None) -> None:
        self._pending.append(
            {"ruleId": rule_id, "at": int(time.time() * 1000), "event": event, "note": note}
        )
        # Режем СТАРЫЕ: последние события важнее первых, а очередь после месяца
        # офлайна не должна расти вечно.
        if len(self._pending) > MAX_PENDING_REPORTS:
            self._pending = self._pending[-MAX_PENDING_REPORTS:]

    async def _async_save(self) -> None:
        await self._store.async_save(
            {
                "version": self._version,
                "rules": self._rules,
                "state": self._state,
                "pending": self._pending,
            }
        )

    # --- диагностика ---

    @property
    def summary(self) -> dict[str, Any]:
        """Что показывать в диагностике интеграции."""
        return {
            "version": self._version,
            "rules": [r["id"] for r in self._rules],
            "pending_reports": len(self._pending),
            "state": self._state,
        }


def _valid(rule: Any) -> bool:
    """Правило пригодно к исполнению.

    ⚠ Проверяем ФОРМУ, а не смысл: смысл — дело менеджера. Но битое правило
    (нет `action`, `everySec` строкой) уронило бы тик до остальных правил, и
    сторож молча перестал бы существовать целиком.
    """
    try:
        return (
            isinstance(rule, dict)
            and isinstance(rule.get("id"), str)
            and rule["id"]
            and isinstance(rule.get("probes"), list)
            and rule["probes"]
            and isinstance(rule.get("healthy"), list)
            and rule["healthy"]
            and float(rule["everySec"]) > 0
            and float(rule["failForSec"]) >= 0
            and isinstance(rule.get("action"), dict)
            and isinstance(rule["action"].get("probes"), list)
            and rule["action"]["probes"]
            and float(rule["action"]["cooldownSec"]) >= 0
            and int(rule["action"]["maxPerDay"]) > 0
        )
    except (TypeError, ValueError, KeyError):
        return False


def _check(results: list[dict[str, Any]], check: dict[str, Any]) -> bool:
    """Одно условие «ответ такой, как ожидали».

    ⚠ Язык беден намеренно (`agent.types.ts`): `ok`, код ответа, поле JSON,
    подстрока. Богаче — значит толкование ответов переезжает сюда, а ему сюда
    нельзя. Условие, которое здесь не выражается, — это разговор про поле
    контракта, а не про код.
    """
    index = check.get("probe", 0)
    if not isinstance(index, int) or index < 0 or index >= len(results):
        return False
    result = results[index]
    if check.get("ok") is not None and bool(result.get("ok")) is not bool(check["ok"]):
        return False
    if check.get("status") is not None and result.get("status") != check["status"]:
        return False
    value: Any = result.get("body")
    if check.get("json"):
        value = _dig(result.get("body"), str(check["json"]))
        if value is None:
            return False
    if check.get("equals") is not None and str(value) != str(check["equals"]):
        return False
    if check.get("contains") is not None and str(check["contains"]) not in str(value or ""):
        return False
    return True


def _dig(body: Any, path: str) -> Any:
    """Достать поле тела-JSON по пути через точку. Не JSON или нет поля — None."""
    import json

    if not isinstance(body, str):
        return None
    try:
        node: Any = json.loads(body)
    except ValueError:
        return None
    for part in path.split("."):
        if not isinstance(node, dict) or part not in node:
            return None
        node = node[part]
    return node
