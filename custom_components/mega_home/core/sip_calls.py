"""Вызовы домофонии через ARI своего Asterisk (`sip_bridge.py`).

Зачем ARI, а не диалплан. Панель нельзя отвечать, пока жилец не взял трубку:
ответ гасит мониторы и обрывает «звонок» у гостя. А телефон жильца появляется
ПОЗЖЕ вызова — push, разблокировка, приложение. Диалплан умеет держать вызов
только ожиданием вслепую; ARI отдаёт канал нам (`Stasis`), и дальше мы решаем:
звоним панели `ring`, телефон набирает `answer` — отвечаем обоим и соединяем
мостом `mixing`. Отбой любой стороны гасит другую.

Тот же поток событий — источник «вызов / отмена» для push (этап 3 плана): это
события SIP самой панели (INVITE/CANCEL), а не Akuvox.

Сверено с `docs.asterisk.org` (Channels/Bridges/Events REST API) и моделями
`rest-api/api-docs/*.json` Asterisk 22: `POST /channels/{id}/ring|answer`,
`DELETE /channels/{id}?reason=`, `POST /bridges?type=mixing`,
`POST /bridges/{id}/addChannel?channel=a,b`, события `StasisStart` (`args`,
`channel`) и `StasisEnd` (`channel`).
"""

from __future__ import annotations

import asyncio
import base64
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass, field
import json
from time import time
from typing import Any

import aiohttp

from .const import LOGGER

# Имя приложения Stasis и пользователь ARI — константы ЗДЕСЬ, не в
# `sip_bridge.py`: тот пишет `ari.conf` и импортирует их отсюда, а не
# наоборот, иначе модули замкнулись бы друг на друга импортом.
ARI_APP = "mega_home"
ARI_USER = "mega_home"

# Пауза переподключения к событиям ARI. Asterisk свой и локальный: связь рвётся
# только когда он перезапускается, и тогда он встаёт за секунды.
RECONNECT_S = 2.0
HISTORY = 20
# ⚠ Панель опознаётся только по адресу, а адрес по UDP в той же сети подделать
# легко (ревью 2026-09-19): чужой INVITE «от панели» разослал бы push о звонке
# всем жильцам. Поэтому у панели один вызов разом и не чаще раза в этот срок.
CALL_COOLDOWN_S = 5.0
# Вызовов разом на дом: панелей у квартиры единицы.
MAX_CALLS = 4


@dataclass
class DoorCall:
    """Один вызов панели: пока звонит — `talk` пуст, взяли трубку — id телефона."""

    panel: str
    caller: str
    started: float = field(default_factory=time)
    talk: str = ""
    bridge: str = ""
    # Сетевой адрес отправителя — как пришёл; пусто, если диалплан его не дал.
    peer: str = ""

    @property
    def key(self) -> str:
        """По чему пауза и «один вызов разом»: адрес, а без него — номер."""
        return self.peer or self.caller

    def payload(self) -> dict[str, str]:
        """Данные события вызова — одни на `call`, `answered`, `cancel` и `ended`.

        ⚠ `peer` — ровно тот, что ушёл в `call`: снаружи по нему опознают панель,
        и событие конца обязано называть ту же, что событие начала.
        """
        return {"caller": self.caller, "call": self.panel, "peer": self.peer}


class DoorCalls:
    """Подписка на события ARI и решения по вызовам."""

    def __init__(
        self,
        session: aiohttp.ClientSession,
        port: int,
        password: str,
        on_event: Callable[[str, dict[str, Any]], None] | None = None,
    ) -> None:
        self._session = session
        self._base = f"http://127.0.0.1:{port}/ari"
        # Заголовком, а не `aiohttp.BasicAuth`: тот объявлен устаревшим, а замена
        # есть не во всех aiohttp, с которыми живут поддерживаемые версии HA.
        token = base64.b64encode(f"{ARI_USER}:{password}".encode()).decode()
        self._auth = {"Authorization": f"Basic {token}"}
        self._on_event = on_event
        self._calls: dict[str, DoorCall] = {}
        self._last_call: dict[str, float] = {}
        self._task: asyncio.Task[None] | None = None
        self.connected = False
        self.history: deque[dict[str, Any]] = deque(maxlen=HISTORY)

    # --- жизненный цикл --------------------------------------------------

    def start(self) -> None:
        if self._task is None or self._task.done():
            self._task = asyncio.ensure_future(self._listen())

    async def stop(self) -> None:
        task, self._task = self._task, None
        if task is not None:
            task.cancel()
        self.connected = False
        self._drop_all()

    def _drop_all(self) -> None:
        """Вызовы умерли вместе с Asterisk или мостом — сказать об этом ВСЛУХ.

        ⚠ Молча (`clear()`, как до 0.5.6) нельзя: событие «вызов» уже ушло
        менеджеру и в push, и без «отмены» телефоны жильцов звонили бы по
        вызову, которого нет.
        """
        for call in list(self._calls.values()):
            self._emit("ended" if call.talk else "cancel", call.payload())
        self._calls.clear()

    def state(self) -> dict[str, Any]:
        return {
            "connected": self.connected,
            "calls": [
                {"caller": c.caller, "since": round(c.started), "talking": bool(c.talk)}
                for c in self._calls.values()
            ],
            "history": list(self.history),
        }

    async def _listen(self) -> None:
        url = f"{self._base}/events?app={ARI_APP}"
        while True:
            try:
                async with self._session.ws_connect(url, headers=self._auth, heartbeat=30) as ws:
                    self.connected = True
                    LOGGER.info("SIP-мост: события ARI подключены")
                    async for message in ws:
                        if message.type != aiohttp.WSMsgType.TEXT:
                            continue
                        try:
                            await self.handle(json.loads(message.data))
                        except Exception:  # noqa: BLE001 - одно событие не роняет подписку
                            LOGGER.warning("SIP-мост: событие не разобрано", exc_info=True)
            except asyncio.CancelledError:
                raise
            except (aiohttp.ClientError, OSError) as err:
                LOGGER.debug("SIP-мост: ARI недоступен: %s", err)
            self.connected = False
            # Asterisk перезапустился — вызовы, что он держал, умерли вместе с ним.
            self._drop_all()
            await asyncio.sleep(RECONNECT_S)

    # --- события ----------------------------------------------------------

    async def handle(self, event: dict[str, Any]) -> None:
        kind = event.get("type")
        channel = event.get("channel") or {}
        channel_id = str(channel.get("id") or "")
        if not channel_id:
            return
        if kind == "StasisStart":
            args = event.get("args") or [""]
            role = args[0]
            extra = str(args[1]) if len(args) > 1 and args[1] else ""
            if role == "panel":
                # Второй аргумент — СЕТЕВОЙ адрес отправителя (`CHANNEL(pjsip,
                # remote_addr)`, «ip:порт»): номер звонящего выбирает сам INVITE.
                await self._ringing(channel_id, channel, extra.rsplit(":", 1)[0])
            elif role == "answer":
                # Второй аргумент — id вызова, которому отвечают: заголовок
                # `X-Mega-Call` INVITE телефона (диалплан менеджера,
                # `asterisk-config.util.ts`); пусто — последний звонящий.
                await self._answer(channel_id, extra)
            else:
                await self._hangup(channel_id, "unallocated")
        elif kind == "StasisEnd":
            await self._ended(channel_id)

    async def _ringing(self, channel_id: str, channel: dict[str, Any], peer: str = "") -> None:
        caller = str((channel.get("caller") or {}).get("number") or "")
        # ⚠ Ограничения — по СЕТЕВОМУ адресу, а не по номеру: номер (CALLERID)
        # выбирает отправитель, и, меняя его, подделка проходила бы паузу каждый
        # раз (повторное ревью 2026-09-19). Сверх того — общий потолок на дом.
        call = DoorCall(channel_id, caller, peer=peer)
        now = time()
        # Отметки старше паузы ничего не решают — не копим их по каждому
        # адресу, с которого когда-либо звонили (подделка шлёт их сколько угодно).
        self._last_call = {k: at for k, at in self._last_call.items() if now - at < CALL_COOLDOWN_S}
        busy = any(c.key == call.key for c in self._calls.values())
        crowded = len(self._calls) >= MAX_CALLS
        if busy or crowded or call.key in self._last_call:
            await self._hangup(channel_id, "busy")
            return
        self._last_call[call.key] = now
        self._calls[channel_id] = call
        # ⚠ Не `answer`: панель считает вызов принятым и гасит мониторы.
        await self._request("POST", f"/channels/{channel_id}/ring")
        # `call` — id вызова: им телефон отвечает именно этой панели, им же
        # менеджер сводит `call` и `cancel` при нескольких панелях.
        #
        # ⚠ `peer` — СЕТЕВОЙ адрес отправителя, и только по нему снаружи можно
        # узнать, КАКАЯ панель звонит: по опознанной панели жилец открывает
        # дверь, а номер (`caller`) выбирает сам отправитель — потому он и не
        # годится нам для ограничений выше. Менеджер и приложение сверяют
        # именно его (`intercom-call-push.service.ts`, `intercom-incoming.ts`),
        # `caller` остаётся подписью в уведомлении.
        self._emit("call", call.payload())

    async def _answer(self, phone: str, target: str = "") -> None:
        """Телефон набрал `answer`: соединяем с вызовом `target` или последним звонящим."""
        waiting = [c for c in self._calls.values() if not c.talk and (not target or c.panel == target)]
        if not waiting:
            # Гость ушёл, пока жилец открывал приложение, или трубку взял другой.
            await self._hangup(phone, "normal")
            return
        call = max(waiting, key=lambda c: c.started)
        call.talk = phone
        await self._request("POST", f"/channels/{call.panel}/answer")
        await self._request("POST", f"/channels/{phone}/answer")
        bridge = await self._request("POST", "/bridges", {"type": "mixing"})
        call.bridge = str((bridge or {}).get("id") or "")
        if not call.bridge:
            LOGGER.warning("SIP-мост: мост не создан — разговора не будет")
            await self._finish(call, "failure")
            return
        await self._request(
            "POST",
            f"/bridges/{call.bridge}/addChannel",
            {"channel": f"{call.panel},{phone}"},
        )
        self._emit("answered", call.payload())

    async def reject(self, call_id: str) -> bool:
        """Жилец отказался: гасим НАШЕ плечо вызова, не трогая остальные.

        ⚠ Именно плечо, а не вызов у панели: групповой вызов — это отдельные
        приглашения каждому адресату, и мониторы в квартире обязаны звонить
        дальше (решение заказчика: локальная домофония Akuvox остаётся как
        есть). Поэтому `/api/call/hangup` самой панели здесь не годится — он
        снял бы вызов целиком.

        ⚠ Причина `busy` → панель получает по нашему INVITE отказ 486. Что она
        сделает с остальными адресатами, решает ЕЁ настройка группового вызова
        («End This Call Only» против «End All Calls»,
        `docs/docs-akuvox/kb-group-call.md`): при «End All Calls» отказ на
        телефоне погасит и мониторы — это настройка объекта, не наш код.

        `False` — такого вызова уже нет (гость ушёл сам): для жильца это тот же
        исход, а не ошибка.
        """
        call = self._calls.get(call_id)
        if call is None:
            return False
        # Событие «отмена» и уборку даст `StasisEnd` этого же канала — второй
        # источник того же события разошёлся бы с первым.
        await self._hangup(call.panel, "busy")
        return True

    async def _ended(self, channel_id: str) -> None:
        call = self._calls.get(channel_id)
        if call is not None:
            self._emit("ended" if call.talk else "cancel", call.payload())
            await self._finish(call, "normal")
            return
        for call in list(self._calls.values()):
            if call.talk == channel_id:
                # Жилец положил трубку — разговор окончен и для гостя.
                self._emit("ended", call.payload())
                await self._finish(call, "normal")
                return

    async def _finish(self, call: DoorCall, reason: str) -> None:
        self._calls.pop(call.panel, None)
        if call.bridge:
            await self._request("DELETE", f"/bridges/{call.bridge}")
        for channel_id in (call.panel, call.talk):
            if channel_id:
                await self._hangup(channel_id, reason)

    # --- ARI ----------------------------------------------------------------

    async def _hangup(self, channel_id: str, reason: str) -> None:
        # 404 здесь норма: сторона, что повесила трубку первой, уже ушла.
        await self._request("DELETE", f"/channels/{channel_id}", {"reason": reason})

    async def _request(
        self, method: str, path: str, params: dict[str, str] | None = None
    ) -> dict[str, Any] | None:
        try:
            async with self._session.request(
                method,
                self._base + path,
                params=params,
                headers=self._auth,
                timeout=aiohttp.ClientTimeout(total=5),
            ) as resp:
                if resp.status >= 400:
                    if resp.status != 404:
                        LOGGER.debug("SIP-мост: %s %s → %s", method, path, resp.status)
                    return None
                if resp.content_type == "application/json":
                    return await resp.json()
                return {}
        except (aiohttp.ClientError, TimeoutError) as err:
            LOGGER.debug("SIP-мост: %s %s не выполнен: %s", method, path, err)
            return None

    def _emit(self, kind: str, data: dict[str, Any]) -> None:
        self.history.append({"at": round(time()), "event": kind, **data})
        LOGGER.info("SIP-мост: %s %s", kind, data.get("caller", ""))
        if self._on_event is not None:
            try:
                self._on_event(kind, data)
            except Exception:  # noqa: BLE001 - потребитель события не роняет вызов
                LOGGER.warning("SIP-мост: обработчик события упал", exc_info=True)
