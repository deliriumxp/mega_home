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
from .sip_config import ARI_APP, ARI_USER

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
    # Сетевой адрес отправителя — по нему пауза и «один вызов разом».
    peer: str = ""


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
            self._calls.clear()
            await asyncio.sleep(RECONNECT_S)

    # --- события ----------------------------------------------------------

    async def handle(self, event: dict[str, Any]) -> None:
        kind = event.get("type")
        channel = event.get("channel") or {}
        channel_id = str(channel.get("id") or "")
        if not channel_id:
            return
        if kind == "StasisStart":
            role = (event.get("args") or [""])[0]
            if role == "panel":
                # Второй аргумент — СЕТЕВОЙ адрес отправителя (`CHANNEL(pjsip,
                # remote_addr)`, «ip:порт»): номер звонящего выбирает сам INVITE.
                args = event.get("args") or []
                peer = str(args[1]).rsplit(":", 1)[0] if len(args) > 1 and args[1] else ""
                await self._ringing(channel_id, channel, peer)
            elif role == "answer":
                # Второй аргумент — id вызова, которому отвечают (`answer-<id>`);
                # пусто — последний звонящий.
                args = event.get("args") or []
                await self._answer(channel_id, str(args[1]) if len(args) > 1 else "")
            else:
                await self._hangup(channel_id, "unallocated")
        elif kind == "StasisEnd":
            await self._ended(channel_id)

    async def _ringing(self, channel_id: str, channel: dict[str, Any], peer: str = "") -> None:
        caller = str((channel.get("caller") or {}).get("number") or "")
        # ⚠ Ограничения — по СЕТЕВОМУ адресу, а не по номеру: номер (CALLERID)
        # выбирает отправитель, и, меняя его, подделка проходила бы паузу каждый
        # раз (повторное ревью 2026-09-19). Сверх того — общий потолок на дом.
        key = peer or caller
        now = time()
        busy = any(c.peer == key for c in self._calls.values())
        crowded = len(self._calls) >= MAX_CALLS
        if busy or crowded or now - self._last_call.get(key, 0.0) < CALL_COOLDOWN_S:
            await self._hangup(channel_id, "busy")
            return
        self._last_call[key] = now
        self._calls[channel_id] = DoorCall(channel_id, caller, peer=key)
        # ⚠ Не `answer`: панель считает вызов принятым и гасит мониторы.
        await self._request("POST", f"/channels/{channel_id}/ring")
        # `call` — id вызова: им телефон отвечает именно этой панели, им же
        # менеджер сводит `call` и `cancel` при нескольких панелях.
        self._emit("call", {"caller": caller, "call": channel_id})

    async def _answer(self, phone: str, target: str = "") -> None:
        """Телефон набрал `answer[-<id>]`: соединяем с этим вызовом или последним звонящим."""
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
        self._emit("answered", {"caller": call.caller, "call": call.panel})

    async def _ended(self, channel_id: str) -> None:
        call = self._calls.get(channel_id)
        if call is not None:
            self._emit("ended" if call.talk else "cancel", {"caller": call.caller, "call": call.panel})
            await self._finish(call, "normal")
            return
        for call in list(self._calls.values()):
            if call.talk == channel_id:
                # Жилец положил трубку — разговор окончен и для гостя.
                self._emit("ended", {"caller": call.caller, "call": call.panel})
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
