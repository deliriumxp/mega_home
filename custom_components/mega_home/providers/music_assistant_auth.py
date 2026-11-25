"""Вход в Яндекс Музыку через мастер Music Assistant."""

from __future__ import annotations

import asyncio
import base64
import binascii
from typing import Any
from urllib.parse import urlsplit
from xml.etree import ElementTree

from ..core.ops_base import OpError

PROVIDER = "yandex_music"
AUTH_ACTIONS = frozenset({"status", "start", "cancel", "disconnect"})
STATION_PROVIDER = "yandex_station"
DEVICE_IMAGE_PREFIX = "data:image/svg+xml;base64,"


class MusicAssistantAuth:
    """Мастер принадлежит MA, а дом хранит только ID запущенной им попытки."""

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._flow: str | None = None

    def reset(self) -> None:
        self._flow = None

    async def providers(self, mass: Any) -> list[dict]:
        return await mass.send_command("config/providers", provider_domain=PROVIDER)

    async def call(self, mass: Any, can_manage: bool, action: str) -> dict:
        if action != "status" and not can_manage:
            raise OpError("Яндекс Музыку может подключить жилец дома", 403)
        async with self._lock:
            configs = await self.providers(mass)
            if len(configs) > 1:
                raise OpError("В Music Assistant подключено несколько аккаунтов Яндекса", 409)
            provider = configs[0] if configs else None
            if action in ("cancel", "disconnect"):
                if self._flow is not None:
                    await self._flow_command(mass, "abort")
                    self._flow = None
                if provider and action == "disconnect":
                    await mass.send_command("config/providers/remove", instance_id=provider["instance_id"])
                    provider = None
            if action == "start" and self._flow is None:
                if provider is None:
                    step = await mass.send_command("config/providers/setup", provider_domain=PROVIDER)
                else:
                    step = await mass.send_command(
                        "config/providers/reconfigure", instance_id=provider["instance_id"],
                    )
                self._flow = step["flow_id"]
                if step.get("type") == "form" and step.get("step_id") == "user":
                    step = await mass.send_command(
                        "config/flows/submit", flow_id=step["flow_id"],
                        values={"method": "device", "remember_session": True},
                    )
                return self._result(step, can_manage)
            if self._flow is not None and can_manage:
                step = await self._flow_command(mass, "get")
                if step is not None:
                    return self._result(step, can_manage)
                self._flow = None
            connected = provider is not None and provider.get("enabled") is not False
            return _account("connected" if connected else "disconnected", can_manage)

    async def stations(self, mass: Any, can_manage: bool, connect: bool) -> dict:
        if not can_manage:
            raise OpError("Колонки подключает жилец с учётной записью дома", 403)
        async with self._lock:
            stations = await mass.send_command("config/providers", provider_domain=STATION_PROVIDER)
            if any(provider.get("enabled") is not False for provider in stations):
                return {"state": "connected"}
            music = await self.providers(mass)
            if len(music) != 1 or music[0].get("enabled") is False:
                return {"state": "needs-music"}
            if not connect:
                return {"state": "ready"}
            if stations:
                step = await mass.send_command(
                    "config/providers/reconfigure", instance_id=stations[0]["instance_id"],
                )
            else:
                step = await mass.send_command("config/providers/setup", provider_domain=STATION_PROVIDER)
            if step.get("type") == "form" and step.get("step_id") == "user":
                step = await mass.send_command(
                    "config/flows/submit", flow_id=step["flow_id"],
                    values={"ym_instance": music[0]["instance_id"]},
                )
            if step.get("type") == "finish":
                return {"state": "connected"}
            return {"state": "unavailable"}

    async def _flow_command(self, mass: Any, command: str) -> dict | None:
        if self._flow is None:
            return None
        flow_id = self._flow
        try:
            return await mass.send_command(f"config/flows/{command}", flow_id=flow_id)
        except Exception as error:
            # MA 2.10 удаляет завершённую попытку. Следующий status читает уже провайдера.
            if f"Unknown (or finished) setup flow: {flow_id}" not in str(error):
                raise
            return None

    def _result(self, step: dict, can_manage: bool) -> dict:
        kind = step.get("type")
        if kind == "progress" and step.get("step_id") == "device_login":
            code, url = _device_details(step.get("image"))
            expires_at = step.get("expires_at")
            if type(expires_at) in (int, float) and code and url:
                return _account("pending", can_manage, code=code, url=url, expiresAt=int(expires_at * 1000))
        if kind == "finish":
            self._flow = None
            return _account("connected", can_manage)
        if kind != "progress":
            self._flow = None
        return _account("failed", can_manage)


def _account(state: str, can_manage: bool, **fields: Any) -> dict:
    return {"state": state, "canManage": can_manage, "login": None, **fields}


def _device_details(image: object) -> tuple[str, str]:
    # MA 2.10 отдаёт код и адрес Device Flow текстовыми элементами SVG.
    if not isinstance(image, str) or not image.startswith(DEVICE_IMAGE_PREFIX) or len(image) > 8192:
        return "", ""
    try:
        svg = ElementTree.fromstring(base64.b64decode(image[len(DEVICE_IMAGE_PREFIX):], validate=True))
        texts = svg.findall("{http://www.w3.org/2000/svg}text")
        url = "https://" + (texts[1].text or "")
        code = (texts[3].text or "").strip()
        host = urlsplit(url).hostname or ""
    except (binascii.Error, ElementTree.ParseError, IndexError, ValueError):
        return "", ""
    if host not in ("ya.ru", "yandex.ru") and not host.endswith(".yandex.ru"):
        return "", ""
    return code, url
