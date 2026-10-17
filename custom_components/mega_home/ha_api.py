"""Home Assistant — служба `ha` транспорта, с учёткой дома.

Решение заказчика 2026-10-17 (`docs/superpowers/specs/2026-10-17-schedules-design.md`
менеджера, часть 2): шлюз закрывает ось «API платформы, на которой стоит дом», а не
функцию. Бандл ходит в REST и WebSocket своего HA обычным `connect`/`stream.open` к
службе `ha` — редактор автоматизаций, `validate_config`, реестры, — и следующей
функции, которой нужен HA, релиз дома не нужен.

⚠ Учётка — долгоживущий токен объекта из менеджера (`homeAssistantToken` карточки,
раздел конфига `house.ha`, который бандлу не уходит — `ops.config`). Пользователей
HA дом не заводит (`docs/ha-official-api.md` менеджера). Подставляет её дом, токен
наружу не уходит: REST — заголовком, WebSocket — фазой входа
(`dev-api-websocket.md`: «the server sends out `auth_required`», ответ — `auth_ok`).
Нет токена — служба без учётки, как была (картинки сущностей по ссылке с токеном в
адресе), и паспорт дома говорит `auth: false`: приложение расписаний не показывает.
"""

from __future__ import annotations

from typing import Any

from homeassistant.core import HomeAssistant

from .core import services
from .core.ops_base import OpError

NAME = "ha"
HANDSHAKE_S = 10


class HaApi:
    """Регистрирует службу `ha` по конфигу дома."""

    def __init__(self, hass: HomeAssistant) -> None:
        self.hass = hass
        self.token = ""

    def apply(self, config: dict[str, Any] | None) -> None:
        self.token = str((((config or {}).get("house") or {}).get("ha") or {}).get("token") or "")
        self.register()

    def register(self) -> None:
        api = self.hass.config.api
        # Порт и TLS — те, что слушает сам HA: на петле с сертификатом домена
        # проверять нечего, `connect` ходит без проверки (`tls_context`).
        port = api.port if api else 8123
        tls = bool(api and api.use_ssl)
        if not self.token:
            services.register(NAME, port, tls=tls)
            return
        services.register(
            NAME,
            port,
            tls=tls,
            headers={"Authorization": f"Bearer {self.token}"},
            ws_open=self._login,
        )

    async def _login(self, socket_: Any) -> None:
        """Фаза входа WebSocket HA до кадров вызывающего (`dev-api-websocket.md`)."""
        first = await socket_.receive_json(timeout=HANDSHAKE_S)
        if first.get("type") != "auth_required":
            raise OpError("Home Assistant не начал вход", 502)
        await socket_.send_json({"type": "auth", "access_token": self.token})
        answer = await socket_.receive_json(timeout=HANDSHAKE_S)
        if answer.get("type") != "auth_ok":
            raise OpError("Home Assistant не принял токен объекта", 502)
