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

import json
from typing import Any

from homeassistant.core import HomeAssistant

from .core import services
from .core.ops_base import OpError

NAME = "ha"
HANDSHAKE_S = 10

# ⚠ Чего учётка объекта через дом НЕ даёт (заказчик 2026-10-17, ревью): выпустить себе токен и
# управлять учётками и системой HA. Такой токен работал бы напрямую с HA и пережил бы любой отзыв
# — сброс подключений, смену пароля учётки, срок гостевой ссылки. Это не перечень «что можно»
# под функции: всё прочее API HA бандлу открыто, закрыт только выход из-под отзыва.
REFUSED_PATHS = ("/auth/", "/api/hassio", "/api/onboarding", "/api/config/auth", "/api/services/hassio/")
REFUSED_FRAMES = ("auth", "auth/", "config/auth/", "config/auth_provider/", "hassio/", "supervisor/")
REFUSED_TEXT = "Учётки и систему Home Assistant через дом не меняют"


def guard(what: str, value: str) -> str | None:
    """Запрет службы `ha` (`services.Service.guard`): путь, тело запроса или кадр сокета. Путь —
    уже приведённый транспортом (`connect.service_path`): `/%61uth/`, `/api/../auth/` сюда не доходят."""
    if what == "path":
        path = value.split("?", 1)[0].lower()
        return REFUSED_TEXT if any(path.startswith(p) for p in REFUSED_PATHS) else None
    # Автоматизация или скрипт с действием супервизора исполнились бы позже от имени HA —
    # тот же выход из-под отзыва, только отложенный.
    if '"hassio.' in value or "'hassio." in value:
        return REFUSED_TEXT
    if what == "body":
        return None
    try:
        frame = json.loads(value)
    except ValueError:
        return None
    if not isinstance(frame, dict):
        return None
    kind = str(frame.get("type") or "")
    if kind in REFUSED_FRAMES or any(kind.startswith(p) for p in REFUSED_FRAMES if p.endswith("/")):
        return REFUSED_TEXT
    # Службы супервизора (`hassio.addon_stdin` и т. п.) — та же система, что и её API.
    if kind == "call_service" and str(frame.get("domain") or "") == "hassio":
        return REFUSED_TEXT
    return None


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
            guard=guard,
        )

    async def _login(self, socket_: Any) -> None:
        """Фаза входа WebSocket HA до кадров вызывающего (`dev-api-websocket.md`).

        ⚠ HA, закрывший сокет посреди входа, даёт `receive_json` не JSON, а кадр закрытия —
        это отказ словами (502), а не падение обработчика.
        """
        try:
            first = await socket_.receive_json(timeout=HANDSHAKE_S)
            if first.get("type") != "auth_required":
                raise OpError("Home Assistant не начал вход", 502)
            await socket_.send_json({"type": "auth", "access_token": self.token})
            answer = await socket_.receive_json(timeout=HANDSHAKE_S)
        except (TypeError, ValueError, AttributeError) as err:
            raise OpError("Home Assistant закрыл соединение при входе", 502) from err
        if answer.get("type") != "auth_ok":
            raise OpError("Home Assistant не принял токен объекта", 502)
