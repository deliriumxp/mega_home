"""Описание ОДНОГО доступа объекта — данные конфига, а не код.

Закрытый список того, что шлюз дома умеет для ЛЮБОГО вендора, и почему именно он,
— `docs/home-gateway.md` в менеджере. Здесь только разбор описания: вендорское
знание живёт в `accessConfigs` менеджера, дом его исполняет.

⚠ Разбор не бросает: конфиг может приехать от менеджера постарше или поновее, и
незнакомое поле — не повод остаться без доступа.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

# Виды доступа, которые дом исполняет. Незнакомый — отказ словами (`gateway.py`).
KINDS = ("http", "tcp", "udp", "mqtt")
AUTH_TYPES = ("none", "basic", "digest", "bearer", "session")
SESSION_PLACES = ("query", "header", "cookie")
EVENT_TYPES = ("webhook", "poll", "mqtt", "tcp")

DEFAULT_METHODS = ("GET", "HEAD", "POST")
ALL_METHODS = ("GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS")
CALL_TIMEOUT = 30.0
# ⚠ Потолок любого срока из описания: дверь не держит соединение дома вечно.
MAX_TIMEOUT = 600.0

# ⚠ СОВМЕСТИМОСТЬ, а не знание дома. Описания Trassir от менеджера до 2026-09-19
# не несут `deny` и `longPoll` — эти списки жили константами дома. Пока в парке
# есть такой менеджер, описание без ключа получает их прежнее значение. Новое
# описание несёт свои списки, и тогда эти строки не читаются вовсе.
LEGACY_DENY = (
    "/login", "/settings", "/objects", "/users", "/ptz", "/archive_export",
    "/export_archive", "/export_task", "/export_cancel", "/jit-export",
)
LEGACY_LONG_POLL = ("/archive_events", "/events")
LEGACY_LONG_POLL_TIMEOUT = 180.0


@dataclass
class SessionSpec:
    """Вход с сессией: куда войти, где в ответе сессия и куда её подставлять."""

    path: str = ""
    method: str = "GET"
    # Параметры входа; `{secret.<поле>}` подставляет дом (`{user}`/`{pass}` —
    # прежние имена полей `username`/`password`).
    params: dict[str, str] = field(default_factory=dict)
    field: str = "sid"
    place: str = "query"
    name: str = "sid"
    ttl: float = 600.0
    # По какому тексту ответа видно, что сессия умерла (часть систем отвечает
    # на это обычным 200). Пусто — повторять не по чему.
    expired: str = ""


@dataclass
class AuthSpec:
    type: str = "none"
    # Имена полей учётки: у одного устройства их бывает несколько наборов.
    user_field: str = "username"
    pass_field: str = "password"
    token_field: str = "token"
    session: SessionSpec | None = None


@dataclass
class AccessDescriptor:
    id: str
    host: str
    kind: str = "http"
    scheme: str = "https"
    port: int = 443
    rtsp_port: int = 554
    vendor: str = ""
    auth: AuthSpec = field(default_factory=AuthSpec)
    # Отпечаток учётки: сменился — дом перечитывает её маршрутом менеджера.
    # Пусто — учётки у доступа нет.
    secret: str = ""
    tls_verify: bool = False
    timeout: float = CALL_TIMEOUT
    long_poll: tuple[str, ...] = ()
    long_poll_timeout: float = LEGACY_LONG_POLL_TIMEOUT
    # Методы HTTP, которые пускает дверь. Умолчание — чтение и команда; PUT,
    # PATCH, DELETE описание открывает само, если устройство ими управляется.
    methods: tuple[str, ...] = DEFAULT_METHODS
    # Запрещено всем / всем, кроме самого менеджера (префиксы путей или топиков).
    deny: tuple[str, ...] = ()
    manager_only: tuple[str, ...] = ()
    # Медиа: имя → шаблон адреса источника go2rtc.
    media: dict[str, str] = field(default_factory=dict)
    # Источники событий устройства (`listeners.py`).
    events: list[dict[str, Any]] = field(default_factory=list)
    # Поток по токену (прежняя форма Trassir): путь, параметры, поле, шаблон.
    stream_path: str = ""
    stream_params: dict[str, str] = field(default_factory=dict)
    stream_field: str = "token"
    stream_url: str = "rtsp://{host}:{rtspPort}/{token}"

    # Прежние имена — их читают драйвер Trassir и спеки двери.
    @property
    def login_path(self) -> str:
        return self.auth.session.path if self.auth.session else ""

    @property
    def login_params(self) -> dict[str, str]:
        return self.auth.session.params if self.auth.session else {}

    @property
    def session_param(self) -> str:
        return self.auth.session.name if self.auth.session else "sid"

    @property
    def session_expired(self) -> str:
        return self.auth.session.expired if self.auth.session else ""

    def is_long_poll(self, path: str) -> bool:
        start = path.split("?", 1)[0].rstrip("/")
        return start in self.long_poll


def _strings(value: Any) -> dict[str, str]:
    if not isinstance(value, dict):
        return {}
    return {str(key): str(item) for key, item in value.items()}


def _texts(value: Any) -> tuple[str, ...]:
    if not isinstance(value, (list, tuple)):
        return ()
    return tuple(str(item) for item in value if str(item))


def _seconds(value: Any, default: float) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    return min(number, MAX_TIMEOUT) if number > 0 else default


def _session(block: Any) -> SessionSpec | None:
    if not isinstance(block, dict) or not block.get("path"):
        return None
    place = str(block.get("place") or "query")
    return SessionSpec(
        path=str(block["path"]),
        method=str(block.get("method") or "GET").upper(),
        params=_strings(block.get("params")),
        field=str(block.get("field") or "sid"),
        place=place if place in SESSION_PLACES else "query",
        name=str(block.get("name") or "sid"),
        ttl=_seconds(block.get("ttl"), 600.0),
        expired=str(block.get("expired") or ""),
    )


def _auth(block: dict[str, Any]) -> AuthSpec:
    raw = block.get("auth")
    if isinstance(raw, dict):
        kind = str(raw.get("type") or "none")
        return AuthSpec(
            type=kind,
            user_field=str(raw.get("userField") or "username"),
            pass_field=str(raw.get("passField") or "password"),
            token_field=str(raw.get("tokenField") or "token"),
            session=_session(raw.get("session")),
        )
    # Прежняя форма: вход сессией полями верхнего уровня.
    if block.get("login"):
        return AuthSpec(
            type="session",
            session=SessionSpec(
                path=str(block["login"]),
                params=_strings(block.get("loginParams")),
                field=str(block.get("sessionField") or "sid"),
                name=str(block.get("sessionParam") or "sid"),
                ttl=_seconds(block.get("sessionTtl"), 600.0),
                expired=str(block.get("sessionExpired") or ""),
            ),
        )
    return AuthSpec()


def descriptor_of(block: Any) -> AccessDescriptor | None:
    """Собрать описание из блока конфига; мусор — «описания нет»."""
    if not isinstance(block, dict):
        return None
    # ⚠ Пробелы — тот же «не задано»: иначе дверь стучалась бы в никуда.
    host = str(block.get("host") or "").strip()
    if not host:
        return None
    long_poll = block.get("longPoll")
    legacy = not isinstance(long_poll, dict)
    events = block.get("events")
    try:
        port = int(block.get("port") or 443)
        rtsp_port = int(block.get("rtspPort") or block.get("rtsp_port") or 554)
    except (TypeError, ValueError):
        return None
    return AccessDescriptor(
        id=str(block.get("id") or block.get("vendor") or "access"),
        host=host,
        kind=str(block.get("kind") or "http"),
        scheme=str(block.get("scheme") or "https"),
        port=port,
        rtsp_port=rtsp_port,
        vendor=str(block.get("vendor") or ""),
        auth=_auth(block),
        secret=str(block.get("secret") or ""),
        tls_verify=block.get("tlsVerify") is True,
        timeout=_seconds(block.get("timeout"), CALL_TIMEOUT),
        long_poll=LEGACY_LONG_POLL if legacy else _texts(long_poll.get("paths")),
        long_poll_timeout=(
            LEGACY_LONG_POLL_TIMEOUT
            if legacy
            else _seconds(long_poll.get("timeout"), LEGACY_LONG_POLL_TIMEOUT)
        ),
        methods=tuple(m.upper() for m in _texts(block.get("methods")) if m.upper() in ALL_METHODS)
        or DEFAULT_METHODS,
        deny=LEGACY_DENY if "deny" not in block else _texts(block.get("deny")),
        manager_only=_texts(block.get("managerOnly")),
        media=_strings(block.get("media")),
        events=[item for item in events if isinstance(item, dict)]
        if isinstance(events, list)
        else [],
        stream_path=str(block.get("streamPath") or ""),
        stream_params=_strings(block.get("streamParams")),
        stream_field=str(block.get("streamField") or "token"),
        stream_url=str(block.get("streamUrl") or "rtsp://{host}:{rtspPort}/{token}"),
    )


def fill(template: str, values: dict[str, str]) -> str:
    """Подставить `{имя}` из словаря; незнакомое имя остаётся как есть.

    ⚠ Не `str.format`: фигурные скобки бывают и в самих данных (JSON в
    параметре), и падать на них нельзя.
    """
    out = template
    for key, value in values.items():
        out = out.replace("{" + key + "}", value)
    return out
