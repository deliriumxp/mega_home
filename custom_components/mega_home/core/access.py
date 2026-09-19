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

from .templating import render

# Виды доступа, которые дом исполняет. Незнакомый — отказ словами (`gateway.py`).
KINDS = ("http", "tcp", "udp", "mqtt", "ws")
AUTH_TYPES = ("none", "basic", "digest", "bearer", "session")
SESSION_PLACES = ("query", "header", "cookie")
EVENT_TYPES = ("webhook", "poll", "stream", "mqtt", "tcp", "tcpServer", "udp", "ws")

DEFAULT_METHODS = ("GET", "HEAD", "POST")
ALL_METHODS = ("GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS")
CALL_TIMEOUT = 30.0
# ⚠ Потолок любого срока из описания: дверь не держит соединение дома вечно.
MAX_TIMEOUT = 600.0

# ⚠ СОВМЕСТИМОСТЬ, а не знание дома. Описания Trassir от менеджера до 2026-09-19
# не несут `auth`, `deny` и `longPoll` — эти списки жили константами дома.
# Прежние значения получает ТОЛЬКО описание прежней формы (без блока `auth`):
# новому описанию без `deny` они не достаются (ревью 2026-09-19 — иначе панель
# домофона молча получала запреты и сроки регистратора).
LEGACY_DENY = (
    "/login", "/settings", "/objects", "/users", "/ptz", "/archive_export",
    "/export_archive", "/export_task", "/export_cancel", "/jit-export",
)
LEGACY_LONG_POLL = ("/archive_events", "/events")
LEGACY_LONG_POLL_TIMEOUT = 180.0


@dataclass
class RequestSpec:
    """Запрос, который дом делает САМ (вход, вызов-подготовка): путь и тело — шаблоны."""

    path: str = ""
    method: str = "GET"
    params: dict[str, str] = field(default_factory=dict)
    # Формат `params` у не-GET: `json` (прежнее поведение), `form`, `query`.
    format: str = "json"
    # Тело целиком шаблоном (вложенный JSON, JSON-RPC, SOAP); тогда `params` — в query.
    body: str = ""
    content_type: str = ""
    # Что забрать из ответа: имя → путь в JSON (`data.token`), `header:<имя>`,
    # `cookie:<имя>` или `text`.
    fields: dict[str, str] = field(default_factory=dict)


@dataclass
class SessionSpec(RequestSpec):
    """Вход с сессией. Сессия — поле `sid` в `fields` (или прежнее `field`)."""

    field: str = "sid"
    place: str = "query"
    name: str = "sid"
    # Значение, которое уходит в `place` под именем `name` (`Bearer {sid}`).
    template: str = "{sid}"
    ttl: float = 600.0
    # По какому тексту ответа видно, что сессия умерла (часть систем отвечает
    # на это обычным 200). Пусто — повторять не по чему.
    expired: str = ""
    # Шаг ДО входа: вызов, чьи поля (`challenge.<имя>`) нужны шаблонам входа.
    challenge: RequestSpec | None = None


@dataclass
class AuthSpec:
    type: str = "none"
    # Имена полей учётки: у одного устройства их бывает несколько наборов.
    user_field: str = "username"
    pass_field: str = "password"
    token_field: str = "token"
    session: SessionSpec | None = None
    # Сверх вида — на КАЖДЫЙ вызов, шаблонами описания (свой заголовок ключа,
    # параметр подписи, обёртка тела с `{body}` — WS-Security ONVIF).
    headers: dict[str, str] = field(default_factory=dict)
    query: dict[str, str] = field(default_factory=dict)
    wrap: str = ""


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
    secret: str = ""
    tls: bool = False
    tls_verify: bool = False
    timeout: float = CALL_TIMEOUT
    long_poll: tuple[str, ...] = ()
    long_poll_timeout: float = LEGACY_LONG_POLL_TIMEOUT
    methods: tuple[str, ...] = DEFAULT_METHODS
    # Запрещено всем / всем, кроме самого менеджера (префиксы путей или топиков).
    deny: tuple[str, ...] = ()
    manager_only: tuple[str, ...] = ()
    # Медиа: имя → шаблон адреса источника go2rtc.
    media: dict[str, str] = field(default_factory=dict)
    # Источники событий устройства (`listeners.py`).
    events: list[dict[str, Any]] = field(default_factory=list)

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
        return path.split("?", 1)[0].rstrip("/") in self.long_poll


def _strings(value: Any) -> dict[str, str]:
    return {str(k): str(v) for k, v in value.items()} if isinstance(value, dict) else {}


def _texts(value: Any) -> tuple[str, ...]:
    return tuple(str(i) for i in value if str(i)) if isinstance(value, (list, tuple)) else ()


def _seconds(value: Any, default: float) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    return min(number, MAX_TIMEOUT) if number > 0 else default


def _request(block: Any, cls: type = RequestSpec, **extra: Any) -> Any:
    if not isinstance(block, dict) or not block.get("path"):
        return None
    fmt = str(block.get("format") or "json")
    return cls(
        path=str(block["path"]),
        method=str(block.get("method") or "GET").upper(),
        params=_strings(block.get("params")),
        format=fmt if fmt in ("json", "form", "query") else "json",
        body=str(block.get("body") or ""),
        content_type=str(block.get("contentType") or ""),
        fields=_strings(block.get("fields")),
        **extra,
    )


def _session(block: Any) -> SessionSpec | None:
    if not isinstance(block, dict):
        return None
    place = str(block.get("place") or "query")
    return _request(
        block,
        SessionSpec,
        field=str(block.get("field") or "sid"),
        place=place if place in SESSION_PLACES else "query",
        name=str(block.get("name") or "sid"),
        template=str(block.get("template") or "{sid}"),
        ttl=_seconds(block.get("ttl"), 600.0),
        expired=str(block.get("expired") or ""),
        challenge=_request(block.get("challenge")),
    )


def _auth(block: dict[str, Any]) -> AuthSpec:
    raw = block.get("auth")
    if isinstance(raw, dict):
        return AuthSpec(
            type=str(raw.get("type") or "none"),
            user_field=str(raw.get("userField") or "username"),
            pass_field=str(raw.get("passField") or "password"),
            token_field=str(raw.get("tokenField") or "token"),
            session=_session(raw.get("session")),
            headers=_strings(raw.get("headers")),
            query=_strings(raw.get("query")),
            wrap=str(raw.get("wrap") or ""),
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
    legacy = "auth" not in block
    long_poll = block.get("longPoll") if isinstance(block.get("longPoll"), dict) else None
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
        tls=block.get("tls") is True,
        tls_verify=block.get("tlsVerify") is True,
        timeout=_seconds(block.get("timeout"), CALL_TIMEOUT),
        long_poll=_texts(long_poll.get("paths")) if long_poll else (LEGACY_LONG_POLL if legacy else ()),
        long_poll_timeout=_seconds((long_poll or {}).get("timeout"), LEGACY_LONG_POLL_TIMEOUT),
        methods=tuple(m.upper() for m in _texts(block.get("methods")) if m.upper() in ALL_METHODS)
        or DEFAULT_METHODS,
        deny=_texts(block.get("deny")) if "deny" in block else (LEGACY_DENY if legacy else ()),
        manager_only=_texts(block.get("managerOnly")),
        media=_strings(block.get("media")),
        events=[item for item in events if isinstance(item, dict)] if isinstance(events, list) else [],
    )


def fill(template: str, values: dict[str, Any]) -> str:
    """Подставить значения в шаблон (`templating.render`, один проход)."""
    return render(template, values)
