"""Универсальная дверь наружу: дом ИСПОЛНЯЕТ вызов, СОСТАВЛЕННЫЙ бандлом.

⚠ Дверь не знает ни одного вендора и не разбирает ни одного ответа. Она умеет
выполнить вызов, ОПИСАННЫЙ в конфиге объекта, и отдать ответ как есть. Что
значат поля ответа — решает бандл, который обновляется сам
(`docs/plan-thin-integration.md`, «Широкая дверь»).

⚠ Закрытый список того, что умеет дверь для ЛЮБОГО вендора, — виды доступа,
авторизация, учётки, медиа, события, уровни — и почему именно он:
`docs/home-gateway.md` в менеджере. Сюда вендорское знание не входит: запреты
путей, пути долгого опроса, вид входа — всё данные описания (`access.py`).
Релиз интеграции ради нового вендора значит, что список не закрыт.

⚠ Учётки через дверь НЕ ходят: их подставляет дом (`access_secrets.py`).

⚠ Границы двери (политика, а не список команд):
  * адресат — только доступ из конфига объекта (никакого «сходи по LAN»);
  * запреты описания: `deny` — всем, `managerOnly` — всем, кроме менеджера;
  * потолок размера ответа и срок: дверь не превращается в выкачивание;
  * путь проверяется таким, каким уйдёт в сеть (нормализация обходов).
"""

from __future__ import annotations

from typing import Any

from .access import (
    CALL_TIMEOUT,
    KINDS,
    LEGACY_LONG_POLL,
    LEGACY_LONG_POLL_TIMEOUT,
    AccessDescriptor,
    descriptor_of,
    fill,
)
from .access_http import (
    MAX_RESPONSE_BYTES,
    AccessDenied,
    AccessUnreachable,
    HttpAccess,
    read_all as _read_all,
)
from .access_secrets import SecretBook, template_values
from .const import LOGGER

__all__ = [
    "AccessDenied", "AccessDescriptor", "AccessGateway", "AccessUnreachable",
    "CALL_TIMEOUT", "LONG_POLL_TIMEOUT", "MAX_RESPONSE_BYTES", "descriptor_of",
    "SCOPE_MANAGER", "SCOPE_RESIDENT", "_read_all",
]

LONG_POLL_TIMEOUT = LEGACY_LONG_POLL_TIMEOUT
# Уровни вызова. ⚠ «Менеджер» ставит только код менеджера в кадре канала
# (`link.py`); запрос жильца снаружи едет внутри `payload` и уровня не меняет.
SCOPE_RESIDENT = "resident"
SCOPE_MANAGER = "manager"


def _call_timeout(path: str) -> float:
    """Срок прежнего описания Trassir: длинный опрос держат, обычный вызов — нет."""
    start = path.split("?", 1)[0].rstrip("/")
    return LONG_POLL_TIMEOUT if start in LEGACY_LONG_POLL else CALL_TIMEOUT


class AccessGateway:
    """Доступы объекта: их учётки, сессии и исполнение описанных вызовов."""

    def __init__(
        self,
        credentials: Any = None,
        sid_provider: Any = None,
        secrets_fetch: Any = None,
        store: Any = None,
        provider_access: str | None = None,
    ) -> None:
        self._descriptors: dict[str, AccessDescriptor] = {}
        self.secrets = SecretBook(secrets_fetch, store, legacy=credentials, legacy_access=provider_access)
        # `provider_access` — id доступа, чью сессию держит драйвер вендора
        # (его называет сам драйвер, дверь вендоров не знает).
        self._http = HttpAccess(self.secrets, sid_provider, provider_access)
        self._mqtt: dict[str, Any] = {}

    # --- описание -------------------------------------------------------

    def apply(self, blocks: Any) -> None:
        """Принять описания из конфига объекта (список блоков `accesses`)."""
        self._descriptors = {}
        self._http.reset()
        for block in blocks if isinstance(blocks, list) else []:
            descriptor = descriptor_of(block)
            if descriptor is not None:
                self._descriptors[descriptor.id] = descriptor
        self.secrets.forget(set(self._descriptors))

    def ids(self) -> list[str]:
        return list(self._descriptors)

    def descriptors(self) -> list[AccessDescriptor]:
        return list(self._descriptors.values())

    def descriptor(self, access: str | None) -> AccessDescriptor | None:
        """Описание по имени; без имени — единственный доступ объекта."""
        if access:
            return self._descriptors.get(access)
        if len(self._descriptors) == 1:
            return next(iter(self._descriptors.values()))
        return None

    # --- политика -------------------------------------------------------

    @staticmethod
    def check(
        descriptor: AccessDescriptor, method: str, path: str, scope: str = SCOPE_RESIDENT
    ) -> str:
        """Пропустить вызов или объяснить, почему нет; вернуть ПРОВЕРЕННЫЙ путь.

        ⚠ Проверять НАДО ТО, ЧТО УЙДЁТ В СЕТЬ, а не то, что прислали. Замер
        стенда 2026-09-13: `/a/../settings/webserver/` мимо запрета проезжал
        целиком — yarl приводил путь к `/settings/webserver/` уже после
        проверки, а в локальном контуре дома аутентификации нет вовсе.
        """
        if method not in descriptor.methods:
            raise AccessDenied(f"Метод {method} через дверь не ходит")
        if not path.startswith("/"):
            raise AccessDenied("Путь начинается с «/»")
        clean = _normalized(path)
        _deny(descriptor, clean.split("?")[0], scope)
        return clean

    # --- исполнение -----------------------------------------------------

    async def call(
        self,
        access: str | None,
        method: str,
        path: str,
        params: dict[str, Any] | None = None,
        body: bytes | None = None,
        session: dict[str, str] | None = None,
        headers: dict[str, str] | None = None,
        scope: str = SCOPE_RESIDENT,
    ) -> tuple[int, str, bytes]:
        """HTTP-вызов по описанию. Ответ отдаётся КАК ЕСТЬ — без разбора."""
        status, kind, payload, _ = await self.call_full(
            access, method, path, params, body, session, headers, scope
        )
        return status, kind, payload

    async def call_full(
        self,
        access: str | None,
        method: str,
        path: str,
        params: dict[str, Any] | None = None,
        body: bytes | None = None,
        session: dict[str, str] | None = None,
        headers: dict[str, str] | None = None,
        scope: str = SCOPE_RESIDENT,
    ) -> tuple[int, str, bytes, dict[str, str]]:
        """То же, что `call`, плюс заголовки ответа, которые видит бандл."""
        descriptor = self._http_descriptor(access)
        method = method.upper()
        # ⚠ Дальше идёт ПРОВЕРЕННЫЙ путь: иначе нормализация в сети вернула бы
        # то, что политика только что отвергла.
        path = self.check(descriptor, method, path, scope)
        return await self._http.request(descriptor, method, path, params, body, session, headers)

    async def exchange(
        self, access: str | None, call: dict[str, Any], scope: str = SCOPE_RESIDENT
    ) -> dict[str, Any]:
        """Вызов доступа НЕ-HTTP вида: `tcp`, `udp`, `mqtt`."""
        from .access_raw import tcp_exchange, udp_exchange

        descriptor = self._known(access)
        if descriptor.kind == "tcp":
            return await tcp_exchange(descriptor, call)
        if descriptor.kind == "udp":
            return await udp_exchange(descriptor, call)
        if descriptor.kind == "mqtt":
            topic = str(call.get("topic") or "")
            if not topic:
                raise AccessDenied("Топик не указан")
            _deny(descriptor, topic, scope)
            from .mqtt import MqttError, as_bytes

            client = await self.mqtt_client(descriptor)
            try:
                await client.publish(
                    topic, as_bytes(call.get("payload")), 1 if call.get("qos") == 1 else 0, call.get("retain") is True
                )
            except MqttError as err:
                raise AccessUnreachable(f"Брокер: {err}") from err
            return {"published": True}
        raise AccessDenied(f"Вызов вида «{descriptor.kind}» идёт другой дорогой")

    async def mqtt_client(self, descriptor: AccessDescriptor, on_message: Any = None) -> Any:
        """Подключение к брокеру доступа: одно на доступ, поднимается по надобности."""
        from .mqtt import MqttClient, MqttError

        client = self._mqtt.get(descriptor.id)
        if client is not None and not client.closed.is_set() and on_message is None:
            return client
        secret = await self.secrets.get(descriptor.id, descriptor.secret) if descriptor.secret else {}
        client = MqttClient(
            descriptor.host,
            descriptor.port,
            secret.get(descriptor.auth.user_field, ""),
            secret.get(descriptor.auth.pass_field, ""),
            tls=descriptor.scheme in ("mqtts", "ssl", "tls"),
            client_id=f"mega_home-{descriptor.id}{'-sub' if on_message else ''}",
            on_message=on_message,
        )
        try:
            await client.connect()
        except MqttError as err:
            raise AccessUnreachable(f"Брокер: {err}") from err
        if on_message is None:
            self._mqtt[descriptor.id] = client
        return client

    async def media_url(
        self, access: str | None, source: str, values: dict[str, str] | None = None
    ) -> str:
        """Адрес источника медиа по шаблону описания (`media`), с учёткой доступа."""
        descriptor = self._known(access)
        template = descriptor.media.get(source)
        if not template:
            raise AccessDenied(f"У доступа нет медиа «{source}»")
        secret = await self.secrets.get(descriptor.id, descriptor.secret) if descriptor.secret else {}
        clean = {k: str(v) for k, v in (values or {}).items() if not str(k).startswith("secret.")}
        return fill(template, template_values(descriptor, secret, clean))

    async def stream_url(self, camera: str, quality: str) -> str:
        """Поток по токену (прежняя форма Trassir)."""
        descriptor = self.descriptor(None)
        if descriptor is None or not descriptor.stream_path:
            raise AccessDenied("Доступ к потоку у объекта не описан")
        return await self._http.token_stream(descriptor, camera, quality)

    async def _client(self, verify: bool = False) -> Any:
        """Соединение HTTP-стороны (замок на решение о сертификате — в спеке)."""
        return await self._http._client(verify)  # noqa: SLF001

    def _known(self, access: str | None) -> AccessDescriptor:
        descriptor = self.descriptor(access)
        if descriptor is None:
            raise AccessDenied("Такого доступа у объекта нет")
        if descriptor.kind not in KINDS:
            # ⚠ Незнакомый вид — конфиг от менеджера поновее: причина словами.
            raise AccessDenied(f"Доступ вида «{descriptor.kind}» дом пока не умеет")
        return descriptor

    def _http_descriptor(self, access: str | None) -> AccessDescriptor:
        descriptor = self._known(access)
        if descriptor.kind != "http":
            raise AccessDenied(f"Доступ вида «{descriptor.kind}» зовётся не HTTP-вызовом")
        return descriptor

    async def async_close(self) -> None:
        await self._http.close()
        for client in list(self._mqtt.values()):
            await client.close()
        self._mqtt.clear()


def _deny(descriptor: AccessDescriptor, target: str, scope: str) -> None:
    for prefix in descriptor.deny:
        if target.startswith(prefix):
            raise AccessDenied(f"{prefix}* через дверь не ходит")
    if scope != SCOPE_MANAGER:
        for prefix in descriptor.manager_only:
            if target.startswith(prefix):
                LOGGER.debug("Дверь: %s только для менеджера", prefix)
                raise AccessDenied(f"{prefix}* — только для менеджера")


def _normalized(path: str) -> str:
    """Путь таким, каким его увидит устройство: без «..», «.» и %-обёрток.

    ⚠ Сначала раскрываем проценты, потом убираем точки-сегменты — иначе
    `/%2e%2e/settings/` проедет мимо (проверено на стенде). Схлопываем и
    повторные «/»: запрет по префиксу иначе обходится лишним слэшем.
    """
    from urllib.parse import unquote

    raw = unquote(path)
    head, sep, tail = raw.partition("?")
    out: list[str] = []
    for part in head.split("/"):
        if part in ("", "."):
            continue
        if part == "..":
            if out:
                out.pop()
            continue
        out.append(part)
    # ⚠ Хвостовой «/» СОХРАНЯЕМ: у Trassir это разные адреса (каталог настроек
    # против значения), нормализация не имеет права менять смысл запроса.
    tailing = "/" if head.endswith("/") and out else ""
    return "/" + "/".join(out) + tailing + (sep + tail if sep else "")
