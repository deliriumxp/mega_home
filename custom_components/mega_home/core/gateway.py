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

import asyncio
from typing import Any

from .access import CALL_TIMEOUT, KINDS, AccessDescriptor, descriptor_of, fill
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
    "CALL_TIMEOUT", "MAX_RESPONSE_BYTES", "descriptor_of",
    "SCOPE_MANAGER", "SCOPE_RESIDENT", "_read_all",
]

# Уровни вызова. ⚠ «Менеджер» ставит только код менеджера в кадре канала
# (`link.py`); запрос жильца снаружи едет внутри `payload` и уровня не меняет.
SCOPE_RESIDENT = "resident"
SCOPE_MANAGER = "manager"


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
        self._mqtt_lock = asyncio.Lock()
        self._closed = False

    @property
    def http(self) -> HttpAccess:
        """HTTP-сторона двери — её авторизацией ходят поток событий и WebSocket."""
        return self._http

    def bind_session(self, access: str, sid_provider: Any, credentials: Any) -> None:
        """Драйвер вендора отдаёт двери свою ЖИВУЮ сессию и учётку — для СВОЕГО доступа.

        ⚠ Имя доступа называет драйвер, а не дверь: дверь вендоров не знает.
        """
        self._http.bind(access, sid_provider)
        self.secrets.bind_legacy(access, credentials)

    # --- описание -------------------------------------------------------

    def apply(self, blocks: Any) -> None:
        """Принять описания из конфига объекта (список блоков `accesses`).

        ⚠ Неизменённые описания пропускаются: конфиг приходит и без правок (опрос,
        канал), и сброс сессий каждый раз стоил бы лишних входов в устройства.
        """
        fresh: dict[str, AccessDescriptor] = {}
        for block in blocks if isinstance(blocks, list) else []:
            descriptor = descriptor_of(block)
            if descriptor is not None:
                fresh[descriptor.id] = descriptor
        if fresh == self._descriptors:
            return
        # Подключения к брокерам, чьё описание сменилось или пропало, снимаются:
        # иначе публикации шли бы в прежний брокер прежней учёткой.
        for access in [a for a, d in self._descriptors.items() if fresh.get(a) != d]:
            client = self._mqtt.pop(access, None)
            if client is not None:
                asyncio.ensure_future(client.close())
        self._descriptors = fresh
        self._http.reset()
        self.secrets.forget(set(fresh))

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
        deny_target(descriptor, clean.split("?")[0], scope)
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
            from .mqtt_calls import mqtt_call

            return await mqtt_call(self, descriptor, call, scope)
        if descriptor.kind == "ws":
            from .access_ws import ws_exchange

            return await ws_exchange(self, descriptor, call, scope)
        raise AccessDenied(f"Вызов вида «{descriptor.kind}» идёт другой дорогой")

    async def secret_of(self, descriptor: AccessDescriptor) -> dict[str, str]:
        """Учётка доступа; менеджер недоступен и кэша нет — НЕДОСТУПНОСТЬ, а не отказ."""
        if not descriptor.secret and self.secrets.legacy_for(descriptor.id) is None:
            return {}
        try:
            return await self.secrets.get(descriptor.id, descriptor.secret)
        except Exception as err:  # noqa: BLE001 — любая беда менеджера
            raise AccessUnreachable(f"Учётка доступа недоступна: {err}") from err

    async def mqtt_client(self, descriptor: AccessDescriptor, on_message: Any = None) -> Any:
        """Подключение к брокеру доступа.

        Без `on_message` — ОБЩЕЕ подключение публикаций (одно на доступ, под
        замком: два одновременных вызова не заводят двух клиентов). С
        `on_message` — СВОЁ подключение подписчика.

        ⚠ Идентификатор клиента уникален (случайный хвост): по MQTT 3.1.1
        (3.1.4-2) брокер рвёт прежнее подключение с тем же id, и два источника
        одного доступа выбивали бы друг друга по кругу.
        """
        from secrets import token_hex

        from .mqtt import MqttClient, MqttError

        async def connect(role: str) -> Any:
            secret = await self.secret_of(descriptor)
            client = MqttClient(
                descriptor.host,
                descriptor.port,
                secret.get(descriptor.auth.user_field, ""),
                secret.get(descriptor.auth.pass_field, ""),
                tls=descriptor.tls or descriptor.scheme in ("mqtts", "ssl", "tls"),
                client_id=f"mega_home-{descriptor.id}-{role}-{token_hex(4)}",
                on_message=on_message,
            )
            try:
                await client.connect()
            except MqttError as err:
                raise AccessUnreachable(f"Брокер: {err}") from err
            return client

        if on_message is not None:
            return await connect("sub")
        async with self._mqtt_lock:
            self._check_open()
            client = self._mqtt.get(descriptor.id)
            if client is None or client.closed.is_set():
                client = await connect("pub")
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
        secret = await self.secret_of(descriptor)
        return fill(template, template_values(descriptor, secret, values))

    def _check_open(self) -> None:
        if self._closed:
            # ⚠ Закрытая дверь новых соединений не заводит: иначе задача, не
            # успевшая умереть к выгрузке, создавала бы сессию, которую никто не
            # закроет (ревью 2026-09-19).
            raise AccessUnreachable("Дом перезапускает интеграцию — повторите запрос")

    def _known(self, access: str | None) -> AccessDescriptor:
        self._check_open()
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
        self._closed = True
        await self._http.close()
        for client in list(self._mqtt.values()):
            await client.close()
        self._mqtt.clear()


def deny_target(descriptor: AccessDescriptor, target: str, scope: str) -> None:
    """Запреты описания для пути, топика или сообщения — одни на все виды доступа."""
    # ⚠ Без учёта регистра: часть устройств отвечает на `/Settings` так же, как на
    # `/settings`, и запрет не должен обходиться заглавной буквой.
    folded = target.lower()
    for prefix in descriptor.deny:
        if folded.startswith(prefix.lower()):
            raise AccessDenied(f"{prefix}* через дверь не ходит")
    if scope != SCOPE_MANAGER:
        for prefix in descriptor.manager_only:
            if folded.startswith(prefix.lower()):
                LOGGER.debug("Дверь: %s только для менеджера", prefix)
                raise AccessDenied(f"{prefix}* — только для менеджера")


def _normalized(path: str) -> str:
    """Путь таким, каким его увидит устройство: без «..», «.» и %-обёрток.

    ⚠ Сначала раскрываем проценты, потом убираем точки-сегменты — иначе
    `/%2e%2e/settings/` проедет мимо (проверено на стенде). Схлопываем и
    повторные «/»: запрет по префиксу иначе обходится лишним слэшем.
    """
    import re
    from urllib.parse import unquote

    # Раскрывается только ПУТЬ: строка запроса уходит как есть, иначе законный
    # `?q=a%26b` менял бы смысл на `?q=a&b` (повторное ревью 2026-09-19).
    encoded, sep, tail = path.partition("?")
    head = unquote(encoded)
    if "?" in head or "#" in head:
        raise AccessDenied("В пути закодирован «?» или «#» — через дверь не ходит")
    # ⚠ После ОДНОГО раскрытия процентов в пути их быть не должно. Ревью
    # 2026-09-19: `/a/%252e%252e/settings/` проходил проверку как `%2e%2e`, а
    # yarl раскрывал его ещё раз и схлопывал точки — в сеть уходил `/settings/`.
    # Законному вызову двойное кодирование пути не нужно, поэтому это отказ.
    if re.search(r"%[0-9A-Fa-f]{2}", head):
        raise AccessDenied("Путь закодирован дважды — через дверь не ходит")
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
