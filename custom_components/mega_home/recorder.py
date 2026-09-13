"""Универсальная дверь к регистратору: дом ИСПОЛНЯЕТ описанный вызов.

⚠ Дом не знает ни одного вендора и не разбирает ни одного ответа. Он умеет
ровно две вещи: выполнить запрос, ОПИСАННЫЙ в конфиге объекта, и отдать ответ
как есть. Что значат поля, где тут дни, где шкала, где «эпоха вместо дня» —
решает бандл, который обновляется сам
(`docs/plan-thin-integration.md`, «Широкая дверь»).

⚠ Почему так, а не «словарь команд». Словарь в доме — это код, который нельзя
обновить: каждая новая надобность приложения просит релиза HACS и перезапуска
Home Assistant на КАЖДОМ объекте. Вечер 2026-09-12 показал и обратную сторону:
в шлюз приехало толкование (`day_bounds`, `trassir_now_us`, «эпоха — не день»)
и выпустило два релиза подряд ради того, что чинится бандлом.

⚠ Учётки через эту дверь НЕ ходят: их подставляет дом, он же держит сессию
(`sid`) и подставляет её в запрос. Телефон жильца знает пути, но не пароли.

⚠ Границы широкой двери (политика, а не список команд):
  * адресат — только регистратор из конфига объекта (никакого «сходи по LAN»);
  * запрещены вход, настройки и всё, что меняет состояние регистратора (запрос
    воспроизведения — можно, перенастройку — нет);
  * потолок размера ответа и срок: дверь не превращается в выкачивание;
  * метод — GET/HEAD/POST; тело уходит как есть.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from time import monotonic
from typing import Any

import aiohttp

from .const import LOGGER

# Срок одного вызова: регистратор местный, но искать кадр в архиве может
# подолгу (замер стенда 2026-09-12: снимок отдаётся за 0,7–1,3 с).
CALL_TIMEOUT = 30
# Потолок ответа. Кадр полного размера — ~390 КБ, конфиг регистратора — сотни
# килобайт; восемь мегабайт ловят ошибку «просим не то», а не ограничивают работу.
MAX_RESPONSE_BYTES = 8 * 1024 * 1024
# Пути, закрытые ВСЕГДА, каким бы ни был вендор: вход, настройки и дерево
# объектов (через него правится состояние регистратора, а не воспроизведение).
DENY_ALWAYS = (
    "/login",
    "/settings",
    "/objects",
    "/users",
    # ⚠ Ниже — не воспроизведение, а ДЕЙСТВИЕ на объекте, и политика двери
    # обещает их не пускать: PTZ физически крутит камеру, экспорт пишет файлы
    # на диск регистратора и занимает его очередь (`sdk-archive-export.md`:
    # локальная и удалённая задачи блокируют друг друга). Пускать это в
    # локальный контур дома, где аутентификации нет, нельзя.
    "/ptz",
    "/archive_export",
    "/export_archive",
    "/export_task",
    "/export_cancel",
    "/jit-export",
)
ALLOWED_METHODS = ("GET", "HEAD", "POST")
# Сколько байт ответа смотрим на маркер протухшей сессии: отказ у регистратора
# короткий, а искать строку в восьмимегабайтном кадре незачем.
MAX_MARKER_BYTES = 4096


@dataclass
class RecorderDescriptor:
    """Описание ОДНОГО регистратора объекта — данные, а не код.

    ⚠ Ничего вендорского в питоне: новый регистратор приезжает этим описанием в
    конфиге (его собирает менеджер), и релиза интеграции для этого не нужно.
    """

    id: str
    host: str
    # ⚠ Схема — ДАННЫЕ: у Trassir SDK живёт на HTTPS, у другого регистратора
    # может быть иначе, и знать это дом не обязан. Замер стенда 2026-09-12: по
    # http:// регистратор молча рвёт соединение — «Server disconnected», а
    # жилец видит «Дом не смог выполнить запрос».
    scheme: str = "https"
    port: int = 443
    rtsp_port: int = 554
    vendor: str = ""
    # Как войти: путь, параметры (с {user}/{pass}) и поле ответа с сессией.
    login_path: str = ""
    login_params: dict[str, str] = field(default_factory=dict)
    session_field: str = "sid"
    # Как зовут сессию в запросе: Trassir — `sid` в строке запроса.
    session_param: str = "sid"
    # Как получить поток: путь, параметры, поле с токеном и шаблон адреса.
    stream_path: str = ""
    stream_params: dict[str, str] = field(default_factory=dict)
    stream_field: str = "token"
    stream_url: str = "rtsp://{host}:{rtspPort}/{token}"
    # Дополнительные запреты этого регистратора (к общим).
    deny: tuple[str, ...] = ()
    # Сколько живёт сессия без запросов (Trassir — 15 минут).
    session_ttl: float = 600.0
    # ⚠ По чему видно, что сессия УМЕРЛА. Регистратор отвечает на это
    # ОБЫЧНЫМ 200 и телом `{"error_code":"no session","success":0}` (замер
    # стенда 2026-09-13) — то есть по коду ответа беду не отличить, а дверь
    # отдавала такое тело наружу как есть, и бандл видел пустой календарь и
    # пустую шкалу, ничего не сообщая. Строка — ДАННЫЕ из конфига: другой
    # вендор скажет то же другими словами. Пусто — повторять не по чему.
    session_expired: str = ""


def descriptor_of(block: Any) -> RecorderDescriptor | None:
    """Собрать описание из блока конфига; мусор — «описания нет».

    ⚠ Не бросаем: конфиг дома может быть от менеджера постарше, и падать из-за
    незнакомого поля нельзя — регистратор просто останется без этой двери.
    """
    if not isinstance(block, dict):
        return None
    # ⚠ Пробелы — тот же «не задано»: иначе описание с пустым хостом уехало бы
    # в работу и дверь стучалась бы в никуда (та же проверка у менеджера).
    host = str(block.get("host") or "").strip()
    if not host:
        return None
    def strings(value: Any) -> dict[str, str]:
        if not isinstance(value, dict):
            return {}
        return {str(key): str(item) for key, item in value.items()}
    return RecorderDescriptor(
        id=str(block.get("id") or block.get("vendor") or "recorder"),
        host=host,
        scheme=str(block.get("scheme") or "https"),
        port=int(block.get("port") or 443),
        rtsp_port=int(block.get("rtspPort") or block.get("rtsp_port") or 554),
        vendor=str(block.get("vendor") or ""),
        login_path=str(block.get("login") or ""),
        login_params=strings(block.get("loginParams")),
        session_field=str(block.get("sessionField") or "sid"),
        session_param=str(block.get("sessionParam") or "sid"),
        stream_path=str(block.get("streamPath") or ""),
        stream_params=strings(block.get("streamParams")),
        stream_field=str(block.get("streamField") or "token"),
        stream_url=str(block.get("streamUrl") or "rtsp://{host}:{rtspPort}/{token}"),
        deny=tuple(str(item) for item in (block.get("deny") or ())),
        session_ttl=float(block.get("sessionTtl") or 600),
        session_expired=str(block.get("sessionExpired") or ""),
    )


class RecorderError(Exception):
    """Дверь не выполнила вызов. Дальше важно ПОЧЕМУ — см. наследников."""


class RecorderDenied(RecorderError):
    """Вызов не проходит ПОЛИТИКУ двери — регистратор тут вообще ни при чём.

    ⚠ Это про НАС: бандл попросил путь или метод, которых дверь не пускает.
    Лечится правкой бандла, и молча уходить с этим на прежние пути нельзя —
    иначе запрет виден только в журнале дома.
    """


class RecorderUnreachable(RecorderError):
    """Регистратор не ответил, отказал во входе или порвал соединение.

    ⚠ Разделено с отказом политики намеренно (2026-09-13). Пока обе беды
    приезжали одним 403, приложение считало ЛЮБУЮ из них за «двери нет» и
    уходило на прежние именованные пути — то есть к ТОМУ ЖЕ недоступному
    регистратору, только другой дорогой. Жилец читал «Дом не смог выполнить
    запрос» вместо «Регистратор не отвечает», а инсталлятор шёл искать поломку
    в доме вместо регистратора.
    """


class RecorderCall:
    """Сессии регистраторов и исполнение описанных вызовов."""

    def __init__(self, credentials: Any = None, sid_provider: Any = None) -> None:
        self._descriptors: dict[str, RecorderDescriptor] = {}
        # Учётка на регистратор: тем же маршрутом менеджера, что и раньше.
        self._credentials = credentials
        # ⚠ ЖИВАЯ сессия драйвера этого объекта. Дверь обязана говорить ТОЙ ЖЕ
        # сессией, что открыла поток: замер стенда 2026-09-13 показал, что
        # вторая сессия того же пользователя не видит чужой поток вовсе —
        # `archive_status` (state/timeline/calendar) отдаёт пустой список, а
        # `archive_events` приходит без `CalendarEvent` и `TimelineEvent`.
        # Со своей сессией дверь молча теряла календарь и шкалу суток.
        self._sid_provider = sid_provider
        self._session: aiohttp.ClientSession | None = None
        self._sids: dict[str, tuple[str, float]] = {}

    # --- описание -------------------------------------------------------

    def apply(self, blocks: Any) -> None:
        """Принять описания из конфига объекта (список блоков `recorders`)."""
        self._descriptors = {}
        self._sids.clear()
        if not isinstance(blocks, list):
            return
        for block in blocks:
            descriptor = descriptor_of(block)
            if descriptor is not None:
                self._descriptors[descriptor.id] = descriptor

    def ids(self) -> list[str]:
        return list(self._descriptors)

    def descriptor(self, recorder: str | None) -> RecorderDescriptor | None:
        """Описание по имени; без имени — единственный регистратор объекта."""
        if recorder:
            return self._descriptors.get(recorder)
        if len(self._descriptors) == 1:
            return next(iter(self._descriptors.values()))
        return None

    # --- политика -------------------------------------------------------

    @staticmethod
    def check(descriptor: RecorderDescriptor, method: str, path: str) -> str:
        """Пропустить вызов или объяснить, почему нет; вернуть ПРОВЕРЕННЫЙ путь.

        ⚠ Это ГРАНИЦА двери, и она намеренно простая: запрет по префиксам путей
        и по методу. Список команд не ведём — иначе новая функция приложения
        снова упрётся в релиз, ради чего дверь и переделывалась.

        ⚠ Проверять НАДО ТО, ЧТО УЙДЁТ В СЕТЬ, а не то, что прислали. Замер
        стенда 2026-09-13: `/a/../settings/webserver/` мимо запрета проезжал
        целиком — префикс `/settings` в нём не первый, а yarl приводит путь к
        `/settings/webserver/` уже после проверки, и регистратор отвечал 200.
        На стенде при этом `sdk_settings_write = 1`, то есть той же дырой
        менялись бы НАСТРОЙКИ регистратора, а в локальном контуре дома
        аутентификации нет вовсе — любой в Wi-Fi объекта.
        """
        if method not in ALLOWED_METHODS:
            raise RecorderDenied(f"Метод {method} через дверь не ходит")
        if not path.startswith("/"):
            raise RecorderDenied("Путь начинается с «/»")
        clean = _normalized(path)
        for prefix in (*DENY_ALWAYS, *descriptor.deny):
            if clean.split("?")[0].startswith(prefix):
                raise RecorderDenied(f"{prefix}* через дверь не ходит")
        return clean

    # --- исполнение -----------------------------------------------------

    async def call(
        self,
        recorder: str | None,
        method: str,
        path: str,
        params: dict[str, Any] | None = None,
        body: bytes | None = None,
        session: dict[str, str] | None = None,
    ) -> tuple[int, str, bytes]:
        """Выполнить описанный вызов. Ответ отдаётся КАК ЕСТЬ — без разбора."""
        descriptor = self.descriptor(recorder)
        if descriptor is None:
            raise RecorderDenied("Такого регистратора у объекта нет")
        method = method.upper()
        # ⚠ Дальше идёт ПРОВЕРЕННЫЙ путь, а не присланный: иначе нормализация
        # в сети вернула бы обратно то, что политика только что отвергла.
        path = self.check(descriptor, method, path)

        url = f"{descriptor.scheme}://{descriptor.host}:{descriptor.port}{path}"
        client = await self._client()
        # ⚠ Два захода: первый обычный, второй — со СВЕЖЕЙ сессией, если
        # регистратор сказал, что прежняя умерла. Без повтора такой ответ уезжал
        # бандлу как есть — обычным 200 с телом «no session», — и у жильца молча
        # пустели календарь и шкала, пока не истечёт наш кэш сессии.
        for attempt in (1, 2):
            query = {str(key): str(value) for key, value in (params or {}).items()}
            # ⚠ Сессию подставляет ДОМ: бандл её не видит и не хранит.
            if descriptor.login_path and path.split("?")[0] != descriptor.login_path:
                query[descriptor.session_param] = await self._sid(descriptor, attempt == 2)
            query.update(session or {})
            try:
                async with client.request(
                    method, url, params=query, data=body, timeout=aiohttp.ClientTimeout(total=CALL_TIMEOUT)
                ) as response:
                    payload = await _read_all(response)
                    status, kind = response.status, response.content_type
            except (aiohttp.ClientError, asyncio.TimeoutError) as err:
                raise RecorderUnreachable(_reason(err)) from err
            if attempt == 1 and self._expired(descriptor, payload):
                LOGGER.debug("Регистратор не признал сессию — входим заново")
                continue
            return status, kind, payload
        raise RecorderUnreachable("Регистратор не признал сессию дважды подряд")

    @staticmethod
    def _expired(descriptor: RecorderDescriptor, payload: bytes) -> bool:
        """Сказал ли регистратор, что сессия умерла. Маркер — из описания."""
        if not descriptor.session_expired:
            return False
        return descriptor.session_expired.encode("utf-8") in payload[:MAX_MARKER_BYTES]

    async def _sid(self, descriptor: RecorderDescriptor, fresh: bool = False) -> str:
        """Сессия регистратора — ЖИВАЯ ДРАЙВЕРСКАЯ, если она есть.

        ⚠ Свой вход остаётся только там, где драйвера нет вовсе (регистратор
        описан конфигом, но объектом не настроен). Своя сессия рядом с
        драйверской — это две беды сразу: состояние чужого потока не читается
        (замер выше) и два входа гоняются в запрет «не чаще раза в 5 секунд с
        одного адреса» (`docs/docs-trassir/sdk-session.md`), а он банит АДРЕС,
        то есть роняет видеонаблюдение целиком, а не один запрос.
        """
        if self._sid_provider is not None:
            # ⚠ Провайдер — ЧУЖОЙ код (драйвер), и падает он своими исключениями:
            # `TrassirError` при недоступном регистраторе, `TrassirAuthError`
            # при неверной учётке. До 0.2.49 дверь входила сама и отвечала на это
            # отказом; с провайдером исключение полетело МИМО обработчиков
            # `ops.recorder_call` и стало неперехваченным 500 — то есть архив и
            # календарь умирали целиком всякий раз, когда регистратор просто
            # медленно отвечает (живой отчёт 2026-09-13). Беда регистратора
            # обязана оставаться вердиктом двери.
            try:
                sid = await self._sid_provider(fresh)
            except Exception as err:  # noqa: BLE001 — любое падение драйвера
                raise RecorderUnreachable(_reason(err)) from err
            if sid:
                return str(sid)
        cached = self._sids.get(descriptor.id)
        if cached and cached[1] > monotonic() and not fresh:
            return cached[0]
        if not descriptor.login_path:
            return ""
        creds = await self._login_credentials()
        params = {
            key: value.replace("{user}", creds[0]).replace("{pass}", creds[1])
            for key, value in descriptor.login_params.items()
        }
        status, _, payload = await self._plain(
            descriptor, descriptor.login_path, params
        )
        sid = _field(payload, descriptor.session_field)
        if status != 200 or not sid:
            # ⚠ Отказ ВХОДА — беда регистратора (или учётки объекта), а не
            # политики двери: прежние пути упрутся в него точно так же.
            raise RecorderUnreachable("Регистратор не пустил дом в сессию")
        self._sids[descriptor.id] = (sid, monotonic() + descriptor.session_ttl)
        return sid

    async def _plain(
        self, descriptor: RecorderDescriptor, path: str, params: dict[str, str]
    ) -> tuple[int, str, bytes]:
        """Запрос БЕЗ подстановки сессии — им же входим.

        ⚠ Ошибка связи превращается в ОТКАЗ двери, а не летит наружу: на выходе
        из дома неожиданное исключение менеджер отдаёт жильцу как «Дом не смог
        выполнить запрос» — причину, которой он не видит. Живой отчёт
        2026-09-12: перемотка падала так, потому что дверь стучалась по http://,
        а регистратор на это молча рвёт соединение.
        """
        client = await self._client()
        url = f"{descriptor.scheme}://{descriptor.host}:{descriptor.port}{path}"
        try:
            async with client.get(
                url, params=params, timeout=aiohttp.ClientTimeout(total=CALL_TIMEOUT)
            ) as response:
                return response.status, response.content_type, await response.content.read()
        except (aiohttp.ClientError, asyncio.TimeoutError) as err:
            raise RecorderUnreachable(_reason(err)) from err

    async def stream_url(self, camera: str, quality: str) -> str:
        """Адрес потока по описанию: дом идёт за токеном сам, шаблон — из данных."""
        descriptor = self.descriptor(None)
        if descriptor is None or not descriptor.stream_path:
            raise RecorderDenied("Регистратор объекта не описан")
        params = {
            key: value.replace("{camera}", camera).replace("{quality}", quality)
            for key, value in descriptor.stream_params.items()
        }
        status, _, payload = await self._read_with_session(descriptor, descriptor.stream_path, params)
        token = _field(payload, descriptor.stream_field)
        if status != 200 or not token:
            raise RecorderUnreachable("Регистратор не выдал поток")
        return descriptor.stream_url.format(
            host=descriptor.host, rtspPort=descriptor.rtsp_port, token=token
        )

    async def _read_with_session(
        self, descriptor: RecorderDescriptor, path: str, params: dict[str, str]
    ) -> tuple[int, str, bytes]:
        query = dict(params)
        if descriptor.login_path:
            query[descriptor.session_param] = await self._sid(descriptor)
        return await self._plain(descriptor, path, query)

    async def _login_credentials(self) -> tuple[str, str]:
        if self._credentials is None:
            raise RecorderDenied("Учётка регистратора недоступна")
        return await self._credentials()

    async def _client(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            # ⚠ Сертификат регистратора САМОПОДПИСАННЫЙ, и проверять его нечем:
            # доверие здесь держится на том, что адрес взят ИЗ КОНФИГА объекта, а
            # не из запроса приложения (решение заказчика 2026-09-12).
            #
            # ⚠ Так же поступает драйвер (`trassir_client.py`, `ssl=False` в
            # трёх местах): дверь и драйвер говорят с ОДНИМ И ТЕМ ЖЕ
            # регистратором, и разная строгость у них означала бы, что дверь не
            # подключается там, где драйвер работает (живой отчёт: «Дом не смог
            # выполнить запрос» — дверь стучалась по http, а по https её
            # останавливала проверка сертификата).
            self._session = aiohttp.ClientSession(
                connector=aiohttp.TCPConnector(ssl=False)
            )
        return self._session

    async def async_close(self) -> None:
        if self._session is not None and not self._session.closed:
            await self._session.close()
        self._session = None
        self._sids.clear()


async def _read_all(response: Any) -> bytes:
    """Тело ЦЕЛИКОМ, но не больше потолка.

    ⚠ `content.read(N)` НЕ читает N байт — он отдаёт то, что уже лежит в буфере,
    и на потоковом ответе это ПЕРВЫЙ КУСОК. Живой прогон настоящего кода против
    настоящего регистратора 2026-09-13: `/archive_status?type=calendar` вернулся
    двумя байтами — `[\n`. Дальше бандл честно разбирал этот огрызок, не находил
    своего токена и показывал пустой календарь и пустую шкалу. Беда
    ПЛАВАЮЩАЯ: короткий ответ успевает прийти одним куском и тогда всё работает,
    а длинный (124 дня календаря, сотни участков шкалы) — нет.

    ⚠ Поэтому читаем кусками до конца и проверяем потолок ПО ХОДУ: иначе
    «потолок» защищал бы от большого ответа тем, что молча портил любой.
    """
    куски: list[bytes] = []
    всего = 0
    async for кусок in response.content.iter_chunked(64 * 1024):
        всего += len(кусок)
        if всего > MAX_RESPONSE_BYTES:
            raise RecorderDenied("Ответ регистратора больше потолка двери")
        куски.append(кусок)
    return b"".join(куски)


def _normalized(path: str) -> str:
    """Путь таким, каким его увидит регистратор: без «..», «.» и %-обёрток.

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
    # против значения), и нормализация не имеет права менять смысл запроса —
    # только убрать обходы запрета.
    tailing = "/" if head.endswith("/") and out else ""
    return "/" + "/".join(out) + tailing + (sep + tail if sep else "")


def _reason(err: Exception) -> str:
    """Причина отказа словами — у сетевых ошибок сообщение часто пустое."""
    text = str(err).strip()
    return f"Регистратор не отвечает: {text}" if text else "Регистратор не отвечает"


def _field(payload: bytes, name: str) -> str:
    """Поле ответа по имени — единственный разбор, который двери позволен.

    ⚠ Это не толкование: имя поля пришло в описании, значение уходит наружу как
    есть. Всё остальное (даты, шкалы, сутки) разбирает бандл.
    """
    if not name or not payload:
        return ""
    import json

    try:
        data = json.loads(payload.decode("utf-8", "ignore"))
    except ValueError:
        LOGGER.debug("Регистратор ответил не-JSON на запрос описания")
        return ""
    if isinstance(data, dict):
        value = data.get(name)
        return str(value) if isinstance(value, (str, int)) else ""
    return ""
