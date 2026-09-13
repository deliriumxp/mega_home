"""SDK client for one TRASSIR recorder standing next to this home.

Everything here is local: the recorder lives on the object's own LAN and this
integration is the only thing that talks to it. The manager never does — it has
no route to the object (docs/trassir-integration-plan.md in the manager repo).

⚠ TWO identities, and neither covers everything. Verified against a live
recorder (4.8.2.0): a session opened with a USER login serves `/channels`,
`/get_video` and `archive_command` but answers `no session` on `/events`, while
a session opened with the SDK PASSWORD (login without a username — TRASSIR calls
that user "Script") is the exact opposite. So "no session" on the event feed is
not an expired sid; it is the wrong door, and the log has to say so, or the next
person spends an afternoon on reconnect logic.

⚠ The recorder's certificate is self-signed, so TLS verification is off here and
only here: the address is a private one the installer typed in.
"""

from __future__ import annotations

import asyncio
import json
import time
from typing import Any

import aiohttp

from .const import (
    LOGGER,
    TRASSIR_EVENTS_TIMEOUT,
    TRASSIR_LOGIN_GAP,
    TRASSIR_SESSION_TTL,
    TRASSIR_TIMEOUT,
)

# Which door a request goes through. `USER` is the operator account, `SDK` is
# the "Script" identity behind the SDK password.
USER = "user"
SDK = "sdk"


class TrassirError(Exception):
    """The recorder could not be reached or refused the request."""


class TrassirAuthError(TrassirError):
    """Credentials were rejected, or the wrong identity was used."""



def _net_text(err: Exception, prefix: str, seconds: float = TRASSIR_TIMEOUT) -> str:
    """Сетевой текст для человека — С ЗАПАСНЫМ, а не каким попало.

    ⚠ str(TimeoutError) ПУСТ: подставить его в шаблон — значит отгрузить в
    журнал «Trassir не отвечает: » с висящим разделителем и без причины.
    Та же ловушка, что у RouterOS (пустой message у сетевых ошибок): запасной
    текст обязателен.
    """
    detail = str(err).strip()
    return (
        f"{prefix}: {detail}"
        if detail
        else f"{prefix}: нет ответа за {seconds:.0f} с"
    )


class TrassirClient:
    """One recorder, two sessions, no interpretation of what comes back."""

    def __init__(
        self,
        session: aiohttp.ClientSession,
        host: str,
        port: int,
        username: str,
        password: str,
        sdk_password: str,
    ) -> None:
        self._session = session
        self._base = f"https://{host}:{port}"
        self._username = username
        self._password = password
        self._sdk_password = sdk_password
        self._sids: dict[str, tuple[str, float]] = {}
        self._locks = {USER: asyncio.Lock(), SDK: asyncio.Lock()}
        # ⚠ ONE gate for both doors: TRASSIR bans the IP for logging in more
        # often than once every five seconds, and it counts the address, not the
        # account. Two independent limiters would race each other into the ban.
        self._login_gate = asyncio.Lock()
        self._last_login = 0.0

    # --- public surface -------------------------------------------------

    async def async_channels(self) -> list[dict[str, Any]]:
        """Every channel of the recorder, as the recorder describes it.

        ⚠ The codec reported here is the ONLY trustworthy one: the SDP of the
        RTSP stream announces H264 even for channels that really send H265.
        """
        payload = await self._json("channels", USER)
        channels = payload.get("channels")
        return channels if isinstance(channels, list) else []

    async def async_events(self) -> list[dict[str, Any]]:
        """Events that happened SINCE THE PREVIOUS CALL on this session.

        ⚠ Not "the last N events": the feed is a per-session queue and it is
        drained by reading it. A fresh session receives a backlog capped at 100,
        which is why the caller must deduplicate — a relogin replays them.

        ⚠ И это ДЛИННЫЙ ОПРОС: сервер держит соединение, пока событий нет
        (замер стенда 2026-09-13 — 19.5, 43.1 и 46.7 с подряд). Свой срок ему
        нужен отдельный и заведомо больший: оборвав опрос, мы не просто пишем в
        журнал «Trassir не отвечает» — мы ТЕРЯЕМ события, которые сервер уже
        счёл отданными (замер: 7 из 8 за минуту).
        """
        payload = await self._request("events", SDK, _timeout=TRASSIR_EVENTS_TIMEOUT)
        return payload if isinstance(payload, list) else []

    async def async_screenshot(self, guid: str, timestamp: int | str | None = None) -> bytes:
        """One JPEG frame: from the archive with a timestamp, live without one.

        ⚠ A timestamp in the FUTURE silently returns the live frame instead of
        an error, so "the archive does not go back that far" looks exactly like
        success. And the frame is the full-size one (~530 KB on the stand):
        whoever shows a list of these has to shrink them first.
        """
        params: dict[str, Any] = {}
        if timestamp is not None:
            params["timestamp"] = timestamp
        return await self._bytes(f"screenshot/{guid}", USER, **params)

    async def async_get_video(
        self, guid: str, stream: str = "archive_main", container: str = "rtsp"
    ) -> str:
        """Open a video session and return its EPHEMERAL token."""
        payload = await self._json("get_video", USER, channel=guid, stream=stream, container=container)
        token = payload.get("token")
        if not isinstance(token, str) or not token:
            raise TrassirError("Trassir не выдал токен видеопотока")
        return token

    async def async_archive_command(
        self, token: str, command: str = "play", **params: Any
    ) -> dict[str, Any]:
        """Drive the archive of an ALREADY OPEN stream.

        ⚠ Order matters and the recorder will not explain it: token → someone
        opens the stream → this command. Called before the stream is open it
        answers `stream is expired`, which reads like a timeout and is not one.
        """
        # ⚠ Пустые параметры ОПУСКАЕМ, а не сериализуем: `str(None)` даёт
        # литерал "None", и регистратор отвечает `timestamp format is not
        # valid` (замер стенда 2026-09-13 — `play` со `stop=None`). Та же
        # ловушка, что у RouterOS (`toParams`), и лечится тем же guard'ом.
        clean = {key: value for key, value in params.items() if value is not None}
        return await self._json("archive_command", USER, command=command, token=token, **clean)

    async def async_archive_events(self, token: str) -> list[dict[str, Any]]:
        """События ОТКРЫТОГО архива: календарь дней, шкала суток, движение.

        ⚠ Это не «список событий», а УВЕДОМЛЕНИЯ о состоянии архива, и часть
        их приходит РОВНО ОДИН РАЗ на открытие потока: замер стенда 2026-09-12 —
        `CalendarEvent` есть только в первом ответе после открытия, дальше его
        нет вовсе. Значит читать их должен тот, кто держит поток и ПОМНИТ
        прочитанное, а не «запросить при отрисовке».

        ⚠ И требует ОТКРЫТОГО потока: без потребителя в ответе один
        `SpeedLimitEvent` — ни календаря, ни шкалы (проверено тремя прогонами).
        """
        payload = await self._request("archive_events", USER, token=token)
        return payload if isinstance(payload, list) else []

    async def async_archive_status(self, kind: str = "timeline") -> list[dict[str, Any]]:
        """Состояние архива ОТКРЫТЫХ потоков этой сессии.

        ⚠ Спрашивается ПОСЛЕ того, как поток открыт и команда отдана: своего
        параметра «по какому каналу» у запроса нет вовсе — ответ приходит
        списком по токенам сессии, и нужный ищет вызывающий.

        ⚠ `type=timeline` отдаёт участки записи ЗА СУТКИ `day_start` в СЕКУНДАХ
        от начала дня. Замер офисного регистратора 2026-09-09: запись ведётся по
        движению, фрагменты по 6-8 секунд с дырами в минуты — то есть «дыра в
        записи» это норма объекта, а не поломка, и показать её обязано
        приложение.
        """
        payload = await self._request("archive_status", USER, type=kind)
        return payload if isinstance(payload, list) else []

    async def async_sid(self, door: str = USER, fresh: bool = False) -> str:
        """Живая сессия этого регистратора — ОДНА на весь дом.

        ⚠ Зачем она наружу: универсальная дверь (`recorder.py`) говорит с ТЕМ ЖЕ
        регистратором, и своя сессия ей не годится. Замер стенда 2026-09-13
        (`archive_status?type=state|timeline|calendar`, вторая сессия того же
        `Admin`): поток, открытый ПЕРВОЙ сессией, ВТОРАЯ не видит вовсе —
        ответ пустой список, а `archive_events` отдаёт только `StateTransition`
        и `TimeChanged`, без `CalendarEvent` и `TimelineEvent`. То есть
        календарь и шкала суток через дверь со своей сессией не работают
        НИКОГДА, а выглядит это как «регистратор не отдаёт дни».

        ⚠ Команды с явным токеном (`archive_command`) — исключение: они проходят
        и чужой сессией (тот же замер). Но раз состояние доступно только
        владельцу потока, сессия у дома должна быть одна: ещё и потому, что
        вход чаще раза в 5 секунд с одного адреса Trassir банит (`sdk-session.md`),
        а два независимых входа гоняются друг с другом именно в этот запрет.
        """
        if fresh:
            # Прежнюю регистратор уже отверг — выкидываем её, а не отдаём снова.
            self._sids.pop(door, None)
        return await self._async_sid(door)

    async def async_ping(self, token: str) -> None:
        """Keep a video token alive (documented as 10 s without traffic).

        ⚠ Plain HTTP on the video port, which speaks both RTSP and HTTP. On the
        stand a token survived 25 s of silence anyway — that is the recorder
        being generous, not a contract, so the ping stays.
        """
        url = self._base.replace("https://", "http://").rsplit(":", 1)[0] + f":555/{token}?ping"
        try:
            async with self._session.get(
                url, timeout=aiohttp.ClientTimeout(total=TRASSIR_TIMEOUT), ssl=False
            ) as response:
                await response.read()
        except (aiohttp.ClientError, asyncio.TimeoutError) as err:
            raise TrassirError(
                _net_text(err, "Trassir не отвечает на продление токена")
            ) from err

    # --- transport ------------------------------------------------------

    async def _json(self, path: str, door: str, **params: Any) -> dict[str, Any]:
        payload = await self._request(path, door, **params)
        if not isinstance(payload, dict):
            raise TrassirError(f"Trassir ответил неожиданным телом на {path}")
        return payload

    async def _request(
        self, path: str, door: str, _timeout: float = TRASSIR_TIMEOUT, **params: Any
    ) -> Any:
        """One SDK call, with a single retry after re-authenticating.

        ⚠ `_timeout` отдельным параметром, а не одним на всех: `/events` —
        длинный опрос, и общий срок обрывал бы его, теряя события.
        """
        for attempt in (1, 2):
            sid = await self._async_sid(door)
            try:
                async with self._session.get(
                    f"{self._base}/{path}",
                    params={**{k: str(v) for k, v in params.items()}, "sid": sid},
                    timeout=aiohttp.ClientTimeout(total=_timeout),
                    ssl=False,
                ) as response:
                    body = await response.json(content_type=None)
            except (aiohttp.ClientError, asyncio.TimeoutError) as err:
                raise TrassirError(_net_text(err, "Trassir не отвечает", _timeout)) from err
            except ValueError as err:
                raise TrassirError(f"Trassir ответил не-JSON на {path}") from err
            if self._is_no_session(body):
                self._sids.pop(door, None)
                if attempt == 1:
                    continue
                raise TrassirAuthError(self._no_session_hint(door, path))
            if isinstance(body, dict) and body.get("success") in (0, "0"):
                raise TrassirError(
                    f"Trassir отклонил запрос {path}: {body.get('error_code') or 'без причины'}"
                )
            return body
        raise TrassirError(f"Trassir не ответил на {path}")

    async def _bytes(self, path: str, door: str, **params: Any) -> bytes:
        for attempt in (1, 2):
            sid = await self._async_sid(door)
            try:
                async with self._session.get(
                    f"{self._base}/{path}",
                    params={**{k: str(v) for k, v in params.items()}, "sid": sid},
                    timeout=aiohttp.ClientTimeout(total=TRASSIR_TIMEOUT),
                    ssl=False,
                ) as response:
                    payload = await response.read()
            except (aiohttp.ClientError, asyncio.TimeoutError) as err:
                raise TrassirError(_net_text(err, "Trassir не отвечает")) from err
            # An error comes back as JSON even where bytes were asked for.
            if payload[:1] == b"{" and b"no session" in payload:
                self._sids.pop(door, None)
                if attempt == 1:
                    continue
                raise TrassirAuthError(self._no_session_hint(door, path))
            # ⚠ И ЛЮБОЙ другой отказ приезжает так же: JSON в теле, статус 200,
            # а вызывающий ждёт картинку. Замер офисного регистратора
            # 2026-09-09: событие сервера (вход пользователя в Trassir) даёт
            # `{"error_code":"channel not found","success":0}` — 46 байт,
            # которые уезжали жильцу как `image/jpeg` и рисовались битым
            # значком. Пусть это будет ошибкой, а не «картинкой».
            if payload[:1] == b"{" and b'"success"' in payload:
                raise TrassirError(_error_text(payload, path))
            return payload
        raise TrassirError(f"Trassir не ответил на {path}")

    async def _async_sid(self, door: str) -> str:
        cached = self._sids.get(door)
        if cached and cached[1] > time.monotonic():
            return cached[0]
        async with self._locks[door]:
            cached = self._sids.get(door)
            if cached and cached[1] > time.monotonic():
                return cached[0]
            sid = await self._async_login(door)
            self._sids[door] = (sid, time.monotonic() + TRASSIR_SESSION_TTL)
            return sid

    async def _async_login(self, door: str) -> str:
        if door == SDK and not self._sdk_password:
            raise TrassirAuthError(
                "Не задан пароль SDK Trassir — без него сервер не отдаёт события"
            )
        params = (
            {"password": self._sdk_password}
            if door == SDK
            else {"username": self._username, "password": self._password}
        )
        async with self._login_gate:
            wait = TRASSIR_LOGIN_GAP - (time.monotonic() - self._last_login)
            if wait > 0:
                # Logging in more often than that gets the address banned, and a
                # banned address takes the whole feature down, not one request.
                await asyncio.sleep(wait)
            try:
                async with self._session.get(
                    f"{self._base}/login",
                    params=params,
                    timeout=aiohttp.ClientTimeout(total=TRASSIR_TIMEOUT),
                    ssl=False,
                ) as response:
                    raw = await response.read()
            except (aiohttp.ClientError, asyncio.TimeoutError) as err:
                raise TrassirError(_net_text(err, "Trassir не отвечает на вход")) from err
            finally:
                self._last_login = time.monotonic()
        sid = self._sid_of(raw)
        if not sid:
            raise TrassirAuthError(
                "Trassir отклонил пароль SDK" if door == SDK
                else "Trassir отклонил логин или пароль"
            )
        LOGGER.debug("Opened a %s session with Trassir", door)
        return sid

    @staticmethod
    def _sid_of(raw: bytes) -> str:
        """⚠ An unset SDK password answers with an EMPTY body, not an error."""
        import json

        try:
            body = json.loads(raw.decode() or "{}")
        except ValueError:
            return ""
        sid = body.get("sid") if isinstance(body, dict) else None
        return sid if isinstance(sid, str) else ""

    @staticmethod
    def _is_no_session(body: Any) -> bool:
        return isinstance(body, dict) and body.get("error_code") == "no session"

    @staticmethod
    def _no_session_hint(door: str, path: str) -> str:
        if door == SDK:
            return (
                f"Trassir не пустил к {path}: проверьте пароль SDK "
                "(Настройки → Веб-сервер → SDK) — учётка пользователя сюда не подходит"
            )
        return f"Trassir не пустил к {path}: проверьте логин и пароль пользователя"


def _error_text(payload: bytes, path: str) -> str:
    """Отказ регистратора, пришедший телом там, где ждали байты."""
    try:
        code = json.loads(payload.decode("utf8", "replace")).get("error_code")
    except (ValueError, AttributeError):
        code = None
    return f"Trassir отказал в {path}: {code}" if code else f"Trassir не отдал {path}"
