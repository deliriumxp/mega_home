"""The home's own view of its TRASSIR recorder: settings, credentials, events.

The manager knows WHERE the recorder is and WHO to log in as; everything that
happens with it at runtime happens here, next to it. That split is the whole
point of the feature (docs/trassir-integration-plan.md in the manager repo): the
manager has no route into the object, and the resident's app never talks to the
recorder directly — it asks this integration.

What this module owns:

* the settings block (`trassir` in the home config) and the credentials, which
  arrive by a SEPARATE request because the config body is handed to the
  resident's browser as-is;
* one poller of `/events`, because TRASSIR has no push of any kind;
* the deduplicated, trimmed feed those events become.

⚠ Timestamps are kept EXACTLY as TRASSIR gave them. Its microseconds are unix
time shifted by the server's timezone, so they are only meaningful to that
server — and handing them straight back is what makes an event's clip open on
the right second. Converting them "to real time" is the bug this note exists to
prevent; our own clock is what needs converting, in the other direction.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any

from homeassistant.core import HomeAssistant
from homeassistant.helpers.storage import Store

from .api import ManagerClient, ManagerError
from .const import (
    LOGGER,
    TRASSIR_CHANNELS_TTL,
    TRASSIR_THUMB_CAP,
    TRASSIR_THUMB_TTL,
    TRASSIR_THUMB_WIDTH,
    TRASSIR_EVENT_CAP,
    TRASSIR_EVENT_RETENTION,
    TRASSIR_POLL_INTERVAL,
    TRASSIR_STORAGE_KEY,
    STORAGE_VERSION,
)
from .trassir_clip import ClipSessions
from .trassir_client import TrassirClient, TrassirError

try:  # Pillow приезжает вместе с Home Assistant; на голом чекауте его может не быть.
    from PIL import Image
except ImportError:  # pragma: no cover - на объекте эта ветка не наступает
    Image = None  # type: ignore[assignment]

DEFAULT_PORT = 8080
DEFAULT_RTSP_PORT = 555
DEFAULT_CLIP_SECONDS = 60


class TrassirGateway:
    """Everything this home does with its recorder, and nothing it does not."""

    def __init__(self, hass: HomeAssistant, manager: ManagerClient, session: Any) -> None:
        self._hass = hass
        self._manager = manager
        self._session = session
        self._store = Store[dict[str, Any]](hass, STORAGE_VERSION, TRASSIR_STORAGE_KEY)
        self._settings: dict[str, Any] = {}
        self._creds: dict[str, str] = {}
        self._fingerprint: str = ""
        self._client: TrassirClient | None = None
        self._task: asyncio.Task[None] | None = None
        self._events: list[dict[str, Any]] = []
        self._seen: set[str] = set()
        self._channels: list[dict[str, Any]] = []
        self._channels_at = 0.0
        # Превью событий: маленький кэш в памяти, а не на диске. Лента листается
        # вверх-вниз, и один и тот же кадр спрашивают несколько раз подряд; на
        # диск это класть незачем — событие живёт неделю, а интерес к нему минуты.
        self._thumbs: dict[str, tuple[float, bytes]] = {}
        # Открытые записи: свой модуль, потому что это ДРУГАЯ тема — сеанс
        # просмотра, а не лента (`trassir_clip.py`).
        self.clips = ClipSessions(self)
        # Одна строка в журнал на СМЕНУ состояния, а не на каждую неудачу: опрос
        # идёт каждые пять секунд, и объект без связи с регистратором иначе
        # засыпал бы лог быстрее, чем его читают.
        self.last_error: str | None = None

    @property
    def hass(self) -> HomeAssistant:
        """Home Assistant этого дома — сеансам клипов нужен он же."""
        return self._hass

    # --- жизненный цикл -------------------------------------------------

    async def async_load(self) -> None:
        """Restore credentials and the feed from disk."""
        stored = await self._store.async_load() or {}
        self._creds = stored.get("credentials") or {}
        self._fingerprint = stored.get("fingerprint") or ""
        self._events = [e for e in (stored.get("events") or []) if isinstance(e, dict)]
        self._seen = {e["id"] for e in self._events if isinstance(e.get("id"), str)}

    async def async_apply(self, config: dict[str, Any]) -> None:
        """Take the `trassir` block of a freshly synchronised config."""
        block = config.get("trassir")
        if not isinstance(block, dict) or not block.get("host"):
            await self.async_stop()
            self._settings = {}
            self._client = None
            return

        settings = {
            "host": str(block.get("host")),
            "port": int(block.get("port") or DEFAULT_PORT),
            "rtspPort": int(block.get("rtspPort") or DEFAULT_RTSP_PORT),
            "clipSeconds": int(block.get("clipSeconds") or DEFAULT_CLIP_SECONDS),
        }
        fingerprint = str(block.get("credentials") or "")
        # ⚠ Учётка перечитывается ровно по отпечатку. Тянуть её каждый раз —
        # лишний поход к менеджеру, а не тянуть никогда — тихо работать со
        # сменённым паролем до перезапуска Home Assistant.
        if fingerprint != self._fingerprint or not self._creds:
            try:
                self._creds = await self._manager.async_trassir_credentials()
                self._fingerprint = fingerprint
                await self._async_save()
            except ManagerError as err:
                if not self._creds:
                    LOGGER.warning("Учётку Trassir получить не удалось: %s", err)
                    return
                # Старая учётка лучше остановки: пароль меняют редко, а связь с
                # менеджером пропадает регулярно.
                LOGGER.warning("Учётка Trassir не обновилась, работаем прежней: %s", err)

        changed = settings != self._settings
        self._settings = settings
        if changed or self._client is None:
            self._client = TrassirClient(
                self._session,
                settings["host"],
                settings["port"],
                self._creds.get("username", ""),
                self._creds.get("password", ""),
                self._creds.get("sdkPassword", ""),
            )
            self._channels = []
            self._channels_at = 0.0
        self._start()

    def _start(self) -> None:
        if self._task and not self._task.done():
            return
        self._task = self._hass.async_create_background_task(
            self._async_poll(), "mega_home_trassir_events"
        )

    async def async_stop(self) -> None:
        """Stop polling. Idempotent — unloading calls it unconditionally."""
        if self._task:
            self._task.cancel()
            self._task = None

    # --- чтение (этап D спрашивает отсюда) ------------------------------

    @property
    def configured(self) -> bool:
        """True when the object has a recorder AND we can log into it."""
        return bool(self._settings.get("host") and self._client)

    @property
    def settings(self) -> dict[str, Any]:
        """Address, ports and clip length, with the manager's defaults applied."""
        return dict(self._settings)

    @property
    def client(self) -> TrassirClient | None:
        """The SDK client, or None while the object has no recorder."""
        return self._client

    def events(
        self, guid: str | None = None, limit: int = 50, before: int | None = None
    ) -> list[dict[str, Any]]:
        """Newest first, optionally one camera, optionally older than `before`."""
        rows = [e for e in self._events if guid is None or e.get("guid") == guid]
        if before is not None:
            rows = [e for e in rows if int(e.get("timestampUs", 0)) < before]
        rows.sort(key=lambda e: int(e.get("timestampUs", 0)), reverse=True)
        return rows[: max(1, min(limit, 200))]

    def event(self, event_id: str) -> dict[str, Any] | None:
        """One event by the id this gateway gave it."""
        return next((e for e in self._events if e.get("id") == event_id), None)

    async def async_cameras(self, tiles: dict[str, str] | None = None) -> list[dict[str, Any]]:
        """Channels of the recorder, in the shape the app needs.

        ⚠ `codec` comes from `/channels` and only from there: the SDP of the
        RTSP stream announces H264 even for channels that send H265, and a
        WebRTC viewer that trusts it shows a black rectangle.

        ⚠ `tile` is how the app learns that a camera it already shows has an
        event feed. It is resolved from the camera's own STREAM ADDRESS, which
        carries the channel guid (`rtsp://host:555/<guid>_m/`) — not by matching
        names. Names get edited on both sides and would silently pair the wrong
        camera with the wrong recording, which is worse than no pairing at all.
        """
        by_guid = tiles or {}
        return [
            {
                "guid": channel.get("guid"),
                "name": channel.get("name"),
                "codec": channel.get("codec"),
                "hasArchive": self._has_archive(channel),
                "tile": by_guid.get(channel.get("guid")),
            }
            for channel in await self._async_channels()
            if channel.get("guid")
        ]

    async def async_thumb(self, event_id: str) -> bytes:
        """Превью одного события — кадр архива на его секунду.

        ⚠ Кадр УМЕНЬШАЕТСЯ здесь. Trassir отдаёт полноразмерный JPEG (~530 КБ
        на стенде) и параметров размера не понимает вовсе: экран из двадцати
        событий, отданный как есть, это десять мегабайт по Wi-Fi жильца — а
        снаружи ещё и через канал менеджера.

        ⚠ Метка события уходит в запрос КАК ЕСТЬ: она в шкале самого Trassir
        (unix + смещение пояса сервера), и «починка» её нашими часами сдвинула
        бы кадр на этот самый пояс.
        """
        event = self.event(event_id)
        if event is None:
            raise TrassirError("Событие не найдено")
        cached = self._thumbs.get(event_id)
        if cached and cached[0] > time.monotonic():
            return cached[1]
        if not self._client:
            raise TrassirError("Видеонаблюдение объекта не настроено")
        raw = await self._client.async_screenshot(event["guid"], event["timestampUs"])
        small = await self._hass.async_add_executor_job(_shrink, raw)
        if len(self._thumbs) >= TRASSIR_THUMB_CAP:
            oldest = min(self._thumbs, key=lambda key: self._thumbs[key][0])
            self._thumbs.pop(oldest, None)
        self._thumbs[event_id] = (time.monotonic() + TRASSIR_THUMB_TTL, small)
        return small

    # --- опрос ----------------------------------------------------------

    async def _async_poll(self) -> None:
        while True:
            try:
                await self._async_poll_once()
            except asyncio.CancelledError:
                raise
            except TrassirError as err:
                self._note(str(err))
            except Exception as err:  # noqa: BLE001 — цикл не имеет права умереть
                self._note(f"неожиданная ошибка опроса Trassir: {err}")
            await asyncio.sleep(TRASSIR_POLL_INTERVAL)

    async def _async_poll_once(self) -> None:
        if not self._client:
            return
        raw = await self._client.async_events()
        self._note(None)
        if not raw:
            return
        names = {c.get("guid"): c.get("name") for c in await self._async_channels()}
        added = 0
        for item in raw:
            row = self._row(item, names)
            if row is None or row["id"] in self._seen:
                continue
            self._seen.add(row["id"])
            self._events.append(row)
            added += 1
        if not added:
            return
        self._trim()
        # Отложенная запись, а не немедленная: опрос идёт каждые пять секунд, и
        # запись на каждое движение перед камерой — это износ флешки объекта.
        self._store.async_delay_save(self._snapshot, 60)

    def _row(self, item: Any, names: dict[Any, Any]) -> dict[str, Any] | None:
        if not isinstance(item, dict):
            return None
        try:
            timestamp = int(item.get("timestamp"))
        except (TypeError, ValueError):
            return None
        guid = item.get("origin")
        kind = item.get("type")
        if not isinstance(guid, str) or not isinstance(kind, str):
            return None
        return {
            # ⚠ Ключ дедупликации И есть идентификатор события: своего id
            # TRASSIR не даёт, а перелогин повторяет до сотни событий заново.
            "id": f"{timestamp}-{guid}-{kind.replace(' ', '_')}",
            "type": kind,
            "guid": guid,
            "cameraName": names.get(guid) or guid,
            "timestampUs": timestamp,
        }

    def _trim(self) -> None:
        # Возраст считаем в шкале самого TRASSIR: сравнивать его метку с нашими
        # часами нельзя, они сдвинуты друг относительно друга.
        newest = max(int(e.get("timestampUs", 0)) for e in self._events)
        floor = newest - TRASSIR_EVENT_RETENTION * 1_000_000
        rows = [e for e in self._events if int(e.get("timestampUs", 0)) >= floor]
        rows.sort(key=lambda e: int(e.get("timestampUs", 0)))
        self._events = rows[-TRASSIR_EVENT_CAP:]
        self._seen = {e["id"] for e in self._events}

    async def _async_channels(self) -> list[dict[str, Any]]:
        if self._channels and time.monotonic() - self._channels_at < TRASSIR_CHANNELS_TTL:
            return self._channels
        if not self._client:
            return []
        try:
            self._channels = await self._client.async_channels()
            self._channels_at = time.monotonic()
        except TrassirError as err:
            # Имя камеры — украшение ленты, а не её условие: без списка каналов
            # события всё равно доедут, подписанные своим guid.
            LOGGER.debug("Список каналов Trassir недоступен: %s", err)
        return self._channels

    @staticmethod
    def _has_archive(channel: dict[str, Any]) -> bool:
        """Бит 2 маски `rights` по доке DSSL.

        ⚠ Подсказка, а не запрет: на живом регистраторе маска не сошлась с
        документированной раскладкой (снят бит «экспорт и скриншоты» при
        работающих скриншотах). Поэтому неизвестное значение считаем «архив
        есть» — отказ всё равно ловится по факту, а маска, понятая неверно,
        спрятала бы от жильца работающую камеру.
        """
        if "rights" not in channel:
            return True
        try:
            return bool(int(channel["rights"]) & 2)
        except (TypeError, ValueError):
            return True

    def _snapshot(self) -> dict[str, Any]:
        return {
            "credentials": self._creds,
            "fingerprint": self._fingerprint,
            "events": self._events,
        }

    async def _async_save(self) -> None:
        await self._store.async_save(self._snapshot())

    def _note(self, error: str | None) -> None:
        if error == self.last_error:
            return
        if error:
            LOGGER.warning("Опрос событий Trassir не идёт: %s", error)
        else:
            LOGGER.info("Опрос событий Trassir пошёл")
        self.last_error = error


def _shrink(raw: bytes) -> bytes:
    """Ужать кадр до ширины превью. Не вышло — отдаём как есть.

    ⚠ Отказ уменьшить не должен ронять ленту: без превью событие остаётся
    событием, а без ленты жилец не видит ничего.
    """
    if Image is None:
        return raw
    from io import BytesIO

    try:
        with Image.open(BytesIO(raw)) as image:
            if image.width <= TRASSIR_THUMB_WIDTH:
                return raw
            height = round(image.height * TRASSIR_THUMB_WIDTH / image.width)
            small = image.convert("RGB").resize((TRASSIR_THUMB_WIDTH, height))
            buffer = BytesIO()
            small.save(buffer, format="JPEG", quality=70, optimize=True)
            return buffer.getvalue()
    except Exception as err:  # noqa: BLE001 - битый кадр не стоит ленты
        LOGGER.debug("Превью события не уменьшилось: %s", err)
        return raw
