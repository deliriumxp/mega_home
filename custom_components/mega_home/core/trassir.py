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

from .api import ManagerClient, ManagerError
from .const import (
    LOGGER,
    TRASSIR_CHANNELS_TTL,
    TRASSIR_THUMB_CAP,
    TRASSIR_THUMB_LEAD,
    TRASSIR_THUMB_TTL,
    TRASSIR_EVENT_CAP,
    TRASSIR_EVENT_RETENTION,
    TRASSIR_POLL_INTERVAL,
    TRASSIR_STORAGE_KEY,
    STORAGE_VERSION,
)
from .gateway import AccessDenied, AccessGateway
from .host import Host
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

    def __init__(self, env: Host, manager: ManagerClient, session: Any) -> None:
        self._env = env
        self._manager = manager
        self._session = session
        self._store = env.store(TRASSIR_STORAGE_KEY, STORAGE_VERSION)
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
        # Универсальная дверь наружу: исполнение ОПИСАННЫХ вызовов
        # (`gateway.py`). Дом при этом не знает ни одного вендора — описание
        # приезжает конфигом, а ответы уходят наружу как есть.
        #
        # ⚠ Живёт ЗДЕСЬ только потому, что учётку и живую сессию держит этот
        # драйвер. Читают её НЕ отсюда: `coordinator.accesses` — дверь не
        # принадлежит видеонаблюдению и обязана работать у объекта, где его нет
        # вовсе (`docs/plan-video-rework.md`, «Сквозной принцип»).
        self.accesses = AccessGateway(
            # ⚠ Учётка СВОЯ у каждого доступа — маршрутом менеджера по отпечатку
            # (`access_secrets.py`); учётка Trassir ниже — только для описаний
            # прежней формы, без отпечатка.
            secrets_fetch=getattr(manager, "async_access_secret", None),
            # Живая сессия драйвера — только своему доступу: id `trassir` ему
            # даёт менеджер (`accessConfigs`), второй доступ с сессией её не получит.
            provider_access="trassir",
            store=env.store("mega_home_access_secrets", 1),
            credentials=self._manager_trassir_credentials,
            # ⚠ Дверь говорит ЖИВОЙ сессией драйвера, а не своей: поток, открытый
            # одной сессией, второй не виден вовсе (замер стенда 2026-09-13 —
            # `archive_status` пустой, `archive_events` без календаря и шкалы).
            sid_provider=self._driver_sid,
        )
        # Одна строка в журнал на СМЕНУ состояния, а не на каждую неудачу: опрос
        # идёт каждые пять секунд, и объект без связи с регистратором иначе
        # засыпал бы лог быстрее, чем его читают.
        self.last_error: str | None = None

    @property
    def env(self) -> Host:
        """Хозяин этого дома — сеансам клипов нужен он же."""
        return self._env

    # --- жизненный цикл -------------------------------------------------

    async def _manager_trassir_credentials(self) -> tuple[str, str]:
        """Учётка регистратора для двери — тем же маршрутом менеджера.

        ⚠ Через дверь учётки НЕ ходят: их подставляет дом, а телефон жильца
        знает пути, но не пароли.
        """
        try:
            creds = await self._manager.async_trassir_credentials()
        except ManagerError as err:
            raise AccessDenied(f"Учётка регистратора недоступна: {err}") from err
        return str(creds.get("username") or ""), str(creds.get("password") or "")

    async def _driver_sid(self, fresh: bool = False) -> str:
        """Сессия драйвера для двери; драйвера нет — пусть дверь входит сама.

        ⚠ `fresh` — регистратор не признал прежнюю. Сессия одна на дом, поэтому
        перевходит именно драйвер: иначе дверь получила бы свою вторую, а поток
        драйвера ей был бы не виден (замер стенда — состояние архива доступно
        только той сессии, что его открыла).
        """
        client = self._client
        if client is None:
            return ""
        return await client.async_sid(fresh=fresh)

    async def async_load(self) -> None:
        """Restore credentials and the feed from disk."""
        stored = await self._store.async_load() or {}
        self._creds = stored.get("credentials") or {}
        self._fingerprint = stored.get("fingerprint") or ""
        self._events = [e for e in (stored.get("events") or []) if isinstance(e, dict)]
        self._seen = {e["id"] for e in self._events if isinstance(e.get("id"), str)}

    async def async_apply(self, config: dict[str, Any]) -> None:
        """Take the `trassir` block of a freshly synchronised config."""
        # ⚠ Описания доступов принимаются ВСЕГДА, и до блока `trassir`: дверь
        # работает и там, где драйвер объекта ещё не настроен.
        self.accesses.apply(config.get("accesses"))
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
                # ⚠ Медиапорт — другой порт, и кадры превью живут на нём.
                int(settings.get("rtspPort") or 555),
            )
            self._channels = []
            self._channels_at = 0.0
        self._start()

    def _start(self) -> None:
        if self._task and not self._task.done():
            return
        self._task = self._env.spawn(
            self._async_poll(), "mega_home_trassir_events"
        )

    async def async_stop(self) -> None:
        """Stop polling. Idempotent — unloading calls it unconditionally."""
        if self._task:
            self._task.cancel()
            self._task = None
        # ⚠ У двери своё соединение с регистратором, и закрыть его больше
        # некому: без этого перезагрузка записи конфигурации оставляет за собой
        # открытую сессию aiohttp (Home Assistant пишет об этом в журнал), а на
        # объекте они копятся — у регистратора предел подключений с адреса
        # (`sdk-session.md`: не более 99).
        await self.accesses.async_close()

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
        # ⚠ Свёртку пар «началось/кончилось» делает БАНДЛ (`foldEndings`), и
        # двойник в доме снят: это чистое толкование данных, то есть ровно тот
        # класс, который план тонкой интеграции запрещает держать в Python —
        # там он стоит релиза HACS на каждом объекте, в бандле обновляется сам.
        # Убрать его следовало сразу, как бандл научился (план это и предписывал).
        if before is not None:
            rows = [e for e in rows if int(e.get("timestampUs", 0)) < before]
        rows.sort(key=lambda e: int(e.get("timestampUs", 0)), reverse=True)
        return rows[: max(1, min(limit, 200))]

    def raw_events(self) -> list[dict[str, Any]]:
        """Лента КАК ЕСТЬ, без склейки — для диагностики и спек."""
        return list(self._events)

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

    async def async_preview(self, guid: str, timestamp_us: int) -> bytes:
        """Кадр архива на метке — превью под пальцем при перемотке.

        ⚠ Одним вызовом ДОМА, а не тремя из телефона: за кадром стоят выдача
        токена, позиционирование и чтение с медиапорта — то есть жизнь сессии,
        и она домашняя. Снаружи это ещё и разница между одним походом через
        менеджер и тремя.
        """
        if not self._client:
            raise TrassirError("Видеонаблюдение объекта не настроено")
        return await self._client.async_preview(guid, int(timestamp_us))

    async def async_thumb(self, event_id: str, lead_s: int | None = None) -> bytes:
        """Превью одного события — кадр архива на его секунду.

        ⚠ Кадр берётся с СУБПОТОКА и больше НЕ уменьшается нами. Замер стенда
        2026-09-13: `screenshot` отдаёт 1920×1128 и 398 КБ, а субпоток с
        `container=jpeg&quality=20` — 704×576 и 10 КБ за 0.08–0.10 с. То есть
        регистратор умеет отдать маленький кадр сам, и вся прежняя машинерия
        (Pillow, уменьшение, обрезка технических полей) была работой вместо
        него.

        ⚠ Обрезка полей тоже не нужна: лишние 48 строк (1128 против 1080) —
        это полоса, которую РИСУЕТ сам `screenshot`; на субпотоке её нет вовсе,
        кадр там 704×576 — честное D1.

        ⚠ Метка события уходит в запрос В ШКАЛЕ TRASSIR (unix + смещение пояса
        сервера) — «починка» её нашими часами сдвинула бы кадр на этот самый
        пояс. Сдвигаем только на `TRASSIR_THUMB_LEAD`, и это сдвиг внутри той же
        шкалы, а не смена шкалы.

        ⚠ Насколько ПОЗЖЕ метки взять кадр, решает ПРИЛОЖЕНИЕ (`lead_s`): это
        решение о том, что показать человеку, а не свойство регистратора.
        `TRASSIR_THUMB_LEAD` остался умолчанием для старых бандлов, которые
        сдвига не присылают, и снимается вместе с прочими умолчаниями, когда
        релизный бандл поднимут.
        """
        event = self.event(event_id)
        if event is None:
            raise TrassirError("Событие не найдено")
        lead = TRASSIR_THUMB_LEAD if lead_s is None else max(0, min(int(lead_s), 60))
        # ⚠ Сдвиг входит в КЛЮЧ кэша: два разных сдвига — два разных кадра, и
        # общий ключ отдавал бы второму запросу картинку первого.
        key = f"{event_id}@{lead}"
        cached = self._thumbs.get(key)
        if cached and cached[0] > time.monotonic():
            return cached[1]
        if not self._client:
            raise TrassirError("Видеонаблюдение объекта не настроено")
        at = int(event["timestampUs"]) + lead * 1_000_000
        # ⚠ СУБПОТОК, а не полный кадр с последующей обрезкой: регистратор сам
        # отдаёт 704×576 и 10 КБ за доли секунды.
        small = await self._client.async_preview(event["guid"], at)
        if len(self._thumbs) >= TRASSIR_THUMB_CAP:
            oldest = min(self._thumbs, key=lambda key: self._thumbs[key][0])
            self._thumbs.pop(oldest, None)
        self._thumbs[key] = (time.monotonic() + TRASSIR_THUMB_TTL, small)
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
                # ⚠ str(TimeoutError) пуст — тот же урок, что у RouterOS:
                # в журнал обязана уехать причина, а не висящее двоеточие.
                detail = str(err).strip()
                self._note(
                    "неожиданная ошибка опроса Trassir "
                    f"({type(err).__name__}): {detail or 'без текста ошибки'}"
                )
            await asyncio.sleep(TRASSIR_POLL_INTERVAL)

    async def _async_poll_once(self) -> None:
        if not self._client:
            return
        raw = await self._client.async_events()
        self._note(None)
        if not raw:
            return
        names = {c.get("guid"): c.get("name") for c in await self._async_channels()}
        # ⚠ Чистим и УЖЕ НАКОПЛЕННОЕ: события не от камер лежали в хранилище
        # объекта неделю (пока не вытеснит возраст), и без этой строки жилец
        # видел бы их в ленте всё это время, хотя новые уже не пускаются.
        kept = [row for row in self._events if row.get("guid") in names]
        if len(kept) != len(self._events):
            self._events = kept
            self._seen = {row["id"] for row in kept}
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
        # ⚠ `/events` отдаёт события ВСЕГО СЕРВЕРА, а не только камер: замер
        # офисного регистратора 2026-09-09 показал в ленте «Login Successful»
        # с origin пользователя. У такого события нет ни камеры, ни кадра —
        # `/screenshot` отвечает `channel not found`, — и в ленте жильца оно
        # выглядело битой картинкой без имени. Пускаем только каналы.
        if guid not in names:
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



