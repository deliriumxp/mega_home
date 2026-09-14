"""Просмотр записи события — тем же WebRTC, что и живая камера.

⚠ Своего транспорта у архива НЕТ, и это главное решение всей функции
(docs/trassir-integration-plan.md, §5а у менеджера). Эфемерный токен Trassir
заводится источником в НАШ go2rtc, а дальше идёт ровно тот путь, которым дом уже
отдаёт камеру: предложение телефона → ответ и кандидаты → медиа напрямую, мимо
менеджера. Поэтому в приложении под запись не появляется ни строчки нового
кода, а удалённый просмотр работает по тем же правилам, что и живой.

⚠ Порядок вызовов НЕ переставлять (проверено на живом регистраторе): токен →
кто-то ОТКРЫЛ поток → `archive_command`. Команда, отданная раньше, отвечает
`stream is expired` — текст читается как таймаут и им не является. Здесь поток
открывает go2rtc, когда к нему приходит потребитель.

⚠ Между PLAY и командой держим ПАУЗУ (`TRASSIR_ARCHIVE_SETTLE`), и вот что о ней
известно ТОЧНО (замеры стенда 2026-09-09, обе стороны проверены повторами):
команда, отданная в ту же миллисекунду, что RTSP PLAY, даёт РОВНО НОЛЬ байтов —
навсегда, а не «медленно», — но только когда регистратору приходится ИСКАТЬ
запись (запрошенное начало не попадает в записанный кусок). Через секунду после
PLAY та же команда с тем же окном отдаёт первый байт сразу. По наведённому окну
нулевая пауза отработала 8 раз из 8.

То есть пауза — не такса, а страховка ровно того случая, где иначе не будет
ничего. На критический путь она при этом почти не ложится: отсчёт идёт от КОНЦА
ПЕРЕГОВОРОВ, а готовность телефона приходит позже неё сама по себе.

⚠ `play` — ОДИН на соединение (там же проверено): повторный `play` по открытому
потоку ненадёжен, со второй-третьей команды данные встают. Поэтому старт архива
ждёт готовности телефона (`ready`), чтобы первый кадр и был началом окна.
Перемотка — ДОКУМЕНТИРОВАННЫЙ `command=seek` по живому токену
(`docs/docs-trassir/sdk-archive-command.md`): тот же токен, тот же поток go2rtc,
та же WebRTC-сессия, кадр продолжается с новой метки; переоткрытие осталось
только у смены качества. Живое поведение seek проверяет прод: если регистратор
его не переварит — откат релиза 0.2.42 возвращает переоткрытие.

⚠ ОКНО записи и ПОЗИЦИЯ — разные вещи, и их нельзя сливать в одно поле. Окно
задаёт событие и живёт всё открытие; позиция — то, откуда играем сейчас. Пока
перемотка подменяла окно позицией, шкала после каждого жеста начиналась заново:
жилец перематывал на середину и снова оказывался «в начале записи».

⚠ Часы архива идут в РЕАЛЬНОМ времени с момента команды `play`, а телефон
показывает первый кадр позже: переговоры ICE/DTLS, раскрутка чтения архива с
диска и ожидание ключевого кадра. Всё, что прошло между командой и первым
кадром, потеряно навсегда — перемотки назад у живого сеанса нет. Старт по
готовности убирает и пропуск, и скачок назад: команда уходит, когда тракт уже
может принять кадры.
"""

from __future__ import annotations

import asyncio
import re
import calendar
from datetime import datetime
from dataclasses import dataclass, field
from http import HTTPStatus
from time import monotonic
from typing import Any

from homeassistant.core import HomeAssistant

from .const import (
    LOGGER,
    TRASSIR_ARCHIVE_MAIN,
    TRASSIR_ARCHIVE_SETTLE,
    TRASSIR_ARCHIVE_SUB,
    TRASSIR_CLIP_IDLE_TIMEOUT,
    TRASSIR_PING_INTERVAL,
    TRASSIR_READY_TIMEOUT,
)
from .trassir_client import TrassirError

# Приставка id клипа. Приложение отдаёт его туда же, куда отдаёт id плитки
# камеры, — в `webrtc`; по приставке дом и понимает, что это запись.
CLIP_PREFIX = "trassir:"


def _stamp(us: int | None) -> str | None:
    """Микросекунды приложения → метка регистратора `20260914T094351`.

    ⚠ СИММЕТРИЧНО тому, как приложение читает метки регистратора: оно разбирает
    их как UTC (`trassirTimeUs`), значит и обратно — UTC. Пояс регистратора в
    расчёте не участвует, гадать про него не нужно: туда и обратно одно число.
    """
    if us is None:
        return None
    from datetime import datetime, timezone

    return datetime.fromtimestamp(us / 1_000_000, timezone.utc).strftime("%Y%m%dT%H%M%S")

async def _play_where_told(client: Any, token: str, start_us: int, stop_us: int) -> dict[str, Any]:
    """`play` с меткой — и ПОВТОР с той, которую назвал сам регистратор.

    ⚠⚠ Запрошенная точка почти всегда попадает в ДЫРУ: архив пишется по
    движению, и суток из двух сотен фрагментов по восемь секунд хватает, чтобы
    промахнуться мимо записи почти всегда. На такой метке регистратор отвечает
    `success: 1`, честно называет ближайший кадр в `first_frame_ts` — И НЕ
    ОТДАЁТ ДАННЫЕ.

    Замер объекта 2026-09-14 (вчерашний день, `play` от полуночи):
      · от полуночи            →  298 КБ, курсор ЗАМЕР на 00:22:42;
      · повтор с 00:22:42      → 2118 КБ, курсор идёт 00:22:42 → 00:22:50.

    ⚠ Ровно ОДИН повтор и только при расхождении: второй круг значил бы, что мы
    спорим с регистратором о его же ответе.
    """
    answer = await client.async_archive_command(
        token, command="play", start=_stamp(start_us), stop=_stamp(stop_us), speed=1
    )
    named = answer.get("first_frame_ts") if isinstance(answer, dict) else None
    stamp = _stamp_of_text(named)
    if stamp and stamp != _stamp(start_us):
        await client.async_archive_command(
            token, command="play", start=stamp, stop=_stamp(stop_us), speed=1
        )
    return answer if isinstance(answer, dict) else {}


def _stamp_of_text(text: str | None) -> str | None:
    """`2026-09-13 00:22:42` → `20260913T002242`; мусор → None.

    ⚠ ПЕРЕСТАНОВКА СИМВОЛОВ, а не разбор даты, и это принципиально: дом НЕ
    толкует ответы регистратора (`docs/plan-thin-integration.md`, «Широкая
    дверь»), а здесь метка регистратора лишь переписывается в его же второй
    формат, чтобы вернуть ему. Ни календаря, ни пояса, ни арифметики суток.
    """
    if not isinstance(text, str):
        return None
    packed = text.strip().replace("-", "").replace(":", "").replace(" ", "T")
    return packed if re.fullmatch(r"\d{8}T\d{6}", packed) else None

def _archive_stream(quality: str | None, remote: bool | None) -> str:
    """Какой поток архива просить у регистратора.

    ⚠ Приложение присылает `main`/`sub`, и это единственный ИСТОЧНИК РЕШЕНИЯ.
    `remote` — умолчание для старых бандлов, которые качества не шлют; `None`
    означает «умолчания нет, оставь как было» (перемотка).
    """
    if quality == "sub":
        return TRASSIR_ARCHIVE_SUB
    if quality == "main":
        return TRASSIR_ARCHIVE_MAIN
    if remote is None:
        return ""
    return TRASSIR_ARCHIVE_SUB if remote else TRASSIR_ARCHIVE_MAIN


@dataclass
class Clip:
    """Один открытый клип: токен Trassir, окно архива и поток go2rtc."""

    token: str
    guid: str
    stream: str
    # Откуда играем. Не знаем — регистратор встанет на ближайшую запись сам.
    start_us: int | None = None
    # ⚠ ОКНО ШКАЛЫ присылает ПРИЛОЖЕНИЕ и дом в него не заглядывает: шкалу
    # рисует оно (`docs/plan-thin-integration.md`, «Широкая дверь»). Здесь оно
    # только хранится и уезжает обратно как есть — считать в доме нечего.
    window_start_us: int | None = None
    window_stop_us: int | None = None
    # `archive_main` дома, `archive_sub` снаружи (см. `TRASSIR_ARCHIVE_*`).
    # Хранится в клипе, чтобы перемотка не роняла качество на субпоток.
    quality: str = TRASSIR_ARCHIVE_MAIN
    session_id: str | None = None
    started: bool = False
    # Когда закончились переговоры: от этого момента отсчитывается пауза перед
    # командой архива (`TRASSIR_ARCHIVE_SETTLE` — иначе ноль байтов).
    offered_at: float = 0.0
    # Куда курсор архива встал НА САМОМ ДЕЛЕ: у записи бывают дыры, и «клип
    # начался не с события» — факт регистратора, а не наш промах.
    first_frame: str | None = None
    # Почему архив не встал: отказ регистратора на команду старта, словами.
    start_error: str | None = None
    # ⚠ Всё остальное оставили приложению: `outOfWindow`, участки записи, дни с
    # архивом и окно шкалы. Дом их не считает и не разбирает — он держит сессию
    # и исполняет описанные вызовы (`recorder.py`),
    # `docs/plan-thin-integration.md` («Широкая дверь»).
    ping: Any = field(default=None, repr=False)
    # Старт вслепую, если готовность не пришла (старое приложение её не шлёт):
    # снимать вместе с клипом, иначе команда догонит закрытый просмотр.
    fallback: Any = field(default=None, repr=False)
    # Сторож открытого, но так и не начатого просмотра: жилец передумал между
    # «открыть» и переговорами, а токен пингуется и держит соединение.
    idle: Any = field(default=None, repr=False)
    # ⚠ Команда архива — одна на соединение, а претендентов на неё двое
    # (готовность телефона и сторож слепого старта). Без замка они успевали
    # оба: `started` ставился ПОСЛЕ await, и вторая команда роняла данные.
    lock: Any = field(default=None, repr=False)


class ClipSessions:
    """Открытые записи этого дома. Живут ровно пока их смотрят."""

    def __init__(self, gateway: Any) -> None:
        self._gateway = gateway
        self._clips: dict[str, Clip] = {}
        # Каналы, у которых ПОСТОЯННОГО адреса нет (см. `async_live_offer`).
        self._no_permanent: set[str] = set()

    async def async_open_at(
        self,
        guid: str,
        timestamp_us: int | None = None,
        camera_name: str | None = None,
        remote: bool = False,
        quality: str | None = None,
        window_start_us: int | None = None,
        window_stop_us: int | None = None,
    ) -> dict[str, Any]:
        """Открыть запись КАНАЛА на метке — классический просмотр архива.

        ⚠ Метки нет — значит «последняя запись», и НИЧЕГО считать не надо:
        регистратор встанет на ближайший записанный кадр сам, а куда он встал,
        дом узнает из его же ответа (`first_frame_ts`). Своих часов в шкале
        Trassir дом не держит и держать не должен — она с поясом сервера.

        ⚠ Окно шкалы считает ПРИЛОЖЕНИЕ и присылает готовым: шкалу рисует оно.
        Дом хранит присланное и уезжает обратно как есть.
        """
        client = self._gateway.client
        if client is None:
            raise TrassirError("Видеонаблюдение объекта не настроено")

        at = int(timestamp_us) if timestamp_us else None
        want = _archive_stream(quality, remote)
        token = await client.async_get_video(guid, want, "rtsp")
        clip = Clip(
            token=token,
            guid=guid,
            window_start_us=int(window_start_us) if window_start_us else None,
            window_stop_us=int(window_stop_us) if window_stop_us else None,
            start_us=at,
            stream=f"trassir_{token}",
            quality=want,
        )
        clip_id = f"{CLIP_PREFIX}{token}"
        self._clips[clip_id] = clip
        self._arm(clip_id, clip)
        return self._describe(clip_id, clip, camera_name=camera_name)

    def _describe(
        self, clip_id: str, clip: Clip, camera_name: str | None = None
    ) -> dict[str, Any]:
        """Ответ приложению об открытом клипе.

        ⚠ `startUs`/`stopUs` — это ОКНО СОБЫТИЯ, а не то, откуда играем: на них
        стоит шкала таймлайна, и подменять их позицией значит начинать шкалу
        заново после каждой перемотки.
        """
        payload: dict[str, Any] = {
            "id": clip_id,
            "startUs": clip.window_start_us,
            "stopUs": clip.window_stop_us,
            "positionUs": clip.start_us,
            # ⚠ Где регистратор встал НА САМОМ ДЕЛЕ. У открытия его ещё нет
            # (`play` уйдёт по готовности) — тогда `null`, и приложению нечего
            # показывать, кроме запрошенного. После перемотки он есть всегда:
            # это ответ на `play`, которым перемотка и заканчивается.
            "firstFrameTs": clip.first_frame,
        }
        if camera_name is not None:
            payload["cameraName"] = camera_name
        return payload

    def _arm(self, clip_id: str, clip: Clip) -> None:
        """Взять токен под охрану СРАЗУ, как он выдан.

        ⚠ Пинг раньше начинался только с переговоров, а токен живёт десять
        секунд без запросов. Между «открыть» и предложением телефона лежит сбор
        ICE-кандидатов, а снаружи ещё и дорога через менеджер — то есть токен
        успевал умереть до того, как go2rtc откроет по нему RTSP, и просмотр
        уходил в долгое молчание вместо картинки.

        ⚠ Вместе с пингом взводится сторож: клип, который так и не начали
        смотреть, иначе пингуется вечно и копит соединения к регистратору
        (`connections_per_ip = -1` на стенде — остановить это будет некому).
        """
        hass = self._gateway.hass
        clip.ping = hass.async_create_background_task(
            self._async_ping(clip), f"mega_home_trassir_ping_{clip.token}"
        )
        clip.idle = hass.async_create_background_task(
            self._async_idle(clip_id), f"mega_home_trassir_idle_{clip.token}"
        )

    async def _async_idle(self, clip_id: str) -> None:
        """Просмотр так и не начался — отпустить токен и поток."""
        await asyncio.sleep(TRASSIR_CLIP_IDLE_TIMEOUT)
        clip = self._clips.get(clip_id)
        if clip is None or clip.session_id:
            return
        LOGGER.debug("Запись открыли, но смотреть не стали — убираем за собой")
        await self._drop(clip_id, clip)

    async def async_offer(
        self,
        hass: HomeAssistant,
        clip_id: str,
        sdp: str,
        remote: bool = False,
        trickle: bool = False,
    ) -> dict[str, Any]:
        """Свести телефон с записью: тот же go2rtc, что и у живой камеры."""
        from .ops import OpError

        clip = self._clips.get(clip_id)
        if clip is None:
            raise OpError("Запись уже закрыта, откройте событие заново", HTTPStatus.NOT_FOUND)
        try:
            from .go2rtc_embed import URL as OWN_URL, is_running
        except Exception as err:  # noqa: BLE001
            raise OpError("Дом не умеет отдавать запись", HTTPStatus.NOT_IMPLEMENTED) from err
        if not is_running():
            # Честный текст: живая камера в этом доме идёт путём Home Assistant,
            # а запись — только своим go2rtc, и молчать об этом нельзя.
            raise OpError(
                "Дом не может отдать запись: не поднят его go2rtc",
                HTTPStatus.SERVICE_UNAVAILABLE,
            )

        from . import webrtc

        settings = self._gateway.settings
        source = f"rtsp://{settings['host']}:{settings['rtspPort']}/{clip.token}"
        # ⚠ `skip_list=True`: имя потока клипа эфемерно (в нём токен), списком
        # его существование не проверяем — это лишний круг на критическом пути.
        answer = await webrtc.negotiate_source(
            hass, OWN_URL, clip.stream, source, sdp, "запись события", remote, True, trickle
        )
        clip.session_id = answer.get("sessionId")
        # ⚠ Момент, от которого отсчитывается пауза перед командой архива:
        # go2rtc открывает RTSP ДО того, как отдаст SDP-ответ (иначе ему нечем
        # объявить кодеки), поэтому «переговоры кончились» — это заведомо
        # «PLAY уже был», а команда в ту же миллисекунду даёт ноль байтов.
        clip.offered_at = monotonic()
        # Смотреть начали — сторож брошенного клипа больше не нужен.
        if clip.idle:
            clip.idle.cancel()
            clip.idle = None
        # ⚠ Команды архива ЗДЕСЬ НЕТ — только прогрев: RTSP открыт, токен
        # держится пингом (взят под охрану ещё при открытии), а `play` уйдёт по
        # готовности телефона (`ready`) либо вслепую по таймауту (старое
        # приложение готовности не шлёт — ему достаётся прежнее поведение).
        clip.fallback = hass.async_create_background_task(
            self._async_fallback(clip_id),
            f"mega_home_trassir_fallback_{clip.token}",
        )
        return answer

    async def async_ready(
        self,
        clip_id: str,
        position_us: int | None = None,
        window_start_us: int | None = None,
        window_stop_us: int | None = None,
    ) -> dict[str, Any]:
        """Телефон готов принимать кадры: отдать архиву ЕДИНСТВЕННУЮ команду.

        Одна команда на соединение — этого требует стенд (см. заголовок):
        повторный `play` по открытому потоку роняет данные. Поэтому готовность —
        это и есть старт, а не «ещё одна команда следом». Опоздавшая готовность
        (сторож уже стартовал вслепую) и повторная — безвредны.
        """
        from .ops import OpError

        clip = self._clips.get(clip_id)
        if clip is None:
            raise OpError("Запись уже закрыта, откройте событие заново", HTTPStatus.NOT_FOUND)
        started = clip.started
        if not started:
            # ⚠ Откуда играть, приложение может узнать ТОЛЬКО когда поток уже
            # открыт: календарь регистратор отдаёт лишь у потока с потребителем
            # (замер стенда 2026-09-13 — без него 0 дней и день `1970-01-01`).
            # Поэтому запись, открытая без метки, получает её здесь. Дом ничего
            # не толкует: он кладёт присланные числа в команду как есть.
            if position_us:
                clip.start_us = int(position_us)
            if window_start_us:
                clip.window_start_us = int(window_start_us)
            if window_stop_us:
                clip.window_stop_us = int(window_stop_us)
            if clip.fallback:
                clip.fallback.cancel()
                clip.fallback = None
            await self._async_play(clip_id, clip)
        # ⚠ Наружу уходит только то, что ответил РЕГИСТРАТОР: где он встал
        # (`first_frame_ts`) и откуда просили. Всё остальное — окно, участки,
        # «записи не было» — считает и толкует приложение: дом в это не лезет
        # (`docs/plan-thin-integration.md`, «Широкая дверь»).
        return {
            "ready": not started,
            "positionUs": clip.start_us,
            "firstFrameTs": clip.first_frame,
            # ⚠ Отказ регистратора уходит НАРУЖУ словами, а не только в журнал
            # дома: без метки `play` он отвергает («start is empty», замер
            # стенда 2026-09-13), и жилец видел чёрный кадр до таймаута
            # проигрывателя — то есть ровно то же, что при потере связи.
            "error": clip.start_error,
        }

    async def async_close(self, hass: HomeAssistant, clip_id: str, session_id: str) -> dict[str, Any]:
        """Жилец закрыл запись: снять сессию, поток и токен.

        ⚠ Убирать обязательно и сразу. Забытый клип держит соединение с
        регистратором и поток в go2rtc; предела соединений на объекте может не
        быть вовсе (`connections_per_ip = -1` на стенде), то есть остановить
        это будет некому.
        """
        from . import webrtc

        webrtc.close_own(hass, session_id)
        clip = self._clips.get(clip_id)
        if clip is None:
            return {"closed": True}
        await self._drop(clip_id, clip)
        return {"closed": True}

    async def _async_fallback(self, clip_id: str) -> None:
        """Старт вслепую, если готовность не пришла.

        ⚠ Только для старых приложений — новые шлют `ready`, как только тракт
        собрался. Срок короче прицела проигрывателя (`LIVE_TIMEOUT` у него):
        вслепую стартовавший архив ещё должен успеть дойти кадрами, иначе
        жилец получит отказ поверх уже идущего видео.
        """
        await asyncio.sleep(TRASSIR_READY_TIMEOUT)
        clip = self._clips.get(clip_id)
        if clip is None or clip.started:
            return
        LOGGER.debug("Готовность записи не пришла — стартуем вслепую")
        await self._async_play(clip_id, clip)

    async def _drop(self, clip_id: str, clip: Clip) -> None:
        """Разобрать соединение целиком: задачи, поток, токен.

        ⚠ Токен отдельно не отпускается — его держит только пинг, а пинг снят:
        дальше регистратор прибирает сам по своему таймауту.
        """
        self._clips.pop(clip_id, None)
        for name in ("ping", "fallback", "idle"):
            task = getattr(clip, name)
            if task:
                task.cancel()
                setattr(clip, name, None)
        await self._async_drop_stream(clip.stream)

    async def async_live_offer(
        self,
        hass: HomeAssistant,
        guid: str,
        sdp: str,
        quality: str,
        remote: bool = False,
        trickle: bool = False,
    ) -> dict[str, Any]:
        """Свести телефон с ЖИВОЙ камерой регистратора — двумя путями.

        ⚠ Постоянный адрес есть НЕ У КАЖДОГО канала. Проверено на стенде: из 12
        каналов один отвечает на `<guid>_m/` и `<guid>_s/` кодом 404 ВСЕГДА, а
        через `get_video` тот же канал отдаётся нормально. Пока путь был один,
        такая камера не открывалась вовсе — жилец видел «wrong response on
        DESCRIBE» (живой отчёт с объекта 2026-09-09, камера «Торговый зал 2»).

        ⚠ Поэтому быстрый путь остаётся быстрым, а запасной — документированный
        (`docs/docs-trassir/sdk-video.md`): токен, а значит пинг и уборка. Канал,
        однажды ответивший отказом, дальше идёт сразу запасным путём: платить
        двумя переговорами за каждое открытие незачем.
        """
        from . import webrtc
        from .go2rtc_embed import URL as OWN_URL

        if guid not in self._no_permanent:
            name, source = self.live_stream(guid, quality)
            try:
                return await webrtc.negotiate_source(
                    hass, OWN_URL, name, source, sdp, "с этой камеры", remote, False, trickle
                )
            except Exception as err:  # noqa: BLE001 — причин отказа много, путь один
                LOGGER.info(
                    "Постоянный адрес канала %s не сработал (%s) — идём по токену",
                    guid,
                    err,
                )
                self._no_permanent.add(guid)

        client = self._gateway.client
        if client is None:
            raise TrassirError("Видеонаблюдение объекта не настроено")
        settings = self._gateway.settings
        token = await client.async_get_video(guid, quality, "rtsp")
        # ⚠ Живой просмотр по токену — это тот же сеанс, что у записи, только
        # без команды архива: `started=True` и говорит «командовать нечем».
        # Отдельного вида сеанса не заводим — уборка, пинг и закрытие по сессии
        # у него обязаны быть теми же самыми.
        clip = Clip(
            token=token,
            guid=guid,
            window_start_us=0,
            window_stop_us=0,
            start_us=0,
            stream=f"trassir_live_{token}",
            quality=quality,
            started=True,
        )
        clip_id = f"{CLIP_PREFIX}{token}"
        self._clips[clip_id] = clip
        self._arm(clip_id, clip)
        source = f"rtsp://{settings['host']}:{settings['rtspPort']}/{token}"
        # ⚠ `skip_list=True`: имя потока запасного пути тоже эфемерно (токен),
        # списком его существование проверять нечего.
        answer = await webrtc.negotiate_source(
            hass, OWN_URL, clip.stream, source, sdp, "с этой камеры", remote, True, trickle
        )
        clip.session_id = answer.get("sessionId")
        if clip.idle:
            clip.idle.cancel()
            clip.idle = None
        return answer

    def live_stream(self, guid: str, quality: str = "main") -> tuple[str, str]:
        """Имя потока go2rtc и источник для ЖИВОЙ камеры регистратора.

        ⚠ Токен не нужен вовсе: у live-канала ссылка ПОСТОЯННАЯ
        (`rtsp://host:555/<guid>_m/`), поэтому здесь нет ни сеанса, ни пинга, ни
        уборки — тем и отличается от записи. И идёт она тем же go2rtc и тем же
        WebRTC: камера видеонаблюдения показывается ровно как любая другая.

        ⚠ Дополнительный поток — тот же постоянный адрес с `_s/`. Документация
        DSSL знает только путь через `get_video` (`docs/docs-trassir/sdk-video.md`),
        но он выдаёт ТОКЕН — то есть сеанс, пинг и уборку на каждое переключение
        качества. Суффикс проверен на стенде замером: `_m` — 2.46 Мбит/с, `_s` —
        0.36 Мбит/с при тех же 22 к/с. Постоянный адрес того стоит: кнопка
        качества не заводит ни одной новой сущности.
        """
        settings = self._gateway.settings
        suffix = "_s" if quality == "sub" else "_m"
        name = f"trassir_live_{guid}" if quality != "sub" else f"trassir_live_sub_{guid}"
        return (name, f"rtsp://{settings['host']}:{settings['rtspPort']}/{guid}{suffix}/")

    def token_of(self, clip_id: str) -> str:
        """Токен открытой записи — им регистратор зовёт её поток.

        ⚠ Нужен универсальной двери: бандл токена не носит (он эфемерный, и в
        телефоне жильца ему делать нечего), а присылает id клипа — подставляет
        токен дом. Раньше дверь доставала его из ПРИВАТНОГО словаря сеансов
        (`clips._clips`) через `getattr`: молча пережило бы любое переименование
        и перестало бы подставлять токен, а выглядело бы это как «регистратор
        не отдаёт календарь».
        """
        clip = self._clips.get(clip_id)
        return clip.token if clip is not None else ""

    def clip_of_session(self, session_id: str) -> str | None:
        """Найти клип по сессии — приложение закрывает просмотр именно ею."""
        for clip_id, clip in self._clips.items():
            if clip.session_id == session_id:
                return clip_id
        return None

    async def _async_play(self, clip_id: str, clip: Clip) -> None:
        """Единственная команда архива этого соединения.

        ⚠ Замок, а не флаг после await: претендентов на эту команду двое —
        готовность телефона и сторож слепого старта, — и `started`, ставившийся
        ПОСЛЕ ответа регистратора, обоих пропускал. Вторая команда по тому же
        соединению роняет данные (факт стенда), то есть гонка выглядела как
        «иногда запись просто встаёт».
        """
        client = self._gateway.client
        if client is None:
            return
        if clip.lock is None:
            clip.lock = asyncio.Lock()
        async with clip.lock:
            if clip.started:
                return
            # ⚠ Регистратор требует ОБА края окна: `play` без `start` он
            # отвергает («start is empty»), а `stop`, сериализованный из
            # пустоты, приезжает строкой "None" и даёт «timestamp format is not
            # valid» (замеры стенда 2026-09-13). Значит окно присылает
            # приложение — своих часов в шкале Trassir у дома нет и не будет, —
            # а дом честно говорит, когда его не прислали, вместо чёрного кадра.
            #
            # ⚠ ПРОВЕРКА ДО `started`, и это не мелочь: команда на соединение
            # одна, и претендентов на неё двое. Сторож слепого старта просыпается
            # через TRASSIR_READY_TIMEOUT и у записи, открытой БЕЗ метки, окна
            # ещё не видит — приложение как раз идёт за днём к календарю. Съев
            # единственную попытку, сторож оставлял бы просмотр мёртвым: пришедшая
            # следом готовность с окном видела бы `started` и не делала ничего.
            if clip.start_us is None or clip.window_stop_us is None:
                clip.start_error = (
                    "Архив не запущен: приложение не прислало, с какого места играть"
                )
                LOGGER.warning("Запись не встала: нет окна воспроизведения")
                return
            clip.started = True
            await self._async_settle(clip)
            try:
                answer = await _play_where_told(
                    client, clip.token, clip.start_us, clip.window_stop_us
                )
            except TrassirError as err:
                # Не роняем просмотр: поток уже сведён, и жилец увидит хотя бы
                # то, что отдаёт регистратор по умолчанию. В лог — словами.
                LOGGER.warning("Запись не встала на событие: %s", err)
                clip.start_error = str(err)
                return
        if self._clips.get(clip_id) is not clip:
            # Закрыли раньше, чем команда дошла: дальше делать нечего, пинг и
            # так снимут закрытием.
            return
        # `first_frame_ts` — куда курсор встал НА САМОМ ДЕЛЕ. У архива бывают
        # дыры, и «клип начался не с события» это факт регистратора, а не наш
        # промах; отдаём его наружу, чтобы приложение могло сказать правду.
        # Куда курсор встал НА САМОМ ДЕЛЕ — ответ регистратора, и он уходит
        # наружу как есть. Что это значит для шкалы и «писали ли вообще»,
        # решает приложение: в доме таких толкований больше нет.
        clip.first_frame = answer.get("first_frame_ts")
        # Старт состоялся — прежняя жалоба больше не про этот просмотр.
        clip.start_error = None

    async def _async_settle(self, clip: Clip) -> None:
        """Выдержать паузу между открытием потока и командой архива.

        ⚠ Не вежливость к регистратору, а условие того, что данные пойдут
        ВООБЩЕ (`TRASSIR_ARCHIVE_SETTLE`): команда, отданная в ту же
        миллисекунду, что RTSP PLAY, даёт ноль байтов навсегда. Обычно ждать не
        приходится — готовность телефона и так приходит позже.
        """
        if not clip.offered_at:
            return
        left = TRASSIR_ARCHIVE_SETTLE - (monotonic() - clip.offered_at)
        if left > 0:
            await asyncio.sleep(left)

    async def _async_ping(self, clip: Clip) -> None:
        """Держать токен живым, пока смотрят.

        ⚠ Пингует ДОМ, а не браузер: у токена десять секунд без запросов, и
        телефон по Wi-Fi этот срок не выдержит — а он ещё и сворачивается.
        """
        client = self._gateway.client
        if client is None:
            return
        while True:
            await asyncio.sleep(TRASSIR_PING_INTERVAL)
            try:
                await client.async_ping(clip.token)
            except (TrassirError, asyncio.CancelledError):
                return
            except Exception as err:  # noqa: BLE001
                LOGGER.debug("Продление токена записи не удалось: %s", err)
                return

    async def _async_drop_stream(self, name: str) -> None:
        try:
            from homeassistant.helpers.aiohttp_client import async_get_clientsession

            from .go2rtc_embed import URL as OWN_URL

            session = async_get_clientsession(self._gateway.hass)
            async with session.delete(f"{OWN_URL}/api/streams?src={name}") as answer:
                await answer.read()
        except Exception as err:  # noqa: BLE001 — уборка не должна ронять закрытие
            LOGGER.debug("Временный поток %s не удалён: %s", name, err)
