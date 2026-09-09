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

⚠ Команда — ОДНА на соединение (там же проверено): повторный `play` по одному
открытому потоку ненадёжен, со второй-третьей команды данные встают. Поэтому
здесь НЕТ перемотки повтором команды: возврат на начало и жест по таймлайну —
это ПЕРЕОТКРЫТИЕ (новый токен, новый поток, новые переговоры), а старт архива
ждёт готовности телефона (`ready`), чтобы первый кадр и был началом окна.

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
    TRASSIR_CLIP_LEAD,
    TRASSIR_CLIP_MAX_SECONDS,
    TRASSIR_PING_INTERVAL,
    TRASSIR_READY_TIMEOUT,
)
from .trassir_client import TrassirError

# Приставка id клипа. Приложение отдаёт его туда же, куда отдаёт id плитки
# камеры, — в `webrtc`; по приставке дом и понимает, что это запись.
CLIP_PREFIX = "trassir:"


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


def _segments(
    rows: Any, token: str, start_us: int, stop_us: int
) -> list[dict[str, int]]:
    """Записанные участки архива внутри окна клипа — из `archive_status`.

    ⚠ Зачем это вообще: на объекте запись ведётся ПО ДВИЖЕНИЮ (замер офисного
    регистратора 2026-09-09: фрагменты по 6-8 секунд, дыры в минуты). Поэтому
    внутри минутного окна события данные есть лишь местами, и в дыре
    проигрыватель честно стоит на последнем кадре — а выглядит это как
    зависшая картинка. Отдать участки наружу дешевле любых догадок: рисует их
    и толкует ПРИЛОЖЕНИЕ (docs/plan-thin-integration.md).

    ⚠ Шкала: `begin`/`end` — СЕКУНДЫ ОТ НАЧАЛА СУТОК `day_start`, и сутки эти
    в шкале самого Trassir (unix + пояс сервера). Поэтому день переводим тем же
    `timegm`, что и `_outside`: местный пояс дома прибавил бы смещение второй
    раз.
    """
    if not isinstance(rows, list):
        return []
    out: list[dict[str, int]] = []
    for row in rows:
        if not isinstance(row, dict) or row.get("token") != token:
            continue
        try:
            day = datetime.strptime(str(row.get("day_start")), "%Y-%m-%d")
        except ValueError:
            continue
        day_us = int(calendar.timegm(day.timetuple())) * 1_000_000
        for piece in row.get("timeline") or []:
            try:
                begin = day_us + int(piece["begin"]) * 1_000_000
                end = day_us + int(piece["end"]) * 1_000_000
            except (KeyError, TypeError, ValueError):
                continue
            # Обрезаем окном клипа: за его краями рисовать нечего, а лишние
            # сутки фрагментов — это килобайты по каналу жильца на каждый тап.
            begin, end = max(begin, start_us), min(end, stop_us)
            if end > begin:
                out.append({"startUs": begin, "stopUs": end})
    out.sort(key=lambda piece: piece["startUs"])
    return out


def _outside(first_frame: str | None, start_us: int, stop_us: int) -> bool:
    """Ближайшая запись лежит вне окна — значит кадров не будет вовсе.

    ⚠ Метка разбирается в шкале САМОГО Trassir (UTC-геттеры), той же, в которой
    считалось окно: «починить» её местным поясом значит прибавить смещение
    второй раз и объявить дырой каждую нормальную запись.
    """
    if not first_frame:
        return False
    try:
        moment = datetime.strptime(first_frame, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        # Регистратор ответил меткой незнакомого вида — молчим, а не врём про
        # отсутствие записи: пустой экран честнее выдуманного объяснения.
        return False
    at_us = int(calendar.timegm(moment.timetuple())) * 1_000_000
    return at_us < start_us or at_us > stop_us


@dataclass
class Clip:
    """Один открытый клип: токен Trassir, окно архива и поток go2rtc."""

    token: str
    guid: str
    # ⚠ ОКНО события — неизменно на всё открытие: его задаёт событие, и шкала
    # таймлайна стоит именно на нём. Перемотка меняет `start_us`, но НЕ его.
    window_start_us: int
    window_stop_us: int
    # Откуда играем сейчас. При открытии равно началу окна.
    start_us: int
    stream: str
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
    # Ближайшая запись оказалась ВНЕ запрошенного окна: кадров не будет вовсе,
    # и приложение обязано сказать это словами, а не молчать чёрным экраном.
    out_of_window: bool = False
    # Записанные участки внутри окна (`_segments`). Пусто — регистратор шкалу
    # не отдал; это не «записи нет», и приложение обязано их различать.
    segments: list[dict[str, int]] = field(default_factory=list)
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

    async def async_open(
        self, event_id: str, remote: bool = False, quality: str | None = None
    ) -> dict[str, Any]:
        """Подготовить запись к просмотру и вернуть её id приложению.

        Само видео ещё не течёт: токен взят, поток go2rtc назван, а команда
        архива уйдёт, когда телефон подключится (см. заголовок модуля).

        ⚠ `quality` присылает ПРИЛОЖЕНИЕ. Правило «дома основной, снаружи суб» —
        политика, а не физика, и её место там, где её видно: приложение знает
        свою дверь лучше нас, а политика в Python стоит релиза HACS на каждом
        объекте (docs/plan-thin-integration.md).

        ⚠ `remote` остался ТОЛЬКО умолчанием для старых бандлов, которые качества
        не присылают, — иначе удалённый жилец получил бы основной архив на
        мобильном канале. Снять вместе с прочими умолчаниями.
        """
        event = self._gateway.event(event_id)
        if event is None:
            raise TrassirError("Событие не найдено")
        client = self._gateway.client
        if client is None:
            raise TrassirError("Видеонаблюдение объекта не настроено")

        settings = self._gateway.settings
        seconds = min(int(settings.get("clipSeconds") or 60), TRASSIR_CLIP_MAX_SECONDS)
        # ⚠ Метка события уходит в окно КАК ЕСТЬ — она в шкале самого Trassir
        # (unix + пояс сервера). Пересчёт по нашим часам сдвинул бы запись на
        # часовой пояс регистратора, а выглядело бы это как «показывает не то».
        start = int(event["timestampUs"]) - TRASSIR_CLIP_LEAD * 1_000_000
        stop = int(event["timestampUs"]) + seconds * 1_000_000

        # Замер стенда 2026-09-09, ради которого приложение и выбирает: основной
        # архив 1.43 Мбит/с и 13.4 к/с, суб — 0.16 Мбит/с и 10.7 к/с.
        want = _archive_stream(quality, remote)
        token = await client.async_get_video(event["guid"], want, "rtsp")
        clip = Clip(
            token=token,
            guid=event["guid"],
            window_start_us=start,
            window_stop_us=stop,
            start_us=start,
            stream=f"trassir_{token}",
            quality=want,
        )
        clip_id = f"{CLIP_PREFIX}{token}"
        self._clips[clip_id] = clip
        self._arm(clip_id, clip)
        return self._describe(clip_id, clip, camera_name=event.get("cameraName"))

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
        self, hass: HomeAssistant, clip_id: str, sdp: str, remote: bool = False
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
        answer = await webrtc.negotiate_source(
            hass, OWN_URL, clip.stream, source, sdp, "запись события", remote
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

    async def async_ready(self, clip_id: str) -> dict[str, Any]:
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
            if clip.fallback:
                clip.fallback.cancel()
                clip.fallback = None
            await self._async_play(clip_id, clip)
        return {
            "ready": not started,
            "positionUs": clip.start_us,
            "firstFrameTs": clip.first_frame,
            # ⚠ Ближайшая запись вне окна — это «в это время не писали», а не
            # наша задержка. Молчание здесь и есть тот чёрный прямоугольник, за
            # которым жилец досиживает до таймаута проигрывателя.
            "outOfWindow": clip.out_of_window,
            # Записанные участки внутри окна: запись на объекте ведётся по
            # движению, и дыры в окне — норма. Рисует и толкует их приложение.
            "segments": clip.segments,
        }

    async def async_seek(
        self, clip_id: str, position_us: int | None, quality: str | None = None
    ) -> dict[str, Any]:
        """Перемотка ПЕРЕОТКРЫТИЕМ: новый токен, новый поток, новые переговоры.

        ⚠ Повтором команды по тому же соединению — НЕЛЬЗЯ (см. заголовок):
        со второй-третьей команды данные встают, и это ровно те «скачки»,
        которые чиним. Поэтому старое соединение разбираем целиком (пинг,
        сторож, поток go2rtc — токен дальше дохнет сам, его больше не пингуем),
        а телефон сводит новый просмотр обычным путём: ответ несёт новый id,
        источник в проигрывателе меняется — и переговоры идут заново.
        """
        from .ops import OpError

        old = self._clips.get(clip_id)
        if old is None:
            raise OpError("Запись уже закрыта, откройте событие заново", HTTPStatus.NOT_FOUND)
        try:
            position = int(position_us)  # type: ignore[arg-type]
        except (TypeError, ValueError) as err:
            raise OpError("Позиция — микросекунды числом", HTTPStatus.BAD_REQUEST) from err
        # ⚠ Кламп, а не отказ: палец на таймлайне не обязан попадать в окно
        # микросекунда в микросекунду, а ронять жест из-за края — хамство.
        # ⚠ Кламп к ОКНУ события, а не к прошлому старту: после перемотки вперёд
        # прошлый старт стал бы новым дном шкалы, и вернуться назад было бы уже
        # нечем — жест «к событию» упирался бы в текущее место.
        position = min(max(position, old.window_start_us), old.window_stop_us)
        client = self._gateway.client
        if client is None:
            raise TrassirError("Видеонаблюдение объекта не настроено")
        # ⚠ Качество по умолчанию берём У СТАРОГО клипа: его выбрала дверь при
        # открытии, а перемотка дверь не меняет. Захардкоженный суб здесь ронял
        # домашний просмотр на субпоток после первого же жеста по таймлайну.
        #
        # ⚠ Кнопка качества у записи идёт ЭТИМ ЖЕ путём, а не своей операцией:
        # поток и токен привязаны к качеству, значит смена качества — это то же
        # переоткрытие, что и перемотка, только позиция остаётся прежней.
        want = _archive_stream(quality, None) or old.quality
        token = await client.async_get_video(old.guid, want, "rtsp")
        await self._drop(clip_id, old)
        clip = Clip(
            token=token,
            guid=old.guid,
            window_start_us=old.window_start_us,
            window_stop_us=old.window_stop_us,
            start_us=position,
            stream=f"trassir_{token}",
            quality=want,
        )
        clip_id = f"{CLIP_PREFIX}{token}"
        self._clips[clip_id] = clip
        self._arm(clip_id, clip)
        return self._describe(clip_id, clip)

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
                    hass, OWN_URL, name, source, sdp, "с этой камеры", remote
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
        answer = await webrtc.negotiate_source(
            hass, OWN_URL, clip.stream, source, sdp, "с этой камеры", remote
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
            clip.started = True
            await self._async_settle(clip)
            try:
                answer = await client.async_archive_command(
                    clip.token,
                    command="play",
                    start=clip.start_us,
                    stop=clip.window_stop_us,
                    speed=1,
                )
            except TrassirError as err:
                # Не роняем просмотр: поток уже сведён, и жилец увидит хотя бы
                # то, что отдаёт регистратор по умолчанию. В лог — словами.
                LOGGER.warning("Запись не встала на событие: %s", err)
                return
        if self._clips.get(clip_id) is not clip:
            # Закрыли раньше, чем команда дошла: дальше делать нечего, пинг и
            # так снимут закрытием.
            return
        # `first_frame_ts` — куда курсор встал НА САМОМ ДЕЛЕ. У архива бывают
        # дыры, и «клип начался не с события» это факт регистратора, а не наш
        # промах; отдаём его наружу, чтобы приложение могло сказать правду.
        clip.first_frame = answer.get("first_frame_ts")
        clip.out_of_window = _outside(clip.first_frame, clip.start_us, clip.window_stop_us)
        # ⚠ ПОСЛЕ команды, а не до: своего параметра «по какому каналу» у
        # `archive_status` нет — он отвечает по токенам ОТКРЫТЫХ потоков, и
        # раньше старта нашего токена там нет вовсе.
        try:
            clip.segments = _segments(
                await client.async_archive_status("timeline"),
                clip.token,
                clip.window_start_us,
                clip.window_stop_us,
            )
        except TrassirError as err:
            # Не роняем просмотр: без шкалы жилец просто не увидит разметку
            # записанного, а видео идёт.
            LOGGER.debug("Шкала архива недоступна: %s", err)
        if clip.out_of_window:
            # ⚠ Проверено на стенде: когда ближайшая запись лежит ПОЗЖЕ конца
            # окна, регистратор отвечает успехом и не присылает НИ ОДНОГО байта.
            # Без этой строки симптом неотличим от «медленно грузится».
            LOGGER.info(
                "В запрошенном окне записи нет: ближайший кадр %s", clip.first_frame
            )

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
