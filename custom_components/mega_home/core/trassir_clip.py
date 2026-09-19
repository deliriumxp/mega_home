"""Просмотр записи события — тем же WebRTC, что и живая камера.

⚠ Своего транспорта у архива НЕТ, и это главное решение всей функции
(docs/trassir-integration-plan.md §3 у менеджера). Эфемерный токен Trassir
заводится источником в НАШ go2rtc, а дальше идёт ровно тот путь, которым дом уже
отдаёт камеру: предложение телефона → ответ и кандидаты → медиа напрямую, мимо
менеджера. Поэтому в приложении под запись не появляется ни строчки нового
кода, а удалённый просмотр работает по тем же правилам, что и живой.

⚠ Порядок вызовов НЕ переставлять (проверено на живом регистраторе): токен →
кто-то ОТКРЫЛ поток → `archive_command`. Команда, отданная раньше, отвечает
`stream is expired` — текст читается как таймаут и им не является. Здесь поток
открывает go2rtc, когда к нему приходит потребитель.

⚠ Сама команда архива живёт в `trassir_archive.py` — там метки, ПАУЗА перед
командой, повтор с названной регистратором метки и правило «`play` один на
играющее соединение» вместе с замерами, которыми оно куплено. Здесь — РЕЕСТР
открытых просмотров и их транспорт; дублировать те факты сюда нельзя, дубль
расходится молча.

⚠ `play` дом отдаёт ОДИН раз и только по готовности телефона (`ready`), чтобы
первый кадр и был началом окна. Перемотка при этом идёт УНИВЕРСАЛЬНОЙ ДВЕРЬЮ, а
не этим модулем: бандл сам шлёт `command=seek`
(`docs/docs-trassir/sdk-archive-command.md`) и обязательный за ним `play` — тот
же токен, тот же поток go2rtc, та же WebRTC-сессия. Дом обязан об этом знать
(`note_gateway_call`): по соединению, которым командует бандл, свой `play` дома
был бы ВТОРЫМ.

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
from http import HTTPStatus
from time import monotonic
from typing import Any

from . import go2rtc_session
from .const import (
    LOGGER,
    TRASSIR_CLIP_IDLE_TIMEOUT,
    TRASSIR_PING_INTERVAL,
    TRASSIR_READY_TIMEOUT,
)
from .trassir_archive import Clip, _archive_stream, async_play, is_archive_command
from .trassir_client import TrassirError

# Приставка id клипа. Приложение отдаёт его туда же, куда отдаёт id плитки
# камеры, — в `webrtc`; по приставке дом и понимает, что это запись.
CLIP_PREFIX = "trassir:"


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
        env = self._gateway.env
        clip.ping = env.spawn(
            self._async_ping(clip), f"mega_home_trassir_ping_{clip.token}"
        )
        clip.idle = env.spawn(
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
        clip_id: str,
        sdp: str,
        remote: bool = False,
        trickle: bool = False,
    ) -> dict[str, Any]:
        """Свести телефон с записью: тот же go2rtc, что и у живой камеры."""
        from .ops_base import OpError

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

        settings = self._gateway.settings
        source = f"rtsp://{settings['host']}:{settings['rtspPort']}/{clip.token}"
        # ⚠ `skip_list=True`: имя потока клипа эфемерно (в нём токен), списком
        # его существование не проверяем — это лишний круг на критическом пути.
        answer = await go2rtc_session.negotiate_source(
            self._gateway.env, OWN_URL, clip.stream, source, sdp, "запись события", remote, True, trickle
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
        clip.fallback = self._gateway.env.spawn(
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
        from .ops_base import OpError

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

    async def async_close(self, clip_id: str, session_id: str) -> dict[str, Any]:
        """Жилец закрыл запись: снять сессию, поток и токен.

        ⚠ Убирать обязательно и сразу. Забытый клип держит соединение с
        регистратором и поток в go2rtc; предела соединений на объекте может не
        быть вовсе (`connections_per_ip = -1` на стенде), то есть остановить
        это будет некому.
        """
        go2rtc_session.close_own(self._gateway.env, session_id)
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
        from .go2rtc_embed import URL as OWN_URL

        if guid not in self._no_permanent:
            name, source = self.live_stream(guid, quality)
            try:
                return await go2rtc_session.negotiate_source(
                    self._gateway.env, OWN_URL, name, source, sdp, "с этой камеры", remote, False, trickle
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
        answer = await go2rtc_session.negotiate_source(
            self._gateway.env, OWN_URL, clip.stream, source, sdp, "с этой камеры", remote, True, trickle
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

    def note_gateway_call(self, clip_id: str, path: str) -> None:
        """Дверью ушла команда архива по ЭТОМУ клипу — соединение НАЧАТО.

        ⚠ Замок против ГОНКИ ДВУХ `play` по одному соединению, и стоит она
        данных. Перемотка идёт универсальной дверью (`ops_door.gateway_call`),
        а бандл шлёт по ней ДВЕ команды: `seek`, а за ним обязательный `play`
        (после `seek` поток СТОИТ — замер объекта 2026-09-13). Пока дверь клип
        начатым не помечала, дом отдавал следом СВОЙ `play` от точки открытия —
        готовностью телефона или сторожем слепого старта, — и это второй `play`
        по уже играющему соединению: данные встают (замер стенда 2026-09-09), а
        в лучшем случае курсор жильца возвращается в начало.

        ⚠ Толкования вызова здесь НЕТ: дом не читает ни `command=`, ни
        параметры, ему довольно факта «через дверь ушёл `archive_command` с этим
        клипом» (`is_archive_command`, `docs/plan-thin-integration.md` —
        «Широкая дверь»). Поэтому пометка верна и для `stop`, и для `next`:
        любой командующий бандл лучше знает, что происходит с соединением, чем
        сторож с таймером.
        """
        clip = self._clips.get(clip_id)
        if clip is None or not is_archive_command(path):
            return
        # Сторож слепого старта снимается в любом случае: по соединению уже
        # командуют, и его команда была бы второй.
        if clip.fallback:
            clip.fallback.cancel()
            clip.fallback = None
        if not clip.started:
            clip.started = True
            LOGGER.debug("Команда архива ушла дверью — своего play дом не отдаёт")

    async def _async_play(self, clip_id: str, clip: Clip) -> None:
        """Единственная команда архива этого соединения (`trassir_archive`).

        ⚠ Реестр отвечает здесь ровно на один вопрос — «этот клип ещё открыт?»:
        сессией владеет он, а ответ регистратора приезжает через секунду-полторы,
        и за это время просмотр успевают закрыть.
        """
        await async_play(
            self._gateway.client, clip, lambda: self._clips.get(clip_id) is clip
        )

    async def _async_ping(self, clip: Clip) -> None:
        """Держать токен живым, пока смотрят.

        ⚠ Пингует ДОМ, а не браузер: токен живёт десять секунд без запросов
        (`docs/docs-trassir/sdk-video.md`, «Важно»), и телефон по Wi-Fi этот
        срок не выдержит — а он ещё и сворачивается.

        ⚠⚠ Цикл ПЕРЕЖИВАЕТ сбой и гаснет только отменой. Раньше он выходил по
        любой ошибке — и один таймаут сети означал смерть потока через десять
        секунд: токен оставался без запросов, регистратор его прибирал, а жилец
        видел замерший кадр. Интервал (`TRASSIR_PING_INTERVAL`) вдвое меньше
        срока токена, то есть одна пропущенная попытка сама по себе не
        смертельна — смертелен именно ВЫХОД из цикла.
        """
        client = self._gateway.client
        if client is None:
            return
        failed = False
        while True:
            await asyncio.sleep(TRASSIR_PING_INTERVAL)
            try:
                await client.async_ping(clip.token)
            except asyncio.CancelledError:
                # ⚠ Отмену НЕ глотаем: просмотр закрыт, и задача обязана
                # умереть штатно — иначе уборка клипа ничего не остановит.
                raise
            except Exception as err:  # noqa: BLE001 — причин отказа много, путь один
                # ⚠ Словами и БЕЗ ШТОРМА: пинг идёт раз в пять секунд, и
                # строка на каждую попытку залила бы журнал объекта. Говорим о
                # СМЕНЕ состояния — первая неудача и возвращение связи.
                if not failed:
                    failed = True
                    LOGGER.warning(
                        "Продление токена записи не удалось (%s) — продолжаем пинговать",
                        err,
                    )
                continue
            if failed:
                failed = False
                LOGGER.warning("Продление токена записи снова проходит")

    async def _async_drop_stream(self, name: str) -> None:
        try:
            from .go2rtc_embed import URL as OWN_URL

            session = self._gateway.env.session()
            async with session.delete(f"{OWN_URL}/api/streams?src={name}") as answer:
                await answer.read()
        except Exception as err:  # noqa: BLE001 — уборка не должна ронять закрытие
            LOGGER.debug("Временный поток %s не удалён: %s", name, err)
