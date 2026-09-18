"""Команда архива одного соединения: метка, пауза, `play` и его повтор.

⚠ Вынесено из `trassir_clip.py`, когда тот перерос порог дробления. Граница
проходит по ВЛАДЕНИЮ, а не по размеру: реестр открытых просмотров держит сессию
(токен, поток go2rtc, пинг, уборка), а здесь живёт то единственное, что дом
говорит архиву, — и правило «команда на соединение одна».

⚠ Порядок «токен → кто-то ОТКРЫЛ поток → `archive_command`» ДОКУМЕНТИРОВАН
(`docs/docs-trassir/sdk-archive-command.md`, блок «Важно»: получить token со
`stream=archive_*`, по нему запросить поток, и только потом командовать) и
подтверждён стендом: команда до открытия отвечает `stream is expired` — текст
читается как таймаут и им не является.

⚠ `play` — ОДИН на ИГРАЮЩЕЕ соединение. Это ЗАМЕР СТЕНДА 2026-09-09, а не
документация: со второй-третьей команды данные переставали идти
(`docs/trassir-integration-plan.md` §1.2). В SDK про это нет ни слова, поэтому
решения здесь выбраны так, чтобы быть верными и если факт строже, чем кажется:
дом не отдаёт второй `play` НИКОГДА — ни сторожем, ни готовностью, ни после
того, как по соединению покомандовал сам бандл.

⚠ Пауза между RTSP PLAY и командой — тоже ЗАМЕР (2026-09-09, повтор): порог
лежит между 0,2 и 0,5 с — прогон с паузами 0 и 0,2 с дал `success: 1` и НОЛЬ
байтов за десять секунд, 0,5 и 1,5 с — данные сразу. `TRASSIR_ARCHIVE_SETTLE`
это запас над измеренным порогом; в документации паузы нет вовсе, снижать её
без нового замера нельзя.
"""

from __future__ import annotations

import asyncio
import re
from dataclasses import dataclass, field
from time import monotonic
from typing import Any

from .const import (
    LOGGER,
    TRASSIR_ARCHIVE_MAIN,
    TRASSIR_ARCHIVE_SETTLE,
    TRASSIR_ARCHIVE_SUB,
)
from .trassir_client import TrassirError

# ⚠ Путь SDK один на ВСЕ команды архива — `play`, `stop`, `seek`, `next`,
# `prev`, `frame_*` (`docs/docs-trassir/sdk-archive-command.md`). Отсюда и
# следует, что факт «по этому соединению уже командуют» читается по ПУТИ, без
# разбора самой команды.
ARCHIVE_COMMAND_PATH = "archive_command"


def is_archive_command(path: str) -> bool:
    """Ушла ли дверью команда архива — по ПУТИ, не заглядывая в команду.

    ⚠ Это ФАКТ, а не толкование вызова: дом не читает ни `command=`, ни
    параметры (`docs/plan-thin-integration.md`, «Широкая дверь»). Читать их и не
    нужно — путь у всех команд архива один, а дому важно ровно одно: по этому
    соединению уже командуют, и свой `play` был бы вторым.
    """
    if not isinstance(path, str):
        return False
    return path.split("?", 1)[0].rstrip("/").rsplit("/", 1)[-1] == ARCHIVE_COMMAND_PATH


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


async def _play_where_told(
    client: Any, token: str, start_us: int, stop_us: int
) -> dict[str, Any]:
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
    # и исполняет описанные вызовы, `docs/plan-thin-integration.md`
    # («Широкая дверь»).
    ping: Any = field(default=None, repr=False)
    # Старт вслепую, если готовность не пришла (старое приложение её не шлёт):
    # снимать вместе с клипом, иначе команда догонит закрытый просмотр.
    fallback: Any = field(default=None, repr=False)
    # Сторож открытого, но так и не начатого просмотра: жилец передумал между
    # «открыть» и переговорами, а токен пингуется и держит соединение.
    idle: Any = field(default=None, repr=False)
    # ⚠ Команда архива — одна на соединение, а претендентов на неё ТРОЕ:
    # готовность телефона, сторож слепого старта и сам бандл, который мотает
    # дверью. Без замка первые два успевали оба: `started` ставился ПОСЛЕ
    # await, и вторая команда роняла данные.
    lock: Any = field(default=None, repr=False)


async def async_settle(clip: Clip) -> None:
    """Выдержать паузу между открытием потока и командой архива.

    ⚠ Не вежливость к регистратору, а условие того, что данные пойдут ВООБЩЕ
    (`TRASSIR_ARCHIVE_SETTLE`, замер в заголовке модуля): команда, отданная в ту
    же миллисекунду, что RTSP PLAY, даёт ноль байтов навсегда. Обычно ждать не
    приходится — готовность телефона и так приходит позже.
    """
    if not clip.offered_at:
        return
    left = TRASSIR_ARCHIVE_SETTLE - (monotonic() - clip.offered_at)
    if left > 0:
        await asyncio.sleep(left)


async def async_play(client: Any, clip: Clip, still_open: Any) -> None:
    """Единственная команда архива этого соединения.

    ⚠ Замок, а не флаг после await: претендентов на эту команду несколько, и
    `started`, ставившийся ПОСЛЕ ответа регистратора, пропускал их всех. Второй
    `play` по играющему соединению роняет данные (замер в заголовке модуля), то
    есть гонка выглядела как «иногда запись просто встаёт».

    `still_open` отвечает на «этот клип ещё открыт?»: сессией владеет реестр, а
    ответ регистратора приезжает через секунду-полторы, и за это время просмотр
    успевают закрыть.
    """
    if client is None:
        return
    if clip.lock is None:
        clip.lock = asyncio.Lock()
    async with clip.lock:
        if clip.started:
            return
        # ⚠ Регистратор требует ОБА края окна: `play` без `start` он отвергает
        # («start is empty»), а `stop`, сериализованный из пустоты, приезжает
        # строкой "None" и даёт «timestamp format is not valid» (замеры стенда
        # 2026-09-13). Значит окно присылает приложение — своих часов в шкале
        # Trassir у дома нет и не будет, — а дом честно говорит, когда его не
        # прислали, вместо чёрного кадра.
        #
        # ⚠ ПРОВЕРКА ДО `started`, и это не мелочь: команда на соединение одна,
        # и претендентов на неё двое. Сторож слепого старта просыпается через
        # TRASSIR_READY_TIMEOUT и у записи, открытой БЕЗ метки, окна ещё не
        # видит — приложение как раз идёт за днём к календарю. Съев
        # единственную попытку, сторож оставлял бы просмотр мёртвым: пришедшая
        # следом готовность с окном видела бы `started` и не делала ничего.
        if clip.start_us is None or clip.window_stop_us is None:
            clip.start_error = (
                "Архив не запущен: приложение не прислало, с какого места играть"
            )
            LOGGER.warning("Запись не встала: нет окна воспроизведения")
            return
        clip.started = True
        await async_settle(clip)
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
    if not still_open():
        # Закрыли раньше, чем команда дошла: дальше делать нечего, пинг и
        # так снят закрытием.
        return
    # Куда курсор встал НА САМОМ ДЕЛЕ — ответ регистратора, и он уходит наружу
    # как есть. У архива бывают дыры, и «клип начался не с события» это факт
    # регистратора, а не наш промах. Что это значит для шкалы и «писали ли
    # вообще», решает приложение: в доме таких толкований больше нет.
    clip.first_frame = answer.get("first_frame_ts")
    # Старт состоялся — прежняя жалоба больше не про этот просмотр.
    clip.start_error = None
