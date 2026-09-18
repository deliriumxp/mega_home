"""Готовые ВАРИАНТЫ фотографий: размер под экран, размытие, ч/б с затенением.

⚠ Зачем это в доме (решение заказчика 2026-09-17). Размытие фона и ч/б снимок
выключенного прибора раньше делал БРАУЗЕР — фильтрами CSS или холстом на
каждом показе. На слабых устройствах объектов это и было главной ценой кадра, а
на старом iPad размытый фон не показывался вовсе. Теперь картинка приезжает
УЖЕ готовой, и браузеру остаётся её нарисовать.

⚠ Считает ТОЛЬКО дом, не менеджер (тоже решение заказчика): два алгоритма
одной картинки дали бы два разных вида, и расхождение всплыло бы на объекте.
Макет телефона у инсталлятора берёт варианты у дома тем же переносом.

⚠ Маршрута своего у вариантов НЕТ: это query к уже существующим
`api/photo/<ключ>` и `api/asset/<ключ>` (`?w=1080&blur=14`). Без query
отдаётся исходник, то есть старое приложение не замечает ничего.

⚠ Вид описан ОПЕРАЦИЯМИ, а не смыслом («размыть на 14», а не «фон комнаты»):
новый вид плитки или фона — правка приложения, а не релиз интеграции.

⚠ Набор вариантов ОГРАНИЧЕН: значения приводятся к ступеням и пределам. HTTP
дома пока без аутентификации (`http.py`), и свободные числа в адресе позволили
бы любому в сети забить диск объекта вариантами одной картинки.

⚠ Варианты под новое устройство считаются при первом запросе и хранятся. Когда
исходник меняется (жилец поставил другое фото, менеджер прислал новую
заготовку), уже использованные виды пересчитываются заранее (`refresh`):
устройства дома не ждут первого показа заново.
"""

from __future__ import annotations

import asyncio
import math
import re
import uuid
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path
from typing import Any, Mapping

from PIL import Image, ImageFilter, ImageOps

from .const import LOGGER
from .host import Host

# Ступени длинной стороны. Приложение просит ступень не меньше экрана или
# плитки; промежуточные значения округляются ВВЕРХ до ближайшей.
SIDES = (360, 540, 720, 1080, 1440, 1920)
# Потолок размытия с запасом над ползунком приложения (`BG_BLUR_MAX` = 40).
MAX_BLUR = 60
# Потолок затенения — тот же, что был у ползунка в менеджере (`TILE_DIM_MAX`).
MAX_DIM = 90
# ⚠ Размытую картинку незачем хранить крупной: она гладкая, и GPU растянет её
# без потерь. Сторона берётся так, чтобы размытие на ней было не меньше этого
# числа пикселей, — иначе после растяжения проступила бы лесенка.
BLUR_MIN_SIGMA = 3.0
QUALITY = 82
# Сколько картинок считается одновременно: у дома слабый процессор, а плиток
# в комнате два десятка.
PARALLEL = 2

# ⚠ Коэффициенты яркости — те же, что у `filter: grayscale()` в CSS
# (Filter Effects, матрица `grayscale`), чтобы вид не сменился при переходе.
_GRAY = (0.2126, 0.7152, 0.0722, 0.0)
_NAME = re.compile(r"^(?P<tag>[a-z])\.(?P<stem>[A-Za-z0-9_-]+)\.(?P<stamp>[0-9a-f]+-[0-9a-f]+)\.(?P<look>[a-z0-9-]+)\.jpg$")


@dataclass(frozen=True)
class Look:
    """Как показать картинку. Все поля уже приведены к допустимым значениям."""

    side: int = 0
    blur: int = 0
    gray: bool = False
    dim: int = 0

    @property
    def slug(self) -> str:
        return f"w{self.side}-b{self.blur}-g{int(self.gray)}-d{self.dim}"

    @classmethod
    def from_slug(cls, slug: str) -> Look | None:
        match = re.fullmatch(r"w(\d+)-b(\d+)-g([01])-d(\d+)", slug)
        if not match:
            return None
        return cls(int(match[1]), int(match[2]), match[3] == "1", int(match[4]))


def look_from_query(query: Mapping[str, str]) -> Look | None:
    """Вид из query запроса; None — просили исходник как есть."""
    side = _int(query.get("w"))
    blur = min(MAX_BLUR, max(0, _int(query.get("blur"))))
    gray = query.get("gray") in ("1", "true")
    dim = min(MAX_DIM, max(0, _int(query.get("dim"))))
    if side > 0:
        side = next((step for step in SIDES if step >= side), SIDES[-1])
    look = Look(max(0, side), blur, gray, dim)
    return None if look == Look() else look


def _int(value: Any) -> int:
    try:
        return int(str(value))
    except (TypeError, ValueError):
        return 0


def render(source: Path, look: Look) -> bytes:
    """Посчитать вариант. Блокирует — звать в executor."""
    with Image.open(source) as image:
        width, height = image.size
        longest = max(width, height)
        side = min(look.side or longest, longest)
        if look.blur:
            side = min(side, max(SIDES[0], math.ceil(BLUR_MIN_SIGMA * longest / look.blur)))
        side = min(side, longest)
        scale = side / longest
        size = (max(1, round(width * scale)), max(1, round(height * scale)))
        # `draft` декодирует JPEG сразу уменьшенным (масштабированием DCT) —
        # на слабом процессоре дома это кратно дешевле полного декодирования.
        image.draft("RGB", size)
        picture = ImageOps.exif_transpose(image).convert("RGB")
    if picture.size != size:
        picture = picture.resize(size, Image.Resampling.LANCZOS)
    if look.blur:
        # Радиус задан в пикселях ИСХОДНИКА (так его понимал и прежний расчёт
        # в браузере), поэтому на уменьшенной картинке он уменьшается вместе с ней.
        picture = picture.filter(ImageFilter.GaussianBlur(look.blur * scale))
    if look.gray:
        picture = picture.convert("L", _GRAY)
    if look.dim:
        keep = (100 - look.dim) / 100
        table = [round(value * keep) for value in range(256)]
        picture = picture.point(table * len(picture.getbands()))
    out = BytesIO()
    picture.save(out, "JPEG", quality=QUALITY, optimize=True)
    return out.getvalue()


class LookStore:
    """Варианты на диске. Методы без `async_` блокируют — звать в executor.

    ⚠ Имя варианта несёт ОТМЕТКУ исходника (время и размер файла): исходник
    фона жильца живёт под постоянным именем, и без отметки после замены фото
    отдавался бы вариант старого. Старые отметки чистит `refresh`.
    """

    def __init__(self, directory: Path, sources: Mapping[str, Path]) -> None:
        self._dir = directory
        # Метка → каталог исходников: `p` — фото жильца, `a` — файлы менеджера.
        self._sources = dict(sources)
        self._locks: dict[str, asyncio.Lock] = {}
        self._gate: asyncio.Semaphore | None = None

    def path(self, tag: str, source: Path, look: Look) -> Path:
        stat = source.stat()
        stamp = f"{stat.st_mtime_ns:x}-{stat.st_size:x}"
        return self._dir / f"{tag}.{source.stem}.{stamp}.{look.slug}.jpg"

    def ensure(self, tag: str, source: Path, look: Look) -> Path:
        target = self.path(tag, source, look)
        if target.is_file():
            return target
        payload = render(source, look)
        self._dir.mkdir(0o755, parents=True, exist_ok=True)
        # ⚠ Имя черновика уникальное: тот же вариант может считать `refresh` в
        # executor одновременно с запросом, и общий черновик они писали бы вдвоём.
        temporary = self._dir / f"{target.name}.{uuid.uuid4().hex}.part"
        temporary.write_bytes(payload)
        temporary.replace(target)
        return target

    async def async_file(
        self, env: Host, tag: str, source: Path, query: Mapping[str, str]
    ) -> Path:
        """Файл для ответа: вариант по query или сам исходник.

        ⚠ Один и тот же вариант не считается дважды: плитки одной комнаты
        запрашивают свои картинки разом, а устройств в доме несколько.
        """
        look = look_from_query(query)
        if look is None:
            return source
        target = await env.run(self.path, tag, source, look)
        # Замков столько, сколько вариантов запрашивали, — набор ограничен (см.
        # шапку), поэтому словарь не чистится.
        lock = self._locks.setdefault(target.name, asyncio.Lock())
        if self._gate is None:
            self._gate = asyncio.Semaphore(PARALLEL)
        async with lock, self._gate:
            return await env.run(self.ensure, tag, source, look)

    def refresh(self) -> None:
        """Пересчитать виды для сменившихся исходников и убрать осиротевшие.

        ⚠ Вид, который уже кто-то запрашивал, — это спрос устройства дома. Когда
        исходник сменился, его варианты пересчитываются ЗАРАНЕЕ, а не при первом
        показе: жилец не должен видеть градиент вместо нового фона.
        Исходник ищется по началу имени до `_`: у файлов менеджера за ним идёт
        версия, и новая версия — это другое имя того же ключа.
        """
        try:
            names = [path.name for path in self._dir.iterdir()]
        except OSError:
            return
        for name in names:
            if name.endswith(".part"):
                continue  # черновик пишется прямо сейчас
            match = _NAME.match(name)
            directory = self._sources.get(match["tag"]) if match else None
            if match is None or directory is None:
                self._drop(name)
                continue
            source = self._current(directory, match["stem"])
            look = Look.from_slug(match["look"])
            fresh = source is not None and look is not None
            if fresh and self.path(match["tag"], source, look).name == name:
                continue
            if fresh:
                try:
                    self.ensure(match["tag"], source, look)
                except (OSError, ValueError):
                    pass
            self._drop(name)

    def _current(self, directory: Path, stem: str) -> Path | None:
        group = stem.split("_", 1)[0]
        exact = directory / f"{stem}{_suffix(directory, stem)}"
        if exact.is_file():
            return exact
        try:
            candidates = sorted(
                path
                for path in directory.iterdir()
                if path.stem.split("_", 1)[0] == group and path.suffix != ".part"
            )
        except OSError:
            return None
        return candidates[-1] if len(candidates) == 1 else None

    def _drop(self, name: str) -> None:
        try:
            (self._dir / name).unlink()
        except OSError:
            pass

    def count(self) -> int:
        """Сколько вариантов хранится — для диагностики."""
        try:
            return len(list(self._dir.glob("*.jpg")))
        except OSError:
            return 0


# --- что отдавать по запросу -------------------------------------------------
#
# ⚠ Живёт ЗДЕСЬ, в одном месте на обе двери: локальные view (`http.py`) и
# перенос снаружи (`relay_api.py`) обязаны отдавать одно и то же — тот же приём,
# что у `photo_keys` в `photos.py`.


async def photo_file(env: Host, coordinator: Any, key: str, query: Mapping[str, str]) -> Path | None:
    """Снимок жильца (или его вариант) по ключу; None — снимка нет."""
    target = coordinator.photos.path(key)
    if not await env.run(target.is_file):
        return None
    return await _look(env, coordinator, "p", target, query)


async def asset_file(
    env: Host, coordinator: Any, key: str, query: Mapping[str, str]
) -> tuple[Path, str] | None:
    """Файл общего канала (или вариант картинки) и его тип; None — файла нет.

    ⚠ Версия в адресе, если она есть, обязана совпасть с версией в конфиге.
    Ответ отдаётся `immutable`, а макет телефона у инсталлятора знает версию
    от МЕНЕДЖЕРА раньше, чем дом успел её выкачать: без сверки под новым
    адресом закэшировалась бы навсегда старая картинка.
    """
    entry = (coordinator.data.get("assets") or {}).get(key)
    if not isinstance(entry, dict) or not isinstance(entry.get("v"), str):
        return None
    if query.get("v") and query["v"] != entry["v"]:
        return None
    target = coordinator.assets.path(key, entry["v"])
    if not await env.run(target.is_file):
        # Манифест файл обещает, а синхронизация ещё не дошла (дом только
        # поднялся, менеджер был недоступен). Это не ошибка приложения.
        return None
    kind = entry.get("type")
    kind = kind if isinstance(kind, str) and kind else "application/octet-stream"
    if not kind.startswith("image/"):
        return target, kind
    served = await _look(env, coordinator, "a", target, query)
    return served, kind if served == target else "image/jpeg"


async def _look(env: Host, coordinator: Any, tag: str, source: Path, query: Mapping[str, str]) -> Path:
    if look_from_query(query) is None:
        return source
    try:
        return await coordinator.looks.async_file(env, tag, source, query)
    except (OSError, ValueError, Image.DecompressionBombError) as err:
        # Картинка не разбирается — лучше показать исходник, чем пустое место.
        LOGGER.warning("Could not prepare a photo variant of %s: %s", source.name, err)
        return source


def _suffix(directory: Path, stem: str) -> str:
    for suffix in (".jpg", ".bin"):
        if (directory / f"{stem}{suffix}").is_file():
            return suffix
    return ""
