"""Обновить интеграцию по команде менеджера: файлы с менеджера, перезапуск HA.

Зачем. Код интеграции — единственное, что не доезжает до дома само: файлы
надо положить на диск, а применяет их только перезапуск Home Assistant
(`docs/mega-home-updates.md` в менеджере, раздел 3). Инсталлятор жмёт кнопку в
карточке объекта — и дом делает остальное.

⚠ Код приходит С МЕНЕДЖЕРА, а не из HACS (решение заказчика 2026-09-20).
Прежняя кнопка звала `update.install` HACS, а тот кэширует список релизов
GitHub и перечитывает его редко: дом ставил вчерашнюю версию или «ничего» и
перезапускался впустую. Менеджер раздаёт тот же код из своего чекаута тем же
манифестом, что и бандл интерфейса (пути и контрольные суммы). HACS остаётся
ВТОРЫМ, ручным путём: релизы на GitHub по-прежнему выходят.

⚠ Черновик и прежняя версия лежат ВНЕ `custom_components` (`<config>/.mega_home_update/`,
та же файловая система — переименование атомарно). Загрузчик Home Assistant
(`loader.py`, `_async_get_custom_components`) берёт ЛЮБОЙ подкаталог с
`manifest.json`, точка в имени его не останавливает, а домен у копии тот же —
`.mega_home.prev` рядом с пакетом спорил бы с ним за домен `mega_home` на каждом
старте, и проигрыш означал ImportError и дом без интеграции (ревью 2026-09-20).

⚠ Половина пакета никогда не встаёт на место: сверяется хеш каждого файла,
пакет обязан содержать `manifest.json` и `__init__.py`, а версия в скачанном
манифесте — совпасть с объявленной (менеджер собирает список обходом каталога
в момент запроса, и во время `git pull` на проде список может быть согласован,
но неполон). Половина пакета — это дом, который после перезапуска не
поднимется вовсе, и лечить его пришлось бы выездом.
"""

from __future__ import annotations

import asyncio
import hashlib
import shutil
from http import HTTPStatus
from json import loads
from pathlib import Path
from typing import Any

from homeassistant.core import HomeAssistant

from .core.api import ManagerClient, ManagerError
from .core.const import INTEGRATION_VERSION, LOGGER
from .core.ops_base import OpError

# Каталог ЭТОГО пакета — то, что подменяем. В спеках подставляется временный.
PACKAGE_DIR = Path(__file__).resolve().parent
# Имя рабочего каталога обновления в каталоге конфигурации HA (рядом с
# `custom_components`, не внутри).
WORK_DIR = ".mega_home_update"
# Манифест — один короткий запрос; скачивание — сотни килобайт, запас для
# медленного канала объекта. ⚠ Сумма плюс пауза перед перезапуском обязана быть
# МЕНЬШЕ срока, который ждёт менеджер (`INTEGRATION_UPDATE_TIMEOUT_MS`): иначе
# инсталлятор видит «не удалось», а дом в это время доводит подмену и падает
# в перезапуск — и жмёт кнопку снова.
MANIFEST_TIMEOUT = 30.0
DOWNLOAD_TIMEOUT = 120.0
# Пауза перед перезапуском: ответ менеджеру обязан уйти ДО того, как HA начнёт
# останавливаться и закроет канал, иначе инсталлятор увидит «дом отключился»
# вместо «обновление поставлено».
RESTART_DELAY = 2.0
# Пределы разумного для манифеста интеграции: больше — это не наш пакет.
MAX_FILES = 400
MAX_TOTAL_BYTES = 16 * 1024 * 1024
# Без этих файлов каталог — не интеграция.
REQUIRED_FILES = ("manifest.json", "__init__.py")

# ⚠ Одно обновление за раз. Каждый запрос канала идёт своей задачей (`link.py`),
# а фронтенд отпускает кнопку по таймауту менеджера — то есть второе нажатие
# во время скачивания реально. Две загрузки в один черновик сносили бы файлы
# друг друга и ставили живьём огрызок пакета без бэкапа. Второй вызов не
# ждёт, а отказывает: инсталлятору нужен ответ «уже идёт», а не вторая подмена.
_LOCK = asyncio.Lock()


async def async_self_update(
    hass: HomeAssistant,
    client: ManagerClient,
    wanted: str | None = None,
    package_dir: Path = PACKAGE_DIR,
) -> dict[str, Any]:
    """Поставить то, что раздаёт менеджер, и перезапустить HA, если есть что загрузить.

    `wanted` — версия, которую назвал менеджер; истина всё равно манифест: это
    то, что мы качаем прямо сейчас, а подсказка могла устареть за секунды.

    ⚠ Перезапуск — ТОЛЬКО когда есть что загрузить: поставили сейчас или на
    диске уже лежит версия, отличная от загруженной (положили раньше, HA не
    перезапускали). Перезапуск «на всякий случай» ронял дом на минуту без
    единой причины (живой объект 2026-09-20).
    """
    if _LOCK.locked():
        raise OpError("Обновление Mega Home уже идёт", HTTPStatus.CONFLICT)
    async with _LOCK:
        return await _self_update(hass, client, wanted, package_dir)


async def _self_update(
    hass: HomeAssistant, client: ManagerClient, wanted: str | None, package_dir: Path
) -> dict[str, Any]:
    installed = await hass.async_add_executor_job(_manifest_version, package_dir)
    try:
        async with asyncio.timeout(MANIFEST_TIMEOUT):
            manifest = await client.async_integration_manifest()
    except ManagerError as err:
        raise OpError(f"Менеджер не отдал код интеграции: {err}", HTTPStatus.BAD_GATEWAY) from err
    except TimeoutError as err:
        raise OpError("Менеджер не ответил манифестом интеграции", HTTPStatus.GATEWAY_TIMEOUT) from err
    target = manifest.get("version")
    files = manifest.get("files")
    if not isinstance(target, str) or not target or not isinstance(files, list):
        raise OpError("Менеджер отдал непонятный манифест интеграции", HTTPStatus.BAD_GATEWAY)
    if wanted and wanted != target:
        LOGGER.debug("Менеджер назвал %s, манифест говорит %s — ставим манифест", wanted, target)

    # ⚠ Ставим по СОДЕРЖИМОМУ, а не по номеру: правка без подъёма версии иначе не
    # доезжала бы никогда. Но НЕ понижаем: на диске может лежать более новая
    # версия из HACS, которую менеджер ещё не подтянул, — кнопка «Применить X»
    # обещает X, а не откат на то, что раздаёт менеджер.
    downgrade = _older(target, installed)
    installing = not downgrade and await hass.async_add_executor_job(_differs, package_dir, files)
    on_disk = installed
    if installing:
        LOGGER.warning("Обновление Mega Home по команде менеджера: %s → %s", installed, target)
        work = package_dir.parent.parent / WORK_DIR
        staging = work / "new"
        try:
            async with asyncio.timeout(DOWNLOAD_TIMEOUT):
                await _download(hass, client, files, staging, target)
        except TimeoutError as err:
            await hass.async_add_executor_job(shutil.rmtree, staging, True)
            raise OpError(
                "Менеджер не успел отдать обновление — перезапуск отменён",
                HTTPStatus.GATEWAY_TIMEOUT,
            ) from err
        except (ManagerError, OSError, ValueError) as err:
            # ⚠ Половина пакета никогда не встаёт на место: черновик выбрасывается,
            # прежний код продолжает работать, перезапуска нет.
            await hass.async_add_executor_job(shutil.rmtree, staging, True)
            raise OpError(f"Обновление не скачано: {err}", HTTPStatus.BAD_GATEWAY) from err
        await hass.async_add_executor_job(_swap, staging, package_dir, work / "prev")
        on_disk = target

    restarting = installing or bool(on_disk and on_disk != INTEGRATION_VERSION)
    if restarting:
        hass.async_create_background_task(_restart_later(hass), "mega_home self-update restart")
    return {
        "loaded": INTEGRATION_VERSION,
        "onDisk": on_disk or None,
        "installed": installed or None,
        "latest": target,
        "target": target,
        "installing": installing,
        "restarting": restarting,
    }


async def _download(
    hass: HomeAssistant, client: ManagerClient, files: list[Any], staging: Path, target: str
) -> None:
    if len(files) > MAX_FILES:
        raise ValueError(f"в манифесте {len(files)} файлов — это не наш пакет")
    total = sum(int(item.get("bytes", 0)) for item in files if isinstance(item, dict))
    if total > MAX_TOTAL_BYTES:
        raise ValueError(f"пакет весит {total} байт — это не наш пакет")
    paths = {item.get("path") for item in files if isinstance(item, dict)}
    missing = [name for name in REQUIRED_FILES if name not in paths]
    if missing:
        raise ValueError(f"в манифесте нет {', '.join(missing)} — это не целая интеграция")
    await hass.async_add_executor_job(_reset_dir, staging)
    for item in files:
        path = item.get("path") if isinstance(item, dict) else None
        digest = item.get("sha256") if isinstance(item, dict) else None
        if not isinstance(path, str) or not isinstance(digest, str):
            raise ValueError("запись манифеста без пути или хеша")
        dest = _resolve_inside(staging, path)
        payload = await client.async_integration_file(path)
        # ⚠ Хеш КАЖДОГО файла: обрезанный ответ прокси и подменённый файл выглядят
        # одинаково — как пакет, который «почти» скачался.
        if hashlib.sha256(payload).hexdigest() != digest:
            raise ValueError(f"не сошлась контрольная сумма {path}")
        await hass.async_add_executor_job(_write, dest, payload)
    staged = await hass.async_add_executor_job(_manifest_version, staging)
    if staged != target:
        raise ValueError(f"скачан manifest.json версии «{staged}», объявлена {target}")


def _differs(package_dir: Path, files: list[Any]) -> bool:
    """Отличается ли пакет на диске от манифеста хоть одним файлом."""
    for item in files:
        if not isinstance(item, dict):
            return True
        path, digest = item.get("path"), item.get("sha256")
        if not isinstance(path, str) or not isinstance(digest, str):
            return True
        try:
            current = hashlib.sha256((package_dir / path).read_bytes()).hexdigest()
        except OSError:
            return True
        if current != digest:
            return True
    return False


def _older(candidate: str, than: str) -> bool:
    """`candidate` строго старше `than` по числовым частям; мусор — не старше."""
    try:
        a = [int(part) for part in candidate.split(".")]
        b = [int(part) for part in than.split(".")]
    except ValueError:
        return False
    length = max(len(a), len(b))
    a += [0] * (length - len(a))
    b += [0] * (length - len(b))
    return a < b


def _manifest_version(package_dir: Path) -> str:
    """Версия из `manifest.json` в каталоге — то, что лежит на диске."""
    try:
        version = loads((package_dir / "manifest.json").read_text("utf-8")).get("version")
        return version if isinstance(version, str) else ""
    except Exception:  # noqa: BLE001 - неизвестная версия на диске = ставим заново
        return ""


def _resolve_inside(root: Path, relative: str) -> Path:
    target = (root / relative).resolve()
    if not str(target).startswith(str(root.resolve()) + "/"):
        raise ValueError(f"путь выходит за каталог пакета: {relative}")
    return target


def _reset_dir(path: Path) -> None:
    shutil.rmtree(path, ignore_errors=True)
    path.mkdir(parents=True, exist_ok=True)


def _write(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)


def _swap(staging: Path, target: Path, previous: Path) -> None:
    """Поставить черновик на место пакета, прежний пакет — в `previous`.

    Два переименования вместо копирования: каждое атомарно, а если второе не
    удалось, первое откатывается — без пакета каталог не остаётся.
    """
    shutil.rmtree(previous, ignore_errors=True)
    target.rename(previous)
    try:
        staging.rename(target)
    except OSError:
        previous.rename(target)
        raise


async def _restart_later(hass: HomeAssistant) -> None:
    await asyncio.sleep(RESTART_DELAY)
    LOGGER.warning("Перезапуск Home Assistant по команде менеджера")
    await hass.services.async_call("homeassistant", "restart", {})
