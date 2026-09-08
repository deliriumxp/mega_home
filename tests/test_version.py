"""Версии, которые дом сообщает менеджеру.

⚠ Их ДВЕ, и разница между ними осмысленна. `version` — код, загруженный при
старте Home Assistant; `disk_version` — то, что лежит на диске, то есть что
положил HACS. После обновления без перезапуска дом живёт в смешанном состоянии
(загруженные модули прежние, импортируемые позже читаются с диска новыми), и
эта пара — единственный способ увидеть снаружи, кого перезапускать. Ни одно из
чисел ничего не запрещает: что дом умеет, менеджер выясняет его ОТВЕТОМ.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

from mega_home.const import INTEGRATION_VERSION
from mega_home.link import ManagerLink, _disk_version, _integration_version

MANIFEST = Path(__file__).resolve().parents[1] / "custom_components" / "mega_home" / "manifest.json"


def test_version_matches_manifest() -> None:
    """Константа и манифест поднимаются вместе — иначе они молча разъедутся.

    Разъехавшись, они дадут ложную подсказку «дом ждёт перезапуска» на каждом
    обновлённом объекте — то есть ровно ту, ради которой пара и заведена.
    """
    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))

    assert INTEGRATION_VERSION == manifest["version"]


def test_loaded_version_does_not_touch_the_disk() -> None:
    """Версия загруженного кода берётся из кода: `hass` для неё не нужен вовсе.

    ⚠ Именно поэтому сюда передаётся `None`. Понадобится `hass` — значит версию
    снова спрашивают у Home Assistant, то есть о том, что лежит на диске, и
    различать «загружено» и «скачано» станет нечем.
    """
    assert _integration_version(None) == INTEGRATION_VERSION


def test_disk_version_reads_the_manifest_next_to_the_code() -> None:
    """Дисковая версия читается из манифеста рядом с модулем — того, что пишет HACS."""
    assert _disk_version() == json.loads(MANIFEST.read_text(encoding="utf-8"))["version"]


def test_hello_carries_both_versions() -> None:
    """Кадр представления несёт обе версии — иначе менеджеру нечего сравнивать.

    ⚠ Дисковая читается блокирующе, поэтому уходит в executor: чтение файла в
    цикле событий Home Assistant — то, за что интеграцию справедливо ругают.
    """

    class _Hass:
        @staticmethod
        async def async_add_executor_job(func, *args):
            return func(*args)

    class _Bundle:
        version = "561c5d8137ae5536"
        last_error = None

    class _Coordinator:
        bundle = _Bundle()

    instance = ManagerLink.__new__(ManagerLink)
    instance._hass = _Hass()
    instance._coordinator = _Coordinator()

    frame = asyncio.run(instance._hello())

    assert frame["t"] == "hello"
    assert frame["version"] == INTEGRATION_VERSION
    assert frame["disk_version"] == _disk_version()
    # ⚠ И какой ИНТЕРФЕЙС дом раздаёт прямо сейчас: без этого «почему у меня
    # старые кнопки» разбирается по скриншотам, а не по карточке объекта.
    assert frame["app_version"] == "561c5d8137ae5536"
    assert frame["app_error"] is None
