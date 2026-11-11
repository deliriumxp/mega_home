"""Смены «включилось / выключилось» в событии устройства по словарю из описания.

Словарь — данные менеджера: подстроки «началось» (`on`) и «кончилось» (`off`), разрез ответа на
сообщения (`split`), подстроки, без которых сообщение не считается (`require`), и те, с которыми
пропускается (`skip`). Дом смысла слов не знает. Один разбор у записи по движению камеры
(`recorder_sink.on_event`) и у датчиков по событиям (`event_sensors.py`): два разбора разошлись бы.
"""

from __future__ import annotations

import json
import re
from typing import Any


def text_of(frame: dict[str, Any]) -> str:
    """Ответ устройства — его текст; событие дома (своя детекция, `motion.py`) — данные одной строкой JSON
    без пробелов: подстроки менеджер пишет под эту форму."""
    data = frame.get("data") if isinstance(frame.get("data"), dict) else {}
    return str(data.get("text") or "") or json.dumps(data, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def steps(
    text: str, on: tuple[str, ...], off: tuple[str, ...], split: str = "", require: tuple[str, ...] = (), skip: tuple[str, ...] = (),
) -> list[bool]:
    """Смены по порядку в тексте.

    ⚠ Не «последнее слово решает»: короткое движение между двумя опросами приходит одним ответом
    «началось, кончилось», и по последнему слову окна не было бы вовсе.
    """
    out: list[bool] = []
    for message in text.split(split) if split else [text]:
        if require and not any(word in message for word in require):
            continue
        if any(word in message for word in skip):
            continue
        marks = sorted(
            [(m.start(), True) for word in on for m in re.finditer(re.escape(word), message)]
            + [(m.start(), False) for word in off for m in re.finditer(re.escape(word), message)]
        )
        out.extend(value for _pos, value in marks)
    return out
