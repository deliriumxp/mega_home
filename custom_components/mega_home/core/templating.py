"""Подстановка `{имя}` в строках описания слушателя (`listeners_out.py`) и поля ответа.

⚠ Только подстановка — никаких вычислений входа (`docs/plan-thin-gateway.md`,
«Слушатели»): шаблон с хэшем или разовым значением — это вендорский вход,
описанный строкой. Стандартные схемы входа — Digest (`digest.py`) и WS-Security
UsernameToken (`wsse.py`) — считает общий HTTP-клиент дома по типу учётки, а не
шаблон; вендорский хэш держит менеджер.
"""

from __future__ import annotations

import json
import re
from collections.abc import Iterable
from typing import Any

_PLACEHOLDER = re.compile(r"\{([^{}]+)\}")

def render(template: str, values: dict[str, Any]) -> str:
    """Подставить значения; незнакомое имя оставляет место как есть."""

    def one(match: re.Match[str]) -> str:
        name = match.group(1)
        return str(values[name]) if name in values else match.group(0)

    return _PLACEHOLDER.sub(one, template)

def dig(data: Any, steps: Iterable[str]) -> Any:
    """Значение по шагам пути: ключ объекта или индекс массива; нет такого — None.

    Один обход на дом: слушатели (`pick`) и обработка ответа (`reduce.py`) — копии
    расходились бы в том, что считать индексом.
    """
    current = data
    for step in steps:
        if isinstance(current, dict):
            current = current.get(step)
        elif isinstance(current, list) and step.isdigit() and int(step) < len(current):
            current = current[int(step)]
        else:
            return None
    return current

def pick(data: Any, path: str) -> str:
    """Поле ответа по пути через точку (`data.token`, `result.id`, `list.0.id`)."""
    current = dig(data, path.split(".") if path else [])
    return str(current) if isinstance(current, (str, int, float)) and not isinstance(current, bool) else ""

def parsed(payload: bytes) -> Any:
    """Тело ответа данными для путей `pick`: JSON, иначе XML, иначе None.

    ⚠ XML — словарём по ЛОКАЛЬНЫМ именам элементов, без пространств имён
    (`Envelope.Body.<Ответ>.<Поле>`): префиксы у устройств разные (`SOAP-ENV:`,
    `env:`, `s:`) при одном смысле, а путь пишет менеджер один на всех. Атрибуты
    не берутся; повторённый элемент — список. Толкования здесь нет: тот же путь
    через точку, что у JSON. Читают слушатель (`carry`, `resetup`) и часы устройства
    для подписи (`wsse.py`).
    """
    if not payload:
        return None
    text = payload.decode("utf-8", "ignore").strip()
    try:
        return json.loads(text)
    except ValueError:
        pass
    if not text.startswith("<"):
        return None
    from xml.etree import ElementTree  # noqa: PLC0415 — только у XML-устройств

    try:
        root = ElementTree.fromstring(text)
    except ElementTree.ParseError:
        return None
    return {_local(root.tag): _xml_node(root)}

def _local(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]

def _xml_node(element: Any) -> Any:
    children = list(element)
    if not children:
        return (element.text or "").strip()
    out: dict[str, Any] = {}
    for child in children:
        key, value = _local(child.tag), _xml_node(child)
        if key not in out:
            out[key] = value
        elif isinstance(out[key], list):
            out[key].append(value)
        else:
            out[key] = [out[key], value]
    return out
