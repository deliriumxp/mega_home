"""Общее для всех операций: отказ, поиск плитки и разбор чисел.

⚠ Отдельным модулем, чтобы классы устройств (`ops_video`, `ops_camera`,
`ops_webrtc`) не тянули друг друга ради одного `OpError`. Всё остальное делится
ПО КЛАССАМ, а не по вендорам — правило заказчика, `CLAUDE.md`.
"""

from __future__ import annotations

from http import HTTPStatus
from typing import Any


class OpError(Exception):
    """A refusal the resident should read, with the status that fits it."""

    def __init__(self, message: str, status: int = HTTPStatus.BAD_REQUEST) -> None:
        super().__init__(message)
        self.message = message
        self.status = int(status)

def find(items: list[dict[str, Any]], item_id: Any) -> dict[str, Any] | None:
    if not isinstance(item_id, str):
        return None
    return next((item for item in items if item.get("id") == item_id), None)

def number(value: Any, low: int, high: int) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError) as err:
        raise ValueError(f"Значение должно быть от {low} до {high}") from err
    if not low <= parsed <= high:
        raise ValueError(f"Значение должно быть от {low} до {high}")
    return parsed

def _int(value: Any, default: int) -> int:
    try:
        return int(str(value))
    except (TypeError, ValueError):
        return default
