"""Проверка адреса менеджера — та часть потока, которой Home Assistant не нужен.

Сам поток (формы, перезагрузка записи) проверяется только запуском в HA; здесь
закрыт разбор адреса, на котором легко ошибиться в обе стороны: и пропустить
мусор, и отвергнуть валидный адрес с портом.
"""

from __future__ import annotations

import pytest

from mega_home.config_flow import check_url


@pytest.mark.parametrize(
    "url",
    [
        "https://mm.example.com",
        # Ровно тот случай, ради которого нужна перенастройка: inbound-порт
        # напрямую, минуя обратный прокси.
        "https://mm.example.com:8055",
        "http://192.168.1.10:8055",
    ],
)
def test_годные_адреса(url: str) -> None:
    assert check_url(url) is None


@pytest.mark.parametrize(
    "url",
    ["mm.example.com:8055", "wss://mm.example.com/inbound/home", "https://", ""],
)
def test_негодные_адреса(url: str) -> None:
    assert check_url(url) == "invalid_url"
