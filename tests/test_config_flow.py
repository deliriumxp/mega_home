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


# --- Ключ настройки: `https://адрес#токен` (`setup_key.py`) ------------------

from mega_home.config_flow import MegaHomeConfigFlow, unique_id_of  # noqa: E402
from mega_home.setup_key import parse_key  # noqa: E402


def test_ключ_целиком_делится_на_адрес_и_токен() -> None:
    assert parse_key("https://mm.example.ru#abc123") == ("https://mm.example.ru", "abc123")


def test_пробелы_по_краям_не_попадают_ни_в_адрес_ни_в_токен() -> None:
    assert parse_key("  https://mm.example.ru # abc123 \n") == ("https://mm.example.ru", "abc123")


def test_слэш_на_конце_адреса_снимается() -> None:
    assert parse_key("https://mm.example.ru/#abc") == ("https://mm.example.ru", "abc")


def test_порт_в_адресе_сохраняется() -> None:
    assert parse_key("http://192.168.1.10:8055#abc") == ("http://192.168.1.10:8055", "abc")


def test_решётка_в_середине_делит_по_первой_остальное_токен() -> None:
    assert parse_key("https://mm.example.ru#ab#cd") == ("https://mm.example.ru", "ab#cd")


def test_ключ_с_адресом_не_берёт_поле_адреса() -> None:
    assert parse_key("https://a.ru#t", "https://b.ru") == ("https://a.ru", "t")


def test_голый_токен_берёт_адрес_из_поля() -> None:
    assert parse_key(" abc ", " https://mm.example.ru/ ") == ("https://mm.example.ru", "abc")


@pytest.mark.parametrize(
    ("raw", "url"),
    [("abc", ""), ("", "https://mm.example.ru"), ("#abc", ""), ("https://mm.example.ru#", ""), ("   ", "")],
)
def test_неполный_ввод_не_разбирается(raw: str, url: str) -> None:
    assert parse_key(raw, url) is None


def test_запись_прежнего_вида_и_личность_не_меняются() -> None:
    """В записи — те же три ключа, что и до ключа настройки, а `unique_id` — тот же хэш токена."""
    data = MegaHomeConfigFlow._clean({"key": "https://mm.example.ru/#tok", "verify_ssl": False})
    assert data == {"manager_url": "https://mm.example.ru", "token": "tok", "verify_ssl": False}
    # Старый способ «адрес + токен» даёт ту же запись и тот же unique_id.
    old = MegaHomeConfigFlow._clean({"key": "tok", "manager_url": "https://mm.example.ru"})
    assert old is not None and unique_id_of(old["token"]) == unique_id_of("tok")
    assert MegaHomeConfigFlow._clean({"key": "tok"}) is None
