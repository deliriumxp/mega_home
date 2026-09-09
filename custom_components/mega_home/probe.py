"""Проба сети по заданию менеджера — инструмент шлюза, а не функция.

Зачем это есть. Мониторинг объекта (директор Control4, Home Assistant,
регистратор, логи) менеджер вёл ЧЕРЕЗ WG-ТУННЕЛЬ: ходил из офиса в LAN объекта
сам. Туннеля нет у части парка, и рвётся он ровно тогда, когда объект интересен;
а интеграция стоит в той же локалке и достаёт те же устройства без него вовсе.

⚠ Здесь НЕТ ни одной проверки: ни «здоров ли директор», ни «что значит этот
ответ». Только два примитива — HTTP-запрос и обмен строками по TCP/TLS. Смысл
пробы (куда идти, что послать, как понять ответ) живёт В МЕНЕДЖЕРЕ и меняется
его деплоем, а не релизом HACS с перезапуском Home Assistant на КАЖДОМ объекте
(docs/plan-thin-integration.md, docs/monitoring-and-logs.md). Новая проверка
мониторинга обязана быть новым ЗАДАНИЕМ, а не новым кодом здесь; если задание
из этих двух примитивов не собирается — это разговор про примитив, а не про
проверку.

⚠ Кто зовёт: ТОЛЬКО менеджер, живым каналом (`link.py`), где объект уже опознан
своим токеном. Локальной HTTP-двери у этой операции нет и быть не должно —
контур дома без аутентификации, и там она стала бы открытым прокси в LAN
объекта для любого, кто в его Wi-Fi.
"""

from __future__ import annotations

import asyncio
import re
import ssl
import time
from http import HTTPStatus
from typing import Any

import aiohttp
from homeassistant.core import HomeAssistant

from .const import LOGGER
from .ops import OpError

# Заданий в одном запросе. Больше — это уже не проба, а обход сети; менеджер
# шлёт их пачкой по одному устройству (статус + версия + канал демонов).
MAX_PROBES = 8
# Потолок ожидания. Свой срок задания меньше — берём его; больше — режем:
# зависшая проба держит соединение дома и его пул.
MAX_TIMEOUT_S = 20.0
# Ответ пробы — диагностический ТЕКСТ (JSON статуса, вывод sysmand), а не файл:
# файлы ездят своим каналом. Обрезанное помечено `truncated`.
MAX_BODY_BYTES = 256 * 1024


async def run(hass: HomeAssistant, payload: dict[str, Any]) -> dict[str, Any]:
    """Выполнить пачку заданий и вернуть результаты В ТОМ ЖЕ ПОРЯДКЕ.

    ⚠ Отказ отдельной пробы — это НЕ отказ операции: недостижимый контроллер
    даёт `ok: false` с причиной, а не 500 на весь запрос. Мониторингу нужна
    именно причина («соединение отклонено», «таймаут»), и потерять её, свалив
    всю пачку, значит показать оператору пустоту вместо диагноза.
    """
    probes = payload.get("probes")
    if not isinstance(probes, list) or not probes:
        raise OpError("Задание пробы пустое", HTTPStatus.BAD_REQUEST)
    if len(probes) > MAX_PROBES:
        raise OpError(
            f"Заданий в одном запросе больше {MAX_PROBES}", HTTPStatus.BAD_REQUEST
        )
    results = await asyncio.gather(*(_one(hass, item) for item in probes))
    return {"results": list(results)}


async def _one(hass: HomeAssistant, item: Any) -> dict[str, Any]:
    if not isinstance(item, dict):
        return _failed("задание не является объектом")
    kind = str(item.get("kind") or "")
    started = time.monotonic()
    try:
        if kind == "http":
            return await _http(hass, item, started)
        if kind == "tcp":
            return await _tcp(item, started)
    except asyncio.TimeoutError:
        return _failed(f"таймаут {_timeout(item) * 1000:.0f} мс", started)
    except (aiohttp.ClientError, OSError, ssl.SSLError) as err:
        # ⚠ Причина словами, а не класс исключения: её читает человек в карточке
        # объекта, и «ConnectionRefusedError» ему не говорит ничего.
        return _failed(_describe(err), started)
    except Exception as err:  # noqa: BLE001 — проба не имеет права ронять канал
        LOGGER.exception("Проба сорвалась: %s", err)
        return _failed("проба сорвалась в доме", started)
    return _failed(f"неизвестный вид пробы «{kind}»", started)


async def _http(
    hass: HomeAssistant, item: dict[str, Any], started: float
) -> dict[str, Any]:
    """Один HTTP(S)-запрос к устройству в LAN объекта.

    ⚠ `insecure` (не проверять сертификат) — норма для этой сети, а не
    небрежность: у контроллера Control4 `CN=<uuid>`, у регистратора
    самоподписанный, и доверенной цепочки для них не существует в принципе.
    Решает это менеджер заданием: он один знает, с кем говорит.
    """
    from homeassistant.helpers.aiohttp_client import async_get_clientsession

    url = str(item.get("url") or "")
    if not url.startswith(("http://", "https://")):
        return _failed("адрес пробы должен быть http(s)", started)
    body = item.get("body")
    headers = item.get("headers") if isinstance(item.get("headers"), dict) else None
    session = async_get_clientsession(hass)
    async with session.request(
        str(item.get("method") or "GET").upper(),
        url,
        headers=headers,
        data=body.encode("utf-8") if isinstance(body, str) else None,
        timeout=aiohttp.ClientTimeout(total=_timeout(item)),
        ssl=False if item.get("insecure") else None,
        # Редирект — это уже другой адрес, а проба спрашивает про ЭТОТ.
        allow_redirects=False,
    ) as answer:
        raw = await answer.content.read(MAX_BODY_BYTES + 1)
        text, truncated = _text(raw)
        ok = 200 <= answer.status < 300
        result = {
            "ok": ok,
            "ms": _ms(started),
            "status": answer.status,
            "body": text,
        }
        if truncated:
            result["truncated"] = True
        if not ok:
            result["error"] = f"HTTP {answer.status}"
        return result


async def _tcp(item: dict[str, Any], started: float) -> dict[str, Any]:
    """Обмен строками по TCP, при надобности под TLS.

    ⚠ Ради этого примитива он и заведён: канал `sysmand` контроллера Control4 —
    TLS без авторизации, ОДНА команда на соединение, и закрывает соединение
    только клиент (docs/control4-sysmand-integration.md). Поэтому здесь
    обязательны признак конца ответа (`readUntil`) И жёсткий срок: без них
    проба висит столько, сколько работает дом.
    """
    host = str(item.get("host") or "")
    port = item.get("port")
    if not host or not isinstance(port, int):
        return _failed("проба TCP без адреса или порта", started)
    timeout = _timeout(item)
    context: ssl.SSLContext | None = None
    if item.get("tls"):
        # ⚠ Контекст СВОЙ, без загрузки системных сертификатов: `create_default_
        # context` читает их с диска, а это блокирующий вызов в цикле событий —
        # Home Assistant за такое ругается в лог на каждой пробе.
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        if item.get("insecure"):
            # См. `_http`: проверять сертификат контроллера нечем.
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE
    reader, writer = await asyncio.wait_for(
        asyncio.open_connection(host, port, ssl=context), timeout
    )
    try:
        send = item.get("send")
        if isinstance(send, str) and send:
            writer.write(send.encode("utf-8"))
            await writer.drain()
        text, hit = await _read_until(reader, item.get("readUntil"), timeout, started)
    finally:
        writer.close()
        # ⚠ Закрытие дожидаемся, но не ценой ответа: у мёртвого сокета
        # `wait_closed` умеет висеть, а результат пробы у нас уже есть.
        try:
            await asyncio.wait_for(writer.wait_closed(), 2)
        except (asyncio.TimeoutError, OSError):
            pass
    result: dict[str, Any] = {"ok": hit, "ms": _ms(started), "body": text}
    if not hit:
        result["error"] = _tcp_error(text, item)
    return result


async def _read_until(
    reader: asyncio.StreamReader,
    marker: Any,
    timeout: float,
    started: float,
) -> tuple[str, bool]:
    """Читать до признака конца, до закрытия или до срока — что раньше.

    Отдаём ДАЖЕ неполный текст: для пробы частичный вывод полезнее пустоты, и
    именно по нему потом видно, на чём собеседник замолчал.
    """
    pattern = re.compile(str(marker)) if isinstance(marker, str) and marker else None
    chunks: list[bytes] = []
    size = 0
    while True:
        left = timeout - (time.monotonic() - started)
        if left <= 0:
            return _text(b"".join(chunks))[0], False
        try:
            chunk = await asyncio.wait_for(reader.read(65536), left)
        except asyncio.TimeoutError:
            return _text(b"".join(chunks))[0], False
        if not chunk:
            # Собеседник закрыл соединение сам: для протокола без признака
            # конца это и есть конец ответа.
            text = _text(b"".join(chunks))[0]
            return text, pattern is None and bool(text)
        chunks.append(chunk)
        size += len(chunk)
        text = _text(b"".join(chunks))[0]
        if pattern is not None and pattern.search(text):
            return text, True
        if size > MAX_BODY_BYTES:
            return text, pattern is None


def _tcp_error(text: str, item: dict[str, Any]) -> str:
    if not text:
        return "соединение закрыто без ответа"
    return f"ответ не завершился признаком «{item.get('readUntil')}»"


def _timeout(item: Any) -> float:
    ms = item.get("timeoutMs") if isinstance(item, dict) else None
    if not isinstance(ms, (int, float)) or isinstance(ms, bool) or ms <= 0:
        return MAX_TIMEOUT_S
    return min(float(ms) / 1000.0, MAX_TIMEOUT_S)


def _text(raw: bytes) -> tuple[str, bool]:
    truncated = len(raw) > MAX_BODY_BYTES
    return raw[:MAX_BODY_BYTES].decode("utf-8", "replace"), truncated


def _ms(started: float) -> int:
    return int((time.monotonic() - started) * 1000)


def _failed(error: str, started: float | None = None) -> dict[str, Any]:
    return {
        "ok": False,
        "ms": _ms(started) if started is not None else 0,
        "error": error,
    }


def _describe(err: Exception) -> str:
    """Сетевая ошибка по-русски — её читает оператор, а не разработчик."""
    if isinstance(err, ConnectionRefusedError):
        return "соединение отклонено (никто не слушает)"
    if isinstance(err, aiohttp.ServerTimeoutError):
        return "таймаут"
    if isinstance(err, ssl.SSLError):
        return f"ошибка TLS: {err}"
    if isinstance(err, aiohttp.ClientConnectorError):
        return f"не удалось соединиться: {err.os_error}"
    text = str(err) or err.__class__.__name__
    return f"сетевая ошибка: {text}"
