"""Проба сети по заданию менеджера — инструмент шлюза, а не функция.

Зачем это есть. Мониторинг объекта (директор Control4, Home Assistant,
регистратор, логи) менеджер вёл ЧЕРЕЗ WG-ТУННЕЛЬ: ходил из офиса в LAN объекта
сам. Туннеля нет у части парка, и рвётся он ровно тогда, когда объект интересен;
а интеграция стоит в той же локалке и достаёт те же устройства без него вовсе.

⚠ Здесь НЕТ ни одной проверки: ни «здоров ли директор», ни «что значит этот
ответ». Смысл пробы (куда идти, что послать, как понять ответ) живёт В МЕНЕДЖЕРЕ
и меняется его деплоем (docs/plan-thin-integration.md, docs/monitoring-and-logs.md).

⚠ Своего транспорта у пробы НЕТ (ревью 2026-10-30, долг `docs/plan-thin-gateway.md`): задание
переводится в вызов `connect` и исполняется им — один адресат, один TLS (`connect_tls.py`), одни
пределы. Прежний свой HTTP/TCP-клиент ходил без проверки адреса и мимо опций TLS, и каждая ось,
закрытая в `connect`, у пробы оставалась открытой. Форма задания и ответа — прежняя: её шлют
мониторинг и сторож менеджера (`probe.types.ts`).

⚠ Кто зовёт: ТОЛЬКО менеджер, живым каналом (`link.py`), где объект уже опознан
своим токеном, и сторож дома правилами того же менеджера (`agent.py`).
"""

from __future__ import annotations

import asyncio
import base64
import ssl
import time
from http import HTTPStatus
from typing import Any
from urllib.parse import urlsplit

import aiohttp

from . import connect
from .host import Host
from .ops_base import OpError

# Заданий в одном запросе. Больше — это уже не проба, а обход сети; менеджер
# шлёт их пачкой по одному устройству (статус + версия + канал демонов).
MAX_PROBES = 8
# Потолок ожидания. Свой срок задания меньше — берём его; больше — режем:
# зависшая проба держит соединение дома и его пул.
MAX_TIMEOUT_S = 20.0
# Ответ пробы — диагностический ТЕКСТ (JSON статуса, вывод sysmand), а не файл:
# файлы ездят своим каналом. Обрезанное помечено `truncated`.
MAX_BODY_BYTES = 256 * 1024


async def run(env: Host, payload: dict[str, Any]) -> dict[str, Any]:
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
        raise OpError(f"Заданий в одном запросе больше {MAX_PROBES}", HTTPStatus.BAD_REQUEST)
    results = await asyncio.gather(*(_one(item) for item in probes))
    return {"results": list(results)}


def request_of(item: dict[str, Any]) -> dict[str, Any]:
    """Задание пробы → вызов `connect`. `insecure` — «не проверять сертификат» и по имени хоста
    (`tlsInsecure`): у контроллера Control4 `CN=<uuid>`, у регистратора самоподписанный, и доверенной
    цепочки для них не существует; устройству по адресу сертификат не проверяется и так."""
    kind = str(item.get("kind") or "")
    timeout = _timeout(item)
    tls_extra = {"tlsInsecure": True} if item.get("insecure") else {}
    if kind == "http":
        url = urlsplit(str(item.get("url") or ""))
        if url.scheme not in ("http", "https") or not url.hostname:
            raise OpError("адрес пробы должен быть http(s)")
        body = item.get("body")
        return {
            "kind": "http",
            "host": url.hostname,
            "port": url.port or (443 if url.scheme == "https" else 80),
            "tls": url.scheme == "https",
            "method": str(item.get("method") or "GET").upper(),
            "path": (url.path or "/") + (f"?{url.query}" if url.query else ""),
            "headers": item.get("headers") if isinstance(item.get("headers"), dict) else {},
            **({"body": body} if isinstance(body, str) else {}),
            "timeout": timeout,
            **tls_extra,
        }
    if kind == "tcp":
        host, port = str(item.get("host") or ""), item.get("port")
        if not host or not isinstance(port, int):
            raise OpError("проба TCP без адреса или порта")
        send = item.get("send")
        marker = item.get("readUntil")
        return {
            "kind": "tcp",
            "host": host,
            "port": port,
            "tls": item.get("tls") is True,
            "sendBase64": base64.b64encode(send.encode("utf-8")).decode("ascii") if isinstance(send, str) and send else "",
            "readUntil": {"deadline": timeout, **({"pattern": marker} if isinstance(marker, str) and marker else {})},
            "timeout": timeout,
            **tls_extra,
        }
    raise OpError(f"неизвестный вид пробы «{kind}»")


async def _one(item: Any) -> dict[str, Any]:
    if not isinstance(item, dict):
        return _failed("задание не является объектом")
    started = time.monotonic()
    try:
        request = request_of(item)
        answer = await connect.perform(request)
    except OpError as err:
        return _failed(_describe(err), started)
    except Exception:  # noqa: BLE001 — проба не имеет права ронять канал
        return _failed("проба сорвалась в доме", started)
    text, truncated = _text(answer)
    if request["kind"] == "http":
        status = int(answer.get("status") or 0)
        result: dict[str, Any] = {"ok": 200 <= status < 300, "ms": _ms(started), "status": status, "body": text}
        if not result["ok"]:
            result["error"] = f"HTTP {status}"
    else:
        # Есть признак конца — ответ годен, когда он найден; нет — когда собеседник закрыл соединение
        # сам (это и есть конец ответа у протокола без признака) или ответ упёрся в потолок.
        pattern = "pattern" in request["readUntil"]
        hit = bool(answer.get("matched")) if pattern else bool(text) and (bool(answer.get("closed")) or truncated)
        result = {"ok": hit, "ms": _ms(started), "body": text}
        if not hit:
            result["error"] = (
                "соединение закрыто без ответа" if not text else f"ответ не завершился признаком «{item.get('readUntil')}»"
            )
    if truncated:
        result["truncated"] = True
    return result


def _describe(err: OpError) -> str:
    """Причина словами для оператора — из первопричины, если она сетевая."""
    cause = err.__cause__
    if isinstance(cause, (asyncio.TimeoutError, aiohttp.ServerTimeoutError)):
        return "таймаут"
    if isinstance(cause, ConnectionRefusedError):
        return "соединение отклонено (никто не слушает)"
    if isinstance(cause, ssl.SSLError):
        return f"ошибка TLS: {cause}"
    if isinstance(cause, aiohttp.ClientConnectorError):
        if isinstance(cause.os_error, ConnectionRefusedError):
            return "соединение отклонено (никто не слушает)"
        return f"не удалось соединиться: {cause.os_error}"
    return err.message


def _timeout(item: Any) -> float:
    ms = item.get("timeoutMs") if isinstance(item, dict) else None
    if not isinstance(ms, (int, float)) or isinstance(ms, bool) or ms <= 0:
        return MAX_TIMEOUT_S
    return min(float(ms) / 1000.0, MAX_TIMEOUT_S)


def _text(answer: dict[str, Any]) -> tuple[str, bool]:
    raw = base64.b64decode(answer["bodyBase64"]) if answer.get("bodyBase64") else str(answer.get("body") or "").encode("utf-8")
    return raw[:MAX_BODY_BYTES].decode("utf-8", "replace"), len(raw) > MAX_BODY_BYTES


def _ms(started: float) -> int:
    return int((time.monotonic() - started) * 1000)


def _failed(error: str, started: float | None = None) -> dict[str, Any]:
    return {"ok": False, "ms": _ms(started) if started is not None else 0, "error": error}
