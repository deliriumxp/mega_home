"""Обход локальной сети объекта по заказу менеджера — инструмент шлюза.

Зачем. Инсталлятору нужно видеть, что стоит в сети клиента, и куда можно войти
по веб-интерфейсу — аналог Network Scan у OvrC. Список DHCP-аренд роутера этого
не даёт: устройство со статикой в нём не появится никогда. Агент ВНУТРИ локалки
(эта интеграция) видит сеть напрямую и обходит её сам; менеджер только заказывает
обход и рисует результат.

⚠ Здесь НЕТ толкования «что это за устройство»: только обнаружение хостов,
перебор веб-портов, снятие заголовка/`<title>` страницы и ПОДТВЕРЖДЕНИЕ
признаков (RTSP/ONVIF — «это видеонаблюдение»). Смысл (какой порт открывать,
что считать веб-интерфейсом, как назвать вендора и как подписать видеонаблюдение
в строке) живёт в менеджере и меняется его деплоем, а не релизом HACS с
перезапуском Home Assistant на КАЖДОМ объекте (docs/plan-thin-integration.md в
менеджере).

⚠ Кто зовёт: ТОЛЬКО менеджер живым каналом, где объект опознан своим токеном.
Локальной HTTP-двери у этой операции нет и быть не должно — контур дома без
аутентификации, и такой обход стал бы открытым сканером чужой сети для любого,
кто в его Wi-Fi.

Почему обнаружение двойное. ARP-таблица ловит всё, что недавно общалось по L2, и
даёт MAC. Чтобы она наполнилась САМА, в каждый адрес подсети уходит пустой UDP —
ядро резолвит соседа и кладёт ответ в `/proc/net/arp`. Плюс TCP-обход: он ловит
хостов за маршрутом (где ARP спрашивает шлюз, а не цель) и тех, кто на ARP не
ответил. Ни привилегий, ни `nmap` для этого не нужно.
"""

from __future__ import annotations

import asyncio
import ipaddress
import re
import socket
import ssl
import time
from http import HTTPStatus
from typing import Any

import aiohttp
from homeassistant.core import HomeAssistant

from .const import LOGGER
from .ops import OpError

# Потолок ожидания одного TCP-соединения. Короче нельзя: домашние железки
# отвечают за десятки миллисекунд, а вот камера под нагрузкой — за полсекунды.
CONNECT_TIMEOUT_S = 0.6
# Соединений разом. 128 — обход /24 за считанные секунды, а полуоткрытых
# соединений столько не держит даже дешёвый роутер.
CONCURRENCY = 128
# Сколько ждём, пока ядро разберётся с ARP после рассылки UDP.
ARP_WAIT_S = 1.5
# Верхний предел подсети: /22 (1022 адреса). Больше — это уже не квартира, а
# офисная сеть, и обход превратился бы в нагрузку на чужой роутер.
MAX_HOSTS = 1022
# Веб-порты: только они и нужны — цель обхода «куда войти мышкой». Список ОБЩИЙ
# с менеджером (`backend/src/modules/lan-scan/web-ports.ts`); расходиться им
# нельзя, иначе один и тот же дом показывал бы разное в зависимости от того,
# есть ли интеграция.
WEB_PORTS = (
    80, 81, 443, 8000, 8008, 8080, 8081, 8082, 8088, 8090,
    8123, 8181, 8443, 8888, 9000, 9001, 9090, 9443, 10000, 5000, 5001,
)
# По каким портам ищем ЖИВЫЕ хосты. Отдельно от `WEB_PORTS` и намеренно короче:
# обход /24 стоит `len(DISCOVERY_PORTS) * 254` соединений, и каждый лишний порт
# тут — секунды. Отказ (RST) на закрытом порту — тоже признак жизни.
DISCOVERY_PORTS = (80, 443, 9)
# Порты, на которых по умолчанию HTTPS: сначала пробуем его, иначе — HTTP.
TLS_PORTS = frozenset({443, 8443, 9443, 5001})
# Порты видеопотока (RTSP) — ОТДЕЛЬНО от веб-портов: спрашиваем про них только
# затем, чтобы отличить видеонаблюдение от всего остального. 554 — стандарт,
# 8554 — второй по частоте, 322 — RTSP поверх TLS (`rtsps`).
RTSP_PORTS = ((554, False), (8554, False), (322, True))
# Запрос `OPTIONS`: единственный метод RTSP, обязанный работать без авторизации
# (RFC 2326, §10.1) — ответ не зависит от того, есть ли у нас пароль.
RTSP_OPTIONS = b"OPTIONS * RTSP/1.0\r\nCSeq: 1\r\nUser-Agent: mega-manager\r\n\r\n"
# Срок диалога по RTSP: камера под нагрузкой отвечает за полсекунды, но обрыв на
# этом сроке — не приговор устройству, поэтому берём с запасом.
VIDEO_TIMEOUT_S = 1.5
# Путь ONVIF-службы устройства: он один и тот же у всех вендоров.
ONVIF_PATH = "/onvif/device_service"
# Тело ONVIF-запроса — `GetSystemDateAndTime`: ЕДИНСТВЕННЫЙ запрос ONVIF,
# который устройство обязано обслужить без авторизации. Спросить что-то
# осмысленнее — получить `401` от каждой настроенной железки и не отличить
# ONVIF от обычной страницы за авторизацией.
ONVIF_BODY = (
    '<?xml version="1.0" encoding="UTF-8"?>'
    '<s:Envelope xmlns:s="http://www.w3.org/2003/05/soap-envelope">'
    '<s:Body><GetSystemDateAndTime xmlns="http://www.onvif.org/ver10/device/wsdl"/>'
    "</s:Body></s:Envelope>"
)
ONVIF_CONTENT_TYPE = "application/soap+xml; charset=utf-8"
# Ответ RTSP-сервера: версия протокола и код. `200 OK` — настроенная камера,
# `401` — та, что требует пароль (и это тоже доказательство: веб-сервис на этом
# порту так не ответит), `404` — регистратор, не знающий `OPTIONS *`.
_RTSP_RE = re.compile(r"^RTSP/\d\.\d\s+\d{3}")
# Конверт SOAP любого вендора: `<s:Envelope>`, `<SOAP-ENV:Envelope>`, `<soapenv:…>`.
_SOAP_RE = re.compile(r"<(?:[\w.-]+:)?envelope[\s>]|soap-env|soapenv", re.IGNORECASE)
_ONVIF_RESPONSE_RE = re.compile(r"getsystemdateandtimeresponse", re.IGNORECASE)
# Снятие страницы: короткий срок и малый потолок — это заголовок, а не файл.
FINGERPRINT_TIMEOUT_S = 3.0
FINGERPRINT_CONCURRENCY = 24
MAX_FINGERPRINT_BYTES = 64 * 1024
# Обратный DNS — подсказка, а не обязательное поле: полсекунды на хост.
NAME_TIMEOUT_S = 0.5
NAME_CONCURRENCY = 32
# UDP-порт для «толчка» ARP. Закрытый и никому не нужный.
ARP_TOUCH_PORT = 9


async def run(hass: HomeAssistant, payload: dict[str, Any]) -> dict[str, Any]:
    """Обойти подсеть и вернуть найденные хосты с открытыми веб-портами."""
    network = _target_network(payload.get("subnet") if payload else None)
    started = time.monotonic()
    ips = [str(ip) for ip in network.hosts()][:MAX_HOSTS]
    LOGGER.info("Обход сети %s (%d адресов) по заказу менеджера", network, len(ips))

    # Толчок ARP и TCP-обход идут ВМЕСТЕ: пока ядро ждёт ответов на ARP,
    # TCP-обход уже отрабатывает, и отдельная пауза на ARP не тратится дважды.
    await hass.async_add_executor_job(_touch_arp, ips)
    alive, arp = await asyncio.gather(
        _tcp_sweep(ips),
        _arp_after(hass),
    )

    hosts: list[dict[str, Any]] = []
    for ip in ips:
        mac = arp.get(ip, "")
        if ip in alive or mac:
            hosts.append({"ip": ip, "mac": mac, "hostname": "", "ports": []})
    # ⚠ Показываем ВСЕ найденные устройства, а не только те, у кого открыт
    # веб-порт: их мониторинг появится позже, и терять их уже здесь нельзя.
    hosts.sort(key=lambda host: ipaddress.ip_address(host["ip"]))
    await _scan_ports(hosts)
    await _fingerprint(hass, hosts)
    await _probe_video(hass, hosts)
    await _names(hosts)

    LOGGER.info(
        "Обход сети %s закончен: %d устройств за %d мс",
        network,
        len(hosts),
        int((time.monotonic() - started) * 1000),
    )
    return {
        "subnet": str(network),
        "ms": int((time.monotonic() - started) * 1000),
        "hosts": hosts,
    }


def _target_network(value: Any) -> ipaddress.IPv4Network:
    """Подсеть обхода: присланная менеджером, иначе — своя.

    ⚠ Проверяем и приватность, и размер: обход — это поток соединений, и
    присланное «0.0.0.0/0» не должно превратить дом в сканер интернета.
    """
    network = _parse_network(value) or _local_network()
    if network is None:
        raise OpError(
            "Не удалось определить подсеть объекта для обхода", HTTPStatus.BAD_REQUEST
        )
    return network


def _parse_network(value: Any) -> ipaddress.IPv4Network | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        network = ipaddress.ip_network(value.strip(), strict=False)
    except ValueError:
        return None
    if not isinstance(network, ipaddress.IPv4Network) or not network.is_private:
        return None
    if network.num_addresses > MAX_HOSTS + 2:
        return None
    return network


def _local_network() -> ipaddress.IPv4Network | None:
    ip = _local_ip()
    if not ip:
        return None
    try:
        return ipaddress.ip_network(f"{ip}/24", strict=False)
    except ValueError:
        return None


def _local_ip() -> str:
    """Свой адрес в основной сети. UDP-`connect` не отправляет пакетов."""
    try:
        probe = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            probe.setblocking(False)
            probe.connect(("8.8.8.8", 80))
            return str(probe.getsockname()[0])
        finally:
            probe.close()
    except OSError:
        return ""


def _touch_arp(ips: list[str]) -> None:
    """Заставить ядро резолвить соседей: пустой UDP в каждый адрес подсети.

    ⚠ Неблокирующим сокетом и с проглоченными ошибками: недостижимый адрес —
    обычное дело в обходе, и падать на нём нельзя.
    """
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    except OSError:
        return
    try:
        sock.setblocking(False)
        for ip in ips:
            try:
                sock.sendto(b"\x00", (ip, ARP_TOUCH_PORT))
            except OSError:
                pass
    finally:
        sock.close()


async def _arp_after(hass: HomeAssistant) -> dict[str, str]:
    """Дать ядру время и прочитать ARP-таблицу."""
    await asyncio.sleep(ARP_WAIT_S)
    return await hass.async_add_executor_job(_arp_table)


def _arp_table() -> dict[str, str]:
    """`/proc/net/arp`: адрес → MAC. Не-Linux и запрет чтения — пустая карта."""
    table: dict[str, str] = {}
    try:
        with open("/proc/net/arp", encoding="utf-8") as handle:
            next(handle, None)
            for line in handle:
                parts = line.split()
                if len(parts) < 4:
                    continue
                ip, _hwtype, flags, mac = parts[0], parts[1], parts[2], parts[3]
                if flags == "0x0" or mac == "00:00:00:00:00:00":
                    continue
                table[ip] = mac.lower()
    except OSError:
        pass
    return table


async def _tcp_sweep(ips: list[str]) -> set[str]:
    """Живые хосты по TCP: открытый порт или RST на закрытом — оба признак."""
    semaphore = asyncio.Semaphore(CONCURRENCY)

    async def one(ip: str) -> str:
        async with semaphore:
            for port in DISCOVERY_PORTS:
                if await _reachable(ip, port):
                    return ip
        return ""

    results = await asyncio.gather(*(one(ip) for ip in ips))
    return {ip for ip in results if ip}


async def _reachable(ip: str, port: int) -> bool:
    """Хост жив: соединение открылось ИЛИ отклонено (RST — ответ живого стека)."""
    try:
        _reader, writer = await asyncio.wait_for(
            asyncio.open_connection(ip, port), CONNECT_TIMEOUT_S
        )
        writer.close()
        return True
    except ConnectionRefusedError:
        return True
    except (asyncio.TimeoutError, OSError):
        return False


async def _is_open(ip: str, port: int) -> bool:
    try:
        _reader, writer = await asyncio.wait_for(
            asyncio.open_connection(ip, port), CONNECT_TIMEOUT_S
        )
        writer.close()
        return True
    except (asyncio.TimeoutError, OSError):
        return False


async def _scan_ports(hosts: list[dict[str, Any]]) -> None:
    semaphore = asyncio.Semaphore(CONCURRENCY)

    async def one(host: dict[str, Any]) -> None:
        async def check(port: int) -> bool:
            async with semaphore:
                return await _is_open(host["ip"], port)

        opened = await asyncio.gather(*(check(port) for port in WEB_PORTS))
        host["ports"] = [
            {"port": port, "tls": port in TLS_PORTS}
            for port, is_open in zip(WEB_PORTS, opened)
            if is_open
        ]

    await asyncio.gather(*(one(host) for host in hosts))


async def _fingerprint(hass: HomeAssistant, hosts: list[dict[str, Any]]) -> None:
    """Снять `Server` и `<title>` с открытых веб-портов: по ним видно, что это."""
    from homeassistant.helpers.aiohttp_client import async_get_clientsession

    session = async_get_clientsession(hass)
    semaphore = asyncio.Semaphore(FINGERPRINT_CONCURRENCY)

    async def one(host: dict[str, Any], port: dict[str, Any]) -> None:
        async with semaphore:
            # ⚠ Схему не угадываем по номеру порта: на 80 сплошь и рядом HTTPS,
            # а на 8080 — HTTP. Пробуем ожидаемую и, если не ответила, вторую.
            order = (True, False) if port["tls"] else (False, True)
            for tls in order:
                info = await _fetch(session, host["ip"], port["port"], tls)
                if info is not None:
                    port["tls"] = tls
                    port.update(info)
                    return

    await asyncio.gather(
        *(one(host, port) for host in hosts for port in host["ports"])
    )


async def _fetch(
    session: aiohttp.ClientSession, ip: str, port: int, tls: bool
) -> dict[str, Any] | None:
    scheme = "https" if tls else "http"
    try:
        async with session.get(
            f"{scheme}://{ip}:{port}/",
            timeout=aiohttp.ClientTimeout(total=FINGERPRINT_TIMEOUT_S),
            # ⚠ Сертификат устройства не проверяем: у камер и контроллеров он
            # самоподписанный, и проверять его нечем (то же решение, что в пробе).
            ssl=False,
            allow_redirects=False,
        ) as answer:
            status = answer.status
            server = str(answer.headers.get("Server") or "")[:80]
            raw = await answer.content.read(MAX_FINGERPRINT_BYTES)
    except (aiohttp.ClientError, asyncio.TimeoutError, OSError, ValueError):
        return None
    return {
        "status": status,
        "server": server,
        "title": _title(raw.decode("utf-8", "replace")),
    }


_TITLE_RE = re.compile(r"<title[^>]*>(.*?)</title>", re.IGNORECASE | re.DOTALL)


def _title(text: str) -> str:
    match = _TITLE_RE.search(text)
    if not match:
        return ""
    return re.sub(r"\s+", " ", match.group(1)).strip()[:80]


async def _probe_video(hass: HomeAssistant, hosts: list[dict[str, Any]]) -> None:
    """Опознать видеонаблюдение: RTSP-порты прямо и ONVIF поверх веб-портов.

    ⚠ Предмет — ВИДЕОНАБЛЮДЕНИЕ, а не «камера»: по RTSP и ONVIF одинаково
    отвечают регистраторы, камеры и вызывные панели домофонии, и по протоколу
    они неразличимы. Толкование того, как это назвать в интерфейсе, живёт в
    менеджере (docs/plan-thin-integration.md).

    ⚠ Отметка ставится по СОСТОЯВШЕМУСЯ диалогу, а не по открытому порту: «554
    открыт» одинаково выглядит у камеры и у чужого сервиса, вставшего на тот же
    номер. Правила опознания ОБЩИЕ с менеджером
    (`backend/src/modules/lan-scan/video-probe.ts`) — расходиться им нельзя.
    """
    semaphore = asyncio.Semaphore(CONCURRENCY)

    async def rtsp_one(host: dict[str, Any], port: int, use_tls: bool) -> None:
        async with semaphore:
            if await _answers_rtsp(host["ip"], port, use_tls):
                host["ports"].append({"port": port, "tls": use_tls, "video": "rtsp"})

    await asyncio.gather(
        *(rtsp_one(host, port, tls) for host in hosts for port, tls in RTSP_PORTS)
    )

    from homeassistant.helpers.aiohttp_client import async_get_clientsession

    session = async_get_clientsession(hass)
    onvif_semaphore = asyncio.Semaphore(FINGERPRINT_CONCURRENCY)

    async def onvif_one(host: dict[str, Any]) -> None:
        async with onvif_semaphore:
            for port in host["ports"]:
                # По порту без ответа страницы не стучимся: там либо не HTTP,
                # либо он молчит, и ждать две попытки по три секунды не за что.
                if port.get("video") or "status" not in port:
                    continue
                answer = await _ask_onvif(
                    session, host["ip"], port["port"], port["tls"]
                )
                if answer and _looks_like_onvif(*answer):
                    port["video"] = "onvif"
                    return

    await asyncio.gather(*(onvif_one(host) for host in hosts))
    for host in hosts:
        host["ports"].sort(key=lambda port: port["port"])


async def _answers_rtsp(ip: str, port: int, use_tls: bool) -> bool:
    """Порт говорит по RTSP: спрашиваем `OPTIONS` и читаем строку статуса."""
    context: ssl.SSLContext | None = None
    if use_tls:
        context = ssl.create_default_context()
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(ip, port, ssl=context), CONNECT_TIMEOUT_S
        )
    except (asyncio.TimeoutError, OSError, ssl.SSLError):
        return False
    try:
        writer.write(RTSP_OPTIONS)
        await writer.drain()
        line = await asyncio.wait_for(reader.readline(), VIDEO_TIMEOUT_S)
    except (asyncio.TimeoutError, OSError, ssl.SSLError):
        return False
    finally:
        writer.close()
    return bool(_RTSP_RE.match(line.decode("utf-8", "replace")))


async def _ask_onvif(
    session: aiohttp.ClientSession, ip: str, port: int, tls: bool
) -> tuple[int, str, dict[str, str]] | None:
    """Спросить веб-порт про ONVIF. `None` — порт не ответил вовсе."""
    scheme = "https" if tls else "http"
    try:
        async with session.post(
            f"{scheme}://{ip}:{port}{ONVIF_PATH}",
            data=ONVIF_BODY,
            headers={"Content-Type": ONVIF_CONTENT_TYPE},
            timeout=aiohttp.ClientTimeout(total=FINGERPRINT_TIMEOUT_S),
            ssl=False,
            allow_redirects=False,
        ) as answer:
            raw = await answer.content.read(MAX_FINGERPRINT_BYTES)
            return answer.status, raw.decode("utf-8", "replace"), dict(answer.headers)
    except (aiohttp.ClientError, asyncio.TimeoutError, OSError, ValueError):
        return None


def _looks_like_onvif(status: int, body: str, headers: dict[str, str]) -> bool:
    """Ответ похож на ONVIF.

    1. тело несёт `GetSystemDateAndTimeResponse` — устройство ОТВЕЧАЕТ на ONVIF;
    2. тело — конверт SOAP: отказ тоже приходит конвертом, тогда как обычный
       веб-сервер на этом адресе отвечает `404` и HTML (спрашиваем-то мы
       ОДИН адрес — ONVIF'овский);
    3. `401`, где ONVIF назван в теле или в `WWW-Authenticate`.

    ⚠ Просто слово «onvif» в теле НЕ считается: оно встречается в веб-мордах
    («настройки ONVIF» на обычной странице), и по нему отметка встала бы на
    устройство, которое об ONVIF только рассказывает.
    """
    if _ONVIF_RESPONSE_RE.search(body):
        return True
    if _SOAP_RE.search(body):
        return True
    if status != 401:
        return False
    text = "\n".join([body, *headers.values()]).lower()
    return "onvif" in text


async def _names(hosts: list[dict[str, Any]]) -> None:
    """Обратный DNS — подсказка для глаза; молчание и ошибка одинаково пусты."""
    loop = asyncio.get_running_loop()
    semaphore = asyncio.Semaphore(NAME_CONCURRENCY)

    async def one(host: dict[str, Any]) -> None:
        async with semaphore:
            try:
                name = await asyncio.wait_for(
                    loop.getnameinfo((host["ip"], 0), socket.NI_NAMEREQD),
                    NAME_TIMEOUT_S,
                )
            except (asyncio.TimeoutError, OSError):
                return
            host["hostname"] = name[0].rstrip(".")[:80]

    await asyncio.gather(*(one(host) for host in hosts))
