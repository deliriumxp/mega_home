"""Обход локальной сети объекта по заказу менеджера — инструмент шлюза.

Зачем. Инсталлятору нужно видеть, что стоит в сети клиента, и куда можно войти
по веб-интерфейсу — аналог Network Scan у OvrC. Список DHCP-аренд роутера этого
не даёт: устройство со статикой в нём не появится никогда. Агент ВНУТРИ локалки
(эта интеграция) видит сеть напрямую и обходит её сам; менеджер только заказывает
обход и рисует результат.

⚠ Здесь НЕТ толкования «что это за устройство»: только обнаружение хостов,
перебор веб-портов и снятие заголовка/`<title>` страницы. Смысл (какой порт
открывать, что считать веб-интерфейсом, как назвать вендора) живёт в менеджере и
меняется его деплоем, а не релизом HACS с перезапуском Home Assistant на КАЖДОМ
объекте (docs/plan-thin-integration.md в менеджере).

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
import time
from http import HTTPStatus
from typing import Any

import aiohttp

from .const import LOGGER
from .host import Host
from .ops_base import OpError

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
# Снятие страницы: короткий срок и малый потолок — это заголовок, а не файл.
FINGERPRINT_TIMEOUT_S = 3.0
FINGERPRINT_CONCURRENCY = 24
MAX_FINGERPRINT_BYTES = 64 * 1024
# Обратный DNS — подсказка, а не обязательное поле: полсекунды на хост.
NAME_TIMEOUT_S = 0.5
NAME_CONCURRENCY = 32
# UDP-порт для «толчка» ARP. Закрытый и никому не нужный.
ARP_TOUCH_PORT = 9
# Порты задания: какие порты проверить у каждого живого хоста и с каких снять страницу, — присылает менеджер
# из своего реестра опознавателей (`device-signatures.ts`): новый вендор с новым портом — деплой менеджера, а
# не релиз дома и не второй проход по сети. Потолок — чтобы задание не стало перебором портов.
MAX_SCAN_PORTS = 64
# Текст страницы без тегов — начало: признак для опознавания там, где `<title>` пуст и заполняется скриптом
# (контроллер вентиляции ATLAS: `<title id=title></title>`, имя — в заставке страницы).
TEXT_CHARS = 160


_RUNNING = asyncio.Lock()


async def run(env: Host, payload: dict[str, Any]) -> dict[str, Any]:
    """Обойти подсеть и вернуть найденные хосты с открытыми веб-портами.

    ⚠ Один обход разом: второй заказ посреди первого (повторное нажатие, две
    вкладки инсталлятора) удвоил бы поток соединений по сети клиента.
    """
    if _RUNNING.locked():
        raise OpError("Обход сети уже идёт — дождитесь результата", HTTPStatus.CONFLICT)
    async with _RUNNING:
        return await _run(env, payload)


async def _run(env: Host, payload: dict[str, Any]) -> dict[str, Any]:
    network = _target_network(payload.get("subnet") if payload else None)
    ports = _ports_of(payload.get("ports") if payload else None)
    started = time.monotonic()
    ips = [str(ip) for ip in network.hosts()][:MAX_HOSTS]
    LOGGER.info("Обход сети %s (%d адресов) по заказу менеджера", network, len(ips))
    # Время этапов — в ответ: где уходят секунды, видно по каждому обходу, а не догадкой.
    phases: dict[str, int] = {}
    mark = time.monotonic()

    def lap(name: str) -> None:
        nonlocal mark
        now = time.monotonic()
        phases[name] = int((now - mark) * 1000)
        mark = now

    # Толчок ARP и TCP-обход идут ВМЕСТЕ: пока ядро ждёт ответов на ARP,
    # TCP-обход уже отрабатывает, и отдельная пауза на ARP не тратится дважды.
    await env.run(_touch_arp, ips)
    alive, arp = await asyncio.gather(
        _tcp_sweep(ips),
        _arp_after(env),
    )
    lap("sweep")

    hosts: list[dict[str, Any]] = []
    for ip in ips:
        mac = arp.get(ip, "")
        if ip in alive or mac:
            hosts.append({"ip": ip, "mac": mac, "hostname": "", "ports": []})
    # ⚠ Показываем ВСЕ найденные устройства, а не только те, у кого открыт
    # веб-порт: их мониторинг появится позже, и терять их уже здесь нельзя.
    hosts.sort(key=lambda host: ipaddress.ip_address(host["ip"]))
    await _scan_ports(hosts, [port for port, _http in ports])
    lap("ports")
    await _fingerprint(env, hosts, {port for port, http in ports if http})
    lap("pages")
    await _names(hosts)
    lap("names")

    LOGGER.info(
        "Обход сети %s закончен: %d устройств за %d мс (%s)",
        network,
        len(hosts),
        int((time.monotonic() - started) * 1000),
        ", ".join(f"{name} {ms}" for name, ms in phases.items()),
    )
    return {
        "subnet": str(network),
        "ms": int((time.monotonic() - started) * 1000),
        "phases": phases,
        "hosts": hosts,
    }


def _ports_of(value: Any) -> list[tuple[int, bool]]:
    """Порты задания `[{port, http}]`; нет или мусор — прежний список веб-портов (все со страницей)."""
    out: list[tuple[int, bool]] = []
    for item in value if isinstance(value, list) else []:
        if not isinstance(item, dict):
            continue
        try:
            port = int(item.get("port"))
        except (TypeError, ValueError):
            continue
        if 0 < port < 65536 and all(port != seen for seen, _ in out):
            out.append((port, item.get("http") is True))
    return out[:MAX_SCAN_PORTS] or [(port, True) for port in WEB_PORTS]


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


async def _arp_after(env: Host) -> dict[str, str]:
    """Дать ядру время и прочитать ARP-таблицу."""
    await asyncio.sleep(ARP_WAIT_S)
    return await env.run(_arp_table)


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
    return await _is_open(ip, port, refused=True)


async def _is_open(ip: str, port: int, refused: bool = False) -> bool:
    """Порт открыт; `refused` — чем считать отказ (RST): для «жив ли хост» — да."""
    try:
        _reader, writer = await asyncio.wait_for(
            asyncio.open_connection(ip, port), CONNECT_TIMEOUT_S
        )
        writer.close()
        return True
    except ConnectionRefusedError:
        return refused
    except (asyncio.TimeoutError, OSError):
        return False


async def _scan_ports(hosts: list[dict[str, Any]], ports: list[int]) -> None:
    semaphore = asyncio.Semaphore(CONCURRENCY)

    async def one(host: dict[str, Any]) -> None:
        async def check(port: int) -> bool:
            async with semaphore:
                return await _is_open(host["ip"], port)

        opened = await asyncio.gather(*(check(port) for port in ports))
        host["ports"] = [
            {"port": port, "tls": port in TLS_PORTS}
            for port, is_open in zip(ports, opened)
            if is_open
        ]

    await asyncio.gather(*(one(host) for host in hosts))


async def _fingerprint(env: Host, hosts: list[dict[str, Any]], http_ports: set[int]) -> None:
    """Снять `Server`, `<title>` и начало текста с открытых веб-портов: по ним видно, что это."""
    session = env.session()
    semaphore = asyncio.Semaphore(FINGERPRINT_CONCURRENCY)

    async def one(host: dict[str, Any], port: dict[str, Any]) -> None:
        async with semaphore:
            # ⚠ Схему не угадываем по номеру порта: на 80 сплошь и рядом HTTPS, а на 8080 — HTTP. Обе — РАЗОМ, а не
            # по очереди: очередь стоила второго срока на каждом порту, где ожидаемая схема молчит (замер офиса
            # 2026-11-08: снятие страниц — 6 с из 11). Ответили обе — берём ожидаемую.
            expected = port["tls"]
            first, second = await asyncio.gather(
                _fetch(session, host["ip"], port["port"], expected),
                _fetch(session, host["ip"], port["port"], not expected),
            )
            if first is None and second is None:
                # ⚠ Камеры EYEPLUS рвут каждый второй запрос подряд (стенд `.90.201`, 2026-10-23): вторая схема
                # разом могла сорвать ожидаемую. Повтор ожидаемой — один, и только там, где не ответила ни одна.
                first = await _fetch(session, host["ip"], port["port"], expected)
            for tls, info in ((expected, first), (not expected, second)):
                if info is not None:
                    port["tls"] = tls
                    port.update(info)
                    return

    await asyncio.gather(
        *(one(host, port) for host in hosts for port in host["ports"] if port["port"] in http_ports)
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
    page = raw.decode("utf-8", "replace")
    return {
        "status": status,
        "server": server,
        "title": _title(page),
        "text": _text(page),
    }


_TITLE_RE = re.compile(r"<title[^>]*>(.*?)</title>", re.IGNORECASE | re.DOTALL)
_HIDDEN_RE = re.compile(r"<(script|style|head)\b.*?</\1\s*>", re.IGNORECASE | re.DOTALL)
_TAG_RE = re.compile(r"<[^>]*>")


def _text(page: str) -> str:
    """Видимый текст страницы — начало, без тегов, скриптов и стилей; смысл ему даёт менеджер."""
    text = _TAG_RE.sub(" ", _HIDDEN_RE.sub(" ", page))
    return re.sub(r"\s+", " ", text).strip()[:TEXT_CHARS]


def _title(text: str) -> str:
    match = _TITLE_RE.search(text)
    if not match:
        return ""
    return re.sub(r"\s+", " ", match.group(1)).strip()[:80]


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
