"""Кэш записи «по движению» в памяти: размер по месту и защита от переполнения (`recorder.py`).

⚠ Кэш в `/dev/shm` — это ОПЕРАТИВНАЯ ПАМЯТЬ. Размер тмпфс её не меряет: в Docker и LXC `/dev/shm`
показывает половину памяти ХОСТА (замер 2026-10-25, объект 192.168.90.250: 7,7 ГБ при лимите
контейнера 4 ГБ). Разбор того же дня: по ~6000 кусков двух камер копились 15–17 часов (3,1 ГБ), пока
HA не ушёл в своп — нагрузка хоста 40 на 4 ядра, 56 % в системе, события HA вставали на минуты.
Причина — переносчик кусков ждал сеть без срока, а ffmpeg продолжал писать; пределом было «сколько
влезет».

Железо, память и вид среды (Docker, LXC, живой хост, Wirenboard) заранее неизвестны, поэтому числа
под них нет. Размер кэша камеры вычисляется на месте из двух вещей:
- что кэшу НУЖНО: поток камеры (битрейт мерится по её же кускам) × время, пока кусок ждёт решения
  «попал в движение или нет», с запасом на всплески;
- что можно ОТДАТЬ: доля доступной памяти (`MemAvailable`, остаток лимита cgroup), делённая на камеры
  кэша. Памяти не хватает — кэш сжимается вплоть до пола, а неизвестна — действует только первое.
"""

from __future__ import annotations

import os
from pathlib import Path

# Пока поток камеры не промерен (первые куски) — допущение «на камеру» для решения о запуске и первого предела.
RAM_PER_CAMERA = 64 * 1024**2
# Доля доступной памяти, которую кэш всех камер вправе занять; остальное — HA, go2rtc, ffmpeg, система.
MEMORY_SHARE = 0.25
# Меньше этого кэш не бывает (идущий кусок и пара закрытых) — иначе запись «по движению» бессмысленна.
FLOOR = 4 * 1024**2
# Запас на всплески потока: ключевые кадры у камер с переменным битрейтом в разы крупнее средних.
BURST = 3.0
# Сколько после закрытия кусок может ждать переноса в хранилище, кроме срока решения, секунд.
TRANSFER_LAG_S = 30.0
# Доступной памяти меньше — кэш на полу, пока она не вернётся: рядом с кэшем кто-то съел остальное.
RAM_HEADROOM = 256 * 1024**2

MEMINFO = Path("/proc/meminfo")
CGROUP_MAX = Path("/sys/fs/cgroup/memory.max")
CGROUP_CURRENT = Path("/sys/fs/cgroup/memory.current")


def memory_available() -> int | None:
    """Сколько памяти реально доступно, байт; `None` — не удалось узнать (кэш тогда мерится только потоком).

    Меньшее из двух: `MemAvailable` (в LXC с lxcfs — контейнера, в Docker и на живом хосте — машины) и
    остаток лимита cgroup v2, если он задан. Свободное место `/dev/shm` сюда не входит: оно память не меряет.
    """
    found: list[int] = []
    try:
        for line in MEMINFO.read_text().splitlines():
            if line.startswith("MemAvailable:"):
                found.append(int(line.split()[1]) * 1024)
                break
    except (OSError, ValueError, IndexError):
        pass
    try:
        limit = CGROUP_MAX.read_text().strip()
        if limit != "max":
            found.append(max(int(limit) - int(CGROUP_CURRENT.read_text()), 0))
    except (OSError, ValueError):
        pass
    return min(found) if found else None


def cap_bytes(rate: float, horizon_s: float, cameras: int, available: int | None) -> int:
    """Предел кэша одной камеры, байт.

    `rate` — байт в секунду по её кускам (0 — не промерен); `horizon_s` — сколько кусок ждёт решения;
    `cameras` — сколько камер делят память; `available` — доступная память или `None`.
    """
    need = int(rate * (horizon_s + TRANSFER_LAG_S) * BURST) if rate > 0 else RAM_PER_CAMERA
    cap = max(need, FLOOR)
    if available is None:
        return cap
    if available < RAM_HEADROOM:
        return FLOOR
    return max(min(cap, int(available * MEMORY_SHARE / max(cameras, 1))), FLOOR)


def ram_fits(cameras: int, shm_free: int) -> bool:
    """Годится ли память под кэш камер «по движению» при запуске (иначе кэш — на диске хранилища)."""
    need = cameras * RAM_PER_CAMERA
    if shm_free < need:
        return False
    available = memory_available()
    return available is None or available >= need + RAM_HEADROOM


def shed(cache: Path, cap: int) -> int:
    """Кэш камеры сверх предела: удалить самые старые куски, пока он больше `cap`; сколько удалено.

    Два самых новых не трогаются: идущий пишется, а закрытый только что ждёт переноса. Только удаление
    в памяти — сеть и хранилище не задействованы, поэтому сброс не зависит от переносчика.
    """
    try:
        entries = sorted((e for e in os.scandir(cache) if e.name.endswith(".mkv")), key=lambda e: e.name)
    except OSError:
        return 0
    sized: list[tuple[str, int]] = []
    for entry in entries:
        try:
            sized.append((entry.path, entry.stat().st_size))
        except OSError:
            continue
    total, removed = sum(size for _path, size in sized), 0
    for path, size in sized[:-2]:
        if total <= cap:
            break
        try:
            os.unlink(path)
        except OSError:
            continue
        total, removed = total - size, removed + 1
    return removed
