"""Вложения к событиям устройств: на событие — описанный вызов, ответ — файлом.

План — `docs/plan-event-attachments.md` менеджера. Кадр гостя в журнале вызовов
снять может только тот, кто работает круглосуточно и принимает вызов, — дом.
⚠ Делается КЛАСС задач, а не «снимок домофона»: протечка, дверь, движение у
вендора без архива — тем же правилом, без релиза на каждый случай.

Правило — данные конфига (`attachments[]`): `on` (доступ, источник, событие),
`request` (форма `ConnectRequest`), `type`, `fallback` (второй запрос).
⚠ Исполняется РЕСУРСНОЙ формой `connect` (`connect.resource`): только GET и
без учётки — правило читает, а не командует; перезагрузить контроллер им
нельзя, ровно поэтому его можно везти в конфиге, в отличие от правил сторожа.

⚠ Событие уходит менеджеру и в поток СРАЗУ, вложение догоняет: снимок камеры —
секунды, а звонок в дверь ждать их не должен.
"""

from __future__ import annotations

import re
from hashlib import sha1
from pathlib import Path
from time import monotonic, time
from typing import Any

from . import connect
from .const import LOGGER
from .host import Host
from .ops_base import OpError
from .photos import PART_STALE_S, write_atomic

DIR = "mega_home_event_files"
TTL_S = 7 * 24 * 3600.0  # ровно столько живёт журнал (`device_store.py`)
MAX_FILES = 300
MAX_BYTES = 512 * 1024
# Уборка вне записи — не чаще: `find` зовётся на каждый показ кадра.
PRUNE_EVERY_S = 3600.0


class EventFiles:
    """Правила вложений, их исполнение и файлы на диске."""

    def __init__(self, env: Host, journal: Any = None) -> None:
        self._env = env
        self._dir = env.path(DIR)
        self._journal = journal
        self._rules: list[dict[str, Any]] = []
        self._next_prune = 0.0

    def apply(self, config: dict[str, Any] | None) -> None:
        rules = (config or {}).get("attachments")
        self._rules = [r for r in rules if _valid(r)] if isinstance(rules, list) else []
        # ⚠ Уборка не только при записи: дом без событий хранил бы файлы сверх срока
        # вечно. `apply` зовётся на старте и каждым опросом; `find` добирает офлайн.
        if self._prune_due():
            self._env.spawn(self._env.run(self._prune), "mega_home event files prune")

    def _prune_due(self) -> bool:
        if monotonic() < self._next_prune:
            return False
        self._next_prune = monotonic() + PRUNE_EVERY_S
        return True

    def on_event(self, frame: dict[str, Any]) -> None:
        """Опубликовано событие: подходящее правило — фоном, не задерживая его."""
        for rule in self._rules:
            on = rule["on"]
            if all(on.get(k) in (None, frame.get(k)) for k in ("access", "source", "event")):
                self._env.spawn(self._attach(frame, rule), "mega_home event file")
                return

    async def _attach(self, frame: dict[str, Any], rule: dict[str, Any]) -> None:
        for request in (rule["request"], rule.get("fallback")):
            if not isinstance(request, dict):
                continue
            try:
                status, content_type, body, _cache = await connect.resource(request)
            except OpError as err:
                LOGGER.warning("Вложение к событию %s не снято: %s", frame.get("event"), err.message)
                continue
            if status != 200 or not body:
                LOGGER.warning("Вложение к событию %s: устройство ответило %s", frame.get("event"), status)
                continue
            if len(body) > MAX_BYTES:
                LOGGER.warning("Вложение к событию %s больше %d байт — не сохраняем", frame.get("event"), MAX_BYTES)
                return
            kind = str(rule.get("type") or content_type)
            await self._env.run(self._save, frame["id"], kind, body)
            if self._journal is not None:
                self._journal.mark(frame.get("access"), frame["id"], {"type": kind, "bytes": len(body)})
            return

    def find(self, event_id: str) -> tuple[Path, str] | None:
        """Файл вложения и его тип; `None` — вложения нет. Блокирует — в executor."""
        if self._prune_due():
            self._prune()
        stem = _stem(event_id)
        for path in self._dir.glob(f"{stem}__*"):
            if path.name.endswith(".part"):
                continue
            return path, path.name.split("__", 1)[1].replace("-", "/", 1)
        return None

    def _save(self, event_id: str, kind: str, body: bytes) -> None:
        self._dir.mkdir(0o755, parents=True, exist_ok=True)
        safe_kind = re.sub(r"[^a-z0-9.+-]", "", kind.lower().replace("/", "-", 1))[:64] or "application-octet-stream"
        write_atomic(self._dir / f"{_stem(event_id)}__{safe_kind}", body)
        self._prune()

    def _prune(self) -> None:
        """Старше срока журнала и сверх потолка — вон; черновики `.part` старше
        часа (обломки оборванной записи) — тоже. Зовут запись, `apply` и `find`."""
        try:
            stats = [(p, p.stat().st_mtime) for p in self._dir.iterdir()]
        except OSError:
            return
        cutoff, part_cutoff = time() - TTL_S, time() - PART_STALE_S
        files = sorted(
            ((p, m) for p, m in stats if "__" in p.name and not p.name.endswith(".part")),
            key=lambda item: item[1],
        )
        stale = [p for i, (p, m) in enumerate(files) if i < len(files) - MAX_FILES or m < cutoff]
        stale += [p for p, m in stats if p.name.endswith(".part") and m < part_cutoff]
        for path in stale:
            path.unlink(missing_ok=True)


def _stem(event_id: str) -> str:
    # Имя — хеш id события, как у фото комнат: значение с провода не выйдет из каталога.
    return sha1(event_id.encode("utf-8")).hexdigest()


def _valid(rule: Any) -> bool:
    """Форма правила. ⚠ Только GET — иначе правило умело бы командовать."""
    if not isinstance(rule, dict) or not isinstance(rule.get("on"), dict) or not isinstance(rule.get("request"), dict):
        return False
    requests = [rule["request"], rule.get("fallback")]
    return all(
        str(r.get("method") or "GET").upper() == "GET" for r in requests if isinstance(r, dict)
    )
