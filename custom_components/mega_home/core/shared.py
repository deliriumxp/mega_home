"""Общие данные жильцов дома: именованные JSON-документы, которые читают и пишут все устройства.

Зачем (решение заказчика 2026-10-20): выбор жильца, общий на дом, — избранные радиостанции и всё
следующее того же рода — должен быть один на все телефоны и мониторы, работать без менеджера и
переживать перезапуск. Это пункт 3 закрытого списка `docs/home-gateway.md` («хранилище жильца»),
а не маршрут под функцию: дом не знает, что лежит в документе, — смысл у бандла.

- Документ — `{rev, data}`. Запись несёт `rev`, который видела: сменился (записал другой телефон)
  — отказ с текущим документом, бандл сливает и повторяет. Иначе два телефона, отметившие по
  станции разом, теряли бы одну отметку молча.
- `data: null` — документ пуст (снят).
- Пределы — число документов и размер: локальный контур в открытом режиме без аутентификации, а
  диск объекта не должен заполняться чем попало (тот же довод, что у правил, `alerts.py`).

⚠ Модуль в ядре без HA (`tests/test_core_without_ha.py`): хранилище — параметром.
"""

from __future__ import annotations

import json
import re
from typing import Any

from .host import Host

STORE_KEY = "mega_home_shared"
NAME = re.compile(r"^[a-z0-9][a-z0-9._-]{0,63}$")
MAX_DOC_BYTES = 64 * 1024
MAX_DOCS = 32


def name_valid(name: str) -> bool:
    return bool(NAME.match(name))


class SharedDocs:
    """Документы жильцов дома: чтение, запись по ревизии, снятие."""

    def __init__(self, env: Host) -> None:
        self._store = env.store(STORE_KEY)
        self._docs: dict[str, dict[str, Any]] = {}

    async def async_load(self) -> None:
        cached = await self._store.async_load() or {}
        docs = cached.get("docs") if isinstance(cached, dict) else None
        self._docs = {
            str(k): {"rev": v["rev"], "data": v["data"]}
            for k, v in (docs or {}).items()
            if name_valid(str(k)) and isinstance(v, dict) and isinstance(v.get("rev"), int) and "data" in v
        }

    def get(self, name: str) -> dict[str, Any]:
        """Документ или пустой `{rev: 0, data: null}` — «ещё не писали» не ошибка."""
        doc = self._docs.get(name)
        return dict(doc) if doc else {"rev": 0, "data": None}

    async def put(self, name: str, rev: Any, data: Any) -> dict[str, Any]:
        """Записать, если `rev` — текущий. Ответ — `{accepted, rev, data}` с документом ПОСЛЕ.

        Отказ по пределам — `ValueError` словами (вызывающий отвечает 400/413).
        """
        current = self.get(name)
        if not isinstance(rev, int) or isinstance(rev, bool) or rev != current["rev"]:
            return {"accepted": False, **current}
        # ⚠ Снятый документ остаётся записью с `data: null`: ревизия не возвращается к 0, и телефон,
        # читавший документ до снятия, не перезапишет пустоту своей старой копией.
        if len(json.dumps(data, ensure_ascii=False).encode("utf-8")) > MAX_DOC_BYTES:
            raise ValueError(f"Документ больше {MAX_DOC_BYTES // 1024} КБ")
        if name not in self._docs and len(self._docs) >= MAX_DOCS:
            raise ValueError(f"В доме уже {MAX_DOCS} документов")
        self._docs[name] = {"rev": current["rev"] + 1, "data": data}
        # ⚠ Сразу на диск, а не раз в период: это единственная копия выбора жильцов.
        await self._save()
        return {"accepted": True, **self.get(name)}

    async def _save(self) -> None:
        await self._store.async_save({"docs": self._docs})
