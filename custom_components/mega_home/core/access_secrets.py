"""Учётки доступов: своя у каждого доступа, набор именованных полей.

⚠ В описании доступа учётки нет — только отпечаток (`secret`): тело конфига
уходит браузеру жильца как есть. Сами поля дом забирает маршрутом менеджера по
токену объекта и наружу не отдаёт никогда (`docs/home-gateway.md` в менеджере).

⚠ Полей бывает больше пары «логин/пароль»: у панели Akuvox учётки HTTP API и
RTSP раздельны. Авторизация и шаблоны ссылаются на поля ПО ИМЕНИ.
"""

from __future__ import annotations

from typing import Any, Awaitable, Callable

from .const import LOGGER

STORE_KEY = "mega_home_access_secrets"

Fetch = Callable[[str], Awaitable[dict[str, str]]]
Legacy = Callable[[], Awaitable[tuple[str, str]]]


class SecretBook:
    """Учётки по доступам. Перечитывается ровно по смене отпечатка."""

    def __init__(
        self, fetch: Fetch | None = None, store: Any = None, legacy: Legacy | None = None, legacy_access: str | None = None
    ) -> None:
        self._fetch = fetch
        self._store = store
        # ⚠ Прежний источник — учётка Trassir драйвера: у описаний без
        # отпечатка (менеджер до 2026-09-19) другой учётки нет. Только своему
        # доступу (`legacy_access`): чужой пароль регистратора другому
        # устройству не отдаётся.
        self._legacy = legacy
        self._legacy_access = legacy_access
        self._cache: dict[str, tuple[str, dict[str, str]]] = {}
        self._loaded = False

    async def get(self, access: str, fingerprint: str) -> dict[str, str]:
        """Поля учётки доступа; нет учётки — пустой словарь."""
        if not fingerprint:
            if self._legacy is None or self._legacy_access not in (None, access):
                return {}
            user, password = await self._legacy()
            return {"username": user, "password": password}
        await self._load()
        cached = self._cache.get(access)
        if cached and cached[0] == fingerprint:
            return cached[1]
        if self._fetch is None:
            return cached[1] if cached else {}
        try:
            fields = await self._fetch(access)
        except Exception as err:  # noqa: BLE001 — менеджер недоступен
            if cached:
                # Прежняя учётка лучше отказа: пароль меняют редко, а связь с
                # менеджером пропадает регулярно.
                LOGGER.warning("Учётка доступа %s не обновилась: %s", access, err)
                return cached[1]
            raise
        clean = {str(k): str(v) for k, v in (fields or {}).items() if isinstance(v, (str, int))}
        self._cache[access] = (fingerprint, clean)
        await self._save()
        return clean

    def forget(self, keep: set[str]) -> None:
        """Снять учётки доступов, которых в конфиге больше нет."""
        for access in [a for a in self._cache if a not in keep]:
            del self._cache[access]

    async def _load(self) -> None:
        if self._loaded or self._store is None:
            self._loaded = True
            return
        self._loaded = True
        stored = await self._store.async_load() or {}
        for access, item in stored.items() if isinstance(stored, dict) else []:
            if isinstance(item, dict) and isinstance(item.get("fields"), dict):
                self._cache[access] = (str(item.get("fingerprint") or ""), item["fields"])

    async def _save(self) -> None:
        if self._store is None:
            return
        await self._store.async_save(
            {a: {"fingerprint": fp, "fields": f} for a, (fp, f) in self._cache.items()}
        )


def template_values(descriptor: Any, secret: dict[str, str], extra: dict[str, str] | None = None) -> dict[str, str]:
    """Значения для шаблонов описания: адрес, порты, поля учётки, прочее.

    ⚠ Поля учётки в адресе (`rtsp://{secret.user}:{secret.pass}@…`) кодируются:
    пароль со «@» или «/» иначе ломает адрес молча.
    """
    from urllib.parse import quote

    values = {
        "host": descriptor.host,
        "port": str(descriptor.port),
        "rtspPort": str(descriptor.rtsp_port),
        "user": secret.get("username", ""),
        "pass": secret.get("password", ""),
    }
    for key, value in secret.items():
        values[f"secret.{key}"] = value
        values[f"secret.{key}|url"] = quote(value, safe="")
    values.update(extra or {})
    return values
