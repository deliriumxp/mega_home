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
# Имена, которые дом заполняет сам (`templating.live_values`, сессия): значения
# вызывающего их не перекрывают.
LIVE_NAMES = {"nonce", "created", "ts", "tsMs", "sid", "body"}

Fetch = Callable[[str], Awaitable[dict[str, str]]]
Legacy = Callable[[], Awaitable[tuple[str, str]]]


class SecretBook:
    """Учётки по доступам. Перечитывается ровно по смене отпечатка."""

    def __init__(
        self, fetch: Fetch | None = None, store: Any = None, legacy: Legacy | None = None, legacy_access: str | None = None
    ) -> None:
        self._fetch = fetch
        self._store = store
        # ⚠ Прежний источник — учётка драйвера вендора (Trassir): у описаний без
        # отпечатка (менеджер до 2026-09-19) другой учётки нет. Только своему
        # доступу: чужой пароль регистратора другому устройству не отдаётся.
        # Ключ `*` — прежнее поведение без привязки (спеки).
        self._legacy: dict[str, Legacy] = {}
        if legacy is not None:
            self._legacy[legacy_access or "*"] = legacy
        self._cache: dict[str, tuple[str, dict[str, str]]] = {}
        self._loaded = False

    def bind_legacy(self, access: str, legacy: Legacy) -> None:
        self._legacy[access] = legacy

    def legacy_for(self, access: str) -> Legacy | None:
        return self._legacy.get(access) or self._legacy.get("*")

    async def get(self, access: str, fingerprint: str) -> dict[str, str]:
        """Поля учётки доступа; нет учётки — пустой словарь."""
        if not fingerprint:
            legacy = self.legacy_for(access)
            if legacy is None:
                return {}
            user, password = await legacy()
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
        """Снять учётки доступов, которых в конфиге больше нет — и с диска тоже.

        ⚠ Без записи учётка снятого доступа жила бы в `.storage` и в бэкапах HA
        до следующего чужого обновления.
        """
        gone = [a for a in self._cache if a not in keep]
        for access in gone:
            del self._cache[access]
        if gone and self._store is not None:
            self._store.async_delay_save(self._snapshot, 0)

    def _snapshot(self) -> dict[str, Any]:
        return {a: {"fingerprint": fp, "fields": f} for a, (fp, f) in self._cache.items()}

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
        await self._store.async_save(self._snapshot())


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
    values["user|url"] = quote(values["user"], safe="")
    values["pass|url"] = quote(values["pass"], safe="")
    for key, value in secret.items():
        values[f"secret.{key}"] = value
        values[f"secret.{key}|url"] = quote(value, safe="")
    # ⚠ Значения ВЫЗЫВАЮЩЕГО (бандла) только ДОБАВЛЯЮТ имена, которых у дома нет,
    # и кодируются. Ревью 2026-09-19: `values: {host: <свой адрес>}` уводил поток
    # вместе с учёткой из шаблона на чужой адрес — любой в Wi-Fi объекта.
    for key, value in (extra or {}).items():
        name = str(key)
        if name in values or name in LIVE_NAMES or name.startswith(("secret.", "challenge.", "carry.", "capture.")) or "|" in name:
            continue
        values[name] = quote(str(value), safe="")
    return values
