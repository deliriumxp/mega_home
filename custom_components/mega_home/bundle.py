"""The resident app bundle: downloaded from the manager, served from disk.

⚠ This is the point of the whole arrangement: the interface is DATA, not code.
Shipping it inside the integration would mean a HACS release and a Home
Assistant restart for every change of a tile — with the bundle downloaded here,
a new interface reaches an object on its own.

Only static files travel this way (js/css/fonts/images), served to the browser
by our own origin. Python never does: a compromised manager must not be able to
execute anything on an object.
"""

from __future__ import annotations

import hashlib
import shutil
from pathlib import Path
from typing import Any

from homeassistant.core import HomeAssistant
from homeassistant.helpers.storage import STORAGE_DIR

from .api import ManagerClient, ManagerError
from .const import LOGGER

BUNDLE_DIR = "mega_home_www"
# Копия, упакованная в релиз: её раздаёт свежая установка, которая ещё ни разу не
# синхронизировалась. Живёт здесь, а не в `http.py`, чтобы хранилище можно было
# создать, ничего не импортируя из HTTP-слоя (тот сам импортирует координатор).
PACKAGED_DIR = Path(__file__).parent / "www"
# Сколько версий держим на диске: активная и предыдущая. Предыдущая — это откат
# без выезда на объект.
KEEP_VERSIONS = 2
MAX_FILES = 200
MAX_TOTAL_BYTES = 32 * 1024 * 1024


class BundleStore:
    """Keeps downloaded bundle versions and says which one to serve.

    ⚠ `version` here is the DIRECTORY name of the active bundle, i.e. the
    manifest version run through `_safe_name` — after a restart that name is all
    that is known about what lies on disk. Everything that compares versions has
    to go through `_safe_name` too; comparing a raw `sha256:…` against the stored
    `sha256-…` matches never, and "never" means re-downloading the whole bundle
    on every nudge (see `async_sync`).
    """

    def __init__(
        self,
        hass: HomeAssistant,
        client: ManagerClient,
        packaged: Path | None = None,
    ) -> None:
        self._hass = hass
        self._client = client
        self._packaged = packaged or PACKAGED_DIR
        self._root = Path(hass.config.path(STORAGE_DIR, BUNDLE_DIR))
        self._active: Path | None = None
        self.version: str | None = None
        # ⚠ Почему бандл не обновился — НАРУЖУ, а не только в debug-журнал.
        # `async_sync` не бросает намеренно (её зовут и опрос, и живой канал, и
        # уронить их нельзя), но из-за этого дом, до которого новый интерфейс не
        # доезжает, выглядел полностью здоровым: интерфейс работает, в журнале
        # тишина, в диагностике ничего. Причина стоила разбирательства по
        # скриншотам вместо одного взгляда в лог.
        self.last_error: str | None = None

    @property
    def active_dir(self) -> Path:
        """Directory the HTTP views must read from right now.

        Falls back to the copy shipped inside the integration: a fresh install
        that has never synchronised still has to show something.
        """
        return self._active or self._packaged

    async def async_load(self) -> None:
        """Pick up the newest complete version left by a previous run."""
        versions = await self._hass.async_add_executor_job(self._stored_versions)
        if versions:
            self._active = versions[-1]
            self.version = self._active.name
            LOGGER.debug("Serving app bundle %s", self.version)

    async def async_sync(self, version: str | None = None) -> bool:
        """Fetch the manifest and download it when it differs. True = switched.

        A failure here is never fatal: the previously downloaded bundle (or the
        packaged one) keeps being served.
        """
        try:
            manifest = await self._client.async_app_manifest()
        except ManagerError as err:
            self.last_error = f"manifest unavailable: {err}"
            LOGGER.debug("App manifest unavailable: %s", err)
            return False

        wanted = manifest.get("version")
        files = manifest.get("files")
        if not isinstance(wanted, str) or not isinstance(files, list):
            self.last_error = "manager returned a malformed app manifest"
            LOGGER.warning("Manager returned a malformed app manifest")
            return False
        if version and version != wanted:
            # The nudge and the manifest disagree — trust the manifest, it is
            # what we are about to download.
            LOGGER.debug("App nudge said %s, manifest says %s", version, wanted)
        # ⚠ Сравниваем ИМЕНА КАТАЛОГОВ, а не сырую версию с именем: `self.version`
        # прошло через `_safe_name` (в `sha256:…` двоеточие стало дефисом), и
        # прямое сравнение не совпадало никогда. Ценой были полная перекачка
        # бандла на каждый nudge и на каждое переподключение канала, а `_swap`
        # при этом сносил каталог, из которого прямо сейчас раздаётся приложение,
        # то есть открывал жильцу окно с 404 на ровном месте.
        name = _safe_name(wanted)
        if name == self.version:
            self.last_error = None
            return False
        if len(files) > MAX_FILES:
            self.last_error = f"manifest lists {len(files)} files — refusing"
            LOGGER.warning("App manifest lists %s files — refusing", len(files))
            return False
        total = sum(int(item.get("bytes", 0)) for item in files)
        if total > MAX_TOTAL_BYTES:
            self.last_error = f"bundle is {total} bytes — refusing"
            LOGGER.warning("App bundle is %s bytes — refusing", total)
            return False

        target = self._root / name
        staging = self._root / f".partial-{name}"
        try:
            await self._hass.async_add_executor_job(_reset_dir, staging)
            for item in files:
                await self._async_fetch_file(staging, item)
        except (ManagerError, OSError, ValueError) as err:
            # ⚠ Half a bundle is a white screen for the resident, so a failed
            # download never becomes the active version: the staging directory
            # is thrown away and the old bundle keeps serving.
            self.last_error = f"download failed: {err}"
            LOGGER.warning("App bundle %s not downloaded: %s", wanted[:19], err)
            await self._hass.async_add_executor_job(
                shutil.rmtree, staging, True
            )
            return False

        await self._hass.async_add_executor_job(_swap, staging, target)
        self._active = target
        self.version = target.name
        self.last_error = None
        LOGGER.info("App bundle updated to %s", self.version)
        await self._hass.async_add_executor_job(self._prune)
        return True

    async def _async_fetch_file(self, staging: Path, item: dict[str, Any]) -> None:
        path = item.get("path")
        digest = item.get("sha256")
        if not isinstance(path, str) or not isinstance(digest, str):
            raise ValueError("manifest entry without a path or a hash")
        target = _resolve_inside(staging, path)
        payload = await self._client.async_app_file(path)
        # ⚠ Проверяем хеш КАЖДОГО файла: обрезанный ответ прокси и подменённый
        # файл выглядят одинаково — как бандл, который «почти» скачался.
        if hashlib.sha256(payload).hexdigest() != digest:
            raise ValueError(f"checksum mismatch for {path}")
        await self._hass.async_add_executor_job(_write, target, payload)

    def _stored_versions(self) -> list[Path]:
        if not self._root.is_dir():
            return []
        versions = [
            path
            for path in sorted(self._root.iterdir(), key=lambda p: p.stat().st_mtime)
            if path.is_dir() and not path.name.startswith(".") and (path / "index.html").is_file()
        ]
        return versions

    def _prune(self) -> None:
        for stale in self._stored_versions()[:-KEEP_VERSIONS]:
            shutil.rmtree(stale, ignore_errors=True)


def _safe_name(version: str) -> str:
    """A version string is used as a directory name — keep it boring."""
    return "".join(char if char.isalnum() else "-" for char in version)[:80]


def _resolve_inside(root: Path, relative: str) -> Path:
    """Resolve a manifest path under root, refusing anything that escapes it."""
    target = (root / relative).resolve()
    if not str(target).startswith(str(root.resolve()) + "/"):
        raise ValueError(f"path escapes the bundle directory: {relative}")
    return target


def _reset_dir(path: Path) -> None:
    shutil.rmtree(path, ignore_errors=True)
    path.mkdir(parents=True, exist_ok=True)


def _write(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)


def _swap(staging: Path, target: Path) -> None:
    shutil.rmtree(target, ignore_errors=True)
    staging.rename(target)
