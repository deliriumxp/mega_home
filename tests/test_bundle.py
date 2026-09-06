"""The app bundle store: what it downloads, and — mostly — what it does not.

The interface travels as data, so this store runs on every nudge from the
manager and on every reconnect of the live link. Whether it recognises the
bundle it already holds is therefore not a detail: getting it wrong means the
whole bundle crosses the wire again and the directory the resident's app is
being served from is deleted and recreated underneath them.
"""

from __future__ import annotations

import asyncio
import hashlib
from pathlib import Path
from typing import Any

from mega_home.bundle import BundleStore


class _Config:
    def __init__(self, root: Path) -> None:
        self._root = root

    def path(self, *parts: str) -> str:
        return str(self._root.joinpath(*parts))


class FakeHass:
    """Executor jobs run inline — the store only uses them for file work."""

    def __init__(self, root: Path) -> None:
        self.config = _Config(root)

    async def async_add_executor_job(self, func: Any, *args: Any) -> Any:
        return func(*args)


class FakeClient:
    """The manager: publishes a bundle and counts what was pulled from it."""

    def __init__(self, files: dict[str, bytes]) -> None:
        self.files = files
        self.downloads: list[str] = []
        self.corrupt: str | None = None

    def publish(self, files: dict[str, bytes]) -> None:
        self.files = files

    async def async_app_manifest(self) -> dict[str, Any]:
        entries = [
            {
                "path": path,
                "sha256": hashlib.sha256(body).hexdigest(),
                "bytes": len(body),
            }
            for path, body in sorted(self.files.items())
        ]
        # Same shape and the same version the manager computes
        # (backend/src/modules/smart-home/home-app.util.ts) — the colon in
        # `sha256:` is exactly what a directory name cannot keep.
        digest = hashlib.sha256(
            "\n".join(f"{item['path']}:{item['sha256']}" for item in entries).encode()
        ).hexdigest()
        return {"version": f"sha256:{digest}", "files": entries}

    async def async_app_file(self, path: str) -> bytes:
        self.downloads.append(path)
        if self.corrupt == path:
            return b"truncated by a proxy"
        return self.files[path]


def store(tmp_path: Path, client: FakeClient) -> BundleStore:
    return BundleStore(FakeHass(tmp_path), client)


def test_before_the_first_download_there_is_nothing_to_serve(tmp_path: Path) -> None:
    """The release carries no copy of the interface (2026-09-06).

    `None` is what tells `http.py` to hand out the "connecting" placeholder, so
    a fresh install must not invent a directory to serve from.
    """
    assert store(tmp_path, FakeClient({})).active_dir is None


def test_holding_the_same_bundle_downloads_nothing(tmp_path: Path) -> None:
    """⚠ Regression: the version was compared against the DIRECTORY name.

    `sha256:…` from the manifest never equalled `sha256-…` on disk, so every
    nudge — and the link sends one on each reconnect — re-downloaded the whole
    bundle and swapped the directory being served.
    """
    client = FakeClient({"index.html": b"<html>", "main-A.js": b"one"})
    bundle = store(tmp_path, client)

    assert asyncio.run(bundle.async_sync()) is True
    assert sorted(client.downloads) == ["index.html", "main-A.js"]

    client.downloads.clear()
    assert asyncio.run(bundle.async_sync()) is False
    assert client.downloads == []


def test_restart_recognises_the_stored_bundle(tmp_path: Path) -> None:
    """Same check across a restart: on disk there is only the directory name."""
    client = FakeClient({"index.html": b"<html>", "main-A.js": b"one"})
    asyncio.run(store(tmp_path, client).async_sync())

    revived = store(tmp_path, client)
    asyncio.run(revived.async_load())
    client.downloads.clear()

    assert asyncio.run(revived.async_sync()) is False
    assert client.downloads == []
    assert (revived.active_dir / "main-A.js").read_bytes() == b"one"


def test_new_interface_switches_and_keeps_the_previous_one(tmp_path: Path) -> None:
    """A bad bundle is fixed by switching back, not by visiting the flat."""
    client = FakeClient({"index.html": b"<html>", "main-A.js": b"one"})
    bundle = store(tmp_path, client)
    asyncio.run(bundle.async_sync())
    first = bundle.active_dir

    client.publish({"index.html": b"<html>", "main-B.js": b"two"})
    assert asyncio.run(bundle.async_sync()) is True
    assert bundle.active_dir != first
    assert (bundle.active_dir / "main-B.js").read_bytes() == b"two"
    assert (first / "main-A.js").is_file()


def test_half_a_bundle_never_becomes_active(tmp_path: Path) -> None:
    """Half a bundle is a white screen, so a failed download stays staged."""
    client = FakeClient({"index.html": b"<html>", "main-A.js": b"one"})
    bundle = store(tmp_path, client)
    asyncio.run(bundle.async_sync())
    good = bundle.active_dir

    client.publish({"index.html": b"<html>", "main-B.js": b"two"})
    client.corrupt = "main-B.js"
    assert asyncio.run(bundle.async_sync()) is False
    assert bundle.active_dir == good
    assert (bundle.active_dir / "main-A.js").read_bytes() == b"one"
