"""Files the manager hands to this home: one store for every kind.

⚠ The point of this module is what is NOT here: no knowledge of what the files
are. The manager names them in the config manifest — `assets: {key: {v, type}}` —
and this store keeps whatever it is told to keep. A new kind of file (a sound, a
font, a floor plan) is a line in the manager and a line in the app; it must
never be a release of this integration, because integration code is the only
thing that does not update itself.

⚠ The file name is a HASH of the key plus the version, exactly as with the
stock photos: "do we already hold this?" is `path.exists()` and never a download
to compare bytes, and a replaced file is a different name — nothing to
invalidate.
"""

from __future__ import annotations

from hashlib import sha1
from pathlib import Path


class AssetStore:
    """Mirrored manager files on disk. Every method blocks — use an executor."""

    def __init__(self, directory: Path) -> None:
        self._dir = directory

    @property
    def directory(self) -> Path:
        return self._dir

    def path(self, key: str, version: str) -> Path:
        name = sha1(key.encode("utf-8")).hexdigest()
        return self._dir / f"{name}_{_safe(version)}.bin"

    def has(self, key: str, version: str) -> bool:
        return self.path(key, version).is_file()

    def save(self, key: str, version: str, payload: bytes) -> None:
        """Write one file aside and rename it: a half-written asset is garbage."""
        self._dir.mkdir(0o755, parents=True, exist_ok=True)
        target = self.path(key, version)
        temporary = target.with_suffix(".part")
        temporary.write_bytes(payload)
        temporary.replace(target)

    def prune(self, wanted: dict[str, str]) -> None:
        """Drop every file the manifest no longer names — old versions included."""
        keep = {self.path(key, version).name for key, version in wanted.items()}
        try:
            stale = [path for path in self._dir.iterdir() if path.name not in keep]
        except OSError:
            return
        for path in stale:
            try:
                path.unlink()
            except OSError:
                continue

    def count(self) -> int:
        """How many files are mirrored — for diagnostics."""
        try:
            return len(list(self._dir.glob("*.bin")))
        except OSError:
            return 0


def _safe(version: str) -> str:
    """The version comes from the manager and becomes a file name — keep it boring."""
    return "".join(char if char.isalnum() else "-" for char in version)[:32]
