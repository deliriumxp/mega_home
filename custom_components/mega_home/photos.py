"""Room background photos, kept on the object.

The resident picks a photo for a room in the app's settings and it becomes the
background of that room. The files live here — on the home's own disk — and not
in the manager or in the browser, because a background is decoration of the
HOME: uploaded from a phone, it has to show up on the hallway tablet too, and it
must survive a browser data wipe. (The theme and the interface scale stay device
settings; those live in the browser on purpose.)

⚠ The file name is a HASH of the room id, never the id itself. Room ids come
from the manager's config and are only conventionally UUIDs — hashing means no
value from the wire can ever escape the directory, and there is nothing to
validate or reject.

⚠ Only what the current config knows as a room can be written: the views check
that before calling `save`. Without it anyone on the network could fill the
object's disk with an unbounded number of files (the HTTP surface has no
authentication yet — see the module docstring in `http.py`).
"""

from __future__ import annotations

from collections.abc import Iterable
from hashlib import sha1
from pathlib import Path

# JPEG only: the app re-encodes whatever the resident picked (HEIC from an
# iPhone included) before uploading, so accepting one format keeps both the
# check and the serving simple.
JPEG_MAGIC = b"\xff\xd8\xff"
MAX_PHOTO_BYTES = 4 * 1024 * 1024


class PhotoStore:
    """Room photos on disk. Every method here blocks — call it in an executor."""

    def __init__(self, directory: Path) -> None:
        self._dir = directory

    @property
    def directory(self) -> Path:
        return self._dir

    def path(self, room_id: str) -> Path:
        return self._dir / f"{sha1(room_id.encode('utf-8')).hexdigest()}.jpg"

    def versions(self, room_ids: Iterable[str]) -> dict[str, str]:
        """Room id -> version for the rooms that have a photo.

        ⚠ The version is a hash of the CONTENT, not the modification time. It
        goes into the image URL, which is served `immutable`, so a version that
        repeats means the resident keeps seeing the photo they just replaced.
        Modification time is not good enough for that: two writes seconds apart
        can carry the same `st_mtime_ns` (filesystem granularity), and the first
        version of this — with the timestamp — failed its own test on the very
        first run. Hashing also makes the version *right* by construction: it
        changes exactly when the picture does.
        """
        versions: dict[str, str] = {}
        for room_id in room_ids:
            path = self.path(room_id)
            try:
                versions[room_id] = self._version(path)
            except OSError:
                continue
        return versions

    def save(self, room_id: str, payload: bytes) -> str:
        """Write one photo and return its new version.

        Written to a temporary file and renamed: a half-written background is
        served to the app as a broken image, and the app polls the list often
        enough to catch exactly that moment.
        """
        self._dir.mkdir(0o755, parents=True, exist_ok=True)
        target = self.path(room_id)
        temporary = target.with_suffix(".part")
        temporary.write_bytes(payload)
        temporary.replace(target)
        return self._version(target)

    @staticmethod
    def _version(path: Path) -> str:
        digest = sha1()
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(64 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()[:16]

    def delete(self, room_id: str) -> bool:
        try:
            self.path(room_id).unlink()
        except OSError:
            return False
        return True

    def count(self) -> int:
        """How many photos are stored — for diagnostics."""
        try:
            return len(list(self._dir.glob("*.jpg")))
        except OSError:
            return 0


class StockPhotoStore:
    """Backgrounds the INSTALLER set in the manager, mirrored onto this disk.

    Not the same thing as `PhotoStore`, and deliberately a separate directory:
    the resident's own photo belongs to the home and is never overwritten by a
    sync, while these are a copy of what the manager holds and are thrown away
    the moment the manager stops naming them.

    ⚠ The file name carries the VERSION from the config, so "do we already hold
    this picture?" is `path.exists()` and never a download to compare bytes.
    A replaced background is a different name — nothing to invalidate.
    """

    def __init__(self, directory: Path) -> None:
        self._dir = directory

    def path(self, room_id: str, version: str) -> Path:
        room = sha1(room_id.encode("utf-8")).hexdigest()
        return self._dir / f"{room}_{_safe_version(version)}.jpg"

    def has(self, room_id: str, version: str) -> bool:
        return self.path(room_id, version).is_file()

    def save(self, room_id: str, version: str, payload: bytes) -> None:
        """Write one background. Written aside and renamed, as with the resident's."""
        self._dir.mkdir(0o755, parents=True, exist_ok=True)
        target = self.path(room_id, version)
        temporary = target.with_suffix(".part")
        temporary.write_bytes(payload)
        temporary.replace(target)

    def prune(self, wanted: dict[str, str]) -> None:
        """Drop every file the config no longer names — old versions included."""
        keep = {self.path(room, version).name for room, version in wanted.items()}
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
        """How many backgrounds are mirrored — for diagnostics."""
        try:
            return len(list(self._dir.glob("*.jpg")))
        except OSError:
            return 0


def _safe_version(version: str) -> str:
    """The version comes from the manager and becomes a file name — keep it boring."""
    return "".join(char if char.isalnum() else "-" for char in version)[:32]
