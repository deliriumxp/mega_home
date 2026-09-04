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
