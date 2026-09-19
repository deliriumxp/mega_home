"""Camera tile crops the resident picks, kept on the object.

The installer already sets a crop when handing the object over — it rides the
composition, `tiles[].crop` in `HomeConfig` (`ops.config`). The resident may
want to nudge it afterwards, standing in front of the camera, exactly like a
tile background (`photos.py`): the adjustment belongs to the HOME, not the
manager, for the same two reasons — it must keep working without the manager,
and it must survive a browser data wipe on the resident's own phone.

⚠ The file name is a HASH of the tile id, never the id itself — same reasoning
as `photos.py`: no value from the wire can ever escape the directory.

⚠ Only what the current config knows as a CAMERA TILE can be written: the view
checks that before calling `save` (`crop_key_known`). Without it anyone on the
local network could fill the object's disk with an unbounded number of files
(the HTTP surface has no authentication yet — see the module docstring in
`http.py`).
"""

from __future__ import annotations

import json
from collections.abc import Iterable
from hashlib import sha1
from pathlib import Path
from typing import Any

# {"x": 0.123, "y": 0.456, "w": 0.789} weighs well under a hundred bytes; this
# leaves headroom without allowing an arbitrary-size upload.
MAX_CROP_BYTES = 1024
_FIELDS = ("x", "y", "w")


class CropStore:
    """Camera tile crops on disk. Every method here blocks — call it in an executor."""

    def __init__(self, directory: Path) -> None:
        self._dir = directory

    def path(self, tile_id: str) -> Path:
        return self._dir / f"{sha1(tile_id.encode('utf-8')).hexdigest()}.json"

    def all(self, tile_ids: Iterable[str]) -> dict[str, dict[str, float]]:
        """Tile id -> crop, for the tiles that actually have one stored."""
        out: dict[str, dict[str, float]] = {}
        for tile_id in tile_ids:
            try:
                raw = self.path(tile_id).read_text("utf-8")
            except OSError:
                continue
            try:
                value = json.loads(raw)
            except ValueError:
                continue
            if crop_value_valid(value):
                out[tile_id] = value
        return out

    def save(self, tile_id: str, crop: dict[str, float]) -> None:
        """Write one crop. Temp file + rename: no half-written JSON ever served."""
        self._dir.mkdir(0o755, parents=True, exist_ok=True)
        target = self.path(tile_id)
        temporary = target.with_suffix(".part")
        temporary.write_text(json.dumps(crop), encoding="utf-8")
        temporary.replace(target)

    def delete(self, tile_id: str) -> bool:
        try:
            self.path(tile_id).unlink()
        except OSError:
            return False
        return True


# --- ключи и тело в конфиге -------------------------------------------------
#
# ⚠ Живут ЗДЕСЬ, в одном месте на оба входа: локальные view (`http.py`) и
# перенос запроса снаружи (`relay_api.py`) обязаны отвечать одинаково — тот же
# приём, что у `photo_key_known`/`photo_keys`.


def crop_keys(config: dict[str, Any]) -> list[str]:
    """Id плиток, у которых МОЖЕТ быть свой кадр: камеры состава."""
    return [
        tile["id"]
        for tile in config.get("tiles", [])
        if tile.get("id") and tile.get("domain") == "camera"
    ]


def crop_key_known(config: dict[str, Any], tile_id: str) -> bool:
    """Можно ли писать кадр под этим id: он обязан быть КАМЕРОЙ состава."""
    return any(
        tile.get("id") == tile_id and tile.get("domain") == "camera"
        for tile in config.get("tiles", [])
    )


def crop_value_valid(payload: Any) -> bool:
    """Тело запроса — ровно то, что рисует редактор: `{x, y, w}`, доли 0..1."""
    if not isinstance(payload, dict) or set(payload.keys()) != set(_FIELDS):
        return False
    return all(
        isinstance(payload[field], (int, float)) and 0 <= payload[field] <= 1
        for field in _FIELDS
    )
