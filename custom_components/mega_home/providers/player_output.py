"""Транспорт трека и наблюдаемое состояние плеера, без зависимости от HA."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime
from math import isfinite
from time import monotonic, time
from typing import Any
from urllib.parse import urlsplit

from ..core.ops_base import OpError
from ..core.source import EntityState
from .music_pick import pick_stream, stream_content_type, stream_format

ACTIVE = {"playing", "paused", "buffering"}
STOPPED = {"unavailable", "unknown", "off", "standby"}
START_SLACK = 2


@dataclass(frozen=True)
class MediaItem:
    id: str
    title: str
    artist: str = ""
    album: str = ""
    image: str = ""
    duration: float = 0


@dataclass(frozen=True)
class Sample:
    state: str
    content: str
    identity: tuple[str, ...]
    position: float | None
    duration: float | None
    at: float

    @classmethod
    def read(cls, state: EntityState | None) -> Sample:
        attrs = state.attributes if state else {}
        position = number(attrs.get("media_position"))
        updated = attrs.get("media_position_updated_at")
        at = monotonic()
        if isinstance(updated, datetime) and position is not None:
            at -= max(0, time() - updated.timestamp())
        content = str(attrs.get("media_content_id") or "")
        identity = tuple(str(attrs.get(key) or "") for key in (
            "media_content_id", "source", "app_name", "media_content_type",
            "media_title", "media_artist", "media_album_name", "media_channel",
        ))
        return cls(state.state if state else "unknown", content, identity, position,
                   number(attrs.get("media_duration")), at)

    def elapsed(self, now: float) -> float:
        return (self.position or 0) + max(0, now - self.at)


def number(value: Any) -> float | None:
    return float(value) if type(value) in (int, float) and isfinite(value) and value >= 0 else None


@dataclass(frozen=True)
class Output:
    content: str
    content_type: str
    proxy: str = ""
    native: bool = False
    opaque: bool = False

    def matches(self, sample: Sample) -> bool:
        if self.native:
            return sample.content.partition(":")[0] == self.content
        if sample.content and sample.content == self.content:
            return True
        if not sample.content or not self.proxy or "://" not in sample.content:
            return False
        try:
            return urlsplit(sample.content).path == urlsplit(self.proxy).path
        except ValueError:
            return False

    def data(self, item: MediaItem) -> dict:
        data = {"media_content_id": self.content, "media_content_type": self.content_type}
        if not self.native:
            data["extra"] = {"metadata": {
                "metadataType": 3,
                "title": item.title,
                "artist": item.artist,
                "album": item.album,
                "albumName": item.album,
                "duration": item.duration or None,
                "images": [{"url": item.image}] if item.image else [],
                "imageUrl": item.image or None,
            }}
        return data


def native_output(track_id: str, profile: Mapping[str, Any]) -> Output | None:
    native_type = profile.get("nativeTrackType")
    return Output(track_id.partition(":")[0], native_type, native=True) if native_type else None


def stream_output(
    item: MediaItem, profile: Mapping[str, Any], answer: dict[str, Any],
    address: Callable[[str, dict], dict],
) -> Output:
    link = pick_stream(answer.get("infos"))
    if link is None:
        raise OpError("Источник не предоставил поток для трека", 503)
    mime = stream_content_type(link["codec"])
    extension = stream_format(link["codec"])
    stream_type = profile["type"].replace("{mime}", mime).replace("{ext}", extension)
    media = address(item.id, answer)
    if not media.get("proxy") or not media.get("mediaSource"):
        raise OpError("Дом не знает локальный адрес для музыкального потока", 503)
    return Output(media["mediaSource"], stream_type, media["proxy"], opaque=profile.get("opaqueTrackId") is True)
