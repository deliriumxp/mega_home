"""Музыкальная служба дома: каталог, выходы и очереди доступны через connect."""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Mapping
from typing import TYPE_CHECKING, Any

from .core.ops import command_spec
from .core.ops_base import OpError, arguments, find
from .providers.media_queue import PlayerQueue
from .providers.music_catalog import queue_ids
from .providers.music_flow import FLOW_CHANNELS, FLOW_RATE, FLOW_WIDTH, FlowSession, FlowStreams
from .providers.music_pick import pick_stream
from .providers.music_proxy import MusicStreamLinks
from .providers.player_output import MediaItem, Output, native_output, number, stream_output
if TYPE_CHECKING:
    from .providers.yandex_music import MusicService
    from .station_auth import StationAuth

TOPIC = "media/playback"
FAVORITES_TOPIC = "media/favorites"


class MusicPlayback:
    def __init__(
        self, coordinator: Any, music: MusicService, station: StationAuth, links: MusicStreamLinks,
        flow: FlowStreams | None = None,
    ) -> None:
        self._home = coordinator
        self._music = music
        self._station = station
        self._links = links
        # Сессии потока: у `flow`-плеера адрес один на всю сессию (`providers/music_flow.py`).
        # Без адреса дома (`origin` = None) сессию не отдать — плееру ответит честный отказ.
        self._flow = flow if flow is not None else FlowStreams(lambda: None)
        self._players: dict[str, PlayerQueue] = {}
        self._profiles: dict[str, dict] = {}
        self._changed()

    @property
    def flow(self) -> FlowStreams:
        return self._flow

    def _changed(self) -> None:
        self._home.topics.bump(TOPIC)

    def _tiles(self) -> list[dict]:
        return (self._home.data or {}).get("tiles", [])

    def _entity(self, tile: dict | None) -> str | None:
        if tile and tile.get("domain") == "media_player" and isinstance(tile.get("stream"), dict):
            return tile.get("entityId")
        return None

    async def call(self, can_manage: bool, request: dict) -> dict:
        action = request.get("action")
        if action == "playback_state":
            return {"players": self._views()}
        if action == "play_queue":
            await self._play(request)
            return {"accepted": True}
        if action == "play_station":
            await self._play_station(request)
            return {"accepted": True}
        if action == "queue_control":
            command = request.get("command")
            if command not in ("previous", "next", "stop"):
                raise OpError("Команда очереди не поддерживается")
            entity = self._entity(find(self._tiles(), request.get("player")))
            queue = self._players.get(entity) if entity else None
            return {"handled": await queue.control(command) if queue else False}
        if action == "track_details":
            raise OpError("Неизвестное действие Яндекс Музыки", 404)
        answer = await self._station.call(can_manage, request)
        if action == "set_favorite":
            self._home.topics.bump(FAVORITES_TOPIC)
        if action == "disconnect":
            for queue in self._players.values():
                queue.clear()
        return self._links.with_address(request.get("id"), answer) if action == "stream" else answer

    async def _play(self, request: dict) -> None:
        tile = find(self._tiles(), request.get("player"))
        entity = self._entity(tile)
        if not entity or tile is None:
            raise OpError("Воспроизведение на этом плеере недоступно", 404)
        ids = queue_ids(request.get("ids"))
        start = request.get("index")
        if type(start) is not int or not 0 <= start < len(ids):
            raise OpError("Неверный начальный трек очереди")
        cursor = _cursor(request.get("continuation"))
        profile = tile["stream"]
        source = self._home.source
        queue = self._players.get(entity)
        if queue is not None and self._profiles.get(entity) != profile:
            queue.close()
            self._flow.drop(tile["id"])
            queue = None
        if queue is None:
            queue = self._queue_for(tile, entity)
            self._players[entity] = queue
            self._profiles[entity] = profile
        session = self._flow.session(tile["id"]) if profile.get("flow") is True else None

        async def load() -> tuple[tuple[MediaItem, ...], int]:
            answer = await self._music.call(False, {"action": "track_details", "ids": ids})
            details = {item["id"]: _media(item) for item in answer["items"]}
            if ids[start] not in details:
                raise OpError("Трек Яндекс Музыки недоступен", 404)
            if not any(self._entity(current) == entity and current["stream"] == profile for current in self._tiles()):
                raise OpError("Настройки плеера изменились. Включите трек повторно", 409)
            return tuple(details[id_] for id_ in ids if id_ in details), sum(id_ in details for id_ in ids[:start])

        async def prepare(item: MediaItem, opened: Callable[[], None]) -> Output:
            state = source.get(entity)
            if not state or state.state in ("unknown", "unavailable") or (
                profile.get("needsKnownState") and state.attributes.get("assumed_state") is True
            ):
                raise OpError("Этот плеер сейчас недоступен", 409)
            if session is not None:
                # Плеер берёт поток ОДНИМ адресом на сессию: элементы сменяются внутри неё
                # (`providers/music_flow.py`), новой команды на каждый трек не нужно.
                return flow_output(self._flow, session, profile, opened)
            if output := native_output(item.id, profile):
                return output
            answer = await self._music.call(False, {"action": "stream", "id": item.id})
            return stream_output(
                item, profile, answer,
                lambda id_, stream: self._links.with_address(id_, stream, opened),
            )

        async def more() -> tuple[tuple[MediaItem, ...], bool]:
            answer = await self._music.call(False, cursor)
            cursor["page"] += 1
            items = tuple(_media(item) for item in answer["items"] if item.get("kind") == "track")
            return items, bool(answer.get("more")) and cursor["page"] <= 999

        await queue.replace(load, prepare, more if cursor else None)

    async def _play_station(self, request: dict) -> None:
        """Станция каталога на плеере — элемент очереди дома, как трек Яндекса.

        Владелец «что играет» — дом: он же отдаёт станцию плееру (`play_stream` описания плитки,
        оба поля приходят готовыми от бандла и проверяются по описанию КОМАНДЫ конфига), и он же
        помнит её для плитки, шторки и второго телефона. Адрес каталога и обложку станции бандл
        присылает данными — дом вендорского каталога не знает.
        """
        tile = find(self._tiles(), request.get("player"))
        entity = self._entity(tile)
        if not entity or tile is None:
            raise OpError("Воспроизведение на этом плеере недоступно", 404)
        item = _station(request.get("station"))
        fields = _stream_fields(tile, entity, request.get("fields"), self._home.source)
        profile = tile["stream"]
        source = self._home.source
        queue = self._players.get(entity)
        if queue is not None and self._profiles.get(entity) != profile:
            queue.close()
            self._flow.drop(tile["id"])
            queue = None
        if queue is None:
            queue = self._queue_for(tile, entity)
            self._players[entity] = queue
            self._profiles[entity] = profile
        session = self._flow.session(tile["id"]) if profile.get("flow") is True else None

        async def load() -> tuple[tuple[MediaItem, ...], int]:
            if not any(self._entity(current) == entity and current["stream"] == profile for current in self._tiles()):
                raise OpError("Настройки плеера изменились. Включите станцию повторно", 409)
            return (item,), 0

        async def prepare(_item: MediaItem, opened: Callable[[], None]) -> Output:
            state = source.get(entity)
            if not state or state.state in ("unknown", "unavailable") or (
                profile.get("needsKnownState") and state.attributes.get("assumed_state") is True
            ):
                raise OpError("Этот плеер сейчас недоступен", 409)
            if session is not None:
                return flow_output(self._flow, session, profile, opened)
            # ⚠ Запуск подтверждает ВЛАДЕЛЕЦ, а не плеер: адрес у станции чужой (CDN источника), и
            # о себе такой плеер может не рассказывать вовсе. Команду приняла HA — этого довольно.
            return Output(
                fields["media_content_id"], fields["media_content_type"],
                opaque=profile.get("opaqueTrackId") is True, owner=True,
            )

        await queue.replace(load, prepare, None)

    def _queue_for(self, tile: dict, entity: str) -> PlayerQueue:
        """Очередь плеера; у `flow`-плеера — с его сессией потока (`providers/music_flow.py`)."""
        profile = tile["stream"]
        session = self._flow.session(tile["id"]) if profile.get("flow") is True else None
        queue = PlayerQueue(
            entity, self._home.source, self._home.env, self._changed, self._links.unwatch,
            flow=session,
        )
        if session is not None:
            session.source_of = self._flow_source
            # Элемент доигран внутри потока — очередь говорит, что дальше; команды плееру нет.
            session.on_end = queue.flow_end
            session.fade_bytes = fade_bytes(profile)
        return queue

    async def _flow_source(self, item: MediaItem) -> str | None:
        """Звук элемента для потока сессии: у станции адрес дан бандлом, у трека — из Яндекса."""
        if item.source:
            return item.source
        answer = await self._music.call(False, {"action": "stream", "id": item.id})
        link = pick_stream(answer.get("infos") if isinstance(answer, dict) else None)
        return link["url"] if link else None

    def _views(self) -> dict[str, dict]:
        players = {}
        for tile in self._tiles():
            entity = self._entity(tile)
            if entity and (queue := self._players.get(entity)) and (view := queue.view()):
                players[tile["id"]] = view
        return players

    def config_changed(self) -> None:
        current = {entity: tile for tile in self._tiles() if (entity := self._entity(tile))}
        for entity, profile in list(self._profiles.items()):
            tile = current.get(entity)
            if tile is not None and tile["stream"] == profile:
                continue
            self._players.pop(entity).close()
            if tile is not None:
                self._flow.drop(tile["id"])
            del self._profiles[entity]

    async def play_configured(self, entity_ids: list[str], track: str) -> list[str]:
        """Запустить трек из сценария на известных выходах и вернуть остальные цели."""
        known = {entity: tile["id"] for tile in self._tiles() if (entity := self._entity(tile))}
        await asyncio.gather(*(
            self._play({"player": known[entity], "ids": [track], "index": 0})
            for entity in dict.fromkeys(entity_ids) if entity in known
        ))
        return [entity for entity in entity_ids if entity not in known]

    def close(self) -> None:
        for queue in self._players.values():
            queue.close()
        self._players.clear()
        self._profiles.clear()
        self._flow.close()


def flow_output(
    flow: FlowStreams, session: FlowSession, profile: Mapping[str, Any], opened: Callable[[], None],
) -> Output:
    """Выход потока-сессии: адрес один на сессию, содержимое меняет дом (`providers/music_flow.py`).

    ⚠ Тип и схема адреса элемента тут не при чём: плееру уходит WAV-поток дома, а не ссылка на
    источник. Подтверждение запуска — начало чтения потока (`on_read` → `Attempt.opened`): у
    плеера, который о себе не рассказывает, другого свидетельства не бывает.
    """
    address = flow.address(session)
    if address is None:
        raise OpError("Дом не знает своего адреса для потока", 503)
    session.on_read = opened
    return Output(
        address, flow_content_type(profile),
        opaque=profile.get("opaqueTrackId") is True, flow=True,
    )


def flow_content_type(profile: Mapping[str, Any]) -> str:
    """Тип `media_content_type` адреса сессии: дом отдаёт WAV (`providers/music_flow.py`)."""
    return str(profile.get("type") or "music").replace("{mime}", "audio/wav").replace("{ext}", "wav")


def fade_bytes(profile: Mapping[str, Any]) -> int:
    """Окно фейда на стыке элементов, байты PCM: `fadeMs` описания плитки, не больше 3 с.

    ⚠ Фейд делает ДОМ (`providers/music_flow.py`, `_Fader`), и только у потока: у плеера, который
    берёт ссылку на элемент, смешивать нечего — там стык как был. Нет `fadeMs` — стык как был.
    """
    value = profile.get("fadeMs")
    if not isinstance(value, (int, float)) or isinstance(value, bool) or value <= 0:
        return 0
    return min(int(value), 3000) * FLOW_CHANNELS * FLOW_WIDTH * FLOW_RATE // 1000


def _station(value: Any) -> MediaItem:
    """Станция от бандла: каталог и обложки — его дело, дому нужны id, подпись и адрес потока."""
    if not isinstance(value, dict):
        raise OpError("Станция не описана")
    station_id, url = value.get("id"), value.get("url")
    if not isinstance(station_id, str) or not station_id.strip():
        raise OpError("Станция без идентификатора")
    if not isinstance(url, str) or not url.strip():
        raise OpError("Станция без адреса потока")
    title, image = value.get("title"), value.get("image")
    return MediaItem(
        # ⚠ id элемента — с приставкой: приложение по ней отличает станцию каталога от трека
        # Яндекса («что играет» оно читает одним путём — из очереди дома).
        id=f"station:{station_id.strip()}",
        title=title.strip() if isinstance(title, str) and title.strip() else station_id.strip(),
        image=image if isinstance(image, str) else "",
        source=url.strip(),
    )


def _stream_fields(tile: dict, entity: str, value: Any, source: Any) -> dict[str, str]:
    """Поля `play_media` станции — по описанию КОМАНДЫ из конфига: браузеру жильца верить нельзя.

    ⚠ Описание команды читается тем же правилом, что у `ops.command` (`core/ops.py`,
    `command_spec`), и разбирается тем же `arguments`: второго места «как звать play_stream» быть
    не должно — разъедется с менеджером молча.
    """
    spec = command_spec(tile, "play_stream")
    if spec is None:
        raise OpError("Плеер не играет поток по ссылке", 404)
    state = source.get(entity)
    try:
        data = arguments(spec, value, state.attributes if state else {})
    except ValueError as err:
        raise OpError(str(err)) from err
    return {
        "media_content_id": str(data.get("media_content_id") or ""),
        "media_content_type": str(data.get("media_content_type") or ""),
    }


def _media(item: dict) -> MediaItem:
    image = item.get("cover") or ""
    if not image.startswith(("https://", "http://")):
        image = ""
    return MediaItem(
        id=item["id"], title=item["title"], artist=item.get("artist") or "", album=item.get("album") or "",
        image=image, duration=(number(item.get("durationMs")) or 0) / 1000,
    )


def _cursor(value: Any) -> dict:
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise OpError("Неверное продолжение каталога")
    action, page = value.get("action"), value.get("page")
    if type(page) is not int or not 1 <= page <= 999:
        raise OpError("Неверная страница продолжения каталога")
    if action in ("liked", "album", "artist", "playlist"):
        return {"action": action, "page": page, "id": value.get("id")}
    if action == "search":
        return {"action": action, "page": page, "query": value.get("query"), "kind": "track"}
    raise OpError("Этот раздел нельзя продолжить в очереди")
