"""Музыкальная служба дома: каталог, выходы и очереди доступны через connect."""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Mapping
from typing import TYPE_CHECKING, Any

from .core.ops import call, command_spec
from .core.ops_base import OpError, arguments, find
from .providers.media_queue import PlayerQueue
from .providers.music_catalog import queue_ids
from .providers.music_flow import FLOW_CHANNELS, FLOW_RATE, FLOW_WIDTH, FlowSession, FlowStreams

# Потолок задержки плеера, мс: как у MA (0–5000), больше — уже не выравнивание, а рассинхрон.
MAX_DELAY_MS = 5000
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
        # Наши группы мультирума: сущность ведущего → сущности участников (ведущего в списке нет).
        # ⚠ Группа — НЕ группировка HA: `media_player.join` группирует только внутри одной интеграции
        # (`entity-media-player.md`), а Sonos с HEOS и прошитый Echo HA не сгруппирует никогда.
        # У нас звук и так один — поток дома (`providers/music_flow.py`): участнику достаточно дать
        # ТОТ ЖЕ адрес сессии, и он играет вместе, без своей очереди.
        self.groups: dict[str, list[str]] = {}
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
                return flow_output(self._flow, session, profile, opened, entity)
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
        # Группа: участникам нужен адрес сессии, пока поток ещё НЕ читают (иначе команда лишняя —
        # элемент сменяется внутри того же потока).
        if self.groups.get(entity) and profile.get("flow") is True and not self._flow.session(tile["id"]).reading:
            await self._wake_group(entity)

    def state_groups(self) -> dict[str, list[str]]:
        """Составы групп для ответа о приборе: каждая сущность группы → весь состав, ведущий первым.

        ⚠ Приложение читает состав ОДНИМ полем (`group_members`, `core/media-group.ts`), не различая
        штатную группировку HA и нашу: иначе у него был бы второй путь «кто с кем играет».
        """
        out: dict[str, list[str]] = {}
        for leader, members in self.groups.items():
            if not members:
                continue
            line = [leader, *members]
            for entity in line:
                out[entity] = line
        return out

    async def group_command(self, tile: dict, name: str, data: dict, by: Any) -> dict | None:
        """`join`/`unjoin` для НАШИХ групп (`core/ops.py` зовёт до штатной службы HA).

        `None` — команда не наша: плеер не берёт поток дома, и `media_player.join` решает как раньше.
        """
        profile = tile.get("stream")
        entity = self._entity(tile)
        if not isinstance(profile, dict) or not entity:
            return None
        if name == "unjoin":
            # ⚠ Снять отметку приложение может С ЛЮБОГО участника (Sonos, станция), а не только с
            # ведущего: `unjoin` для плитки без потока дома ушёл бы в HA и не погасил бы ничего —
            # музыка на снятом плеере продолжала играть (живой баг 2026-11-22).
            return await self._leave_group(entity)
        if profile.get("flow") is not True:
            return None  # ведущим группы может быть только плеер с потоком дома
        members = [str(member) for member in (data.get("group_members") or []) if str(member) != entity]
        for member in dict.fromkeys(members):
            member_tile = self._tile_of(member)
            member_profile = (member_tile or {}).get("stream")
            # Участник — ЛЮБОЙ плеер со `stream` (Sonos, HEOS, станция): звук ему даёт дом. Ведущим
            # же может быть только `flow`-плеер — его очередь ведёт поток.
            if not isinstance(member_profile, dict):
                raise OpError("В одну группу — только плееры, которые играют поток дома", 409)
            state = self._home.source.get(member)
            if state is not None and state.attributes.get("assumed_state") is True:
                raise OpError("Плеер в облачном режиме поток дома не примет", 409)
        if any(entity in group for group in self.groups.values()):
            await self._leave_group(entity)  # был участником чужой группы — сперва выходим
        self.groups[entity] = list(dict.fromkeys(members))
        self._changed()
        await self._wake_group(entity)
        return {"accepted": True, "group": [entity, *self.groups[entity]]}

    async def _leave_group(self, entity: str) -> dict:
        """Выйти из группы: ушедший гаснет, ведущий и остальные участники играют дальше."""
        for leader, members in list(self.groups.items()):
            if entity == leader:
                self.groups.pop(leader, None)
                await self._stop_players(members)
            elif entity in members:
                left = [member for member in members if member != entity]
                if left:
                    self.groups[leader] = left
                else:
                    self.groups.pop(leader, None)
                await self._stop_players([entity])
            else:
                continue
            self._changed()
        return {"accepted": True}

    async def _stop_players(self, entities: list[str]) -> None:
        for entity in entities:
            await call(self._home.source, "media_player", "media_stop", {"entity_id": entity})

    async def _wake_group(self, leader: str) -> None:
        """Участникам — ТОТ ЖЕ адрес сессии ведущего: их плеер забирает поток сам, без очереди.

        ⚠ Команду зовём только когда поток ещё НЕ читают (`_play`/`_play_station`): у устройства,
        которое новый `media_url` поверх живого потока не берёт, повторная команда была бы
        бесполезна, а на смену элемента внутри потока она и не нужна — адрес тот же.

        Формат провода — MP3 всегда (`providers/music_flow.py`): участник объявляет свои типы, и
        `stream.mp3` в его списке обязан быть — иначе честный отказ, а не тишина.
        """
        members = self.groups.get(leader) or []
        if not members:
            return
        tile = self._tile_of(leader)
        session = self._flow.session(tile["id"]) if tile is not None else None
        for member in members:
            member_tile = self._tile_of(member)
            if member_tile is None:
                continue
            # Токен — СВОЙ у участника: по нему дом знает, чью задержку применить к чтению.
            address = self._flow.address(session, member) if session is not None else None
            if address is None:
                raise OpError("Дом не знает своего адреса для потока", 503)
            media_type = flow_content_type(member_tile.get("stream") or {})
            options = _member_accepts(member_tile)
            if options is not None and media_type not in options:
                raise OpError("Плеер не примет поток дома в этом формате", 409)
            fields = _stream_fields(
                member_tile, member,
                {"media_content_id": address, "media_content_type": media_type},
                self._home.source,
            )
            await call(self._home.source, "media_player", "play_media", {"entity_id": member, **fields})

    def _tile_of(self, entity: str) -> dict | None:
        return next((tile for tile in self._tiles() if self._entity(tile) == entity), None)

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
                return flow_output(self._flow, session, profile, opened, entity)
            # ⚠ Запуск подтверждает ВЛАДЕЛЕЦ, а не плеер: адрес у станции чужой (CDN источника), и
            # о себе такой плеер может не рассказывать вовсе. Команду приняла HA — этого довольно.
            return Output(
                fields["media_content_id"], fields["media_content_type"],
                opaque=profile.get("opaqueTrackId") is True, owner=True,
            )

        await queue.replace(load, prepare, None)
        if self.groups.get(entity) and profile.get("flow") is True and not self._flow.session(tile["id"]).reading:
            await self._wake_group(entity)

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
            # Задержка плеера — из текущего конфига на каждый кусок: правка в составе слышна сразу.
            session.delay_of = self._delay
        return queue

    def _delay(self, entity: str) -> int:
        """Задержка плеера, мс — из ТЕКУЩЕГО состава, а не из снимка на старте потока.

        ⚠ Инсталлятор включает музыку и крутит задержку в свойствах плеера: значение обязано
        доехать до играющего потока без перезапуска (просьба заказчика 2026-11-22).
        """
        tile = self._tile_of(entity)
        value = ((tile or {}).get("stream") or {}).get("delayMs")
        if not isinstance(value, (int, float)) or isinstance(value, bool) or value <= 0:
            return 0
        return min(int(value), MAX_DELAY_MS)

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
        self.groups.clear()
        self._flow.close()


def flow_output(
    flow: FlowStreams, session: FlowSession, profile: Mapping[str, Any], opened: Callable[[], None],
    player: str,
) -> Output:
    """Выход потока-сессии: адрес один на сессию, содержимое меняет дом (`providers/music_flow.py`).

    ⚠ Тип и схема адреса элемента тут не при чём: плееру уходит WAV-поток дома, а не ссылка на
    источник. Подтверждение запуска — начало чтения потока (`on_read` → `Attempt.opened`): у
    плеера, который о себе не рассказывает, другого свидетельства не бывает.
    """
    address = flow.address(session, player)
    if address is None:
        raise OpError("Дом не знает своего адреса для потока", 503)
    session.on_read = opened
    return Output(
        address, flow_content_type(profile),
        opaque=profile.get("opaqueTrackId") is True, flow=True,
    )


def flow_content_type(profile: Mapping[str, Any]) -> str:
    """Тип `media_content_type` адреса сессии. ⚠ Поток дома — MP3 (см. `providers/music_flow.py`)."""
    return str(profile.get("type") or "music").replace("{mime}", "audio/mpeg").replace("{ext}", "mp3")


def _member_accepts(tile: dict) -> list[str] | None:
    """Какие типы потока участник объявил (`options` его команды `play_stream`); `None` — не объявлял.

    ⚠ Свои типы объявляет Яндекс Станция (`stream.{ext}` → `stream.aac`/`stream.mp3`); наш поток —
    MP3, и в её списке он есть. ESPHome, Sonos, HEOS типов не объявляют — им можно что угодно.
    """
    spec = command_spec(tile, "play_stream") or {}
    options = ((spec.get("fields") or {}).get("media_content_type") or {}).get("options")
    return options if isinstance(options, list) else None


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
