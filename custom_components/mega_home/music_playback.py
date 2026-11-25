"""Музыкальная служба дома: каталог, выходы и очереди доступны через connect."""

from __future__ import annotations

import asyncio
import shutil
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any

from .core.ops import call
from .core.ops_base import OpError, find
from .music_latency import MAX_LATENCY_MS, MusicLatency
from .music_groups import MusicGroups
from .music_output import flow_address, flow_content_type, flow_output, member_accepts, output_profile, stream_fields
from .providers.media_queue import PlayerQueue
from .providers.music_catalog import queue_ids
from .providers.music_flow import FlowSession, FlowStreams
from .providers.music_flow_feed import fade_window
from .providers.music_pick import pick_stream
from .providers.music_proxy import MusicStreamLinks
from .providers.player_output import MediaItem, Output, native_output, number, stream_output

if TYPE_CHECKING:
    from .providers.yandex_music import MusicService
    from .station_auth import StationAuth

TOPIC = "media/playback"
FAVORITES_TOPIC = "media/favorites"

# Потолок сдвига плеера, мс — тот же, что у замера (`MAX_LATENCY_MS`): Станция держит ~10 с, и
# прежние 5000 (как у MA) не давали задержать остальные колонки её группы на нужное.
MAX_DELAY_MS = MAX_LATENCY_MS


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
        self.multiroom = MusicGroups(self)
        self._group_lock = asyncio.Lock()
        self._latency = MusicLatency(self)
        self.assistant: Callable[[bool, dict], Awaitable[dict | None]] | None = None
        self._players: dict[str, PlayerQueue] = {}
        self._profiles: dict[str, tuple[str, dict]] = {}
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
        if self.assistant is not None:
            answer = await self.assistant(can_manage, request)
            if answer is not None:
                if action == "playback_state":
                    answer["players"] = {**self._views(), **answer["players"]}
                elif action == "set_favorite":
                    self._home.topics.bump(FAVORITES_TOPIC)
                elif action in ("play_queue", "play_station", "queue_control"):
                    self._changed()
                return answer
        if action == "calibration_ping":
            return self._latency.ping()
        if action in ("calibration_measure", "calibration_check", "calibration_result", "calibration_stop"):
            async with self._group_lock:
                if action == "calibration_measure":
                    return await self._latency.measure(request.get("player"))
                if action == "calibration_check":
                    return await self._latency.check(request.get("players"))
                if action == "calibration_stop":
                    return await self._latency.stop(request.get("token"))
                return await self._latency.result(request.get("token"))
        if action == "playback_state":
            return {"players": self._views()}
        if action in ("play_queue", "play_station"):
            entity = self._entity(find(self._tiles(), request.get("player")))
            async with self._group_lock:
                self._latency.interrupt(entity)
                # Выбор музыки у участника меняет общий источник группы через её ведущего:
                # открытый поток остаётся тем же, разъединение — только явный `unjoin`.
                owner = self.multiroom.owner(entity) if entity else None
                if owner is not None and owner != entity:
                    owner_tile = self._tile_of(owner)
                    if owner_tile is None:
                        raise OpError("Ведущая колонка сейчас недоступна", 409)
                    request = {**request, "player": owner_tile["id"]}
                await self._play_request(request)
            return {"accepted": True}
        if action == "queue_control":
            command = request.get("command")
            if command not in ("previous", "next", "stop", "seek"):
                raise OpError("Команда очереди не поддерживается")
            entity = self._entity(find(self._tiles(), request.get("player")))
            raw_position = request.get("value")
            position = raw_position if isinstance(raw_position, (int, float)) and not isinstance(raw_position, bool) else None
            async with self._group_lock:
                # У участника своей очереди нет: общий поток ведёт владелец группы, поэтому
                # «назад/вперёд/стоп/перемотка» переводим на его очередь, а не шлём напрямую в HA.
                owner = self.multiroom.owner(entity) if entity else None
                queue = self._players.get(owner or entity) if entity else None
                handled = await queue.control(command, position) if queue else False
                # «Стоп» очереди кончает сессию: отметки группы больше не о чем держать —
                # иначе приложение показывало «играют вместе» над молчащими колонками
                # (ревью 2026-11-24). Пауза группу НЕ разъединяет: это делает только стоп.
                if handled and command == "stop" and owner is not None:
                    await self.multiroom.leave(owner)
                return {"handled": handled}
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
        queue = self._queue_of(tile, entity)
        async def load() -> tuple[tuple[MediaItem, ...], int]:
            answer = await self._music.call(False, {"action": "track_details", "ids": ids})
            details = {item["id"]: _media(item) for item in answer["items"]}
            if ids[start] not in details:
                raise OpError("Трек Яндекс Музыки недоступен", 404)
            if not any(self._entity(current) == entity and output_profile(current["stream"]) == output_profile(profile) for current in self._tiles()):
                raise OpError("Настройки плеера изменились. Включите трек повторно", 409)
            return tuple(details[id_] for id_ in ids if id_ in details), sum(id_ in details for id_ in ids[:start])

        async def prepare(item: MediaItem, opened: Callable[[], None]) -> Output:
            state = source.get(entity)
            if not state or state.state in ("unknown", "unavailable") or (
                profile.get("needsKnownState") and state.attributes.get("assumed_state") is True
            ):
                raise OpError("Этот плеер сейчас недоступен", 409)
            # ⚠ Нативная дорожка — только там, где плеер правда её играет. У станции `nativeTrackType`
            # работает в ЛОКАЛЬНОМ режиме; в облачном её `play_media` типа `track` не принимает, и HA
            # отвечает отказом — жилец видел «Home Assistant отклонил команду». Признак режима — тот же
            # `assumed_state`, что у потока: облачный режим его ставит (README YandexStation).
            native = native_output(item.id, profile)
            if native is not None and profile.get("needsKnownState") and (
                state.attributes.get("assumed_state") is True
            ):
                raise OpError(
                    "Станция в облачном режиме: включите местное управление или выберите другую колонку", 409
                )
            if isinstance(session := queue.flow, FlowSession):
                # Плеер берёт поток ОДНИМ адресом на сессию: элементы сменяются внутри неё
                # (`providers/music_flow.py`), новой команды на каждый трек не нужно.
                return flow_output(self._flow, session, profile, opened, entity)
            if native is not None:
                return native
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
        # Участники с открытым потоком продолжают играть; оборванный выход получает адрес повторно.
        if self.multiroom.groups.get(entity):
            await self.multiroom.wake(entity)

    def state_groups(self) -> dict[str, list[str]]:
        return self.multiroom.state_groups()

    async def group_command(self, tile: dict, name: str, data: dict, by: Any) -> dict | None:
        if (tile.get("stream") or {}).get("externalQueue"):
            return None  # Команду группы исполнит интеграция плеера через HA.
        async with self._group_lock:
            return await self.multiroom.command(tile, name, data, by)

    async def replace_player_source(
        self, tile: dict, name: str, data: dict, execute: Callable[[], Awaitable[dict]],
    ) -> dict:
        entity = self._entity(tile)
        if entity is None:
            return await execute()
        async with self._group_lock:
            self._latency.interrupt(entity)
            # Выбор у участника меняет общую группу, а не выводит его из неё молча.
            owner = self.multiroom.owner(entity)
            if owner is not None and owner != entity:
                owner_tile = self._tile_of(owner)
                if owner_tile is None:
                    raise OpError("Ведущая колонка сейчас недоступна", 409)
                tile, entity = owner_tile, owner
            if self.multiroom.contains(entity):
                if name == "play_stream":
                    return await self._replace_group_stream(tile, entity, data)
                # TV/USB/line-in захватить в общий поток нельзя — это звук самого ведущего.
                # Оставлять группу на старом потоке после смены входа ещё хуже: жилец видел бы
                # новый источник на ведущем и старую музыку у участников.
                raise OpError("Аппаратный источник ведущего недоступен в группе — разъедините её", 409)
            self._drop_queue(entity, tile)
            return await execute()

    async def _replace_group_stream(self, tile: dict, entity: str, data: dict) -> dict:
        """Прямой URL как элемент общей очереди: поток группы не пересоздаётся."""
        url = str(data.get("media_content_id") or "")
        if not url:
            raise OpError("Поток без адреса", 400)
        metadata = ((data.get("extra") or {}).get("metadata") or {}) if isinstance(data.get("extra"), dict) else {}
        item = MediaItem(
            id=f"stream:{url}", title=metadata.get("title") or url, artist=metadata.get("artist") or "",
            album=metadata.get("album") or "", image=metadata.get("imageUrl") or "", source=url,
        )
        profile = tile["stream"]
        queue = self._queue_of(tile, entity)
        async def load() -> tuple[tuple[MediaItem, ...], int]:
            return (item,), 0
        async def prepare(_item: MediaItem, opened: Callable[[], None]) -> Output:
            state = self._home.source.get(entity)
            if not state or state.state in ("unknown", "unavailable"):
                raise OpError("Этот плеер сейчас недоступен", 409)
            if isinstance(session := queue.flow, FlowSession):
                return flow_output(self._flow, session, profile, opened, entity)
            return Output(url, str(data.get("media_content_type") or "music"))
        await queue.replace(load, prepare, None)
        await self.multiroom.wake(entity)
        return {"accepted": True}

    async def _play_request(self, request: dict) -> None:
        await (self._play(request) if request["action"] == "play_queue" else self._play_station(request))

    def _drop_queue(self, entity: str, tile: dict) -> None:
        """Снять очередь и сессию плеера: следующий запуск заведёт их по НОВЫМ правилам.

        ⚠ Нужно именно при переводе на поток дома: `_queue_for` решает про сессию по `_forced` на
        момент создания, и очередь, заведённая раньше, осталась бы без неё навсегда.
        """
        if (queue := self._players.pop(entity, None)) is not None:
            queue.close()
        previous = self._profiles.pop(entity, None)
        self._flow.drop(previous[0] if previous is not None else tile["id"])

    def _system_of(self, entity: str) -> str | None:
        """Ключ системы вендора по описанию плитки; `None` — своего мультирума у плеера нет."""
        tile = self._tile_of(entity)
        system = ((tile or {}).get("stream") or {}).get("sync")
        return system if isinstance(system, str) and system else None

    def _stream_output_fields(self, session: FlowSession, entity: str) -> dict[str, str]:
        tile = self._tile_of(entity)
        if tile is None:
            raise OpError("Колонка для потока не найдена", 404)
        profile = tile["stream"]
        media_type = flow_content_type(profile)
        formats = member_accepts(tile)
        if formats is not None and media_type not in formats:
            raise OpError("Плеер не примет поток дома в этом формате", 409)
        address = self._flow.address(session, entity)
        if address is None:
            raise OpError("Дом не знает своего адреса для потока", 503)
        return stream_fields(tile, entity, {
            "media_content_id": flow_address(profile, address),
            "media_content_type": media_type,
        }, self._home.source)

    async def play_flow(self, session: FlowSession, entity: str) -> None:
        fields = self._stream_output_fields(session, entity)
        await call(self._home.source, "media_player", "play_media", {"entity_id": entity, **fields})

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
        fields = stream_fields(tile, entity, request.get("fields"), self._home.source)
        profile = tile["stream"]
        source = self._home.source
        queue = self._queue_of(tile, entity)
        async def load() -> tuple[tuple[MediaItem, ...], int]:
            if not any(self._entity(current) == entity and output_profile(current["stream"]) == output_profile(profile) for current in self._tiles()):
                raise OpError("Настройки плеера изменились. Включите станцию повторно", 409)
            return (item,), 0

        async def prepare(_item: MediaItem, opened: Callable[[], None]) -> Output:
            state = source.get(entity)
            if not state or state.state in ("unknown", "unavailable") or (
                profile.get("needsKnownState") and state.attributes.get("assumed_state") is True
            ):
                raise OpError("Этот плеер сейчас недоступен", 409)
            if isinstance(session := queue.flow, FlowSession):
                if not session.reading and (binary := await self._home.env.run(shutil.which, "ffmpeg")):
                    await session.warm_radio(binary)
                return flow_output(self._flow, session, profile, opened, entity)
            # ⚠ Запуск подтверждает ВЛАДЕЛЕЦ, а не плеер: адрес у станции чужой (CDN источника), и
            # о себе такой плеер может не рассказывать вовсе. Команду приняла HA — этого довольно.
            return Output(
                fields["media_content_id"], fields["media_content_type"],
                opaque=profile.get("opaqueTrackId") is True, owner=True,
            )

        await queue.replace(load, prepare, None)
        if self.multiroom.groups.get(entity):
            await self.multiroom.wake(entity)

    def _queue_of(self, tile: dict, entity: str) -> PlayerQueue:
        """Очередь плеера под ТЕКУЩЕЕ описание плитки: сменился профиль выхода — заводим заново.

        ⚠ Одно место на трек, станцию и прямой поток группы: правило в трёх копиях уже разошлось —
        поток группы брал очередь со старым профилем как есть.
        """
        queue = self._players.get(entity)
        if queue is not None and self._profiles.get(entity) != (tile["id"], output_profile(tile["stream"])):
            self._drop_queue(entity, tile)
            queue = None
        if queue is None:
            queue = self._queue_for(tile, entity)
            self._players[entity] = queue
            self._profiles[entity] = (tile["id"], output_profile(tile["stream"]))
        return queue

    def _queue_for(self, tile: dict, entity: str) -> PlayerQueue:
        """Очередь плеера; у `flow`-плеера — с его сессией потока (`providers/music_flow.py`)."""
        profile = tile["stream"]
        # ⚠ Сессию потока заводим ЛЮБОМУ плееру со `stream`, а не только «потоковому»: источник звука —
        # дом, поэтому вести группу может любой плеер («выбрал любой плеер, включил
        # музыку, добавил другие»), а участникам нужно, чтобы ведущий уже играл поток дома.
        session = (
            self._flow.session(tile["id"])
            if profile and (profile.get("flow") is True or entity in self.multiroom.groups)
            else None
        )
        queue = PlayerQueue(
            entity, self._home.source, self._home.env, self._changed, self._links.unwatch,
            flow=session,
        )
        if session is not None:
            self._configure_flow(session, queue, profile)
        return queue

    def _configure_flow(self, session: FlowSession, queue: PlayerQueue, profile: dict) -> None:
        session.source_of = self._flow_source
        session.on_end = queue.flow_end
        session.fade_bytes = fade_window()
        session.delay_of = self._delay
        # Одиночной колонке при ручном запуске и смене — начало пачкой (`FlowSession.prime_solo`).
        session.solo_burst_of = self._solo_burst
        entity = queue.entity
        # Трек зазвучит, когда колонке ушёл его первый кадр, плюс её задержка: группа выровнена по
        # самой поздней системе, поэтому слышно всех через `max(latencyMs)` группы.
        session.on_item_start = lambda item, at: queue.flow_started(item, at, self._latency.audible(entity))

    async def group_flow(self, tile: dict, entity: str) -> FlowSession:
        session = self._flow.session(tile["id"])
        queue = self._players.get(entity)
        if session.current is None and queue is not None and queue.flow is None:
            self._configure_flow(session, queue, tile["stream"])
            await queue.promote(session)
        if session.current is None:
            raise OpError("Сначала включите музыку на этой колонке — тогда к ней добавятся остальные", 409)
        return session

    def _delay(self, entity: str) -> int:
        """Сдвиг плеера, мс: по ТЕКУЩЕМУ составу группы и замерам её систем (`stream.latencyMs`).

        ⚠ Считается на каждый кусок, а не берётся из снимка: состав группы меняется на ходу, а
        задержка — свойство модели колонки, которое менеджер кладёт всем колонкам системы.
        """
        return self._latency.delay(entity, MAX_DELAY_MS)

    def _solo_burst(self, entity: str) -> int:
        """Пачка одиночной колонке, мс; в группе — 0 (сдвиг держит выравнивание, а не пачка)."""
        multiroom = self.multiroom
        owner = multiroom.owner(entity) or entity
        if any(member not in multiroom._vendor for member in multiroom.groups.get(owner, [])):
            return 0
        return self._latency.solo_burst(entity)

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

    async def config_changed(self) -> None:
        # ⚠ Только под `_group_lock`: слушатель зовётся из задачи координатора между
        # await'ами `join`/воспроизведения, и без блокировки он снимал очередь или ведущего
        # группы посреди операции — `KeyError` → 500 жильцу (ревью 2026-11-24).
        async with self._group_lock:
            current = {(self._entity(tile), tile["id"]): tile for tile in self._tiles() if self._entity(tile)}
            self.multiroom.reconcile({entity for entity, _ in current})
            for entity, (tile_id, profile) in list(self._profiles.items()):
                tile = current.get((entity, tile_id))
                if tile is not None and output_profile(tile["stream"]) == profile:
                    continue
                # Адрес потока принадлежит плитке, на которой открыли очередь.
                self._drop_queue(entity, {"id": tile_id})

    async def play_configured(self, entity_ids: list[str], track: str) -> list[str]:
        """Запустить трек из сценария на известных выходах и вернуть остальные цели."""
        known = {entity: tile["id"] for tile in self._tiles() if (entity := self._entity(tile))}
        await asyncio.gather(*(
            self.call(False, {"action": "play_queue", "player": known[entity], "ids": [track], "index": 0})
            for entity in dict.fromkeys(entity_ids) if entity in known
        ))
        return [entity for entity in entity_ids if entity not in known]

    def close(self) -> None:
        self._latency.close()
        for queue in self._players.values():
            queue.close()
        self._players.clear()
        self._profiles.clear()
        self.multiroom.clear()
        self._flow.close()


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
