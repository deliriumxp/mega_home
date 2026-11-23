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
from .providers.music_pick import pick_stream
from .providers.music_proxy import MusicStreamLinks
from .providers.player_output import MediaItem, Output, native_output, number, stream_output

if TYPE_CHECKING:
    from .providers.yandex_music import MusicService
    from .station_auth import StationAuth

TOPIC = "media/playback"
FAVORITES_TOPIC = "media/favorites"

# Потолок задержки плеера, мс: как у MA (0–5000), больше — уже не выравнивание, а рассинхрон.
MAX_DELAY_MS = 5000
# Бит STOP в `supported_features` (`core-media-player-const.py`, `MediaPlayerEntityFeature.STOP`).
_STOP_FEATURE = 4096


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
        # Плееры, которых перевели на поток дома, и их последний запрос воспроизведения: ведущим группы
        # может стать любой плеер (`group_command`), а участникам нужен поток дома, а не его нативная дорожка.
        self._forced: set[str] = set()
        self._requests: dict[str, dict] = {}
        # Наши группы мультирума: сущность ведущего → сущности участников (ведущего в списке нет).
        # ⚠ Группа — НЕ группировка HA: `media_player.join` группирует только внутри одной интеграции
        # (`entity-media-player.md`), а Sonos с HEOS и прошитый Echo HA не сгруппирует никогда.
        # У нас звук и так один — поток дома (`providers/music_flow.py`): участнику достаточно дать
        # ТОТ ЖЕ адрес сессии, и он играет вместе, без своей очереди.
        self.groups: dict[str, list[str]] = {}
        # Колонки, присоединённые к ведущему ВЕНДОРОМ (вторая и дальше колонка одной системы):
        # участник → ведущий. Снимаются `unjoin`, а не паузой — иначе остаются в группе вендора.
        self._vendor: dict[str, str] = {}
        # Кому и какую сессию дома уже отдали: участник → токен сессии. Без этого каждая смена
        # элемента повторяла `play_media` ВСЕМ участникам, и колонки уходили вразнобой — живой
        # симптом «плееры играют разные источники».
        self._served: dict[str, str] = {}
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
        if action in ("play_queue", "play_station"):
            entity = self._entity(find(self._tiles(), request.get("player")))
            if entity:
                # Помним, чем плеер играет: ведущему группы повторим ЕГО же запрос, уже через дом.
                self._requests[entity] = dict(request)
            await (self._play(request) if action == "play_queue" else self._play_station(request))
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
        # ⚠ Сессию потока заводим ЛЮБОМУ плееру со `stream`, а не только «потоковому»: источник звука —
        # дом (заказчик 2026-11-22), поэтому вести группу может любой плеер («выбрал любой плеер, включил
        # музыку, добавил другие»), а участникам нужно, чтобы ведущий уже играл поток дома.
        session = (
            self._flow.session(tile["id"])
            if profile and (profile.get("flow") is True or entity in self._forced)
            else None
        )

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
            if session is not None:
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
        # Группа: участникам нужен адрес сессии, пока поток ещё НЕ читают (иначе команда лишняя —
        # элемент сменяется внутри того же потока).
        if self.groups.get(entity) and not self._flow.session(tile["id"]).reading:
            await self._wake_group(entity)

    def state_groups(self) -> dict[str, list[str]]:
        """Составы групп для ответа о приборе: каждая сущность группы → весь состав, ведущий первым.

        ⚠ Приложение читает состав ОДНИМ полем (`group_members`, `core/media-group.ts`), не различая
        штатную группировку HA и нашу: иначе у него был бы второй путь «кто с кем играет».

        ⚠⚠ Колонки, сведённые ВЕНДОРОМ вокруг своего ведущего, тоже в составе: жилец отметил их, и
        галка обязана стоять. Без них приложение показывало группу из одного ведущего, и снять
        отметку с такой колонки было нечем (живой симптом 2026-11-23).
        """
        out: dict[str, list[str]] = {}
        for leader, members in self.groups.items():
            if not members:
                continue
            # Порядок — как жилец отметил: ведущий, затем участники нашей группы, затем вендорские.
            line = [leader, *members]
            for member, head in self._vendor.items():
                if head in line and member not in line:
                    line.append(member)
            if len(line) < 2:
                continue
            for entity in line:
                out[entity] = line
        return out

    async def group_command(self, tile: dict, name: str, data: dict, by: Any) -> dict | None:
        """`join`/`unjoin` для НАШИХ групп (`core/ops.py` зовёт до штатной службы HA).

        ⚠⚠ `None` — команда НЕ наша, и это ГЛАВНЫЙ случай: Sonos, HEOS, WiiM, LinkPlay — одна система
        со своим мультирумом, и группирует её ШТАТНО Home Assistant (`media_player.join`, бит GROUPING).
        Наша группа существует только там, где своего мультирума нет: её ведёт поток дома
        (`stream.flow`, прошитый Echo), и участником становится любой плеер со `stream` — в одиночку.
        Живой разбор 2026-11-23: дом перехватывал `join` у Sonos и HEOS, до HA команда не доходила, и
        «сонос+сонос» не группировался вовсе. Правило: свой мультирум — вендору, наш — где его нет.
        """
        profile = tile.get("stream")
        entity = self._entity(tile)
        if not isinstance(profile, dict) or not entity:
            return None
        if name == "unjoin":
            # Нашу группу снимаем сами, вендорскую — отдаём HA.
            if not self._in_our_group(entity):
                return None
            return await self._leave_group(entity)
        members = [str(member) for member in (data.get("group_members") or []) if str(member) != entity]
        # ⚠⚠ ГЛАВНОЕ РЕШЕНИЕ, и оно про СОСТАВ, а не про марку ведущего. Заказчик (с самого начала,
        # повторено 2026-11-23): «колонки одного вендора группируются средствами вендора, колонки
        # разных вендоров — нашим мини-MA с выравниванием задержек».
        #
        # Одна система → штатный `media_player.join` HA, вендор сводит свои колонки сам. Разные
        # системы → группу ведёт дом: HA вне одной интеграции не группирует вовсе
        # (`entity-media-player.md`), и смешанный `join` он отклонил бы — жилец видел бы «Home
        # Assistant отклонил команду». У станции своего мультирума нет — она всегда наша.
        if self._same_system(entity, members):
            return None
        if profile.get("flow") is not True and not self._flow.session(tile["id"]).reading:
            # ⚠ Ведущим нашей группы может быть ЛЮБОЙ плеер со `stream`: источник звука — дом
            # (заказчик 2026-11-22). Плеер, игравший сам (нативная дорожка станции, своя ссылка),
            # переводим на поток дома и повторяем ЕГО ЖЕ запрос — иначе участникам нечего слушать.
            #
            # ⚠⚠ Очередь СНИМАЕМ: она могла быть заведена до этого решения и тогда ведёт плеер БЕЗ
            # сессии потока (`_queue_for` смотрит на `_forced` на момент создания). Повторный запуск
            # с прежней очередью сессию не завёл бы, и участникам достался бы пустой поток — ровно
            # то, отчего «отметка колонок ничего не включает».
            self._forced.add(entity)
            self._drop_queue(entity, tile)
            request = self._requests.get(entity)
            if request is not None:
                await self.call(True, request)
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
        # ⚠⚠ `join` ДОБАВЛЯЕТ к группе, как одноимённая служба HA, а не заменяет состав: жилец
        # отмечает колонки по одной, и каждая следующая отметка иначе стирала бы предыдущую
        # (живой симптом 2026-11-23: «отметка колонок ничего не включает» при второй галке).
        known = self.groups.get(entity, [])
        fresh = [member for member in dict.fromkeys(members) if member not in known]
        self.groups[entity] = [*known, *fresh]
        self._changed()
        # Будим ТОЛЬКО новых участников: у уже играющих адрес тот же, и лишняя команда оборвала бы
        # им поток на середине.
        await self._wake_group(entity, only=fresh)
        return {"accepted": True, "group": [entity, *self.groups[entity]]}

    def _same_system(self, leader: str, members: list[str]) -> bool:
        """Все участники — ОДНА система вендора? Тогда `join` исполняет штатная служба HA.

        ⚠ Правило заказчика (с самого начала, повторено 2026-11-23): «колонки одного вендора
        группируются средствами вендора, колонки разных вендоров — нашим мини-MA с выравниванием
        задержек». Решает СОСТАВ, а не марка ведущего: Sonos+Sonos и HEOS+HEOS сводит вендор,
        Sonos+HEOS — только мы.

        ⚠ Плеер без системы (`sync` в описании потока нет — Яндекс Станция, прошитый Echo) своей
        системой не считается НИКОГДА: у станции своего мультирума нет («Яндекс не умеет в
        мультирум, поэтому там только наша реализация»), а Echo — наш поток. Поэтому группа с ним
        всегда наша, даже если он один: пустая система ≠ система.
        """
        systems = {self._system_of(entity) for entity in (leader, *members)}
        return len(systems) == 1 and None not in systems

    def _drop_queue(self, entity: str, tile: dict) -> None:
        """Снять очередь и сессию плеера: следующий запуск заведёт их по НОВЫМ правилам.

        ⚠ Нужно именно при переводе на поток дома: `_queue_for` решает про сессию по `_forced` на
        момент создания, и очередь, заведённая раньше, осталась бы без неё навсегда.
        """
        if (queue := self._players.pop(entity, None)) is not None:
            queue.close()
        self._profiles.pop(entity, None)
        self._flow.drop(tile["id"])

    def _system_of(self, entity: str) -> str | None:
        """Ключ системы вендора по описанию плитки; `None` — своего мультирума у плеера нет."""
        tile = self._tile_of(entity)
        system = ((tile or {}).get("stream") or {}).get("sync")
        return system if isinstance(system, str) and system else None

    def _in_our_group(self, entity: str) -> bool:
        """Колонка в группе, которую ВЕДЁТ ДОМ: ведущая, наш участник или вендорский.

        ⚠ Вендорский участник тоже наш: группу собрал дом (ведущему дали поток, остальные колонки
        его системы свели штатным `join`). Без него `unjoin` такой колонки уходил в HA и звук на ней
        не гас — живой симптом 2026-11-23 «не отключают звук при снятии отметки».
        """
        return entity in self.groups or entity in self._vendor or any(
            entity in members for members in self.groups.values()
        )

    async def _leave_group(self, entity: str) -> dict:
        """Выйти из группы: ушедший гаснет, ведущий и остальные участники играют дальше."""
        for leader, members in list(self.groups.items()):
            if entity == leader:
                self.groups.pop(leader, None)
                await self._stop_players(members)
                # Колонки, сведённые вендором вокруг этого ведущего, гасим вместе с ним: он больше
                # не играет, а вендорская группа без звука живёт своей жизнью (свои списки треков).
                for member in [m for m, head in self._vendor.items() if head == leader]:
                    self._vendor.pop(member, None)
                    self._served.pop(member, None)
                    await self._stop_players([member])
            elif entity in members:
                left = [member for member in members if member != entity]
                if left:
                    self.groups[leader] = left
                else:
                    self.groups.pop(leader, None)
                if not await self._vendor_leave(entity):
                    await self._stop_players([entity])
            else:
                continue
            self._changed()
        return {"accepted": True}

    async def _stop_players(self, entities: list[str]) -> None:
        """Погасить плееры — ТОЙ службой, которую прибор объявил в своей маске.

        ⚠ `media_player.media_stop` HA принимает только у плеера с битом STOP
        (`core-media-player-services.yaml`: `supported_features: …STOP`). Требуй мы его у всех,
        плеер без STOP отвечал бы отказом службы — жилец видел бы «Home Assistant отклонил команду»
        на снятии галки мультирума. У HEOS и станции STOP нет, пауза есть: гасим паузой.
        """
        for entity in entities:
            # Плеер больше не играет наш поток: следующая отметка обязана отдать его заново.
            self._served.pop(entity, None)
            state = self._home.source.get(entity)
            mask = getattr(state, "attributes", {}).get("supported_features") if state else None
            features = int(mask) if isinstance(mask, (int, float)) and not isinstance(mask, bool) else 0
            service = "media_stop" if features & _STOP_FEATURE else "media_pause"
            await call(self._home.source, "media_player", service, {"entity_id": entity})

    async def _wake_group(self, leader: str, only: list[str] | None = None) -> None:
        """Участникам — ТОТ ЖЕ адрес сессии ведущего: их плеер забирает поток сам, без очереди.

        `only` — разбудить ТОЛЬКО этих участников (новых в группе): у уже играющих адрес тот же, и
        повторная команда оборвала бы им звук.

        ⚠ Пустой поток не отдаём НИКОГДА. Если у ведущего очередь не заведена (музыку включили не из
        приложения — с пульта, голосом, у вендора), адрес его сессии существует, но за ним нет ни
        одного элемента: участник получал бы вечную тишину без единого отказа — живой симптом
        «отметка колонок ничего не включает». Такому ведущему — `unjoin`'ся и честный отказ, чтобы
        приложение сказало, что включить музыку надо на ведущей колонке.

        ⚠ Команду зовём только когда поток ещё НЕ читают (`_play`/`_play_station`): у устройства,
        которое новый `media_url` поверх живого потока не берёт, повторная команда была бы
        бесполезна, а на смену элемента внутри потока она и не нужна — адрес тот же.

        Формат провода — MP3 всегда (`providers/music_flow.py`): участник объявляет свои типы, и
        `stream.mp3` в его списке обязан быть — иначе честный отказ, а не тишина.
        """
        members = self.groups.get(leader) or []
        if only is not None:
            members = [member for member in members if member in only]
        if not members:
            return
        tile = self._tile_of(leader)
        session = self._flow.session(tile["id"]) if tile is not None else None
        if session is None or session.current is None:
            # Группа без звука — не группа: снимаем её, а жильцу говорим, что включить и где.
            self.groups.pop(leader, None)
            self._changed()
            raise OpError(
                "Сначала включите музыку на этой колонке — тогда к ней добавятся остальные", 409
            )
        # ⚠⚠ РАЗНЫЕ СИСТЕМЫ СВОДИМ ЧЕРЕЗ ИХ ВЕДУЩИХ (заказчик 2026-11-23): «ведущий плеер одной
        # системы через мини-MA сгруппировать с ведущим другой системы (ведущим — первый выбранный),
        # и раздать на остальные колонки системы, если пользователь их отметил».
        #
        # Значит наш поток получает ТОЛЬКО первый отмеченный колонки каждой системы, а остальные её
        # колонки вендор сводит вокруг него сам (штатный `join` HA). Иначе мы забирали бы у вендора
        # его собственную работу и его синхронность — то, из-за чего Sonos+Sonos перестал звучать.
        #
        # Плеер без системы (Яндекс Станция, прошитый Echo) — система из одной колонки: свой мультирум
        # вендор ему не дал, поэтому наш поток получает каждая такая колонка (`_system_of` → None).
        delegates: list[str] = []
        rest: dict[str, list[str]] = {}
        heads: dict[str, str] = {}
        for member in dict.fromkeys([leader, *members]):
            system = self._system_of(member)
            if system is None:
                delegates.append(member)  # своя «система» из одной колонки — вести её некому
            elif system not in heads:
                heads[system] = member
                delegates.append(member)
            else:
                rest.setdefault(system, []).append(member)
        # Ведущий группы уже играет свой поток — ему не переотдаём (это оборвало бы звук).
        await self._give_stream(session, [member for member in delegates if member != leader])
        for system, members_of in rest.items():
            await self._vendor_join(heads[system], members_of)

    async def _give_stream(self, session: Any, members: list[str]) -> None:
        """Отдать НАШ поток этим колонкам — по одной команде `play_media` каждой.

        ⚠⚠ Команда уходит ТОЛЬКО тому, у кого этой сессии ещё нет. Адрес один на сессию, элементы
        сменяются внутри неё — повторная команда на каждой смене трека (и на каждой новой отметке)
        перезапускала плееру поток, колонки стартовали вразнобой и расходились: живой симптом
        «плееры играют разные источники» (2026-11-23).
        """
        for member in members:
            member_tile = self._tile_of(member)
            if member_tile is None:
                continue
            # ⚠⚠ Участник перестаёт играть СВОЁ: его очередь и сессию снимаем. Иначе прежняя
            # очередь продолжала следить за плеером, видела «играет» и перезапускала ЕГО источник —
            # живой симптом «плееры играют разные источники» (и плитка показывала свой трек).
            self._drop_queue(member, member_tile)
            if self._served.get(member) == session.token and session.reading:
                continue  # поток этого плеера ЖИВ — повторная команда оборвала бы ему звук
            # Токен — СВОЙ у участника: по нему дом знает, чью задержку применить к чтению.
            address = self._flow.address(session, member)
            if address is None:
                raise OpError("Дом не знает своего адреса для потока", 503)
            member_profile = member_tile.get("stream") or {}
            media_type = flow_content_type(member_profile)
            options = _member_accepts(member_tile)
            if options is not None and media_type not in options:
                raise OpError("Плеер не примет поток дома в этом формате", 409)
            fields = _stream_fields(
                member_tile, member,
                # ⚠⚠ Адрес приводится к схеме ЭТОГО плеера, как в `flow_output`: Sonos принимает
                # только `x-rincon-mp3radio://…`, и на голом `https` отклоняет `play_media` — жилец
                # видел «Home Assistant отклонил команду», а колонка молчала (живой разбор 2026-11-23).
                {"media_content_id": flow_address(member_profile, address), "media_content_type": media_type},
                self._home.source,
            )
            await call(self._home.source, "media_player", "play_media", {"entity_id": member, **fields})
            self._served[member] = session.token

    async def _vendor_join(self, delegate: str, members: list[str]) -> None:
        """Остальные колонки системы — штатным `join` HA вокруг её ведущего.

        ⚠ Вендор сводит свои колонки точнее нас: звук им даёт не дом, а своя система, поэтому
        достаточно присоединить их к ведущему, который уже играет наш поток. Связь запоминаем:
        снимать такую колонку нужно `unjoin`, а не паузой (пауза из вендорской группы не выводит).

        ⚠ Повторный `join` уже сведённой пары не зовём: служба принимает его молча, но вендор
        пересобирает группу — и звук на секунду рвётся у всех её колонок.
        """
        if self._tile_of(delegate) is None:
            return
        for member in members:
            if self._vendor.get(member) == delegate:
                continue
            await call(
                self._home.source, "media_player", "join",
                {"entity_id": delegate, "group_members": [member]},
            )
            self._vendor[member] = delegate

    async def _vendor_leave(self, member: str) -> bool:
        """Вывести колонку из ВЕНДОРСКОЙ группы; `False` — она в неё не входила у нас."""
        head = self._vendor.pop(member, None)
        if head is None:
            return False
        self._served.pop(member, None)
        # `unjoin` — у самой колонки: так она выходит из группы, которую собрал вендор. Пауза
        # оставила бы её участником, и звук вернулся бы на первом же воспроизведении.
        await call(self._home.source, "media_player", "unjoin", {"entity_id": member})
        return True

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
        # ⚠ Сессию потока заводим ЛЮБОМУ плееру со `stream`, а не только «потоковому»: источник звука —
        # дом (заказчик 2026-11-22), поэтому вести группу может любой плеер («выбрал любой плеер, включил
        # музыку, добавил другие»), а участникам нужно, чтобы ведущий уже играл поток дома.
        session = (
            self._flow.session(tile["id"])
            if profile and (profile.get("flow") is True or entity in self._forced)
            else None
        )

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
        if self.groups.get(entity) and not self._flow.session(tile["id"]).reading:
            await self._wake_group(entity)

    def _queue_for(self, tile: dict, entity: str) -> PlayerQueue:
        """Очередь плеера; у `flow`-плеера — с его сессией потока (`providers/music_flow.py`)."""
        profile = tile["stream"]
        # ⚠ Сессию потока заводим ЛЮБОМУ плееру со `stream`, а не только «потоковому»: источник звука —
        # дом (заказчик 2026-11-22), поэтому вести группу может любой плеер («выбрал любой плеер, включил
        # музыку, добавил другие»), а участникам нужно, чтобы ведущий уже играл поток дома.
        session = (
            self._flow.session(tile["id"])
            if profile and (profile.get("flow") is True or entity in self._forced)
            else None
        )
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
        self._vendor.clear()
        self._flow.close()


def flow_address(profile: Mapping[str, Any], address: str) -> str:
    """Адрес сессии В ВИДЕ, который примет ЭТОТ плеер: схема из описания плитки.

    ⚠ Без схемы Sonos отклоняет адрес — HA отдаёт отказ службы, и жилец видит «Home Assistant
    отклонил команду» (живой разбор 2026-11-23: «сонос+сонос не работает»). Sonos нужен
    `x-rincon-mp3radio://`, и `https` остаётся ВНУТРИ схемы: без неё он идёт по http, а Яндекс-CDN
    отдаёт только https. То же правило в приложении — `streamAddress` (`core/radio-play.ts`).
    """
    scheme = profile.get("scheme")
    if not isinstance(scheme, str) or not scheme:
        return address
    if address.startswith("https://"):
        return f"{scheme}://{address}"
    return address.replace("http://", f"{scheme}://", 1)


def flow_output(
    flow: FlowStreams, session: FlowSession, profile: Mapping[str, Any], opened: Callable[[], None],
    player: str,
) -> Output:
    """Выход потока-сессии: адрес один на сессию, содержимое меняет дом (`providers/music_flow.py`).

    ⚠ Звук отдаёт дом, но адрес всё равно приводится к схеме ЭТОГО плеера (`flow_address`): у Sonos
    он без схемы не принимается. Подтверждение запуска — начало чтения потока (`on_read` →
    `Attempt.opened`): у плеера, который о себе не рассказывает, другого свидетельства не бывает.
    """
    address = flow.address(session, player)
    if address is None:
        raise OpError("Дом не знает своего адреса для потока", 503)
    session.on_read = opened
    return Output(
        flow_address(profile, address), flow_content_type(profile),
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
