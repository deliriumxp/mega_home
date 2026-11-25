"""Управление выходами смешанного мультирума и их вендорскими связями."""

from __future__ import annotations

import asyncio

from typing import TYPE_CHECKING, Any

from ..core.ops import call
from ..core.ops_base import OpError
from ..core.topics import MEDIA_GROUPS
from .media_queue import pause_native
from .music_flow import FlowSession

if TYPE_CHECKING:
    from .music_playback import MusicPlayback

# MediaPlayerEntityFeature.STOP, docs/docs-ha/core-media-player-const.py.
_STOP_FEATURE = 4096
_PAUSE_FEATURE = 1


class MusicGroups:
    def __init__(self, playback: MusicPlayback) -> None:
        self.playback = playback
        self.groups: dict[str, list[str]] = {}
        self._vendor: dict[str, str] = {}
        self._served: dict[str, str] = {}

    def clear(self) -> None:
        self.groups.clear()
        self._vendor.clear()
        self._served.clear()

    def state_groups(self) -> dict[str, list[str]]:
        groups = {}
        for leader, members in self.groups.items():
            state = self.playback._home.source.get(leader)
            if state is None or state.state in ("unknown", "unavailable"):
                continue
            live = [member for member in members if self._available(member)]
            if live:
                groups[leader] = live
                for entity in (leader, *live):
                    groups[entity] = [leader, *live]
        return groups

    def reconcile(self, alive: set[str]) -> None:
        """Убрать из наших записей выходы, которых больше нет в составе.

        ⚠ Пропавший ведущий убирает ВСЮ связанную запись — и вендорские связи его участников:
        иначе `_vendor`/`_served` ссылались на группу, которой нет, а пустой список участников
        молча блокировал ранний возврат HA для односистемной группы (`existing is None` уже
        ложь) — «сонос+сонос» собирался нашим потоком вместо вендора (ревью 2026-11-24).
        """
        for leader, members in list(self.groups.items()):
            if leader not in alive:
                self.groups.pop(leader, None)
                for member in members:
                    self._vendor.pop(member, None)
                    self._served.pop(member, None)
                continue
            live = [member for member in members if member in alive]
            if live != members:
                if live:
                    self.groups[leader] = live
                else:
                    self.groups.pop(leader, None)
                for member in members:
                    if member not in alive:
                        self._vendor.pop(member, None)
                        self._served.pop(member, None)

    def _available(self, entity: str) -> bool:
        tile = self.playback._tile_of(entity)
        state = self.playback._home.source.get(entity)
        return tile is not None and state is not None and state.state not in ("unknown", "unavailable")

    async def command(self, tile: dict, name: str, data: dict, by: Any = None) -> dict | None:
        """Перестройка смешанной группы выполняется целиком одним вызовом."""
        entity = self.playback._entity(tile)
        if entity is None:
            return None
        if name == "unjoin":
            if self.contains(entity):
                return await self.leave(entity)
            return None
        if name == "turn_off":
            return await self.leave(entity) if self.contains(entity) else None
        if name in ("play", "pause", "play_pause", "stop"):
            owner = self.owner(entity)
            if owner is not None:
                return await self.transport(owner, name, by)
            # Одиночный плеер нашего потока — та же группа из одной колонки. Пауза мимо дома
            # останавливала только плеер, а поток шёл дальше: через секунды читателя снимали по
            # переполнению, дорожка гасла, и «пуск» возобновлять было уже нечего.
            if name != "stop" and self._streaming(tile, entity):
                return await self.transport(entity, name, by)
            return None
        if name != "join":
            return None
        return await self._join_group(tile, entity, data.get("group_members") or [])

    def _streaming(self, tile: dict, entity: str) -> bool:
        """Плеер играет очередь дома нашим потоком (элемент есть, устройство его читает или читало)."""
        session = self.playback._flow.current(tile["id"])
        queue = self.playback._players.get(entity)
        return session is not None and session.current is not None and queue is not None and queue.flow is session

    async def _join_group(self, tile: dict, leader: str, requested: list[str]) -> dict | None:
        owner = self.owner(leader)
        if owner is not None and owner != leader:
            owner_tile = self.playback._tile_of(owner)
            if owner_tile is None:
                raise OpError("Ведущая колонка сейчас недоступна", 409)
            return await self._join_group(owner_tile, owner, requested)
        # Пустая запись (все участники выбыли) — как отсутствующая: иначе односистемная
        # группа после чистки состава шла нашим потоком, а не вендору HA (ревью 2026-11-24).
        existing = self.groups.get(leader) or None
        native = self._native_members(leader) if existing is None else []
        target = list(dict.fromkeys([*(existing or native), *requested]))
        target = [member for member in target if member != leader]
        if existing is None and self._same_system(leader, target):
            return None
        for member in target:
            member_tile = self.playback._tile_of(member)
            if member_tile is None:
                raise OpError("В одну группу — только плееры, которые играют поток дома", 409)
            state = self.playback._home.source.get(member)
            if state is None or state.state in ("unknown", "unavailable"):
                raise OpError("Колонка сейчас недоступна", 409)
            if state.attributes.get("assumed_state") is True:
                raise OpError("Плеер в облачном режиме поток дома не примет", 409)

        session = await self.playback.group_flow(tile, leader)

        if existing is None:
            self.groups[leader] = list(native)
            self._vendor.update({member: leader for member in native})
            for member in native:
                self.playback._drop_queue(member, self.playback._tile_of(member))
        try:
            for member in target:
                if member in self.groups[leader]:
                    if member not in self._vendor and self._served.get(member) == session.token:
                        if session.has_opened(member) and not session.reading_player(member):
                            await self.wake(leader, adding=member)
                    continue
                if self.contains(member):
                    await self.leave(member)
                else:
                    await self._detach_native(member)
                await self.wake(leader, adding=member)
                self.groups[leader].append(member)
        finally:
            # `.get`, а не `[...]`: запись могла снять чистка состава между await'ами.
            confirmed = self.groups.get(leader) or []
            if not confirmed or self._same_system(leader, confirmed):
                self.groups.pop(leader, None)
                for member in confirmed:
                    self._vendor.pop(member, None)
            self.playback._home.topics.bump(MEDIA_GROUPS)
        return {"accepted": True, "group": [leader, *self.groups.get(leader, [])]}

    def _native_members(self, leader: str) -> list[str]:
        state = self.playback._home.source.get(leader)
        members = state.attributes.get("group_members") if state is not None else None
        if not isinstance(members, (list, tuple)) or not members or members[0] != leader:
            return []
        return [member for member in members[1:] if self.playback._tile_of(member) is not None]

    async def _detach_native(self, entity: str) -> None:
        state = self.playback._home.source.get(entity)
        members = state.attributes.get("group_members") if state is not None else None
        if self.playback._system_of(entity) and isinstance(members, (list, tuple)) and len(members) > 1:
            await self._leave_system(entity)

    async def _leave_system(self, entity: str) -> None:
        await call(self.playback._home.source, "media_player", "unjoin", {"entity_id": entity})

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
        systems = {self.playback._system_of(entity) for entity in (leader, *members)}
        return len(systems) == 1 and None not in systems

    def owner(self, entity: str) -> str | None:
        return next((leader for leader, members in self.groups.items() if entity == leader or entity in members), None)

    def contains(self, entity: str) -> bool:
        return self.owner(entity) is not None

    async def leave(self, entity: str) -> dict:
        """Убрать выход после исполнения команды и пересчитать ведущего его системы."""
        leader = self.owner(entity)
        if leader is None:
            return {"accepted": True}
        members = self.groups[leader]
        tile = self.playback._tile_of(leader)
        # Только lookup: выход из группы не заводит пустую сессию (ревью 2026-11-24).
        session = self.playback._flow.current(tile["id"]) if tile is not None else None

        async def detach(member: str) -> None:
            if not await self._vendor_leave(member):
                await self.stop_player(member)
                if session is not None:
                    self.playback._flow.revoke(session, member)
            if member in members:
                members.remove(member)

        try:
            if entity == leader:
                for member in reversed(members.copy()):
                    if member in self._vendor:
                        await detach(member)
                for member in members.copy():
                    await detach(member)
                await detach(leader)
                if tile is not None:
                    self.playback._drop_queue(leader, tile)
            else:
                followers = [member for member in members if self._vendor.get(member) == entity]
                detached: list[str] = []
                try:
                    for member in followers:
                        if await self._vendor_leave(member):
                            detached.append(member)
                    await detach(entity)
                finally:
                    for member in detached:
                        members.remove(member)
                for member in detached:
                    await self.wake(leader, adding=member)
                    members.append(member)
        finally:
            if not members or self._same_system(leader, members):
                self.groups.pop(leader, None)
                for member in members:
                    self._vendor.pop(member, None)
            self.playback._home.topics.bump(MEDIA_GROUPS)
        return {"accepted": True}

    async def transport(self, leader: str, command: str, by: Any = None) -> dict:
        tile = self.playback._tile_of(leader)
        if tile is None:
            raise OpError("Ведущая колонка сейчас недоступна", 409)
        source = self.playback._home.source
        # Только lookup: заводить пустую сессию ради паузы не нужно (ревью 2026-11-24).
        session = self.playback._flow.current(tile["id"])
        if session is None:
            raise OpError("Сначала включите музыку на ведущей колонке", 409)
        queue = self.playback._players.get(leader)
        state = source.get(leader)
        resume = command == "play" or (command == "play_pause" and (
            (queue is not None and queue.group_paused) or state is None or state.state not in ("playing", "buffering")
        ))
        if resume and session.current is None:
            raise OpError("Сначала включите музыку на ведущей колонке", 409)
        if resume:
            session.resume()
        else:
            session.pause()

        delegates = [leader, *(member for member in self.groups.get(leader, []) if member not in self._vendor)]
        if resume and delegates == [leader]:
            # Одиночная колонка набирает буфер пачкой, а не в темпе эфира (`prime_solo`): иначе пуск
            # после паузы у Станции ждал бы её ~10 с буфера.
            session.prime_solo(leader)

        async def apply(entity: str) -> None:
            sign = by if entity == leader else None
            if resume:
                # ⚠⚠ Пуск после паузы — ВСЕГДА заново адрес сессии, а не `media_play`. Станция на
                # `media_play` перезапускает свой адрес с типом, который сообщает сама (`music`), и
                # такой запуск отвергает молча (журнал HA obj36: «Unsupported local media: …/stream/…
                # music»); прежнее правило «читает — значит `media_play`» срабатывало лишь до того,
                # как колонка закрывала соединение на паузе, — отсюда «пуск то работает, то нет».
                service = "play_media"
            else:
                current = source.get(entity)
                mask = current.attributes.get("supported_features") if current is not None else None
                features = int(mask) if type(mask) in (int, float) else 0
                if command == "stop":
                    # ⚠ Явный «стоп» гасит, а не ставит на паузу: маска STOP при бите, иначе
                    # пауза (у HEOS и станции STOP нет — правило `_stop_players`). Раньше здесь
                    # всегда брался PAUSE-first, и «стоп» группы не останавливал плеер с паузой.
                    service = "media_stop" if features & _STOP_FEATURE else "media_pause"
                else:
                    service = "media_pause" if features & _PAUSE_FEATURE or not features & _STOP_FEATURE else "media_stop"
            if service == "play_media":
                await self.playback.play_flow(session, entity)
            else:
                await call(source, "media_player", service, {"entity_id": entity}, by=sign)
            if service == "media_stop":
                session.disconnect(entity)

        results = await asyncio.gather(*(apply(entity) for entity in delegates), return_exceptions=True)
        leader_ok = bool(results) and not isinstance(results[0], BaseException)
        if leader_ok:
            # Состояние очереди фиксируем только по ведущему: участник мог не ответить, но это
            # повод докомандовать ему при следующем нажатии, а не считать группу в другой фазе.
            if queue is not None:
                queue.set_paused(not resume)
        elif resume:
            # Ведущий не возобновился: общий поток снова держим на паузе, следующий play_pause
            # сделает resume, а не лишний проход по уже играющим участникам.
            session.pause()
        for result in results:
            if isinstance(result, BaseException):
                raise result
        return {"accepted": True}

    async def stop_player(self, entity: str) -> None:
        """У части выходов HA есть пауза, но нет команды STOP."""
        source = self.playback._home.source
        state = source.get(entity)
        mask = state.attributes.get("supported_features") if state is not None else None
        features = int(mask) if isinstance(mask, (int, float)) and not isinstance(mask, bool) else 0
        service = "media_stop" if features & _STOP_FEATURE else "media_pause"
        await call(source, "media_player", service, {"entity_id": entity})
        self._served.pop(entity, None)

    async def wake(self, leader: str, adding: str | None = None) -> None:
        """Вычислить ведущих систем по всему составу, включая добавляемый выход."""
        members = list(self.groups.get(leader, []))
        if adding is not None and adding not in members:
            members.append(adding)
        if not members:
            return
        tile = self.playback._tile_of(leader)
        # Только lookup: пересчёт ведущих не заводит пустую сессию (ревью 2026-11-24).
        session = self.playback._flow.current(tile["id"]) if tile is not None else None
        if session is None or session.current is None:
            raise OpError("Сначала включите музыку на этой колонке — тогда к ней добавятся остальные", 409)

        delegates: list[str] = []
        rest: dict[str, list[str]] = {}
        heads: dict[str, str] = {}
        for member in dict.fromkeys([leader, *members]):
            system = self.playback._system_of(member)
            if system is None:
                delegates.append(member)  # своя «система» из одной колонки — вести её некому
            elif system not in heads:
                heads[system] = member
                delegates.append(member)
            else:
                rest.setdefault(system, []).append(member)
        # Ведущий группы уже играет свой поток — ему не переотдаём (это оборвало бы звук).
        await self._give_stream(session, [member for member in delegates if member != leader and (adding is None or member == adding)])
        for system, members_of in rest.items():
            await self._vendor_join(heads[system], [member for member in members_of if adding is None or member == adding])

    async def _give_stream(self, session: FlowSession, members: list[str]) -> None:
        """Отдать НАШ поток этим колонкам — по одной команде `play_media` каждой.

        ⚠⚠ Команда уходит ТОЛЬКО тому, у кого этой сессии ещё нет. Адрес один на сессию, элементы
        сменяются внутри неё — повторная команда на каждой смене трека (и на каждой новой отметке)
        перезапускала плееру поток, колонки стартовали вразнобой и расходились: живой симптом
        «плееры играют разные источники» (2026-11-23).
        """
        for member in members:
            member_tile = self.playback._tile_of(member)
            if member_tile is None:
                continue
            # ⚠⚠ Участник перестаёт играть СВОЁ: его очередь и сессию снимаем. Иначе прежняя
            # очередь продолжала следить за плеером, видела «играет» и перезапускала ЕГО источник —
            # живой симптом «плееры играют разные источники» (и плитка показывала свой трек).
            if self._served.get(member) == session.token and (
                session.reading_player(member) or not session.has_opened(member)
            ):
                continue
            own = self.playback._players.get(member)
            if own is not None and own.plays_native():
                # То же, что у ведущего (`PlayerQueue.promote`): своё воспроизведение колонки — на паузу
                # до нашего потока, иначе она вернётся к нему, когда её снимут с группы.
                await pause_native(self.playback._home.source, member)
            try:
                await self.playback.play_flow(session, member)
            except BaseException:
                self.playback._flow.revoke(session, member)
                raise
            self.playback._drop_queue(member, member_tile)
            self._served[member] = session.token

    async def _vendor_join(self, delegate: str, members: list[str]) -> None:
        """Остальные колонки системы — штатным `join` HA вокруг её ведущего.

        ⚠ Вендор сводит свои колонки точнее нас: звук им даёт не дом, а своя система, поэтому
        достаточно присоединить их к ведущему, который уже играет наш поток. Связь запоминаем:
        снимать такую колонку нужно `unjoin`, а не паузой (пауза из вендорской группы не выводит).

        ⚠ Повторный `join` уже сведённой пары не зовём: служба принимает его молча, но вендор
        пересобирает группу — и звук на секунду рвётся у всех её колонок.
        """
        if self.playback._tile_of(delegate) is None:
            return
        for member in members:
            if self._vendor.get(member) == delegate:
                continue
            await call(
                self.playback._home.source, "media_player", "join",
                {"entity_id": delegate, "group_members": [member]},
            )
            self._vendor[member] = delegate
            if (tile := self.playback._tile_of(member)) is not None:
                self.playback._drop_queue(member, tile)

    async def _vendor_leave(self, member: str) -> bool:
        """Вывести колонку из ВЕНДОРСКОЙ группы; `False` — она в неё не входила у нас."""
        head = self._vendor.get(member)
        if head is None:
            return False
        # `unjoin` — у самой колонки: так она выходит из группы, которую собрал вендор. Пауза
        # оставила бы её участником, и звук вернулся бы на первом же воспроизведении.
        await self._leave_system(member)
        self._vendor.pop(member, None)
        return True
