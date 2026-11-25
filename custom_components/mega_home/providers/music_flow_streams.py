"""Реестр сессий потока дома: токен адреса → сессия (`music_flow.FlowSession`), одна на плеер."""

from __future__ import annotations

import secrets
from collections.abc import Callable

from ..core.const import URL_PREFIX
from .music_flow import FlowSession


class FlowStreams:
    """Адреса сессий: токен → сессия, один на плеер (адрес не меняется между элементами).

    Адрес — сам себе пропуск (случайный токен, `providers/music_proxy.py`): плеер в локальной сети
    учётки не приносит.
    """

    def __init__(self, origin: Callable[[], str | None]) -> None:
        self._origin = origin
        self._by_player: dict[str, FlowSession] = {}
        # Токен адреса → (сессия, плеер); токен один на пару, чтобы правка задержки не меняла адрес.
        self._tokens: dict[tuple[str, str], str] = {}
        self._players: dict[str, tuple[FlowSession, str]] = {}

    def session(self, player: str) -> FlowSession:
        """Сессия плеера; у одного плеера — одна на весь дом (адрес её не меняется)."""
        session = self._by_player.get(player)
        if session is None:
            session = FlowSession(secrets.token_urlsafe(9))
            self._by_player[player] = session
        return session

    def current(self, player: str) -> FlowSession | None:
        """Сессия плеера, только если она уже есть: для lookup'ов, где новую заводить не нужно —
        иначе после каждой паузы/выхода оставался бы пустой зарегистрированный объект."""
        return self._by_player.get(player)

    def address(self, session: FlowSession, player: str, *, aac: bool = False) -> str | None:
        """Абсолютный адрес сессии ДЛЯ ЭТОГО плеера; `None` — дом не знает своего адреса.

        ⚠ Токен — на (сессия, плеер), а не один на сессию: по нему маршрут узнаёт, ЧЬЁ это чтение,
        и применяет его задержку (`FlowSession.serve`, `delay_of`). Группе адрес один по звуку,
        но у каждого участника свой токен.
        """
        base = self._origin()
        if not base:
            return None
        key = (session.token, player)
        suffix = ".aac" if aac else ""
        token = self._tokens.get(key)
        if token is not None and token.endswith(".aac") != aac:
            self.revoke(session, player)
            token = None
        if token is None:
            token = self._tokens[key] = secrets.token_urlsafe(9) + suffix
        self._players[token] = (session, player)
        # Тот же путь, что у короткого адреса трека (`http_music.py`).
        return f"{base.rstrip('/')}{URL_PREFIX}/stream/{token}"

    def revoke(self, session: FlowSession, player: str) -> None:
        if token := self._tokens.pop((session.token, player), None):
            self._players.pop(token, None)
        session.forget(player)

    def retire(self, session: FlowSession) -> None:
        session.retired = True
        for token_session, player in list(self._tokens):
            if token_session == session.token:
                self.revoke(session, player)
        session.stop()

    def find(self, token: str) -> tuple[FlowSession, str] | None:
        """Сессия и плеер по адресу; `None` — не наш токен."""
        return self._players.get(token)

    def drop(self, player: str) -> None:
        session = self._by_player.pop(player, None)
        if session is not None:
            self.retire(session)

    def close(self) -> None:
        for session in list(self._by_player.values()):
            self.retire(session)
        self._by_player.clear()
        self._tokens.clear()
