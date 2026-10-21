"""Темы дома: «факт X изменился» — сигналом всем открытым потокам приложения.

Зачем (заказчик 2026-10-21: «более универсально, на будущее»). Дом хранит то, что пишут жильцы и
инсталлятор: общие документы, правила уведомлений, фоны, участки кадра, бандл интерфейса. Записал
один телефон — остальные узнавали только перечитав сами: опросом под каждую функцию или никогда до
перезагрузки. Тема — общий канал для всего такого: путь записи поднимает её номер, поток
(`events.py`, снаружи — `watch.py` через менеджер) несёт кадр `changed` `{topic, rev}`, а снимок
`states` — карту номеров `revs`.

- Сигнал, а не данные: данные читаются тем же запросом, что и всегда, — второй путь доставки
  разошёлся бы с первым.
- Номер живёт в памяти; приложение сравнивает на НЕРАВЕНСТВО, а не «больше», поэтому пропавшая из
  карты тема — тоже изменение. ⚠ Отсчёт — со СЛУЧАЙНОГО смещения на каждый запуск (и перезагрузку
  записи): с единицы тема, записанная после перезапуска, дошла бы до того же номера, что телефон
  помнил до него, и смена потерялась бы молча (ревью 2026-10-21).
- Карта в `states` — ради обрыва: телефон, проснувшийся или переподключившийся, сверяет номера и
  перечитывает пропущенное; без неё сигнал, пришедший во время обрыва, терялся бы молча.
- Смысла темы дом не знает: имя — строка того, кто пишет, как имя документа в `shared.py`.

⚠ Модуль в ядре без HA (`tests/test_core_without_ha.py`).
"""

from __future__ import annotations

from collections.abc import Callable
from secrets import randbelow

Listener = Callable[[str, int], None]

# Темы путей записи дома. ⚠ Имена — контракт с бандлом (`home-changes.ts` менеджера): только
# добавлять.
SHARED = "shared/"  # + имя документа (`shared.py`)
ALERTS = "alerts"
PHOTOS = "photos"
CROPS = "crops"
APP = "app"


class Topics:
    """Номера тем и подписчики на их смену."""

    def __init__(self) -> None:
        self._revs: dict[str, int] = {}
        self._listeners: list[Listener] = []
        self._start = randbelow(2**31)

    def bump(self, topic: str) -> int:
        rev = self._revs.get(topic, self._start) + 1
        self._revs[topic] = rev
        for listener in list(self._listeners):
            listener(topic, rev)
        return rev

    def revs(self) -> dict[str, int]:
        return dict(self._revs)

    def subscribe(self, listener: Listener) -> Callable[[], None]:
        self._listeners.append(listener)

        def unsubscribe() -> None:
            if listener in self._listeners:
                self._listeners.remove(listener)

        return unsubscribe
