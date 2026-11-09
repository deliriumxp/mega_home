"""Пауза перед повтором канала: плановый перезапуск менеджера не загоняет дом в долгую паузу."""

from __future__ import annotations

from mega_home.core.relink import RESTART_WINDOW, Relink


class _Clock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


def test_обычный_обрыв_растущая_пауза_до_минуты():
    relink = Relink(clock=_Clock(), rand=lambda: 0.0)
    relink.closed(None)
    assert [relink.next() for _ in range(6)] == [5, 10, 20, 40, 60, 60]


def test_перезапуск_менеджера_частый_повтор_со_сдвигом():
    relink = Relink(clock=_Clock(), rand=lambda: 1.0)
    relink.closed(1012)
    delays = [relink.next() for _ in range(30)]
    # Полминуты простоя — дом стучится не реже раза в 3,5 с, а не ждёт 40 с.
    assert all(2.0 <= d <= 3.5 for d in delays)


def test_после_окна_перезапуска_обычная_пауза():
    clock = _Clock()
    relink = Relink(clock=clock, rand=lambda: 0.0)
    relink.closed(1012)
    clock.now = RESTART_WINDOW + 1
    assert [relink.next() for _ in range(3)] == [5, 10, 20]


def test_подключение_сбрасывает_и_паузу_и_окно():
    relink = Relink(clock=_Clock(), rand=lambda: 0.0)
    relink.closed(1012)
    relink.connected()
    relink.closed(1006)
    assert relink.next() == 5
