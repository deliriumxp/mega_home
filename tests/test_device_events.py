"""Концентратор событий устройств (`core/device_events.py`): пределы буфера и журнала."""

from __future__ import annotations

import logging

from mega_home.core import device_events as de


def test_свой_срок_ожидания_подтверждения(monkeypatch) -> None:
    """Уведомление по датчику ждёт менеджера час: протечка при моргнувшем интернете не теряется.

    Звонку домофона старше минуты смысла нет — у остальных событий срок прежний.
    """
    now = [1000.0]
    monkeypatch.setattr(de, "time", lambda: now[0])
    hub = de.EventHub()
    hub.publish("intercom", "s", "ring")
    alert = hub.publish("home", "t", "alert", ttl=3600)
    now[0] += 120
    assert [f["id"] for f in hub.attach(lambda frame: True)] == [alert["id"]]
    assert "ttl" not in alert
    now[0] += 3600
    assert hub.attach(lambda frame: True) == []


def test_переполнение_буфера_предупреждает_один_раз_и_считает(caplog, monkeypatch) -> None:
    monkeypatch.setattr(de, "BUFFER", 3)
    hub = de.EventHub()
    with caplog.at_level(logging.WARNING):
        for _ in range(10):
            hub.publish("a", "s", "e")
    assert hub.dropped == 7
    assert len([r for r in caplog.records if "буфер полон" in r.message]) == 1
    # Буфер освободился — следующее переполнение снова слышно.
    for frame_id in list(hub._unacked):  # noqa: SLF001
        hub.ack(frame_id)
    hub.publish("a", "s", "e")
    for _ in range(4):
        hub.publish("a", "s", "e")
    assert len([r for r in caplog.records if "буфер полон" in r.message]) == 2
