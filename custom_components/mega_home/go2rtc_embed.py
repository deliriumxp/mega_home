"""Патч HA-управляемого go2rtc для приёма извне — без аддона.

HA слушает ``webrtc: listen: ":18555/tcp" ice_servers: []``
(``homeassistant/components/go2rtc/server.py``) — без UDP, srflx 0.
Патчим шаблон в памяти до старта сервера на ``:8555`` + stun.
Действует со следующего старта HA (модули уже в sys.modules).
"""

from __future__ import annotations

try:
    import homeassistant.components.go2rtc.server as _ha_server

    _ha_server._GO2RTC_CONFIG_FORMAT = _ha_server._GO2RTC_CONFIG_FORMAT.replace(
        'listen: ":18555/tcp"', 'listen: ":8555"'
    ).replace(
        "ice_servers: []",
        "ice_servers:\n    - urls: [stun:stun.l.google.com:19302]\n    - urls: [stun:stun.home-assistant.io:3478]\n    - urls: [stun:stun.home-assistant.io:80]",
    )
    if "stun:8555" not in _ha_server._GO2RTC_CONFIG_FORMAT:
        _ha_server._GO2RTC_CONFIG_FORMAT = _ha_server._GO2RTC_CONFIG_FORMAT.replace(
            'listen: ":8555"',
            'listen: ":8555"\n  candidates:\n    - stun:8555',
        )
except Exception:  # noqa: BLE001
    pass
