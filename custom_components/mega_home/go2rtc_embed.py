"""Встроенный go2rtc для mega_home — без аддона и без отдельного докера.

HA-управляемый go2rtc слушает ``webrtc: listen: ":18555/tcp"`` без UDP
(``homeassistant/components/go2rtc/server.py``), поэтому ``srflx`` не
собирает и наружу отдать нечего. Для удалёнки нужен ``:8555`` TCP+UDP и
``stun:8555``. Заводить аддон ради одного порта — лишний компонент на
каждый объект, поэтому интеграция патчит формат HA и поднимает свой
go2rtc тем же бинарём (``shutil.which("go2rtc")``).

Патч действует со следующего старта HA; встроенный процесс даёт эффект
сразу без перезапуска.
"""

from __future__ import annotations

import asyncio
import logging
import shutil
import tempfile
from pathlib import Path

from homeassistant.core import HomeAssistant

from .const import LOGGER

_GO2RTC_YAML = """api:
  listen: "127.0.0.1:1985"
webrtc:
  listen: ":8555"
  candidates:
    - stun:8555
  ice_servers:
    - urls: [stun:stun.l.google.com:19302]
    - urls: [stun:stun.home-assistant.io:3478]
    - urls: [stun:stun.home-assistant.io:80]
rtsp:
  listen: "127.0.0.1:18555"
"""

# Патч HA-шаблона — чтобы после перезапуска HA его managed-сервер тоже
# слушал :8555, а не :18555/tcp. Делаем при импорте, до старта сервера.
try:
    import homeassistant.components.go2rtc.server as _ha_server

    _ha_server._GO2RTC_CONFIG_FORMAT = _ha_server._GO2RTC_CONFIG_FORMAT.replace(
        'listen: ":18555/tcp"', 'listen: ":8555"'
    ).replace(
        "ice_servers: []",
        "ice_servers:\n    - urls: [stun:stun.l.google.com:19302]\n    - urls: [stun:stun.home-assistant.io:3478]",
    )
    # Если вдруг уже есть candidates — не дублируем stun
    if "stun:8555" not in _ha_server._GO2RTC_CONFIG_FORMAT:
        _ha_server._GO2RTC_CONFIG_FORMAT = _ha_server._GO2RTC_CONFIG_FORMAT.replace(
            'listen: ":8555"',
            'listen: ":8555"\n  candidates:\n    - stun:8555',
        )
except Exception:  # noqa: BLE001
    pass

_proc: asyncio.subprocess.Process | None = None
_tmp: str | None = None
_EMBEDDED_URL = "http://127.0.0.1:1985"


async def _patch_ha_provider(hass: HomeAssistant) -> None:
    """Заставить HA-провайдер WebRTC смотреть на наш :1985 вместо :11984."""
    try:
        from homeassistant.components.go2rtc import DOMAIN as GO2RTC_DOMAIN
        from homeassistant.components.go2rtc.const import HA_MANAGED_URL

        # Подменяем URL в уже созданном конфиге провайдера, если он есть
        from homeassistant.components.go2rtc import _DATA_GO2RTC

        if GO2RTC_DOMAIN in hass.data and _DATA_GO2RTC in hass.data:
            cfg = hass.data[_DATA_GO2RTC]
            # cfg is Go2RtcConfig(url, session) — пересоздаём с нашим URL
            from homeassistant.helpers.aiohttp_client import async_get_clientsession

            session = async_get_clientsession(hass)
            # dataclass frozen — создаём новый
            from dataclasses import replace

            try:
                hass.data[_DATA_GO2RTC] = replace(cfg, url=_EMBEDDED_URL)
                LOGGER.info("Patched HA go2rtc provider to %s", _EMBEDDED_URL)
            except Exception:
                pass
    except Exception as err:  # noqa: BLE001
        LOGGER.debug("HA go2rtc patch skipped: %s", err)


async def async_start(hass: HomeAssistant) -> bool:
    """Запустить встроенный go2rtc, если его ещё нет. True — запущен."""
    global _proc, _tmp
    if _proc and _proc.returncode is None:
        return True

    binary = await hass.async_add_executor_job(shutil.which, "go2rtc")
    if not binary:
        LOGGER.warning("go2rtc binary not found — WebRTC удалённо не заработает (нужен go2rtc)")
        return False

    # Конфиг во временном файле — как у HA (server.py _create_temp_file)
    tmp = tempfile.NamedTemporaryFile(prefix="mega_home_go2rtc_", suffix=".yaml", delete=False)
    tmp.write(_GO2RTC_YAML.encode())
    tmp.close()
    _tmp = tmp.name
    LOGGER.info("Starting embedded go2rtc %s with %s", binary, _tmp)

    try:
        _proc = await asyncio.create_subprocess_exec(
            binary, "-c", _tmp,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
    except Exception as err:
        LOGGER.warning("Failed to start embedded go2rtc: %s", err)
        return False

    # Подождать строку "INF [api] listen" как в HA server.py
    try:
        async with asyncio.timeout(10):
            assert _proc.stdout is not None
            async for line in _proc.stdout:
                txt = line.decode(errors="ignore")
                LOGGER.debug("go2rtc: %s", txt.strip())
                if "INF [api] listen" in txt or "INF [webrtc]" in txt:
                    break
                if _proc.returncode is not None:
                    break
    except TimeoutError:
        LOGGER.warning("Embedded go2rtc did not start in time")
        await async_stop()
        return False

    if _proc.returncode is not None:
        LOGGER.warning("Embedded go2rtc exited with %s", _proc.returncode)
        await async_stop()
        return False

    LOGGER.info("Embedded go2rtc started on :8555 (stun:8555)")
    await _patch_ha_provider(hass)
    return True


async def async_stop() -> None:
    """Остановить встроенный go2rtc."""
    global _proc, _tmp
    proc = _proc
    _proc = None
    if proc and proc.returncode is None:
        try:
            proc.terminate()
            try:
                await asyncio.wait_for(proc.wait(), timeout=5)
            except TimeoutError:
                proc.kill()
        except ProcessLookupError:
            pass
    if _tmp:
        try:
            Path(_tmp).unlink(missing_ok=True)
        except Exception:
            pass
        _tmp = None
