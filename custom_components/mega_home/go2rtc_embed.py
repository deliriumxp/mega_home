"""Свой go2rtc для WebRTC — одна схема, без патча HA.

HA-управляемый ``:18555/tcp`` без UDP не трогаем. Вместо этого интеграция
поднимает свой go2rtc на ``:8555`` ``candidates: [stun:8555]`` тем же
бинарём ``shutil.which("go2rtc")``. Никаких правок ``core``.
"""

from __future__ import annotations

import asyncio
import shutil
import tempfile
from pathlib import Path

from homeassistant.core import HomeAssistant

from .const import LOGGER

_YAML = """api:
  listen: "127.0.0.1:1985"
webrtc:
  listen: ":8555"
  candidates: [stun:8555]
  ice_servers:
    - urls: [stun:stun.l.google.com:19302]
    - urls: [stun:stun.home-assistant.io:3478]
"""

_proc: asyncio.subprocess.Process | None = None
_tmp: str | None = None
_URL = "http://127.0.0.1:1985"


async def async_start(hass: HomeAssistant) -> bool:
    global _proc, _tmp
    if _proc and _proc.returncode is None:
        return True
    binary = await hass.async_add_executor_job(shutil.which, "go2rtc")
    if not binary:
        LOGGER.warning("go2rtc binary not found — WebRTC удалённо не заработает")
        return False
    tmp = tempfile.NamedTemporaryFile(prefix="mega_home_go2rtc_", suffix=".yaml", delete=False)
    tmp.write(_YAML.encode())
    tmp.close()
    _tmp = tmp.name
    LOGGER.info("Starting mega_home go2rtc %s :8555", binary)
    try:
        _proc = await asyncio.create_subprocess_exec(binary, "-c", _tmp, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT)
    except Exception as err:
        LOGGER.warning("Failed to start go2rtc: %s", err)
        return False
    try:
        async with asyncio.timeout(10):
            assert _proc.stdout is not None
            async for line in _proc.stdout:
                txt = line.decode(errors="ignore")
                if "INF [api] listen" in txt or "INF [webrtc]" in txt:
                    break
                if _proc.returncode is not None:
                    break
    except TimeoutError:
        LOGGER.warning("go2rtc did not start in time")
        await async_stop()
        return False
    if _proc.returncode is not None:
        await async_stop()
        return False
    # Подменить HA-провайдер на наш URL, чтобы offer шёл в :1985
    try:
        from homeassistant.components.go2rtc import _DATA_GO2RTC, DOMAIN as GO2RTC_DOMAIN
        from homeassistant.helpers.aiohttp_client import async_get_clientsession
        from dataclasses import replace

        session = async_get_clientsession(hass)
        if GO2RTC_DOMAIN in hass.data and _DATA_GO2RTC in hass.data:
            cfg = hass.data[_DATA_GO2RTC]
            hass.data[_DATA_GO2RTC] = replace(cfg, url=_URL, session=session)
            LOGGER.info("HA go2rtc provider config patched to %s", _URL)
        else:
            from homeassistant.components.go2rtc import Go2RtcConfig  # type: ignore

            hass.data.setdefault(GO2RTC_DOMAIN, {})
            hass.data[_DATA_GO2RTC] = Go2RtcConfig(url=_URL, session=session)
        # Уже созданные провайдеры хранят url/rest_client в runtime_data — тоже патчим
        for ent in hass.config_entries.async_entries(GO2RTC_DOMAIN):
            prov = getattr(ent, "runtime_data", None)
            if prov and hasattr(prov, "_url"):
                try:
                    prov._url = _URL
                    prov._session = session
                    from go2rtc_client import Go2RtcRestClient

                    prov._rest_client = Go2RtcRestClient(session, _URL)
                    LOGGER.info("HA go2rtc provider instance %s patched to %s", ent.entry_id, _URL)
                except Exception as err2:  # noqa: BLE001
                    LOGGER.debug("provider instance patch skipped: %s", err2)
    except Exception as err:  # noqa: BLE001
        LOGGER.debug("go2rtc provider patch skipped: %s", err)
    LOGGER.info("mega_home go2rtc started on :8555 stun:8555")
    return True


async def async_stop() -> None:
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
