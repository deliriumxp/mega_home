"""Свой go2rtc для WebRTC — одна схема, без патча HA core.

Поднимает ``:8555 candidates: [stun:8555]`` тем же бинарём ``go2rtc``.
HA-управляемый ``:18555/tcp`` не трогаем — наш процесс независим.
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
URL = "http://127.0.0.1:1985"


async def async_start(hass: HomeAssistant) -> bool:
    global _proc, _tmp
    if _proc and _proc.returncode is None:
        return True
    binary = await hass.async_add_executor_job(shutil.which, "go2rtc")
    if not binary:
        LOGGER.debug("go2rtc binary not found — WebRTC через HA, без нас")
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
    LOGGER.info("mega_home go2rtc started on :8555 stun:8555 (%s)", URL)
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


def is_running() -> bool:
    return _proc is not None and _proc.returncode is None
