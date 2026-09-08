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
    # Ждём любой признак жизни 3с — go2rtc логирует по-разному в разных версиях,
    # а stdout может быть буферизован. Главное — процесс не умер.
    seen = False
    try:
        async with asyncio.timeout(5):
            assert _proc.stdout is not None
            async for line in _proc.stdout:
                txt = line.decode(errors="ignore").strip()
                if txt:
                    LOGGER.debug("go2rtc: %s", txt)
                    if "INF [api] listen" in txt or "INF [webrtc]" in txt or "listen addr=" in txt:
                        seen = True
                        break
                if _proc.returncode is not None:
                    break
                # Достаточно 1.5с жизни без падения — считаем стартовавшим
                if seen:
                    break
    except TimeoutError:
        pass
    # Если процесс жив через 1с после старта — считаем ok, даже без строки
    await asyncio.sleep(1)
    if _proc.returncode is not None:
        # Собрать хвост логов для диагностики
        out = ""
        try:
            if _proc.stdout:
                out = (await asyncio.wait_for(_proc.stdout.read(), timeout=0.5)).decode(errors="ignore")[:2000]
        except Exception:
            pass
        LOGGER.warning("go2rtc did not start in time (exit %s) out=%s", _proc.returncode, out.strip()[:500])
        await async_stop()
        return False
    LOGGER.info("mega_home go2rtc started on :8555 stun:8555 (%s) seen=%s", URL, seen)
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
