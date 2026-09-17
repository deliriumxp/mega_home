"""SIP-мост домофонии: свой Asterisk рядом с Home Assistant — схема своего go2rtc.

Зачем. Вызывная панель говорит только SIP (замер стенда 2026-09-17: прямой
IP-вызов панели — обычный `INVITE` на 5060, RTSP монитор открывает лишь ради
показа гостя до ответа), а телефон жильца — WebRTC. Переводит между ними готовый
Asterisk; своего SIP-стека здесь нет и не будет (`docs/intercom-remote.md` в
менеджере, Часть II).

⚠ Этап ПРОВЕРКИ: мост принимает прямой IP-вызов из локальной сети, звонит пять
секунд, отвечает и возвращает звук и видео эхом. Так проверяется ровно то, от
чего зависит вся домофония: ставится ли Asterisk в контейнер Home Assistant на
объекте, переживает ли его перезапуск и слышен ли разговор через него.

⚠ Включается ТОЛЬКО конфигом объекта (`intercom.sipBridge`), а не релизом:
релиз доезжает до всех домов, а мост ставит пакеты в их контейнер.

⚠ Несущие правила — те же, что стоили отказов go2rtc (`go2rtc_embed.py`):
* **порт проверяется ДО старта** — занят, значит не поднимаемся и говорим, чем;
* **сирота усыновляется** — Asterisk прошлого запуска HA держит 5060, и новый
  слушатель уже не встанет; отвечает НАШ сокет управления — берём его себе;
* **stdout вычитывается всегда, процесс останавливается всегда.**

⚠ Всё своё — в `.storage/mega_home_sip/`: `/etc` контейнера не трогаем, и
удаление каталога возвращает дом в прежний вид (пакеты уйдут с первым же
обновлением образа HA).
"""

from __future__ import annotations

import asyncio
from collections import deque
from pathlib import Path
import shutil
import socket
from time import monotonic
from typing import Any

from homeassistant.core import HomeAssistant
from homeassistant.helpers.storage import STORAGE_DIR

from .const import LOGGER

# Прямой IP-вызов панели Akuvox идёт на 5060 по UDP (статья Intercom Call
# Configuration и запись трафика стенда) — порт у панели настраивается, но
# меняется для всех её адресатов разом, поэтому подстраиваемся мы.
SIP_PORT = 5060
# Медиа моста. Узкий диапазон: вызовов в доме единицы, а каждый порт — это
# правило, которое придётся объяснять, если между панелью и хостом есть фильтр.
RTP_START = 20000
RTP_END = 20200
# Пакеты из репозитория Alpine, на котором собран образ Home Assistant.
# ⚠ Контейнер пересоздаётся при КАЖДОМ обновлении HA, и установка пропадает —
# сколько стоит её повтор, пишем в `install_seconds`.
PACKAGES = ("asterisk", "asterisk-srtp")
INSTALL_TIMEOUT = 300.0
# Сколько ждём, пока поднимется SIP-слушатель. Asterisk грузит модули секунды;
# полминуты — запас для слабого железа, а не норма.
READY_TIMEOUT = 30.0
LOG_TAIL = 40
STORE_DIR = "mega_home_sip"
# Откуда принимаем вызов без регистрации. ⚠ Только частные сети: мост на
# 5060 без аутентификации, и вызов «из интернета» означал бы чужое эхо на нашем
# канале. Адреса конкретных панелей приедут конфигом вместе с модулем домофонии.
PRIVATE_NETS = ("10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16")


class SipBridge:
    """Жизненный цикл моста одного Home Assistant."""

    def __init__(self, hass: HomeAssistant) -> None:
        self._hass = hass
        self._root = Path(hass.config.path(STORAGE_DIR, STORE_DIR))
        self._proc: asyncio.subprocess.Process | None = None
        self._drain: asyncio.Task[None] | None = None
        self._lock = asyncio.Lock()
        self._task: asyncio.Task[None] | None = None
        self._ready = False
        self._adopted = False
        self._wanted = False
        self._log: deque[str] = deque(maxlen=LOG_TAIL)
        # Почему моста нет. ⚠ Причины разные и лечатся в разных местах: выключен
        # в менеджере, нечем поставить, не поставился, порт занят, не поднялся.
        self._why = "выключен в конфиге объекта"
        self.install_seconds: float | None = None

    # --- снаружи --------------------------------------------------------

    def apply(self, config: dict[str, Any] | None) -> None:
        """Принять свежий конфиг объекта: включить или снять мост.

        ⚠ Не ждёт и не может ждать: зовётся из цикла синхронизации конфига, а
        установка пакетов идёт минутами — опрос, бандл и старт интеграции
        встали бы на всё это время. Работа уходит в фоновую задачу, и следующий
        конфиг, пришедший посреди установки, её не дублирует.
        """
        block = (config or {}).get("intercom")
        self._wanted = isinstance(block, dict) and block.get("sipBridge") is True
        if self._task is not None and not self._task.done():
            return
        if self._wanted == self.is_running():
            return
        self._task = self._hass.async_create_background_task(
            self._async_reconcile(), "mega_home sip bridge"
        )

    async def _async_reconcile(self) -> None:
        async with self._lock:
            if self._wanted and not self.is_running():
                await self._async_start()
            elif not self._wanted and (self._proc is not None or self._ready):
                await self._async_stop()
                self._why = "выключен в конфиге объекта"

    async def async_stop(self) -> None:
        """Остановка Home Assistant или выгрузка записи."""
        task, self._task = self._task, None
        if task is not None and not task.done():
            task.cancel()
        async with self._lock:
            await self._async_stop()

    def is_running(self) -> bool:
        return self._ready and (self._proc is None or self._proc.returncode is None)

    def state(self) -> dict[str, Any]:
        """Для диагностики интеграции."""
        return {
            "enabled": self._wanted,
            "running": self.is_running(),
            "adopted": self._ready and self._adopted,
            "why": "" if self.is_running() else self._why,
            "sip_port": SIP_PORT,
            "rtp": f"{RTP_START}-{RTP_END}",
            "install_seconds": self.install_seconds,
            "log": list(self._log),
        }

    # --- старт и остановка ---------------------------------------------

    async def _async_start(self) -> None:
        binary = await self._async_binary()
        if not binary:
            return
        await self._hass.async_add_executor_job(write_config, self._root)
        if await self._async_ctl("core show uptime") is not None:
            # Сирота прошлого запуска: конфиг наш, перечитываем его и живём дальше.
            await self._async_ctl("core reload")
            self._ready, self._adopted, self._why = True, True, ""
            LOGGER.info("SIP-мост: усыновлён Asterisk прошлого запуска")
            return
        busy = await self._hass.async_add_executor_job(_port_busy)
        if busy:
            self._why = f"порт {busy} занят — мост не поднимаем"
            LOGGER.warning("SIP-мост: %s", self._why)
            return
        try:
            self._proc = await asyncio.create_subprocess_exec(
                binary,
                "-f",
                # Без цветовых кодов: лог уходит в диагностику текстом.
                "-n",
                "-C",
                str(self._root / "etc" / "asterisk.conf"),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
            )
        except OSError as err:
            self._why = f"Asterisk не запустился: {err}"
            LOGGER.warning("SIP-мост: %s", self._why)
            return
        self._log.clear()
        self._drain = asyncio.ensure_future(self._read_output())
        self._adopted = False
        if await self._async_await_listener():
            self._ready, self._why = True, ""
            LOGGER.info("SIP-мост готов: UDP %s, медиа %s-%s", SIP_PORT, RTP_START, RTP_END)
            return
        self._why = (
            f"SIP-слушатель не поднялся за {READY_TIMEOUT:.0f} с. "
            f"Лог: {' | '.join(self._log) or 'пусто'}"
        )
        LOGGER.warning("SIP-мост: %s", self._why)
        await self._async_stop()

    async def _async_stop(self) -> None:
        ready, adopted = self._ready, self._adopted
        self._ready = self._adopted = False
        proc, self._proc = self._proc, None
        if proc is not None and proc.returncode is None:
            try:
                proc.terminate()
                try:
                    await asyncio.wait_for(proc.wait(), timeout=10)
                except TimeoutError:
                    proc.kill()
                    await proc.wait()
            except ProcessLookupError:
                pass
        elif ready and adopted:
            # Усыновлённый: handle нет, останавливаем через его сокет управления.
            await self._async_ctl("core stop now")
        drain, self._drain = self._drain, None
        if drain is not None:
            drain.cancel()

    async def _async_binary(self) -> str | None:
        """Asterisk в контейнере; нет — ставим пакетами Alpine."""
        which = self._hass.async_add_executor_job
        binary = await which(shutil.which, "asterisk")
        if binary:
            return binary
        apk = await which(shutil.which, "apk")
        if not apk:
            self._why = "в системе нет ни Asterisk, ни apk — поставить нечем"
            LOGGER.warning("SIP-мост: %s", self._why)
            return None
        LOGGER.info("SIP-мост: ставлю %s", " ".join(PACKAGES))
        started = monotonic()
        code, output = await _run([apk, "add", "--no-cache", *PACKAGES], INSTALL_TIMEOUT)
        self.install_seconds = round(monotonic() - started, 1)
        binary = await which(shutil.which, "asterisk")
        if code != 0 or not binary:
            tail = " | ".join(output.strip().splitlines()[-5:]) or "без вывода"
            self._why = f"пакеты не поставились (код {code}, {self.install_seconds} с): {tail}"
            LOGGER.warning("SIP-мост: %s", self._why)
            return None
        LOGGER.info("SIP-мост: пакеты поставлены за %s с", self.install_seconds)
        return binary

    async def _async_await_listener(self) -> bool:
        """«Поднялся» — это SIP-транспорт в ответе Asterisk, а не живой процесс."""
        deadline = monotonic() + READY_TIMEOUT
        while monotonic() < deadline:
            if self._proc is None or self._proc.returncode is not None:
                return False
            answer = await self._async_ctl("pjsip show transports")
            if answer and "transport-udp" in answer:
                return True
            await asyncio.sleep(0.5)
        return False

    async def _async_ctl(self, command: str) -> str | None:
        """Команда НАШЕМУ Asterisk через его сокет; None — никто не ответил."""
        binary = await self._hass.async_add_executor_job(shutil.which, "asterisk")
        if not binary:
            return None
        conf = str(self._root / "etc" / "asterisk.conf")
        code, output = await _run([binary, "-n", "-C", conf, "-rx", command], 10)
        return output if code == 0 else None

    async def _read_output(self) -> None:
        proc = self._proc
        if proc is None or proc.stdout is None:
            return
        try:
            async for line in proc.stdout:
                text = line.decode(errors="ignore").strip()
                if text:
                    self._log.append(text)
                    LOGGER.debug("asterisk: %s", text)
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 - чтение лога не роняет интеграцию
            LOGGER.debug("SIP-мост: чтение вывода остановлено", exc_info=True)


async def _run(argv: list[str], timeout: float) -> tuple[int, str]:
    """Короткая команда: код и вывод. Не дождались — процесс снимается."""
    try:
        proc = await asyncio.create_subprocess_exec(
            *argv, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT
        )
    except OSError as err:
        return -1, str(err)
    try:
        output, _ = await asyncio.wait_for(proc.communicate(), timeout)
    except TimeoutError:
        proc.kill()
        await proc.wait()
        return -1, f"не уложилось в {timeout:.0f} с"
    return proc.returncode or 0, output.decode(errors="ignore")


def _port_busy() -> str:
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as probe:
        # Без SO_REUSEADDR: нужен честный ответ «занят», а не место рядом.
        try:
            probe.bind(("0.0.0.0", SIP_PORT))
        except OSError:
            return f"UDP {SIP_PORT}"
    return ""


def write_config(root: Path) -> None:
    """Разложить конфиг моста по своим каталогам (синхронно, в executor)."""
    for name in ("etc", "lib", "run", "log", "spool", "cache", "agi-bin"):
        (root / name).mkdir(parents=True, exist_ok=True)
    for name, text in render_config(root).items():
        (root / "etc" / name).write_text(text, encoding="utf-8")


def render_config(root: Path) -> dict[str, str]:
    """Файлы конфига Asterisk. Чистая функция — её и проверяют тесты."""
    identify = "\n".join(f"match={net}" for net in PRIVATE_NETS)
    return {
        "asterisk.conf": f"""[directories]
astcachedir => {root}/cache
astetcdir => {root}/etc
astmoddir => /usr/lib/asterisk/modules
astvarlibdir => {root}/lib
astdbdir => {root}/lib
astkeydir => {root}/lib
astdatadir => /usr/share/asterisk
astagidir => {root}/agi-bin
astspooldir => {root}/spool
astrundir => {root}/run
astlogdir => {root}/log
astsbindir => /usr/sbin

[options]
verbose = 3
""",
        # ⚠ autoload, а не белый список: зависимости PJSIP (sorcery, pjproject,
        # сессия, SDP) легко недосчитаться, и мост молча не встанет. Модули без
        # своего конфига сами отказываются грузиться; снимаем только железо и
        # то, что ходит в сеть без спроса.
        "modules.conf": """[modules]
autoload = yes
noload => chan_dahdi.so
noload => chan_mobile.so
noload => chan_unistim.so
noload => chan_iax2.so
noload => chan_console.so
noload => res_corosync.so
noload => res_hep.so
noload => res_hep_pjsip.so
noload => res_hep_rtcp.so
noload => res_xmpp.so
noload => chan_motif.so
""",
        "logger.conf": """[general]
[logfiles]
console => notice,warning,error,verbose
messages => notice,warning,error,verbose
""",
        "rtp.conf": f"""[general]
rtpstart={RTP_START}
rtpend={RTP_END}
""",
        "pjsip.conf": f"""[global]
type=global
user_agent=mega_home-sip-bridge

[transport-udp]
type=transport
protocol=udp
bind=0.0.0.0:{SIP_PORT}

[panel]
type=endpoint
transport=transport-udp
context=from-panel
disallow=all
allow=ulaw,alaw,h264
direct_media=no
rtp_symmetric=yes
dtmf_mode=rfc4733

[panel]
type=identify
endpoint=panel
{identify}
""",
        # Любой номер, которым панель позвала мост: прямой IP-вызов приходит как
        # `sip:<IP>@<IP>`, поэтому образец начинается с цифры или буквы.
        "extensions.conf": """[general]
static=yes
writeprotect=yes

[from-panel]
exten => _[0-9a-zA-Z].,1,NoOp(SIP-мост: вызов от ${CALLERID(all)} на ${EXTEN})
 same => n,Ringing()
 same => n,Wait(5)
 same => n,Answer()
 same => n,Echo()
 same => n,Hangup()
""",
    }
