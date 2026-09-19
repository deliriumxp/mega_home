"""SIP-мост домофонии: свой Asterisk рядом с Home Assistant — схема своего go2rtc.

Зачем. Вызывная панель говорит только SIP (замер стенда 2026-09-17: прямой
IP-вызов панели — обычный `INVITE` на 5060, RTSP монитор открывает лишь ради
показа гостя до ответа), а телефон жильца — WebRTC. Переводит между ними готовый
Asterisk; своего SIP-стека здесь нет и не будет (`docs/intercom-remote.md` в
менеджере, Часть II).

Мост принимает прямой IP-вызов панели и держит его звонящим, пока телефон по
WebSocket не наберёт `answer` (`sip_calls.py`); `echo` — звук телефон ⇄ мост
без панели.

⚠ Диалплан — файлами МЕНЕДЖЕРА (`docs/plan-thin-gateway.md`, пункт 4):
`pjsip.conf`, `extensions.conf`, `ari.conf` приходят общим каналом файлов
(`assets.py`) под ключами `asterisk/*.conf`, дом их только кладёт на диск и
перезапускает Asterisk. Пароль ARI — ЕДИНСТВЕННАЯ подстановка, которую делает
дом: менеджер шлёт `ari.conf` с плейсхолдером `{ari_password}`, дом генерирует
пароль сам (как раньше) и подставляет его при записи файла. Инфраструктурные
файлы (`asterisk.conf`, `modules.conf`, `logger.conf`, `http.conf`, `rtp.conf`)
остаются данными процесса, а не диалплана, и пишутся домом как прежде.

⚠ Вход только двумя дверями: панели — с адресов из конфига объекта
(`intercom.panels`), телефон — с loopback, куда его пускает лишь канал
менеджера (`stream.py`, служба «asterisk» реестра `services.py`). Учётки у
телефона нет, права жильца проверяет менеджер.

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
import json
import secrets
import shutil
import socket
from pathlib import Path
from time import monotonic
from typing import Any

from . import services
from .assets import AssetStore
from .const import LOGGER
from .host import Host
from .sip_calls import DoorCalls

# Прямой IP-вызов панели идёт на 5060 по UDP; порт у панели настраивается, но
# меняется для всех её адресатов разом, поэтому подстраиваемся мы.
SIP_PORT = 5060
# HTTP-сервер Asterisk: на нём SIP поверх WebSocket (путь `/ws`) для телефона
# жильца и ARI для самой интеграции — один порт на оба, так устроен Asterisk.
HTTP_PORT = 8188
# Медиа моста. Узкий диапазон: вызовов в доме единицы.
RTP_START = 20000
RTP_END = 20200
# Тот же STUN, что Home Assistant по умолчанию отдаёт своему WebRTC.
STUN = "stun.home-assistant.io:3478"
LOOPBACK = "127.0.0.1"

DIALPLAN_KEYS = ("asterisk/pjsip.conf", "asterisk/extensions.conf", "asterisk/ari.conf")
_DIALPLAN_FILES = {
    "asterisk/pjsip.conf": "pjsip.conf",
    "asterisk/extensions.conf": "extensions.conf",
    "asterisk/ari.conf": "ari.conf",
}

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

def _dialplan_versions(config: dict[str, Any] | None) -> dict[str, str]:
    """Версии файлов диалплана из манифеста `assets` — те же поля, что у `assets.py`."""
    raw = (config or {}).get("assets") or {}
    out: dict[str, str] = {}
    for key in DIALPLAN_KEYS:
        entry = raw.get(key)
        if isinstance(entry, dict) and isinstance(entry.get("v"), str) and entry["v"]:
            out[key] = entry["v"]
    return out

def load_secrets(root: Path) -> dict[str, str]:
    """Пароль ARI: придуман один раз и живёт в каталоге моста.

    ⚠ Пароль не меняется от перезапуска к перезапуску: сирота прошлого запуска
    усыновляется с тем конфигом, что уже прочитал, и новый пароль ARI его бы
    от нас запер.
    """
    path = root / "secrets.json"
    try:
        stored = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        stored = {}
    changed = False
    if not isinstance(stored.get("ari"), str) or len(stored["ari"]) < 16:
        stored["ari"] = secrets.token_urlsafe(18)
        changed = True
    if changed:
        root.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(stored), encoding="utf-8")
        path.chmod(0o600)
    return stored

def _infra_config(root: Path) -> dict[str, str]:
    """Файлы процесса Asterisk, не диалплана — их пишет дом, как раньше."""
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
        # ⚠ Только loopback: WebSocket телефона приходит каналом менеджера
        # (`stream.py` пускает loopback по имени службы «asterisk»), учётки у
        # телефона нет. TLS не нужен: снаружи шифрует WSS менеджера, внутри —
        # loopback.
        "http.conf": f"""[general]
servername=mega_home
enabled=yes
bindaddr={LOOPBACK}
bindport={HTTP_PORT}
""",
        "rtp.conf": f"""[general]
rtpstart={RTP_START}
rtpend={RTP_END}
icesupport=yes
stunaddr={STUN}
""",
    }

def _write_dialplan(root: Path, files: dict[str, bytes], ari_password: str) -> None:
    """Диалплан МЕНЕДЖЕРА как есть — кроме одной подстановки в `ari.conf`."""
    for key, raw in files.items():
        name = _DIALPLAN_FILES[key]
        text = raw.decode("utf-8")
        if name == "ari.conf":
            # ⚠ Единственная подстановка дома: пароль ARI дом генерирует и
            # хранит сам (`load_secrets`), менеджер его не видит.
            text = text.replace("{ari_password}", ari_password)
        (root / "etc" / name).write_text(text, encoding="utf-8")

def _write_all(root: Path, keys: dict[str, str], dialplan: dict[str, bytes]) -> None:
    for name in ("etc", "lib", "run", "log", "spool", "cache", "agi-bin"):
        (root / name).mkdir(parents=True, exist_ok=True)
    for name, text in _infra_config(root).items():
        (root / "etc" / name).write_text(text, encoding="utf-8")
    _write_dialplan(root, dialplan, keys["ari"])

class SipBridge:
    """Жизненный цикл моста одного Home Assistant."""

    def __init__(self, env: Host, assets: AssetStore, on_event: Any = None) -> None:
        self._env = env
        self._assets = assets
        # Куда уходят события вызова (`device_events.py`): вызов, отмена, ответ,
        # конец. Это события SIP самой панели, а не вендора.
        self._on_event = on_event
        self._root = env.path(STORE_DIR)
        self._proc: asyncio.subprocess.Process | None = None
        self._drain: asyncio.Task[None] | None = None
        self._lock = asyncio.Lock()
        self._task: asyncio.Task[None] | None = None
        self._ready = False
        self._adopted = False
        self._wanted = False
        # Версии диалплана из конфига объекта и те, с которыми он записан.
        self._dialplan_versions: dict[str, str] = {}
        self._written: dict[str, str] | None = None
        self._log: deque[str] = deque(maxlen=LOG_TAIL)
        # Почему моста нет. ⚠ Причины разные и лечатся в разных местах: выключен
        # в менеджере, нечем поставить, не поставился, порт занят, не поднялся,
        # диалплан ещё не пришёл файлами.
        self._why = "выключен в конфиге объекта"
        self._keys: dict[str, str] = {}
        self.calls: DoorCalls | None = None
        self.install_seconds: float | None = None

    # --- снаружи --------------------------------------------------------

    def apply(self, config: dict[str, Any] | None) -> None:
        """Принять свежий конфиг объекта: включить или снять мост.

        ⚠ Не ждёт и не может ждать: зовётся из цикла синхронизации конфига, а
        установка пакетов и чтение файлов идут секундами-минутами — опрос,
        бандл и старт интеграции встали бы на всё это время. Работа уходит в
        фоновую задачу, и следующий конфиг, пришедший посреди установки, её не
        дублирует.
        """
        block = (config or {}).get("intercom")
        self._wanted = isinstance(block, dict) and block.get("sipBridge") is True
        self._dialplan_versions = _dialplan_versions(config)
        if self._task is not None and not self._task.done():
            # Задача дочитает свежие `_wanted`/`_dialplan_versions` сама: они
            # уже записаны.
            return
        running = self.is_running()
        if self._wanted == running and not (running and self._dialplan_versions != self._written):
            return
        self._task = self._env.spawn(self._async_reconcile(), "mega_home sip bridge")

    async def _async_reconcile(self) -> None:
        async with self._lock:
            if self._wanted and not self.is_running():
                await self._async_start()
            elif self._wanted and self._dialplan_versions != self._written:
                if await self._async_write():
                    await self._async_ctl("core reload")
                    LOGGER.info("SIP-мост: диалплан обновлён менеджером")
            elif not self._wanted and (self._proc is not None or self._ready):
                await self._async_stop()
                self._why = "выключен в конфиге объекта"

    async def _async_write(self) -> bool:
        """Разложить конфиг по каталогам. `False` — диалплан ещё не приехал файлами."""
        dialplan = await self._env.run(self._read_dialplan)
        if dialplan is None:
            self._why = "диалплан Asterisk ещё не пришёл файлами от менеджера"
            return False
        self._keys = await self._env.run(load_secrets, self._root)
        await self._env.run(_write_all, self._root, self._keys, dialplan)
        self._written = dict(self._dialplan_versions)
        return True

    def _read_dialplan(self) -> dict[str, bytes] | None:
        out: dict[str, bytes] = {}
        for key in DIALPLAN_KEYS:
            version = self._dialplan_versions.get(key)
            if not version or not self._assets.has(key, version):
                return None
            out[key] = self._assets.path(key, version).read_bytes()
        return out

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
            "why": self._why if not self.is_running() else "",
            "sip_port": SIP_PORT,
            # Только каналом менеджера: слушает loopback, служба «asterisk»
            # реестра `services.py`.
            "ws": f"ws://{LOOPBACK}:{HTTP_PORT}/ws",
            "rtp": f"{RTP_START}-{RTP_END}",
            "calls": self.calls.state() if self.calls else None,
            "install_seconds": self.install_seconds,
            "log": list(self._log),
        }

    # --- старт и остановка ---------------------------------------------

    async def _async_start(self) -> None:
        binary = await self._async_binary()
        if not binary:
            return
        if not await self._async_write():
            return
        if await self._async_ctl("core show uptime") is not None:
            # Сирота прошлого запуска: конфиг наш, перечитываем его и живём дальше.
            await self._async_ctl("core reload")
            self._ready, self._adopted, self._why = True, True, ""
            self._start_calls()
            services.register("asterisk", HTTP_PORT)
            LOGGER.info("SIP-мост: усыновлён Asterisk прошлого запуска")
            return
        busy = await self._env.run(_port_busy)
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
            self._start_calls()
            services.register("asterisk", HTTP_PORT)
            LOGGER.info(
                "SIP-мост готов: UDP %s, WebSocket %s, медиа %s-%s",
                SIP_PORT, HTTP_PORT, RTP_START, RTP_END,
            )
            return
        self._why = (
            f"SIP-слушатель не поднялся за {READY_TIMEOUT:.0f} с. "
            f"Лог: {' | '.join(self._log) or 'пусто'}"
        )
        LOGGER.warning("SIP-мост: %s", self._why)
        await self._async_stop()

    def _start_calls(self) -> None:
        if self.calls is None:
            self.calls = DoorCalls(
                self._env.session(), HTTP_PORT, self._keys["ari"], self._on_event
            )
        self.calls.start()

    async def _async_stop(self) -> None:
        if self.calls is not None:
            await self.calls.stop()
        ready, adopted = self._ready, self._adopted
        self._ready = self._adopted = False
        services.unregister("asterisk")
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
        which = self._env.run
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
        binary = await self._env.run(shutil.which, "asterisk")
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
    for kind, name, port in (
        (socket.SOCK_DGRAM, "UDP", SIP_PORT),
        (socket.SOCK_STREAM, "TCP", HTTP_PORT),
    ):
        with socket.socket(socket.AF_INET, kind) as probe:
            # Без SO_REUSEADDR: нужен честный ответ «занят», а не место рядом.
            try:
                probe.bind(("0.0.0.0", port))
            except OSError:
                return f"{name} {port}"
    return ""
