"""Конфиг Asterisk для SIP-моста домофонии (`sip_bridge.py`). Чистые функции.

Каждое утверждение о ключах ниже сверено с образцами Asterisk 22
(`configs/samples/*.conf.sample`) и справкой `docs.asterisk.org`, а не взято
по аналогии; где сверки нет — сказано прямо.
"""

from __future__ import annotations

import json
from pathlib import Path
import secrets
from typing import Any

# Прямой IP-вызов панели Akuvox идёт на 5060 по UDP (статья Intercom Call
# Configuration и запись трафика стенда) — порт у панели настраивается, но
# меняется для всех её адресатов разом, поэтому подстраиваемся мы.
SIP_PORT = 5060
# HTTP-сервер Asterisk: на нём SIP поверх WebSocket (путь `/ws`) для телефона
# жильца и ARI для самой интеграции. Один порт на оба — так устроен Asterisk
# (`http.conf`), второй HTTP-сервер он не поднимает.
HTTP_PORT = 8188
# Медиа моста. Узкий диапазон: вызовов в доме единицы, а каждый порт — это
# правило, которое придётся объяснять, если между панелью и хостом есть фильтр.
RTP_START = 20000
RTP_END = 20200
# Тот же STUN, что Home Assistant по умолчанию отдаёт своему WebRTC
# (`webrtc.py`): телефон снаружи ходит через TURN, и до его relay-адреса мост
# достучится только со своим внешним адресом, узнанным здесь (`stunaddr`).
STUN = "stun.home-assistant.io:3478"
# Имя приложения Stasis: вызовы, которыми управляет интеграция (`sip_calls.py`).
ARI_APP = "mega_home"
ARI_USER = "mega_home"
# Эндпойнт телефона жильца. ⚠ Этап проверки: одна учётка на дом с паролем,
# который дом придумал сам. Учётки жильцов приватным путём — этап 2 плана
# (`docs/intercom-remote.md` в менеджере).
RESIDENT = "resident"
# Откуда принимаем вызов без регистрации. ⚠ Только частные сети: мост на
# 5060 без аутентификации. Адреса конкретных панелей приедут конфигом вместе с
# модулем домофонии.
PRIVATE_NETS = ("10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16")


def load_secrets(root: Path) -> dict[str, str]:
    """Пароли ARI и жильца: придуманы один раз и живут в каталоге моста.

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
    for key in ("ari", "resident"):
        if not isinstance(stored.get(key), str) or len(stored[key]) < 16:
            stored[key] = secrets.token_urlsafe(18)
            changed = True
    if changed:
        root.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(stored), encoding="utf-8")
        path.chmod(0o600)
    return stored


def write_config(root: Path) -> dict[str, str]:
    """Разложить конфиг моста по своим каталогам (синхронно, в executor)."""
    for name in ("etc", "lib", "run", "log", "spool", "cache", "agi-bin"):
        (root / name).mkdir(parents=True, exist_ok=True)
    keys = load_secrets(root)
    for name, text in render_config(root, keys).items():
        (root / "etc" / name).write_text(text, encoding="utf-8")
    return keys


def render_config(root: Path, keys: dict[str, Any]) -> dict[str, str]:
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
        # ⚠ Слушаем ВСЕ адреса: WebSocket телефона приходит каналом дома
        # (`stream.py`), а тот открывает TCP только на ЧАСТНЫЙ адрес хоста, не на
        # loopback. TLS не нужен: снаружи шифрует WSS менеджера, внутри — LAN.
        "http.conf": f"""[general]
servername=mega_home
enabled=yes
bindaddr=0.0.0.0
bindport={HTTP_PORT}
""",
        # ARI открыт только loopback'у (per-user `permit`/`deny`, `ari.conf.sample`):
        # HTTP-сервер общий с WebSocket и смотрит в LAN, а ARI — это полный
        # контроль над вызовами.
        "ari.conf": f"""[general]
enabled = yes

[{ARI_USER}]
type = user
read_only = no
password = {keys["ari"]}
password_format = plain
deny = 0.0.0.0/0.0.0.0
permit = 127.0.0.1/255.255.255.255
""",
        "rtp.conf": f"""[general]
rtpstart={RTP_START}
rtpend={RTP_END}
icesupport=yes
stunaddr={STUN}
""",
        # ⚠ `endpoint_identifier_order`: по умолчанию `ip,username,anonymous`
        # (`pjsip.conf.sample`), и телефон, пришедший каналом дома с ЧАСТНОГО
        # адреса хоста, опознался бы по IP как панель — без пароля. Имя первым:
        # `resident` требует пароль, у панели в From номер, а не это имя.
        "pjsip.conf": f"""[global]
type=global
user_agent=mega_home-sip-bridge
endpoint_identifier_order=username,ip

[transport-udp]
type=transport
protocol=udp
bind=0.0.0.0:{SIP_PORT}

[transport-ws]
type=transport
protocol=ws
bind=0.0.0.0

[panel]
type=endpoint
transport=transport-udp
context=from-panel
disallow=all
allow=ulaw,alaw
direct_media=no
rtp_symmetric=yes
dtmf_mode=rfc4733

[panel]
type=identify
endpoint=panel
{identify}

[{RESIDENT}]
type=auth
auth_type=userpass
username={RESIDENT}
password={keys["resident"]}

[{RESIDENT}]
type=endpoint
transport=transport-ws
context=from-resident
auth={RESIDENT}
webrtc=yes
disallow=all
allow=ulaw,alaw
""",
        # Вызов панели не отвечаем: он уходит интеграции (`sip_calls.py`), та
        # звонит панели «ring» и держит её, пока жилец не наберёт `answer`.
        # Прямой IP-вызов приходит как `sip:<IP>@<IP>`, поэтому любой номер.
        # `echo` — проверка звука телефон ⇄ мост без панели.
        "extensions.conf": f"""[general]
static=yes
writeprotect=yes

[from-panel]
exten => _[0-9a-zA-Z].,1,NoOp(SIP-мост: вызов от ${{CALLERID(all)}} на ${{EXTEN}})
 same => n,Stasis({ARI_APP},panel)
 same => n,Hangup()

[from-resident]
exten => answer,1,Stasis({ARI_APP},answer)
 same => n,Hangup()
exten => echo,1,Answer()
 same => n,Echo()
 same => n,Hangup()
""",
    }
