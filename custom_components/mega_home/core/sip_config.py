"""Конфиг Asterisk для SIP-моста домофонии (`sip_bridge.py`). Чистые функции.

Каждое утверждение о ключах ниже сверено с образцами Asterisk 22
(`configs/samples/*.conf.sample`) и справкой `docs.asterisk.org`, а не взято
по аналогии; где сверки нет — сказано прямо.
"""

from __future__ import annotations

import ipaddress
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
# Эндпойнт телефона жильца. ⚠ Учётки у него нет (решение заказчика
# 2026-09-19): телефон доходит до моста ТОЛЬКО каналом менеджера (`stream.py`
# пускает loopback ровно на `HTTP_PORT`), права жильца проверяет менеджер, а
# HTTP-сервер моста слушает один loopback — из Wi-Fi объекта его не видно.
# Опознаётся по адресу: всё, что пришло с loopback, — телефон.
RESIDENT = "resident"
LOOPBACK = "127.0.0.1"


def panel_addresses(block: Any) -> tuple[str, ...]:
    """Адреса панелей из конфига объекта (`intercom.panels`): от них — без регистрации.

    ⚠ Только литеральный частный IPv4, не loopback: вызов с 5060 принимается
    без аутентификации, и loopback здесь сделал бы панелью телефон жильца.
    Нет адресов — мост не принимает вызов ни от кого: безопасный умолчательный
    отказ вместо прежних «все частные сети».
    """
    raw = block.get("panels") if isinstance(block, dict) else None
    found: list[str] = []
    for item in raw if isinstance(raw, list) else []:
        try:
            address = ipaddress.ip_address(str(item).strip())
        except ValueError:
            continue
        if (
            address.version == 4
            and address.is_private
            and not address.is_loopback
            and str(address) not in found
        ):
            found.append(str(address))
    return tuple(sorted(found))


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
    for key in ("ari",):
        if not isinstance(stored.get(key), str) or len(stored[key]) < 16:
            stored[key] = secrets.token_urlsafe(18)
            changed = True
    if changed:
        root.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(stored), encoding="utf-8")
        path.chmod(0o600)
    return stored


def write_config(root: Path, panels: tuple[str, ...] = ()) -> dict[str, str]:
    """Разложить конфиг моста по своим каталогам (синхронно, в executor)."""
    for name in ("etc", "lib", "run", "log", "spool", "cache", "agi-bin"):
        (root / name).mkdir(parents=True, exist_ok=True)
    keys = load_secrets(root)
    for name, text in render_config(root, keys, panels).items():
        (root / "etc" / name).write_text(text, encoding="utf-8")
    return keys


def render_config(
    root: Path, keys: dict[str, Any], panels: tuple[str, ...] = ()
) -> dict[str, str]:
    """Файлы конфига Asterisk. Чистая функция — её и проверяют тесты."""
    # Секция `identify` без `match` Asterisk не примет, поэтому без панелей
    # её нет вовсе: вызов с 5060 ни с чем не совпадёт и будет отклонён.
    panel_identify = (
        "[panel]\ntype=identify\nendpoint=panel\n"
        + "".join(f"match={address}\n" for address in panels)
        if panels
        else "; адресов панелей в конфиге объекта нет — вызов не примем ни от кого\n"
    )
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
        # ⚠ Только loopback: WebSocket телефона приходит каналом менеджера
        # (`stream.py` пускает loopback ровно на этот порт), а учётки у телефона
        # нет — слушай мы LAN, позвонить в дверь смог бы любой в Wi-Fi объекта.
        # TLS не нужен: снаружи шифрует WSS менеджера, внутри — loopback.
        "http.conf": f"""[general]
servername=mega_home
enabled=yes
bindaddr={LOOPBACK}
bindport={HTTP_PORT}
""",
        # ARI открыт только loopback'у (per-user `permit`/`deny`, `ari.conf.sample`):
        # HTTP-сервер общий с WebSocket телефона, а ARI — это полный контроль
        # над вызовами; второй замок на случай, если `bindaddr` когда-то раскроют.
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
        # ⚠ `endpoint_identifier_order=ip` без `username` и `anonymous`: оба
        # конца опознаются по адресу — панель по своему из конфига, телефон по
        # loopback. По имени из From опознавать нельзя: у телефона нет пароля,
        # и чужой вызов с именем `resident` стал бы телефоном.
        "pjsip.conf": f"""[global]
type=global
user_agent=mega_home-sip-bridge
endpoint_identifier_order=ip

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

{panel_identify}
[{RESIDENT}]
type=endpoint
transport=transport-ws
context=from-resident
webrtc=yes
disallow=all
allow=ulaw,alaw

[{RESIDENT}]
type=identify
endpoint={RESIDENT}
match={LOOPBACK}
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
