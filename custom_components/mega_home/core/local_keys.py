"""Ключи локальных подключений: кого пускает локальная дверь дома.

Решения заказчика 2026-10-17 (`docs/superpowers/specs/2026-10-17-schedules-design.md`
менеджера, часть 1):
- режим объекта — переключатель менеджера «Закрыть новые локальные подключения».
  Открыт (умолчание, сегодняшнее поведение): дом пускает всех и каждому без ключа
  ВЫДАЁТ ключ — его запоминают. Закрыт: пускает только с ключом;
- устройство, вошедшее через облако, получает ключ от менеджера (пропуск в адресе
  локальной двери); подтверждений в приложениях нет;
- смена пароля или отключение учётки снимает ключи её устройств — `epoch` учётки.

⚠ Ключ проверяется БЕЗ хранилища: `1.<устройство>.<учётка>.<epoch>.<подпись>`,
подпись — HMAC секретом объекта из конфига дома. Отзыв всех разом — новый секрет
(«Сбросить локальные подключения» в менеджере), отзыв по учётке — её `epoch`.
Своего списка у дома нет, поэтому переустановка Home Assistant с нуля не стирает
ни одной привязки: конфиг приезжает тот же.

⚠ Перенос через менеджера сюда не ходит: снаружи пускает сессия жильца
(`docs/remote-access.md`), а это — только дверь в Wi-Fi объекта.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import re
import secrets
from typing import Any

COOKIE = "mm_local"
# Копия ключа из хранилища браузера: ею приложение восстанавливает потерянную куку
# и ею же локальная страница предъявляет пропуск из адреса.
HEADER = "X-MM-Local-Key"
# Кука живёт «навсегда»: забыть привязку вправе только жилец, удалив приложение.
COOKIE_MAX_AGE = 10 * 365 * 24 * 3600
NONE = "-"
_PART = re.compile(r"^[A-Za-z0-9_-]{1,64}$")


def sign(secret: str, device: str, account: str = NONE, epoch: str = NONE) -> str:
    """Ключ устройства. ⚠ Пара с менеджером (`local-key.util.ts`): вектор в спеках обеих сторон."""
    body = f"1.{device}.{account}.{epoch}"
    mac = hmac.new(secret.encode("utf-8"), f"mm-local:{body}".encode("utf-8"), hashlib.sha256)
    return f"{body}.{base64.urlsafe_b64encode(mac.digest()).decode('ascii').rstrip('=')}"


class LocalKeys:
    """Режим и проверка ключей по конфигу дома (раздел `house.local`)."""

    def __init__(self) -> None:
        self.secret = ""
        self.closed = False
        self.accounts: dict[str, int] = {}

    def apply(self, config: dict[str, Any] | None) -> None:
        local = ((config or {}).get("house") or {}).get("local") or {}
        self.secret = str(local.get("secret") or "")
        self.closed = local.get("closed") is True
        accounts = local.get("accounts") if isinstance(local.get("accounts"), dict) else {}
        self.accounts = {str(k): v for k, v in accounts.items() if isinstance(v, int)}

    def valid(self, key: str | None) -> bool:
        parts = (key or "").split(".")
        if not self.secret or len(parts) != 5 or parts[0] != "1":
            return False
        _, device, account, epoch, _mac = parts
        if not all(_PART.match(p) for p in (device, account, epoch)):
            return False
        if not hmac.compare_digest(sign(self.secret, device, account, epoch), key or ""):
            return False
        if account == NONE:
            return True
        return epoch.isdigit() and self.accounts.get(account) == int(epoch)

    def admit(self, cookie: str | None, header: str | None) -> tuple[bool, str | None, bool]:
        """`(пустить, ключ запроса, положить ли его в куку)`.

        ⚠ Действующая кука СИЛЬНЕЕ ключа из заголовка: пропуск, пришедший позже,
        не перепривязывает монитор, подключённый в открытом режиме, к учётке —
        иначе перевыдача ссылки приветствия молча отключила бы его.
        ⚠ Нет секрета (менеджер ещё не прислал раздел) — пускаем, как до ключей:
        закрыть дом может только конфиг, а не его отсутствие.
        """
        if not self.secret:
            return True, None, False
        if self.valid(cookie):
            return True, cookie, False
        if self.valid(header):
            return True, header, True
        if not self.closed:
            return True, sign(self.secret, secrets.token_urlsafe(12)), True
        return False, None, False
