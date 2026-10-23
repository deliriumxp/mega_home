"""Ключи локальных подключений и раздел конфига «только для дома».

Решения заказчика 2026-10-17 (`docs/superpowers/specs/2026-10-17-schedules-design.md`
менеджера): открытый режим — сегодняшнее поведение и раздаёт ключи; закрытый
пускает только с ключом; уже подключённые не отваливаются; смена пароля учётки
снимает ключи её устройств.
"""

from __future__ import annotations

from mega_home.core import ops
from mega_home.core.local_keys import LocalKeys, sign

# ⚠ Тот же вектор — в спеке менеджера (`local-keys.util.spec.ts`): ключ подписывает
# менеджер, проверяет дом, и разойтись им нельзя.
VECTOR = "1.dev1.42.7.tkwQpYSxVg1iYuBWGqWGgY4gPIZHupkQJb2KxLcy2cU"


def _keys(closed: bool = False, accounts: dict[str, int] | None = None) -> LocalKeys:
    keys = LocalKeys()
    keys.apply({"house": {"local": {"secret": "s3cret", "closed": closed, "accounts": accounts or {"42": 7}}}})
    return keys


def test_подпись_совпадает_с_менеджером() -> None:
    assert sign("s3cret", "dev1", "42", "7") == VECTOR


def test_без_секрета_дверь_открыта_как_до_ключей() -> None:
    keys = LocalKeys()
    keys.apply({"rooms": []})
    assert keys.admit(None, None) == (True, None, False)


def test_открытый_режим_раздаёт_ключ_и_запоминает_устройство() -> None:
    keys = _keys()
    allowed, key, fresh = keys.admit(None, None)
    assert allowed and fresh and keys.valid(key)

    # Закрыли — устройство с ключом в куке работает дальше, новое — нет.
    keys.apply({"house": {"local": {"secret": "s3cret", "closed": True, "accounts": {"42": 7}}}})
    assert keys.admit(key, None) == (True, key, False)
    assert keys.admit(None, None) == (False, None, False)


def test_пропуск_из_облака_пускает_в_закрытый_дом() -> None:
    keys = _keys(closed=True)
    assert keys.admit(None, VECTOR) == (True, VECTOR, True)


def test_отозванная_копия_и_свежий_пропуск_через_запятую() -> None:
    """Пароль сменили: копия в браузере отозвана, но пропуск из облака того же входа годен."""
    keys = _keys(closed=True, accounts={"42": 7})
    revoked = sign("s3cret", "dev1", "42", "6")
    assert keys.admit(None, f"{revoked},{VECTOR}") == (True, VECTOR, True)


def test_кука_сильнее_пропуска_монитор_не_перепривязывается() -> None:
    keys = _keys(closed=True)
    own = sign("s3cret", "panel")
    assert keys.admit(own, VECTOR) == (True, own, False)


def test_смена_пароля_снимает_ключи_учётки_но_не_открытого_режима() -> None:
    keys = _keys(closed=True, accounts={"42": 8})
    panel = sign("s3cret", "panel")
    assert not keys.valid(VECTOR)
    assert keys.valid(panel)
    # Учётки нет в конфиге (отключена) — её ключи тоже не действуют.
    keys = _keys(closed=True, accounts={"5": 1})
    assert not keys.valid(VECTOR)


def test_новый_секрет_снимает_все_ключи() -> None:
    keys = LocalKeys()
    keys.apply({"house": {"local": {"secret": "other", "closed": True}}})
    assert not keys.valid(VECTOR)
    assert not keys.valid(sign("s3cret", "panel"))


def test_подделка_и_мусор_не_проходят() -> None:
    keys = _keys(closed=True)
    assert not keys.valid(VECTOR[:-1] + ("A" if VECTOR[-1] != "A" else "B"))
    assert not keys.valid("1.dev1.42.7")
    assert not keys.valid("2.dev1.42.7." + VECTOR.rsplit(".", 1)[1])
    assert not keys.valid("1.d/v.-.-.x")
    assert not keys.valid(None)


class _Coordinator:
    data = {
        "version": "v", "rooms": [], "house": {"local": {"secret": "s3cret"}, "ha": {"token": "t"}},
        "recorder": {"storage": "/mnt/mega-home/storage", "cameras": [{"id": "c"}],
                     "mount": {"type": "cifs", "source": "//nas/v", "user": "u", "pass": "nas-pass"}},
    }
    routes: list[str] = []
    bundle = None


def test_раздел_house_не_уходит_браузеру() -> None:
    """⚠ Тело `config` раздаётся обеим дверям — секрет ключей и токен HA остаются в доме."""
    answer = ops.config(_Coordinator())
    assert "house" not in answer
    assert "s3cret" not in repr(answer) and "'t'" not in repr(answer)
    # Учётка сетевой папки записи — тоже дому: бандлу из раздела записи нужны камеры.
    assert "nas-pass" not in repr(answer) and answer["recorder"]["cameras"] == [{"id": "c"}]
