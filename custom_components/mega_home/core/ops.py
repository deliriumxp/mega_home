"""The four operations the resident app needs, independent of transport.

They are reached two ways and must answer identically:

* locally — the app served inside the house calls the HTTP views (`http.py`);
* remotely — the manager forwards the resident's request over the live link
  (`link.py`), because the resident's phone cannot reach this house directly.

⚠ That is the whole reason this module exists. Before it, the logic lived in the
view handlers, welded to `web.Request` and `web.Response`; the link would have
had to fake HTTP requests or grow a second copy of the same rules — and two
copies of "which service does this command map to" is exactly the kind of pair
that silently drifts apart.

Answers are plain data; refusals are `OpError`, which each transport renders in
its own way (an HTTP status here, a frame field there) with the SAME wording:
the resident must read the same sentence whether they are home or away.
"""

from __future__ import annotations

from http import HTTPStatus
from typing import Any
from urllib.parse import quote

from .const import LOGGER
from .ops_base import OpError, _int, find, number
from .probe import run as run_probe
from .scan import run as run_scan
from .source import CommandRejected, CommandUnknown, EntityState, StateSource
from .ops_camera import _camera_urls, _warm_cameras, camera_entity, camera_frame
from .ops_video import (
    _guid_of,
    _tiles_by_guid,
    _trassir_guid,
    gateway_call,
    lead_of,
    trassir,
    trassir_cameras,
    trassir_clip_at,
    trassir_events,
    trassir_preview,
    trassir_ready,
    trassir_thumb,
    video_id,
)
from .ops_webrtc import webrtc_candidates, webrtc_close, webrtc_offer

# ⚠ Имена выше ИМПОРТИРУЮТСЯ РАДИ ЧУЖИХ ВЫЗОВОВ: и двери (`http.py`), и линк
# (`link.py`), и тесты зовут их как `ops.<имя>`. Это фасад модуля — тот же
# приём, что у `trassir.ts` в бандле: части читаются порознь, а точка входа
# остаётся одна.
__all__ = [
    "OpError",
    "camera_entity",
    "camera_frame",
    "command",
    "config",
    "entity_view",
    "find",
    "gateway_call",
    "lead_of",
    "number",
    "run",
    "scenario",
    "states",
    "trassir",
    "trassir_cameras",
    "trassir_clip_at",
    "trassir_events",
    "trassir_preview",
    "trassir_ready",
    "trassir_thumb",
    "video_id",
    "webrtc_candidates",
    "webrtc_close",
    "webrtc_offer",
]

async def run(
    coordinator: Any,
    op: str,
    payload: dict[str, Any] | None,
    remote: bool = False,
) -> Any:
    """Run one operation by name. Unknown name is a refusal, not a crash.

    ⚠ `remote` — жилец пришёл ЧЕРЕЗ МЕНЕДЖЕРА, а не локальной дверью. Ответы от
    этого не меняются и меняться не должны: разница «дома/снаружи» живёт в
    адресе базы, а не в наборе функций. Признак нужен ровно там, где физика
    разная, — сколько ждать внешний адрес дома (дома он не нужен вовсе).
    """
    if coordinator is None or not coordinator.data:
        raise OpError(
            "Дом ещё не синхронизирован с менеджером", HTTPStatus.SERVICE_UNAVAILABLE
        )
    data = payload or {}
    if op == "config":
        return config(coordinator)
    if op == "states":
        return states(coordinator)
    if op == "command":
        return await command(coordinator, data)
    if op == "scenario":
        return await scenario(coordinator, data)
    if op == "webrtc":
        return await webrtc_offer(coordinator, data, remote)
    if op == "webrtc-close":
        return webrtc_close(coordinator, data)
    if op == "webrtc-candidates":
        # ⚠ Именованная операция тут — исключение, а не привычка: сигналинг
        # WebRTC не ресурс HTTP, через перенос (`http`) он не едет. Trickle —
        # часть ТОГО ЖЕ обмена, что `webrtc`, поэтому и дверь та же.
        return await webrtc_candidates(data)
    if op == "http":
        # Перенос ОБЫЧНОГО запроса к API этого дома: жилец снаружи должен уметь
        # ровно то же, что дома, и теми же путями (`relay_api.py`).
        #
        # ⚠ Именованные операции выше остаются ради уже работающих домов и
        # менеджеров. НОВЫХ сюда добавлять не надо: каждая такая операция — это
        # функция, которой снаружи нет, пока её не написали в трёх местах и не
        # раскатали релизом на каждый объект. Для этого и есть перенос.
        #
        # ⚠ Импорт ЛОКАЛЬНЫЙ: `relay_api` зовёт этот модуль, и на уровне файла
        # это был бы цикл.
        from .relay_api import handle

        return await handle(coordinator, data)
    if op == "probe":
        # Проба устройства объекта по заданию МЕНЕДЖЕРА (`probe.py`): мониторинг
        # больше не ходит в LAN объекта по WG-туннелю, которого у части парка
        # нет вовсе. Здесь только примитивы — что и зачем спрашивать, знает
        # менеджер, и меняется это его деплоем, а не релизом HACS.
        return await run_probe(coordinator.env, data)
    if op == "scan":
        # Обход локальной сети объекта по заказу МЕНЕДЖЕРА (`scan.py`): какие
        # устройства стоят и куда из них можно войти по веб-интерфейсу. Список
        # DHCP-аренд роутера этого не даёт — статика в нём не появляется.
        return await run_scan(coordinator.env, data)
    if op == "self-update":
        # Обновить интеграцию и перезапустить дом по кнопке инсталлятора. Как —
        # знает только адаптер (в HA это HACS и `homeassistant.restart`), ядро
        # лишь зовёт то, что он дал; у адаптера без этого умения — честный отказ.
        updater = getattr(coordinator, "self_update", None)
        if updater is None:
            raise OpError("Этот дом не умеет обновляться удалённо", HTTPStatus.NOT_IMPLEMENTED)
        return await updater()
    raise OpError("Неизвестная операция", HTTPStatus.NOT_FOUND)

def config(coordinator: Any) -> dict[str, Any]:
    """Состав дома из кэша — плюс ПАСПОРТ САМОГО ДОМА.

    ⚠ `integration` — ответ на вопрос «что этот дом умеет», и завести его
    стоило полутора суток разбора. Раньше сказать это снаружи было НЕЧЕМ:
    версия уходила только менеджеру (`link.py`), а с любой другой стороны
    оставалось гадать по маршрутам — и гадание врало. Дверь превью была
    ОБЪЯВЛЕНА с 0.2.53 и не зарегистрирована до 0.2.60; её `404` побайтово
    совпадал с ответом на выдуманный путь, и «маршрута нет» читалось как «дом
    старой версии» на объекте, обновлённом и перезапущенном не раз.

    ⚠ НОВОГО МАРШРУТА ДЛЯ ЭТОГО НЕ ЗАВОДИТСЯ, и это принципиально: правило
    требует сперва обойтись общим каналом, а «что такое этот дом» — ровно то,
    за чем приложение и так приходит первым запросом (`CLAUDE.md`, «Новый
    маршрут в шлюзе»). Ответ на `config` и без того читают все двери.

    ⚠ `routes` — список ПОДНЯТЫХ путей, а не объявленных классов: мёртвую дверь
    он показывает отсутствием, и повторить историю превью станет нечем.
    Заодно это единственный честный способ спросить дом «а ты это умеешь?» — по
    версии судить нельзя, версия говорит лишь о намерении.

    ⚠ `accesses` — что дом умеет ДОСТАТЬ снаружи: имя доступа, его ВИД и вендор
    за ним. Без этого бандл вынужден гадать по версии, а версия говорит лишь о
    намерении (урок 2026-09-14 с дверью превью, объявленной и не
    зарегистрированной). Здесь ровно то, что приехало конфигом и было ПРИНЯТО:
    описание с пустым хостом дом молча отбрасывает, и увидеть это иначе нельзя.

    ⚠ Ни адресов, ни портов, ни учёток: тело `config` уходит браузеру жильца
    как есть. Паспорт отвечает «что есть», а не «как туда ходить».
    """
    from .const import INTEGRATION_VERSION

    # ⚠ И ПОЧЕМУ дом чего-то не может — тоже сюда. Причина у него была всегда
    # (`go2rtc_embed._why`), но лежала в диагностике Home Assistant, за
    # токеном: снаружи — ни из приложения, ни из скрипта — её было не достать,
    # и «Дом не может отдать запись: не поднят его go2rtc» оставалось без
    # продолжения. Паспорт для того и заведён: он говорит не только «что умею»,
    # но и «чего не могу и по какой причине».
    try:
        from .go2rtc_embed import state as go2rtc_state

        media = go2rtc_state()
    except Exception as err:  # noqa: BLE001 — паспорт важнее одной строки в нём
        media = {"running": False, "why": f"состояние go2rtc не прочиталось: {err}"}

    return {
        **(coordinator.data or {}),
        "integration": {
            "version": INTEGRATION_VERSION,
            "appVersion": coordinator.bundle.version if coordinator.bundle else None,
            "routes": list(getattr(coordinator, "routes", [])),
            "accesses": _accesses(coordinator),
            "go2rtc": media,
        },
    }

def _accesses(coordinator: Any) -> list[dict[str, str]]:
    """Доступы, которые дом ПРИНЯЛ: имя, вид, вендор. Двери нет — пустой список."""
    door = getattr(coordinator, "accesses", None)
    if door is None:
        return []
    out: list[dict[str, str]] = []
    for access in door.ids():
        descriptor = door.descriptor(access)
        if descriptor is None:
            continue
        out.append(
            {"id": descriptor.id, "kind": descriptor.kind, "vendor": descriptor.vendor}
        )
    return out

def states(coordinator: Any) -> dict[str, Any]:
    """Current states of every tile, read straight from the source of this home."""
    source: StateSource = coordinator.source
    entities = [
        entity_view(tile, source.get(tile["entityId"]) if tile.get("entityId") else None)
        for tile in coordinator.data.get("tiles", [])
    ]
    _warm_cameras(coordinator)
    return {
        # Always connected: this runs inside the home, so there is no link to
        # lose between here and Home Assistant. When the manager forwards this
        # answer to a resident who is away, the link being up is what let the
        # answer arrive at all.
        "connected": True,
        # `configVersion` rides along on purpose: the app polls states every few
        # seconds anyway, so this is the cheapest way to tell a phone that has
        # been open for days that the installer added a socket.
        "configVersion": coordinator.version,
        "appVersion": coordinator.bundle.version if coordinator.bundle else None,
        "entities": entities,
    }

async def command(
    coordinator: Any, payload: dict[str, Any]
) -> dict[str, Any]:
    """One command for one tile, mapped onto a Home Assistant service call."""
    tile = find(coordinator.data.get("tiles", []), payload.get("id"))
    if tile is None:
        raise OpError("Устройство не найдено", HTTPStatus.NOT_FOUND)
    if not tile.get("entityId"):
        # Same wording as the manager: the element is in the project but was
        # never pushed to Home Assistant, so there is nothing to command.
        raise OpError(
            "Элемент ещё не отправлен в Home Assistant — управлять им пока нечем"
        )

    name = payload.get("command")
    spec = command_spec(tile, name)
    if spec is None:
        raise OpError("Команда не поддерживается устройством", HTTPStatus.NOT_FOUND)

    try:
        data = service_data(spec, payload.get("value"))
    except ValueError as err:
        raise OpError(str(err)) from err

    await call(
        coordinator.source, spec["domain"], spec["service"], {"entity_id": tile["entityId"], **data}
    )
    # ⚠ Ответ несёт НОВОЕ состояние плитки, а не только «принято». Иначе
    # приложению остаётся либо ждать следующего снимка (тап выглядит
    # непринятым почти секунду), либо рисовать угаданное состояние — и то и
    # другое неправильно там, где настоящее состояние лежит в двух шагах.
    # Служба вызвана блокирующе, поэтому машина состояний уже обновлена.
    return {
        "accepted": True,
        "entity": entity_view(tile, coordinator.source.get(tile["entityId"])),
    }

async def scenario(
    coordinator: Any, payload: dict[str, Any]
) -> dict[str, Any]:
    """Run one scenario (a Home Assistant script)."""
    item = find(coordinator.data.get("scenarios", []), payload.get("id"))
    if item is None:
        raise OpError("Сценарий не найден", HTTPStatus.NOT_FOUND)
    if not item.get("entityId"):
        raise OpError("Сценарий не создан в Home Assistant", HTTPStatus.NOT_FOUND)
    return await call(coordinator.source, "script", "turn_on", {"entity_id": item["entityId"]})

async def call(
    source: StateSource, domain: str, service: str, data: dict[str, Any]
) -> dict[str, Any]:
    """Command the source of states and turn its refusals into plain answers.

    A service can be missing outright — `climate.set_temperature` does not exist
    on an installation with no climate integration loaded — and that raises.
    Without this the resident would get a bare 500 for a house that is simply
    not set up yet.
    """
    try:
        # Источник ждёт выполнения: ответ обязан нести состояние ПОСЛЕ команды
        # (см. command).
        await source.call(domain, service, data)
    except CommandUnknown as err:
        LOGGER.warning("Service %s.%s is not available", domain, service)
        raise OpError(
            "Home Assistant не умеет выполнять эту команду на этом объекте",
            HTTPStatus.NOT_FOUND,
        ) from err
    except CommandRejected as err:
        LOGGER.warning("Service %s.%s rejected the payload: %s", domain, service, err)
        raise OpError("Home Assistant отклонил команду") from err
    return {"accepted": True}

def command_spec(tile: dict[str, Any], name: Any) -> dict[str, Any] | None:
    """Чем исполнять команду: службой ИЗ КОНФИГА — другого источника больше нет.

    ⚠ Ради этого затевался тонкий шлюз (docs/plan-thin-integration.md, фаза 2).
    Пока карта команд жила в Python, новый управляемый домен — вентилятор, замок,
    пылесос — стоил релиза HACS и перезапуска Home Assistant НА КАЖДОМ объекте.
    Теперь менеджер кладёт службу в конфиг плитки, и она доезжает обычной
    синхронизацией. Не заводи таблицу доменов здесь снова: она немедленно начнёт
    расходиться с менеджерской (`tileCommands` в `smart-home-view.util.ts`), а
    чинится такое расхождение только выездом.

    ⚠ Фолбэк `COMMAND_SERVICES`/`LEGACY_ARGS` убран в 0.1.14 (фаза 3). Он жил
    ровно один выпуск — на дом, чей кэш конфига старше кода. Теперь такого дома
    не бывает: обновление интеграции идёт через HACS, то есть по интернету, а
    тот же интернет приносит и конфиг с картой команд.
    """
    if not isinstance(name, str):
        return None
    described = (tile.get("commands") or {}).get(name)
    if not isinstance(described, dict) or not described.get("service"):
        return None
    return {
        "domain": described.get("domain") or tile["domain"],
        "service": described["service"],
        "arg": described.get("arg"),
        "min": described.get("min"),
        "max": described.get("max"),
    }

def service_data(spec: dict[str, Any], value: Any) -> dict[str, Any]:
    """Единственный аргумент команды, проверенный по описанным границам.

    ⚠ Границы приходят из конфига, но проверяет их ЭТА сторона: службу зовём мы,
    а браузеру жильца верить нельзя. Аргумент без границ — строковый (режим
    термостата), с границами — число.
    """
    arg = spec.get("arg")
    if not arg:
        return {}
    low, high = spec.get("min"), spec.get("max")
    if low is None or high is None:
        return {arg: str(value or "")}
    return {arg: number(value, low, high)}

# Что из атрибутов наружу НЕ уходит.
#
# ⚠ Список короткий намеренно. Это не «фильтр полезного» — атрибуты уходят
# ЦЕЛИКОМ (решение 2026-09-06, см. смежный комментарий в
# smart-home-view.util.ts менеджера): приложение живёт только внутри Home
# Assistant, и сокращать то, что он уже посчитал, — работа без выгоды. Здесь
# только то, чему в браузере жильца делать нечего: `access_token` это секрет, из
# которого мы уже собрали адреса кадра и потока камеры, и отдать его отдельным
# полем значит отдать право собрать любой другой адрес того же HA.
HIDDEN_ATTRIBUTES = frozenset({"access_token"})

def _public_attributes(attributes: Any) -> dict[str, Any]:
    return {
        key: value
        for key, value in (attributes or {}).items()
        if key not in HIDDEN_ATTRIBUTES
    }

def entity_view(tile: dict[str, Any], state: EntityState | None) -> dict[str, Any]:
    """Что дом отвечает о приборе: сырое состояние Home Assistant и атрибуты.

    ⚠ Проекции здесь БОЛЬШЕ НЕТ (docs/plan-thin-integration.md, фаза 1).
    `power`, `playing`, яркость, позицию, температуру и способности считает
    ПРИЛОЖЕНИЕ — в одном месте на весь продукт (`ha-entity.ts`). Раньше то же
    самое считалось трижды: здесь, в менеджере и на экране, — и каждое новое
    поле экрана стоило правки в двух репозиториях, из которых ЭТОТ доезжает до
    объекта только релизом HACS и перезапуском Home Assistant. Ради того, чтобы
    в интеграции нечему было ломаться, всё это отсюда и убрано: не возвращай.

    ⚠ Камера — единственное исключение, и оно не растёт: адреса кадра и потока
    строятся из `entity_id` и `access_token`, а наружу не уходит ни то, ни
    другое (`entity_id` намеренно, токен как секрет).

    ⚠ `name` и `roomId` УБРАНЫ в 0.1.14 (фаза 3), после того как бандл со
    склейкой повышен в релиз. Подписи и комнаты приложение берёт из конфига
    (`HomeTiles` в `home-shape.ts`) — дом отвечает только тем, что знает Home
    Assistant. Не возвращай их «для надёжности»: два источника одного имени
    разъедутся ровно тогда, когда инсталлятор переименует прибор, а конфиг и
    состояния придут разными дорогами.

    ⚠ У МЕНЕДЖЕРА (`toEntityView` в `smart-home-view.util.ts`) те же два поля
    ОСТАЮТСЯ, и это не рассинхрон: его ответ читает ещё и превью в карточке
    объекта (`ManagerHomeBackend`), а оно склейку с конфигом не делает — там
    конфига нет вовсе.
    """
    domain = tile["domain"]
    raw = state.state if state else None
    attributes = state.attributes if state else {}
    unavailable = raw in ("unavailable", "unknown")
    values: dict[str, Any] = {"value": raw}

    if domain == "camera":
        values.update(_camera_urls(tile.get("entityId"), attributes))
        # Каким способом Home Assistant отдаёт живое видео: `hls` или `web_rtc`.
        # Пока не читает никто — приложение показывает MJPEG, который умеет любой
        # браузер без единой зависимости, — но от поля зависит удалённый просмотр,
        # и узнать его задним числом неоткуда.
        values["streamType"] = attributes.get("frontend_stream_type")

    # ⚠ Камера видеонаблюдения ДОСТУПНА без сущности Home Assistant: её показывает
    # сам дом, забирая поток у регистратора. Считать её недоступной значило бы
    # написать жильцу «Нет данных» поверх работающей камеры.
    available = bool(video_id(tile)) or (state is not None and not unavailable)

    return {
        "id": tile["id"],
        "domain": domain,
        "state": values,
        "attributes": _public_attributes(attributes),
        "available": available,
        "updatedAt": int(state.last_updated.timestamp() * 1000) if state else None,
    }
