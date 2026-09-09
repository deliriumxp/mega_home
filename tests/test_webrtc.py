"""Удалённый просмотр камеры: обмен предложением и ответом по каналу менеджера.

Проверяется здесь то, ради чего файл `webrtc.py` вообще существует: разрозненные
сообщения Home Assistant (ответ отдельно, кандидаты отдельно) сводятся в ОДИН
пакет, потому что канал до менеджера — запрос-ответ, а не подписка. И то, что
при любом отказе сессия камеры закрывается: иначе go2rtc держал бы поток с
камеры после каждой неудачной попытки жильца.

⚠ Модули камеры Home Assistant подменены здесь, а не в `conftest.py`: их видит
только этот тест, и подмена должна уметь врать по-разному (камера без WebRTC,
камера с ошибкой, молчащая камера).
"""

from __future__ import annotations

import asyncio
import sys
import types
from dataclasses import dataclass
from http import HTTPStatus
from typing import Any

import pytest

from mega_home import ops


class _StreamType:
    HLS = "hls"
    WEB_RTC = "web_rtc"


@dataclass(frozen=True)
class _Message:
    pass


@dataclass(frozen=True)
class _Answer(_Message):
    answer: str


@dataclass(frozen=True)
class _Candidate(_Message):
    candidate: Any


@dataclass(frozen=True)
class _Error(_Message):
    code: str
    message: str


class _Ice:
    """Кандидат так, как его отдаёт Home Assistant: с готовым `to_dict()`."""

    def __init__(self, value: str) -> None:
        self._value = value

    def to_dict(self) -> dict[str, Any]:
        return {"candidate": self._value, "sdpMLineIndex": 0}


class _Capabilities:
    def __init__(self, types: set[str]) -> None:
        self.frontend_stream_types = types


class _Camera:
    """Камера, которая отвечает сценарием: список сообщений и задержка перед ним."""

    def __init__(
        self,
        messages: list[_Message] | None = None,
        *,
        webrtc: bool = True,
        delay: float = 0.0,
        raises: Exception | None = None,
        frame: bytes | None = None,
    ) -> None:
        self.frame = frame
        self.camera_capabilities = _Capabilities(
            {_StreamType.WEB_RTC} if webrtc else {_StreamType.HLS}
        )
        self._messages = messages or []
        self._delay = delay
        self._raises = raises
        self.offers: list[tuple[str, str]] = []
        self.closed: list[str] = []

    async def async_handle_async_webrtc_offer(self, offer_sdp, session_id, send_message):
        if self._raises:
            raise self._raises
        self.offers.append((offer_sdp, session_id))

        async def replay() -> None:
            await asyncio.sleep(self._delay)
            for message in self._messages:
                send_message(message)

        asyncio.ensure_future(replay())

    def close_webrtc_session(self, session_id: str) -> None:
        self.closed.append(session_id)


class _Coordinator:
    def __init__(self, tiles: list[dict[str, Any]]) -> None:
        self.data = {"tiles": tiles}
        self.version = None
        self.bundle = None


class _Hass:
    """Home Assistant ровно в том объёме, в каком его трогает постер."""

    class _States:
        def get(self, _entity_id):  # noqa: ANN001, D102
            return None

    def __init__(self) -> None:
        self.states = _Hass._States()
        self.tasks: list[Any] = []

    def async_create_task(self, coro):  # noqa: ANN001, D102
        self.tasks.append(coro)
        return coro


# ─── Свой go2rtc (:8555) — тот путь, что активен с 0.2.9 ──────────────────────
#
# ⚠ Библиотека go2rtc_client в тесте подменена ЦЕЛИКОМ: проверяются решения
# ЭТОГО файла — форма кандидатов, реестр сессий, отказ без маскировки, — а не
# сетевой клиент (его пишет edenhaus).


@dataclass(frozen=True)
class _GoAnswer:
    sdp: str


@dataclass(frozen=True)
class _GoCandidate:
    candidate: str


@dataclass(frozen=True)
class _GoWsError:
    error: str


@dataclass(frozen=True)
class _GoOffer:
    sdp: str
    ice_servers: list


class _Go2RtcWsClient:
    """Копия публичной поверхности Go2RtcWsClient, нужной переговорам."""

    instances: list[_Go2RtcWsClient] = []

    def __init__(self, session, url, *, source=None, destination=None) -> None:
        assert source or destination, "source or destination must be set"
        self.url = url
        self.source = source
        self.sent: list[Any] = []
        self.closed = False
        self.subscribers: list = []
        _Go2RtcWsClient.instances.append(self)

    def subscribe(self, callback) -> object:
        self.subscribers.append(callback)
        return lambda: None

    async def send(self, message) -> None:
        self.sent.append(message)

    async def close(self) -> None:
        self.closed = True

    def receive(self, message) -> None:
        """Доставить сообщение подписчикам — как это делает rx-таска либы."""
        for subscriber in self.subscribers:
            subscriber(message)


class _Producer:
    def __init__(self, url: str) -> None:
        self.url = url


class _Stream:
    def __init__(self, producers: list[_Producer]) -> None:
        self.producers = producers


class _StreamsApi:
    def __init__(self) -> None:
        self.streams: dict[str, _Stream] = {}
        self.added: list[tuple[str, list]] = []
        self.fail = False

    async def list(self) -> dict[str, _Stream]:
        if self.fail:
            raise RuntimeError("go2rtc is down")
        return self.streams

    async def add(self, name: str, sources) -> None:
        self.added.append((name, list(sources)))


class _Go2RtcRestClient:
    instances: list[_Go2RtcRestClient] = []
    # Классовый флаг: `negotiate` строит СВОЙ клиент, инстанс из теста до него
    # не дотянется. Сбрасывается monkeypatch'ем в самом тесте.
    fail_next = False

    def __init__(self, session, url) -> None:
        self.streams = _StreamsApi()
        self.streams.fail = type(self).fail_next
        _Go2RtcRestClient.instances.append(self)


class _OwnCamera:
    """Камера для пути своего go2rtc: stream_source и платформа, без провайдера HA."""

    def __init__(self, source: str = "rtsp://cam/stream") -> None:
        self._source = source
        self.offers: list[tuple[str, str]] = []
        self.closed: list[str] = []

        class _Platform:
            platform_name = "generic"

        self.platform = _Platform()

    async def stream_source(self) -> str:
        return self._source

    async def async_handle_async_webrtc_offer(self, offer_sdp, session_id, send_message) -> None:
        """Провайдерный путь HA: помечаем, если сюда всё-таки отходили."""
        self.offers.append((offer_sdp, session_id))

    def close_webrtc_session(self, session_id: str) -> None:
        self.closed.append(session_id)


class _OwnHass:
    """`hass` с тем единственным, что нужно пути: планировщик задач."""

    def __init__(self) -> None:
        self.tasks: list = []

    def async_create_task(self, coro) -> None:
        import asyncio

        self.tasks.append(coro)
        asyncio.ensure_future(coro)


@pytest.fixture
def own_go2rtc(monkeypatch):
    """Включить «свой go2rtc работает» и подменить библиотеку клиента."""
    import mega_home.go2rtc_embed as embed
    from mega_home import webrtc

    go2rtc_client = types.ModuleType("go2rtc_client")
    go2rtc_client.Go2RtcRestClient = _Go2RtcRestClient
    go2rtc_ws = types.ModuleType("go2rtc_client.ws")
    go2rtc_ws.Go2RtcWsClient = _Go2RtcWsClient
    go2rtc_ws.WebRTCAnswer = _GoAnswer
    go2rtc_ws.WebRTCCandidate = _GoCandidate
    go2rtc_ws.WsError = _GoWsError
    go2rtc_ws.WebRTCOffer = _GoOffer
    go2rtc_util = types.ModuleType("homeassistant.components.go2rtc.util")

    def get_camera_identifier(camera):  # type: ignore[no-untyped-def]
        return "camera.hall"

    go2rtc_util.get_camera_identifier = get_camera_identifier
    added = {
        "go2rtc_client": go2rtc_client,
        "go2rtc_client.ws": go2rtc_ws,
        "homeassistant.components.go2rtc": types.ModuleType(
            "homeassistant.components.go2rtc"
        ),
        "homeassistant.components.go2rtc.util": go2rtc_util,
    }
    sys.modules.update(added)
    monkeypatch.setattr(embed, "is_running", lambda: True)
    monkeypatch.setattr(embed, "URL", "http://127.0.0.1:1985")
    monkeypatch.setattr(webrtc, "ANSWER_TIMEOUT", 0.2)

    _Go2RtcWsClient.instances.clear()
    _Go2RtcRestClient.instances.clear()
    try:
        yield types.SimpleNamespace(ws=_Go2RtcWsClient, rest=_Go2RtcRestClient)
    finally:
        for name in added:
            sys.modules.pop(name, None)
        # Реестр сессий не должен протекать между тестами.
        webrtc._own_sessions.clear()


CAMERA_TILE = {"id": "cam1", "domain": "camera", "entityId": "camera.hall"}


@pytest.fixture(autouse=True)
def _ha_camera_modules():
    """Подменить модули камеры Home Assistant на время теста.

    ⚠ Заодно чистится память постеров (`webrtc._frames`): кадр кэшируется в
    модуле, и без уборки снимок одного теста отвечал бы за камеру другого.
    """
    from mega_home import webrtc as _webrtc

    _webrtc._frames.clear()
    _webrtc._grabbing.clear()
    package = types.ModuleType("homeassistant.components.camera")
    const = types.ModuleType("homeassistant.components.camera.const")
    const.StreamType = _StreamType
    webrtc_module = types.ModuleType("homeassistant.components.camera.webrtc")
    webrtc_module.WebRTCMessage = _Message
    webrtc_module.WebRTCAnswer = _Answer
    webrtc_module.WebRTCCandidate = _Candidate
    webrtc_module.WebRTCError = _Error
    helper = types.ModuleType("homeassistant.components.camera.helper")

    cameras: dict[str, _Camera] = {}

    def get_camera_from_entity_id(hass, entity_id):
        from homeassistant.exceptions import HomeAssistantError

        if entity_id not in cameras:
            raise HomeAssistantError("Camera not found")
        return cameras[entity_id]

    helper.get_camera_from_entity_id = get_camera_from_entity_id

    class _Image:
        def __init__(self, content: bytes) -> None:
            self.content_type = "image/jpeg"
            self.content = content

    async def async_get_image(hass, entity_id, timeout=10, width=None, height=None):
        from homeassistant.exceptions import HomeAssistantError

        camera = cameras.get(entity_id)
        if camera is None or camera.frame is None:
            raise HomeAssistantError("Unable to get image")
        package.asked.append((entity_id, width))
        return _Image(camera.frame)

    package.async_get_image = async_get_image
    package.asked = []
    added = {
        "homeassistant.components.camera": package,
        "homeassistant.components.camera.const": const,
        "homeassistant.components.camera.webrtc": webrtc_module,
        "homeassistant.components.camera.helper": helper,
    }
    sys.modules.update(added)
    try:
        yield cameras
    finally:
        for name in added:
            sys.modules.pop(name, None)


@pytest.fixture(autouse=True)
def _instant_gathering(monkeypatch):
    """Окно сбора кандидатов в тесте не ждём — проверяется сбор, а не часы."""
    from mega_home import webrtc

    monkeypatch.setattr(webrtc, "CANDIDATE_WINDOW", 0.02)
    monkeypatch.setattr(webrtc, "ANSWER_TIMEOUT", 0.2)


def run(coro):
    return asyncio.run(coro)


def test_ответ_и_кандидаты_едут_одним_пакетом(_ha_camera_modules):
    camera = _Camera(
        [_Answer("v=0 answer"), _Candidate(_Ice("candidate:1 udp")), _Candidate(_Ice("candidate:2 srflx"))]
    )
    _ha_camera_modules["camera.hall"] = camera

    result = run(
        ops.run(
            object(),
            _Coordinator([CAMERA_TILE]),
            "webrtc",
            {"id": "cam1", "offer": "v=0 offer"},
        )
    )

    assert result["answer"] == "v=0 answer"
    # ⚠ Кандидаты обязаны быть В ОТВЕТЕ: go2rtc отдаёт ответ сразу, а адрес, по
    # которому его видно снаружи, присылает следом. Ответ без них — соединение,
    # которому некуда встать.
    assert result["candidates"] == [
        {"candidate": "candidate:1 udp", "sdpMLineIndex": 0},
        {"candidate": "candidate:2 srflx", "sdpMLineIndex": 0},
    ]
    assert camera.offers[0][0] == "v=0 offer"
    # Сессия жива: её закроет жилец, закрыв просмотр.
    assert camera.closed == []
    assert result["sessionId"] == camera.offers[0][1]


def test_камера_без_webrtc_отказывает_понятно(_ha_camera_modules):
    _ha_camera_modules["camera.hall"] = _Camera(webrtc=False)

    with pytest.raises(ops.OpError) as err:
        run(
            ops.run(
                object(),
                _Coordinator([CAMERA_TILE]),
                "webrtc",
                {"id": "cam1", "offer": "v=0"},
            )
        )
    assert err.value.status == HTTPStatus.NOT_IMPLEMENTED


def test_молчание_камеры_закрывает_сессию(_ha_camera_modules):
    camera = _Camera([])
    _ha_camera_modules["camera.hall"] = camera

    with pytest.raises(ops.OpError) as err:
        run(
            ops.run(
                object(),
                _Coordinator([CAMERA_TILE]),
                "webrtc",
                {"id": "cam1", "offer": "v=0"},
            )
        )
    assert err.value.status == HTTPStatus.GATEWAY_TIMEOUT
    # ⚠ Иначе go2rtc держал бы поток с камеры после каждой неудачной попытки.
    assert camera.closed == [camera.offers[0][1]]


def test_ошибка_камеры_едет_текстом_и_закрывает_сессию(_ha_camera_modules):
    camera = _Camera([_Error("go2rtc_webrtc_offer_failed", "Stream source is not supported")])
    _ha_camera_modules["camera.hall"] = camera

    with pytest.raises(ops.OpError) as err:
        run(
            ops.run(
                object(),
                _Coordinator([CAMERA_TILE]),
                "webrtc",
                {"id": "cam1", "offer": "v=0"},
            )
        )
    assert "Stream source is not supported" in err.value.message
    assert camera.closed == [camera.offers[0][1]]


def test_закрытие_просмотра_отпускает_камеру(_ha_camera_modules):
    camera = _Camera([])
    _ha_camera_modules["camera.hall"] = camera

    result = run(
        ops.run(
            object(),
            _Coordinator([CAMERA_TILE]),
            "webrtc-close",
            {"id": "cam1", "sessionId": "s7"},
        )
    )
    assert result == {"closed": True}
    assert camera.closed == ["s7"]


def test_сущность_берётся_из_состава_а_не_из_запроса(_ha_camera_modules):
    """⚠ Единственная защита от «покажи чужую камеру».

    В запросе жильца лежит id ПЛИТКИ его дома; `entity_id` в него подставить
    негде, и подставленный — не читается.
    """
    camera = _Camera([_Answer("v=0 answer")])
    _ha_camera_modules["camera.hall"] = camera
    _ha_camera_modules["camera.neighbour"] = _Camera([_Answer("чужая")])

    result = run(
        ops.run(
            object(),
            _Coordinator([CAMERA_TILE]),
            "webrtc",
            {"id": "cam1", "entityId": "camera.neighbour", "offer": "v=0"},
        )
    )
    assert result["answer"] == "v=0 answer"


def test_не_камера_и_ненайденная_плитка_отказывают(_ha_camera_modules):
    coordinator = _Coordinator(
        [CAMERA_TILE, {"id": "l1", "domain": "light", "entityId": "light.hall"}]
    )
    with pytest.raises(ops.OpError):
        run(ops.run(object(), coordinator, "webrtc", {"id": "l1", "offer": "v=0"}))
    with pytest.raises(ops.OpError) as err:
        run(ops.run(object(), coordinator, "webrtc", {"id": "нет", "offer": "v=0"}))
    assert err.value.status == HTTPStatus.NOT_FOUND


def test_предложение_обязательно(_ha_camera_modules):
    _ha_camera_modules["camera.hall"] = _Camera([])
    with pytest.raises(ops.OpError):
        run(ops.run(object(), _Coordinator([CAMERA_TILE]), "webrtc", {"id": "cam1"}))


def test_постер_отдаётся_сырыми_байтами(_ha_camera_modules):
    """⚠ Единственная картинка через менеджер, и она посчитана: ОДИН кадр на
    открытие камеры. Переговоры длятся секунды, адресов Home Assistant снаружи
    нет, и без кадра просмотр открывается чёрным прямоугольником.

    ⚠ `ops.camera_frame` отдаёт `(contentType, bytes)`, а не base64
    (2026-09-08): кодирование — забота двери, которой оно нужно
    (`relay_api.handle`), а не этого обработчика пути. До этой правки кадр
    кодировался тут же и сразу декодировался вызывающей стороной — впустую,
    на каждый кадр до 400 КБ.

    ⚠ Не именованная операция, а обработчик ПУТИ `api/camera-frame/<плитка>`:
    его зовут обе двери — локальная (`http.py`) и перенос (`relay_api.py`).
    Новых операций канала мы не заводим (design.md, 0.2.0)."""
    import sys

    _ha_camera_modules["camera.hall"] = _Camera(frame=b"\xff\xd8jpeg")

    content_type, raw = run(
        ops.camera_frame(object(), _Coordinator([CAMERA_TILE]), {"id": "cam1"})
    )
    assert raw == b"\xff\xd8jpeg"
    assert content_type == "image/jpeg"
    # Просим уменьшенный кадр: постер показывают, пока идёт соединение.
    assert sys.modules["homeassistant.components.camera"].asked == [("camera.hall", 640)]


def test_кадр_отдаётся_из_памяти_а_не_с_камеры(_ha_camera_modules):
    """⚠ Постер прикрывает секунды переговоров, а сам стоил столько же: снимок
    у камеры без снапшот-адреса поднимает ffmpeg и ждёт ключевого кадра. Второй
    заход обязан отвечать из памяти — иначе просмотр открывается пустым."""
    import sys

    hass = _Hass()
    _ha_camera_modules["camera.hall"] = _Camera(frame=b"\xff\xd8jpeg")

    first = run(ops.camera_frame(hass, _Coordinator([CAMERA_TILE]), {"id": "cam1"}))
    second = run(ops.camera_frame(hass, _Coordinator([CAMERA_TILE]), {"id": "cam1"}))

    assert second[1] == first[1]
    # Камеру дёрнули РОВНО раз, второй кадр пришёл из памяти.
    assert sys.modules["homeassistant.components.camera"].asked == [("camera.hall", 640)]
    # И свежий кадр не тянет за собой фоновое обновление.
    assert hass.tasks == []


def test_опрос_состояний_греет_кадр_заранее(_ha_camera_modules):
    """⚠ Открытое приложение — единственный признак «камеру сейчас откроют»,
    который есть у дома. К моменту открытия кадр обязан уже лежать, иначе
    жилец смотрит на пустоту ровно столько, сколько идут переговоры."""
    import sys

    hass = _Hass()
    _ha_camera_modules["camera.hall"] = _Camera(frame=b"\xff\xd8jpeg")

    ops.states(hass, _Coordinator([CAMERA_TILE]))
    assert len(hass.tasks) == 1
    run(hass.tasks[0])

    content_type, raw = run(ops.camera_frame(hass, _Coordinator([CAMERA_TILE]), {"id": "cam1"}))

    assert raw == b"\xff\xd8jpeg"
    # Кадр снят ОДИН раз — заранее; открытие камеры не стоило похода к ней.
    assert sys.modules["homeassistant.components.camera"].asked == [("camera.hall", 640)]


def test_слишком_большой_кадр_отклоняется(_ha_camera_modules, monkeypatch):
    """⚠ Кадр едет кадром вебсокета до менеджера: переросший предел не
    обрезается, а ЗАКРЫВАЕТ канал — объект ушёл бы в офлайн от одного нажатия."""
    from mega_home import webrtc

    monkeypatch.setattr(webrtc, "MAX_SNAPSHOT_BYTES", 4)
    _ha_camera_modules["camera.hall"] = _Camera(frame=b"too long a frame")

    with pytest.raises(ops.OpError) as err:
        run(ops.camera_frame(object(), _Coordinator([CAMERA_TILE]), {"id": "cam1"}))
    assert err.value.status == HTTPStatus.REQUEST_ENTITY_TOO_LARGE


def test_камера_без_кадра_отказывает_понятно(_ha_camera_modules):
    _ha_camera_modules["camera.hall"] = _Camera()
    with pytest.raises(ops.OpError) as err:
        run(ops.camera_frame(object(), _Coordinator([CAMERA_TILE]), {"id": "cam1"}))
    assert err.value.message == "Камера не отдала кадр"


# ─── Свой go2rtc (:8555) — активный путь с 0.2.9 ─────────────────────────────


def _own_negotiate(ws_messages):
    """Прогнать переговоры через свой go2rtc и вернуть (ответ, ws)."""

    async def scenario():
        task = asyncio.ensure_future(
            ops.run(
                object(), _Coordinator([CAMERA_TILE]), "webrtc", {"id": "cam1", "offer": "v=0 offer"}
            )
        )
        # Всё в подменах разрешается без уступки циклу: за один sleep(0)
        # переговоры доходят до ожидания ответа, ws уже создан.
        await asyncio.sleep(0)
        ws = _Go2RtcWsClient.instances[-1]
        for message in ws_messages:
            ws.receive(message)
        return await task, ws

    return run(scenario())


def test_свой_go2rtc_кандидаты_едут_с_mline(_ha_camera_modules, own_go2rtc):
    """⚠ Спека W3C: непустой кандидат без sdpMid и sdpMLineIndex — TypeError
    из addIceCandidate, и фронтенд глотает отказ КАЖДОГО кандидата как «минус
    один путь». Без m-line это «минус ВСЕ пути»: ICE не вставал никогда, и
    именно поэтому переговоры проходили, а видео — нет. Ноль — видео-секция
    оффера; так нормализует и сам HA (RTCIceCandidateInit), и фронтенд go2rtc
    (sdpMid: '0')."""
    _ha_camera_modules["camera.hall"] = _OwnCamera()

    result, ws = _own_negotiate(
        [_GoAnswer("v=0 answer"), _GoCandidate("candidate:1 UDP typ srflx")]
    )

    assert result["answer"] == "v=0 answer"
    assert result["candidates"] == [
        {"candidate": "candidate:1 UDP typ srflx", "sdpMLineIndex": 0}
    ]
    # Поток добавлен своему go2rtc: generic-камера едет с ffmpeg-префиксом,
    # как это делает провайдер HA.
    assert _Go2RtcRestClient.instances[-1].streams.added == [
        ("camera.hall", ["ffmpeg:rtsp://cam/stream"])
    ]
    # Сессия зарегистрирована: живая ws обязана пережить запрос.
    from mega_home import webrtc

    # (когда открыта, клиент): срок нужен, чтобы забытая сессия не держала
    # камеру вечно — телефон с убитым приложением `close` не пришлёт никогда.
    assert webrtc._own_sessions[result["sessionId"]][1] is ws


def test_отказ_своего_go2rtc_не_подменяется_фолбэком(_ha_camera_modules, own_go2rtc, monkeypatch):
    """⚠ Отказ go2rtc раньше глотался как «own не используется» и подменялся
    попыткой HA-провайдера, которая на доме без go2rtc в HA врала «камера не
    умеет WebRTC» — настройщик чинил не то."""
    camera = _OwnCamera()
    _ha_camera_modules["camera.hall"] = camera
    monkeypatch.setattr(_Go2RtcRestClient, "fail_next", True)

    with pytest.raises(ops.OpError) as err:
        run(
            ops.run(
                object(), _Coordinator([CAMERA_TILE]), "webrtc", {"id": "cam1", "offer": "v=0"}
            )
        )
    assert err.value.message == "Дом не смог начать трансляцию с этой камеры"
    # К HA-провайдеру не отходили вовсе.
    assert camera.offers == []


def test_ошибка_своего_go2rtc_закрывает_ws(_ha_camera_modules, own_go2rtc):
    """Сессия не должна переживать отказ: go2rtc держал бы поток с камеры."""
    _ha_camera_modules["camera.hall"] = _OwnCamera()

    with pytest.raises(ops.OpError) as err:
        _own_negotiate([_GoWsError("Stream is busy")])
    assert "Stream is busy" in err.value.message

    from mega_home import webrtc

    assert not webrtc._own_sessions


def test_закрытие_просмотра_закрывает_сессию_своего_go2rtc(_ha_camera_modules, own_go2rtc):
    """⚠ `close_webrtc_session` камеры про сессии своего go2rtc не знает: без
    реестра «закрыл шторку» никогда не отпускал камеру — go2rtc держал
    RTSP-поток до перезапуска HA."""
    _ha_camera_modules["camera.hall"] = _OwnCamera()
    result, ws = _own_negotiate(
        [_GoAnswer("v=0 answer"), _GoCandidate("candidate:1 typ host")]
    )

    from mega_home import webrtc

    async def scenario():
        webrtc.close(_OwnHass(), "camera.hall", result["sessionId"])
        # close() планирует закрытие — даём задаче выполниться.
        await asyncio.sleep(0.01)

    run(scenario())
    assert ws.closed
    assert not webrtc._own_sessions
    # HA-камеру не трогали: сессия была не её.
    assert _ha_camera_modules["camera.hall"].closed == []


# ─── Ранний выход из окна кандидатов ─────────────────────────────────────────
#
# ⚠ Зеркало `GATHER_GRACE_MS` браузера: ответ go2rtc приходит сразу, а его
# srflx — следом за обменом со STUN. Ждать всё окно ради опоздавших — лишняя
# секунда переговоров на КАЖДОЕ открытие камеры.


def test_ответ_не_ждёт_всё_окно_когда_srflx_уже_есть(_ha_camera_modules, monkeypatch):
    """Ответ + srflx: пакет уходит после grace, а не через всё окно."""
    from time import monotonic

    from mega_home import webrtc

    monkeypatch.setattr(webrtc, "CANDIDATE_WINDOW", 5.0)
    monkeypatch.setattr(webrtc, "CANDIDATE_GRACE", 0.1)

    class _SlowCamera(_Camera):
        async def async_handle_async_webrtc_offer(self, offer_sdp, session_id, send_message):
            async def replay() -> None:
                send_message(_Answer("v=0 answer"))
                await asyncio.sleep(0.05)
                send_message(_Candidate(_Ice("candidate:1 udp typ srflx")))

            asyncio.ensure_future(replay())

    _ha_camera_modules["camera.hall"] = _SlowCamera()

    start = monotonic()
    result = run(
        ops.run(
            object(),
            _Coordinator([CAMERA_TILE]),
            "webrtc",
            {"id": "cam1", "offer": "v=0 offer"},
        )
    )
    elapsed = monotonic() - start

    assert result["candidates"] == [
        {"candidate": "candidate:1 udp typ srflx", "sdpMLineIndex": 0}
    ]
    # Со старым `sleep(CANDIDATE_WINDOW)` здесь было бы 5 секунд.
    assert elapsed < 2.0


def test_без_srflx_ждём_всё_окно_как_раньше(_ha_camera_modules, monkeypatch):
    """Только host: внешнего адреса нет — ждём всё окно, опоздавшие важны."""
    from time import monotonic

    from mega_home import webrtc

    monkeypatch.setattr(webrtc, "CANDIDATE_WINDOW", 0.3)

    camera = _Camera(
        [_Answer("v=0 answer"), _Candidate(_Ice("candidate:1 udp typ host"))]
    )
    _ha_camera_modules["camera.hall"] = camera

    start = monotonic()
    result = run(
        ops.run(
            object(),
            _Coordinator([CAMERA_TILE]),
            "webrtc",
            {"id": "cam1", "offer": "v=0 offer"},
        )
    )
    elapsed = monotonic() - start

    assert result["candidates"] == [
        {"candidate": "candidate:1 udp typ host", "sdpMLineIndex": 0}
    ]
    assert elapsed >= 0.3


def test_свой_go2rtc_не_ждёт_всё_окно(_ha_camera_modules, own_go2rtc, monkeypatch):
    """Тот же ранний выход на активном пути своего go2rtc."""
    from time import monotonic

    from mega_home import webrtc

    monkeypatch.setattr(webrtc, "CANDIDATE_WINDOW", 5.0)
    monkeypatch.setattr(webrtc, "CANDIDATE_GRACE", 0.1)
    _ha_camera_modules["camera.hall"] = _OwnCamera()

    async def scenario():
        task = asyncio.ensure_future(
            ops.run(
                object(),
                _Coordinator([CAMERA_TILE]),
                "webrtc",
                {"id": "cam1", "offer": "v=0 offer"},
            )
        )
        await asyncio.sleep(0)
        ws = _Go2RtcWsClient.instances[-1]
        ws.receive(_GoAnswer("v=0 answer"))
        await asyncio.sleep(0.05)
        ws.receive(_GoCandidate("candidate:1 UDP typ srflx"))
        return await task

    start = monotonic()
    result = run(scenario())
    elapsed = monotonic() - start

    assert result["candidates"] == [
        {"candidate": "candidate:1 UDP typ srflx", "sdpMLineIndex": 0}
    ]
    assert elapsed < 2.0


def test_без_внешнего_адреса_ждём_дольше_только_снаружи() -> None:
    """⚠ Ответ без `srflx` телефону СНАРУЖИ бесполезен — ему некуда идти.

    Живой отчёт с объекта 2026-09-09: «Кандидаты дома: host 2», ICE навсегда в
    `checking`, переговоры 2644 мс — короткое окно истекло, пока go2rtc холодным
    ходил к STUN, и дом ответил одними host-кандидатами. Повторное открытие той
    же камеры проходило нормально.

    ⚠ Дома ждать нечего: телефон в той же сети, host-кандидатов ему довольно, —
    поэтому длинное окно только для переноса.
    """
    from mega_home.webrtc import CANDIDATE_WINDOW, CANDIDATE_WINDOW_COLD

    assert CANDIDATE_WINDOW_COLD > CANDIDATE_WINDOW


def test_окно_кандидатов_выбирается_дверью() -> None:
    import asyncio as aio

    from mega_home import webrtc

    async def scenario(remote: bool) -> float:
        started = aio.get_event_loop().time()
        # Готовность не наступает никогда: меряем, каким окном нас оборвало.
        await webrtc._wait_candidates(aio.Event(), lambda: False, remote)  # noqa: SLF001
        return aio.get_event_loop().time() - started

    import pytest as _pytest

    monkey = _pytest.MonkeyPatch()
    monkey.setattr(webrtc, "CANDIDATE_WINDOW", 0.05)
    monkey.setattr(webrtc, "CANDIDATE_WINDOW_COLD", 0.25)
    try:
        assert aio.run(scenario(False)) < 0.2, "дома — короткое окно"
        assert aio.run(scenario(True)) > 0.2, "снаружи — длинное"
    finally:
        monkey.undo()
