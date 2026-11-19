"""Привязка на дом, чтение гостя и восстановление токенов."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest

pytest.importorskip("ya_passport_auth")
pytest.importorskip("yandex_music")

from mega_home.core.host import PlainHost
from mega_home.core.ops_base import OpError
from mega_home.providers import yandex_music as music


def _credentials() -> music.Credentials:
    return music.Credentials(
        x_token=music.SecretStr("passport-token"),
        music_token=music.SecretStr("music-token"),
        refresh_token=music.SecretStr("refresh-token"),
        display_login="семья",
    )


def test_вход_общий_для_семьи_и_гостей_но_изолирован_по_домам(monkeypatch, tmp_path):
    class Passport:
        @staticmethod
        @asynccontextmanager
        async def create(**_):
            yield Passport()

        async def start_device_login(self, **_):
            return SimpleNamespace(user_code="kod", verification_url="https://ya.ru/device", expires_in=300)

        async def poll_device_until_confirmed(self, _device):
            return _credentials()

    class Client:
        def __init__(self, token):
            assert token == "music-token"

        async def init(self):
            return self

        async def search(self, query, **_):
            assert query == "Кино"
            track = SimpleNamespace(id=1, title="Звезда", artists=[SimpleNamespace(name="Кино")])
            return SimpleNamespace(tracks=SimpleNamespace(results=[track], per_page=10, total=1))

    monkeypatch.setattr(music, "PassportClient", Passport)
    monkeypatch.setattr(music, "ClientAsync", Client)

    async def run():
        env = PlainHost(tmp_path)
        home = music.MusicService(env, "one")
        other = music.MusicService(env, "two")
        start = await home.call(True, {"action": "start"})
        assert start["state"] == "pending" and start["code"] == "kod"
        guest = await home.call(False, {"action": "status"})
        assert guest == {"state": "disconnected", "canManage": False}
        with pytest.raises(OpError) as denied:
            await home.call(False, {"action": "cancel", "canManage": True})
        assert denied.value.status == 403
        attempt = home._attempt
        assert attempt is not None and attempt.task is not None
        await attempt.task

        # Как после перезапуска: читается та же привязка дома с диска.
        home = music.MusicService(env, "one")
        assert await home.call(True, {"action": "status"}) == {"state": "connected", "login": "семья", "canManage": True}
        assert await home.call(False, {"action": "status"}) == {"state": "connected", "login": None, "canManage": False}
        guest_catalog = await home.call(False, {"action": "search", "query": "Кино", "kind": "track"})
        assert guest_catalog == {"items": [{"id": "1", "title": "Звезда", "kind": "track", "artist": "Кино"}], "more": False}
        assert await other.call(True, {"action": "status"}) == {"state": "disconnected", "canManage": True}
        assert (tmp_path / "mega_home_yandex_music.one").stat().st_mode & 0o777 == 0o600
        with pytest.raises(OpError) as denied:
            await home.call(False, {"action": "disconnect"})
        assert denied.value.status == 403
        await home.call(True, {"action": "disconnect"})
        assert (await home.call(False, {"action": "status"}))["state"] == "disconnected"
        await env.close()

    asyncio.run(run())


def test_временный_отказ_сохраняет_привязку_и_обновление_токена_одно(monkeypatch, tmp_path):
    from yandex_music.exceptions import NetworkError

    refreshed = 0

    class Passport:
        @staticmethod
        @asynccontextmanager
        async def create(**_):
            yield Passport()

        async def refresh_music_token(self, _):
            nonlocal refreshed
            refreshed += 1
            return music.SecretStr("new-token")

    class Client:
        def __init__(self, token):
            self.token = token

        async def init(self):
            if self.token == "music-token":
                raise music.UnauthorizedError("старый")
            return self

        async def search(self, *_args, **_kwargs):
            raise NetworkError("Unavailable For Legal Reasons (451)")

    monkeypatch.setattr(music, "PassportClient", Passport)
    monkeypatch.setattr(music, "ClientAsync", Client)

    async def run():
        env = PlainHost(tmp_path)
        service = music.MusicService(env, "home")
        await music._save(env.store("mega_home_yandex_music.home", private=True), _credentials())
        query = {"action": "search", "query": "Кино", "kind": "track"}
        answers = await asyncio.gather(service.call(True, query), service.call(False, query), return_exceptions=True)
        assert all(isinstance(answer, OpError) and answer.status == 503 and "из сети дома" in str(answer) for answer in answers)
        assert refreshed == 1
        assert (await service.call(True, {"action": "status"}))["state"] == "connected"
        await env.close()

    asyncio.run(run())


def test_плейлисты_объединяются_и_их_треки_читаются_страницами():
    from mega_home.providers.music_catalog import catalog, PAGE_SIZE

    own = SimpleNamespace(owner=SimpleNamespace(uid=42), kind=1, title="Свой")
    saved = SimpleNamespace(owner=SimpleNamespace(uid=21), kind=1, title="Сохранённый")
    track = SimpleNamespace(id=11, title="Трек", artists=[])

    class Client:
        async def users_playlists_list(self):
            return [own]

        async def users_likes_playlists(self):
            return [SimpleNamespace(playlist=own), SimpleNamespace(playlist=saved)]

        async def users_playlists(self, kind, user_id):
            assert (user_id, kind) == ("21", 1)
            return SimpleNamespace(tracks=[SimpleNamespace(track=track)] * (PAGE_SIZE + 1))

        async def users_likes_tracks(self):
            return SimpleNamespace(tracks=[SimpleNamespace(track_id="11", timestamp=None)])

        async def tracks(self, ids):
            assert ids == ["11"]
            return [track]

    async def run():
        client = Client()
        items = (await catalog(client, "playlists", {}))["items"]
        assert [item["id"] for item in items] == ["42:1", "21:1"]
        first = await catalog(client, "playlist", {"id": "21:1", "page": 0})
        assert len(first["items"]) == PAGE_SIZE and first["more"]
        second = await catalog(client, "playlist", {"id": "21:1", "page": 1})
        assert len(second["items"]) == 1 and not second["more"]
        assert await catalog(client, "liked", {}) == second

    asyncio.run(run())


def test_поток_трека_отдаёт_варианты_библиотеки(monkeypatch, tmp_path):
    """Свой подписанный запрос не нужен: варианты даёт клиент `yandex_music`."""

    class Client:
        def __init__(self, _token):
            pass

        async def init(self):
            return self

        async def tracks_download_info(self, track_id):
            assert track_id == "123"

            async def aac():
                return "https://cdn.yandex/track.aac"

            async def mp3():
                return "https://cdn.yandex/track.mp3"

            async def broken():
                from yandex_music.exceptions import NetworkError

                raise NetworkError("CDN отказал")

            return [
                SimpleNamespace(codec="aac", bitrate_in_kbps=192, get_direct_link_async=aac),
                SimpleNamespace(codec="flac", bitrate_in_kbps=700, get_direct_link_async=broken),
                SimpleNamespace(codec="mp3", bitrate_in_kbps=320, get_direct_link_async=mp3),
            ]

    monkeypatch.setattr(music, "ClientAsync", Client)

    async def run():
        env = PlainHost(tmp_path)
        service = music.MusicService(env, "home")
        await music._save(env.store("mega_home_yandex_music.home", private=True), _credentials())
        answer = await service.call(True, {"action": "stream", "id": "123"})
        assert answer == {
            "infos": [
                {"url": "https://cdn.yandex/track.aac", "codec": "aac", "bitrate": 192},
                {"url": "https://cdn.yandex/track.mp3", "codec": "mp3", "bitrate": 320},
            ],
            # Прежний вид ответа: старый бандл читает `url`/`format`, а `infos` не знает.
            "url": "https://cdn.yandex/track.mp3",
            "format": "mp3",
        }
        await env.close()

    asyncio.run(run())


def test_выбор_варианта_и_формата_для_сценария():
    from mega_home.providers.music_pick import pick_stream, stream_format

    infos = [
        {"url": "https://cdn/hi.flac", "codec": "flac", "bitrate": 900},
        {"url": "https://cdn/aac.aac", "codec": "aac", "bitrate": 192},
        {"url": "https://cdn/mp3.mp3", "codec": "mp3", "bitrate": 320},
    ]
    assert pick_stream(infos)["url"] == "https://cdn/mp3.mp3"
    assert pick_stream([info for info in infos if info["codec"] != "mp3"])["url"] == "https://cdn/aac.aac"
    assert stream_format("aac-mp4") == "aac"
    assert stream_format("mp3") == "mp3"


def test_поток_трека_отклоняет_чужой_адрес():
    from mega_home.providers import music_stream

    async def run():
        with pytest.raises(OpError) as refused:
            await music_stream.stream(SimpleNamespace(), {"id": "not-a-track"})
        assert refused.value.status == 400

    asyncio.run(run())


def test_альбом_и_исполнитель_отдают_треки_страницами():
    """Сценариям нужны не только плейлисты: альбом и исполнитель — те же страницы."""
    from mega_home.providers.music_catalog import PAGE_SIZE, catalog

    track = SimpleNamespace(id=7, title="Трек", artists=[SimpleNamespace(name="Кино")])

    class Client:
        async def albums_with_tracks(self, album_id):
            assert album_id == "42"
            return SimpleNamespace(volumes=[[track] * (PAGE_SIZE + 1)])

        async def artists_tracks(self, artist_id, page, page_size):
            assert (artist_id, page, page_size) == ("9", 0, PAGE_SIZE)
            return SimpleNamespace(tracks=[track] * PAGE_SIZE, pager=SimpleNamespace(total=PAGE_SIZE + 1))

    async def run():
        client = Client()
        first = await catalog(client, "album", {"id": "42", "page": 0})
        assert len(first["items"]) == PAGE_SIZE and first["more"]
        second = await catalog(client, "album", {"id": "42", "page": 1})
        assert len(second["items"]) == 1 and not second["more"]
        artist = await catalog(client, "artist", {"id": "9", "page": 0})
        assert len(artist["items"]) == PAGE_SIZE and artist["more"]

    asyncio.run(run())


def test_обложка_альбом_и_длительность_доезжают_до_плеера():
    """Вкладка «Плеер» показывает композицию: обложка, альбом и длительность — из каталога."""
    from mega_home.providers.music_catalog import _item

    track = SimpleNamespace(
        id=5,
        title="Песня",
        artists=[SimpleNamespace(name="Кино")],
        cover_uri="avatars.yandex.net/get-music-content/1/2/%%",
        albums=[SimpleNamespace(id=77, title="Звезда", original_release_year=1990, genre="rock")],
        duration_ms=180_000,
        explicit=True,
        lyrics_available=True,
        content_warning="explicit",
        version="Remaster",
    )
    assert _item(track, "track") == {
        "id": "5",
        "title": "Песня",
        "kind": "track",
        "artist": "Кино",
        "coverUri": "avatars.yandex.net/get-music-content/1/2/%%",
        # Прежнее поле для бандлов до 0.6.34: без него обложка не грузится.
        "cover": "https://avatars.yandex.net/get-music-content/1/2/400x400",
        "album": "Звезда",
        "albumId": "77",
        "year": 1990,
        "genre": "rock",
        "durationMs": 180_000,
        "explicit": True,
        "lyricsAvailable": True,
        "contentWarning": "explicit",
        "version": "Remaster",
    }


def test_отключение_не_ждёт_поиска_и_старый_ответ_не_уходит_клиенту(monkeypatch, tmp_path):
    async def run():
        searching, continue_search = asyncio.Event(), asyncio.Event()

        class Client:
            def __init__(self, _token):
                pass

            async def init(self):
                return self

            async def search(self, *_args, **_kwargs):
                searching.set()
                await continue_search.wait()
                return None

        monkeypatch.setattr(music, "ClientAsync", Client)
        env = PlainHost(tmp_path)
        service = music.MusicService(env, "home")
        await music._save(env.store("mega_home_yandex_music.home", private=True), _credentials())
        pending = asyncio.create_task(service.call(False, {"action": "search", "query": "Кино", "kind": "track"}))
        await searching.wait()
        answer = await asyncio.wait_for(service.call(True, {"action": "disconnect"}), 1)
        assert answer["state"] == "disconnected"
        continue_search.set()
        with pytest.raises(OpError) as exc:
            await pending
        assert exc.value.status == 412
        await env.close()

    asyncio.run(run())


def test_отказ_музыкального_api_после_выдачи_токена_сохраняет_привязку(monkeypatch, tmp_path):
    class Passport:
        @staticmethod
        @asynccontextmanager
        async def create(**_):
            yield Passport()

        async def refresh_music_token(self, _):
            return music.SecretStr("new-token")

    class Client:
        def __init__(self, _token):
            pass

        async def init(self):
            return self

        async def search(self, *_args, **_kwargs):
            raise music.UnauthorizedError("api refused")

    monkeypatch.setattr(music, "PassportClient", Passport)
    monkeypatch.setattr(music, "ClientAsync", Client)

    async def run():
        env = PlainHost(tmp_path)
        service = music.MusicService(env, "home")
        await music._save(env.store("mega_home_yandex_music.home", private=True), _credentials())
        with pytest.raises(OpError) as exc:
            await service.call(False, {"action": "search", "query": "Кино", "kind": "track"})
        assert exc.value.status == 503
        assert (await service.call(True, {"action": "status"}))["state"] == "connected"
        await env.close()

    asyncio.run(run())
