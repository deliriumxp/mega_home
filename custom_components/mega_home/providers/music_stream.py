"""Прямая ссылка на трек Яндекс Музыки — для плеера дома.

Ссылку выдаёт дом: музыкальный токен живёт только у него. Приложение получает готовый
адрес и отдаёт его плееру существующей командой `play_stream` (`media_player.play_media`).

⚠ Устроено как в провайдере Music Assistant 2.10.12 (`providers/yandex_music/streaming.py`):
сначала `/get-file-info` с `transports=raw` — он отдаёт НЕзашифрованную ссылку, которую
плеер берёт сам. Если Яндекс ответил зашифрованной ссылкой (в ответе есть `key`) или не
ответил, берём `download-info` — прямую ссылку на файл. AES-CTR поток не годится: его
пришлось бы расшифровывать дому, а плеер ждёт обычный URL.

⚠ Предпочитаем MP3: он играет у всех плееров парка. AAC — запасной вариант.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import re
import time
from typing import Any

from yandex_music.exceptions import UnauthorizedError, YandexMusicError
from yandex_music.utils.sign_request import DEFAULT_SIGN_KEY

from ..core.ops_base import OpError

TRACK_ID = re.compile(r"[0-9]{1,20}")
TRANSPORT = "raw"
# Лучший MP3 (320 кбит/с) — как «высокое» качество MA. Если его нет — обычное.
QUALITY = "lossless"
CODECS = "mp3,aac-mp4,aac,he-aac"
FALLBACK_QUALITY = "nq"
FALLBACK_CODECS = "mp3,aac-mp4,aac,he-aac"

# Плеер понимает только `mp3` и `aac` (`core/radio-play.ts`). FLAC и прочее не берём:
# не вся парк его умеет, а контейнер ещё и требует своего MIME.
PLAYABLE = ("mp3", "mpeg", "aac", "aac-mp4", "he-aac", "he-aac-mp4")


async def stream(client: Any, request: dict) -> dict:
    """Ответ на действие `stream`: `{url, format}` или отказ, который прочитает жилец."""
    track_id = request.get("id")
    if not isinstance(track_id, str) or not TRACK_ID.fullmatch(track_id):
        raise OpError("Неверный адрес трека")

    # Два захода `/get-file-info`: сначала лучшее качество, затем обычное. Ошибку
    # первого не показываем — второй заход часто отвечает там, где первый отказал.
    for quality, codecs in ((QUALITY, CODECS), (FALLBACK_QUALITY, FALLBACK_CODECS)):
        try:
            link = await _file_info(client, track_id, quality, codecs)
        except UnauthorizedError:
            raise
        except (YandexMusicError, TimeoutError, ValueError, OSError):
            link = None
        if link:
            return link

    link = await _download_info(client, track_id)
    if link:
        return link
    raise OpError("Яндекс Музыка не отдала поток трека. Попробуйте ещё раз", 503)


async def _file_info(client: Any, track_id: str, quality: str, codecs: str) -> dict | None:
    """`/get-file-info` с `transports=raw` — обычная ссылка без расшифровки."""
    params = signed_params(track_id, int(time.time()), quality, codecs)
    result = await client._request.get(f"{client.base_url}/get-file-info", params=params)
    if not isinstance(result, dict):
        return None
    # yandex-music 3 больше не приводит ключи внутри `result` к snake_case.
    info = result.get("download_info") or result.get("downloadInfo")
    if not isinstance(info, dict) or info.get("key"):
        return None
    url = info.get("url")
    if not isinstance(url, str) or not url.startswith(("https://", "http://")):
        return None
    return {"url": url, "format": _format(info.get("codec"))}


async def _download_info(client: Any, track_id: str) -> dict | None:
    """Запасной путь MA: `download-info` отдаёт прямую ссылку на файл (~60 секунд)."""
    try:
        infos = await client.tracks_download_info(track_id, get_direct_links=True)
    except UnauthorizedError:
        raise
    except (YandexMusicError, TimeoutError, ValueError, OSError):
        return None
    usable = [info for info in infos or [] if _playable(info)]
    if not usable:
        return None
    chosen = max(usable, key=lambda info: (info.codec.lower() in ("mp3", "mpeg"), info.bitrate_in_kbps or 0))
    url = getattr(chosen, "direct_link", None)
    if not isinstance(url, str) or not url:
        return None
    return {"url": url, "format": _format(chosen.codec)}


def _playable(info: Any) -> bool:
    codec = getattr(info, "codec", None)
    return isinstance(codec, str) and codec.lower() in PLAYABLE and bool(getattr(info, "direct_link", None))


def _format(codec: Any) -> str:
    name = codec.lower() if isinstance(codec, str) else ""
    return "aac" if name.startswith(("aac", "he-aac")) else "mp3"


def signed_params(track_id: str, timestamp: int, quality: str, codecs: str) -> dict[str, Any]:
    """Параметры подписанного запроса `/get-file-info` (правило MA, `api_client.py`)."""
    compact = codecs.replace(",", "")
    message = f"{timestamp}{track_id}{quality}{compact}{TRANSPORT}"
    sign = hmac.new(DEFAULT_SIGN_KEY.encode(), message.encode(), hashlib.sha256)
    return {
        "ts": timestamp,
        "trackId": track_id,
        "quality": quality,
        "codecs": codecs,
        "transports": TRANSPORT,
        # Yandex ждёт 43 символа: base64 без одного «=».
        "sign": base64.b64encode(sign.digest()).decode()[:-1],
    }
