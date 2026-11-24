"""Поток AAC/ADTS через FFmpeg для нативного выхода Sonos S2."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from contextlib import suppress


async def stream_aac(
    binary: str, source: Callable[[Callable[[bytes], Awaitable[None]]], Awaitable[None]],
    write: Callable[[bytes], Awaitable[None]],
) -> None:
    # Ограничиваем анализ входа: MP3 идёт в реальном времени, а заголовки кадра известны.
    process = await asyncio.create_subprocess_exec(
        binary, "-f", "mp3", "-probesize", "576", "-analyzeduration", "1",
        "-i", "pipe:", "-c:a", "aac", "-b:a", "160k", "-f", "adts",
        "-loglevel", "error", "pipe:1", stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL,
    )
    assert process.stdin is not None and process.stdout is not None

    async def feed(chunk: bytes) -> None:
        process.stdin.write(chunk)
        await process.stdin.drain()

    async def input_stream() -> None:
        try:
            await source(feed)
        finally:
            process.stdin.close()

    input_task = asyncio.create_task(input_stream())
    try:
        while chunk := await process.stdout.read(8192):
            await write(chunk)
        if not input_task.done():
            raise OSError("FFmpeg закрыл AAC-выход")
        await input_task
        if await process.wait():
            raise OSError("FFmpeg не смог закодировать AAC")
    finally:
        input_task.cancel()
        await asyncio.gather(input_task, return_exceptions=True)
        if process.returncode is None:
            with suppress(ProcessLookupError):
                process.kill()
        await process.wait()
