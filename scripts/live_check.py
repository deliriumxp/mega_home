"""Боевой прогон НАСТОЯЩЕГО кода интеграции против НАСТОЯЩЕГО регистратора.

Ни заглушек, ни пересказа: берём `trassir_client` и `recorder` как есть и
проходим ими весь путь архива — так же, как их зовёт дом.

⚠ ЗАЧЕМ ЭТО ЕСТЬ. Юнит-спеки проверяют наш код против НАШИХ представлений о
регистраторе, и ровно там прячется целый класс беды: заглушка отдавала тело
ответа одним куском, а живой поток отдаёт первым куском — и дверь месяцами
возвращала бандлу два байта вместо календаря (2026-09-13). Симптом у жильца был
«то показывает, то нет», а стоил он трёх выпусков и вечера проверок НА ОБЪЕКТЕ.
Этот скрипт закрывает разрыв: он гоняет тот же код по живому регистратору и
говорит «да/нет» за полминуты.

    TRASSIR_HOST=192.168.88.200 TRASSIR_USER=Admin TRASSIR_PASS=... \
    TRASSIR_SDK=... python3 scripts/live_check.py

⚠ Учётка берётся из окружения и в репозитории её нет. Прогонять стоит перед
каждым выпуском, который трогает `trassir_*` или `recorder.py`.
"""
import asyncio, importlib, sys, types, json, time

КОРЕНЬ = __import__('pathlib').Path(__file__).resolve().parent.parent
sys.path.insert(0, str(КОРЕНЬ / 'custom_components'))
pkg = types.ModuleType('mega_home'); pkg.__path__ = [str(КОРЕНЬ / 'custom_components' / 'mega_home')]
sys.modules['mega_home'] = pkg
TC = importlib.import_module('mega_home.trassir_client')
RC = importlib.import_module('mega_home.recorder')
import aiohttp

import os

HOST = os.environ.get('TRASSIR_HOST', '')
PORT = int(os.environ.get('TRASSIR_PORT', '8080'))
RTSP = int(os.environ.get('TRASSIR_RTSP', '555'))
USER = os.environ.get('TRASSIR_USER', '')
PASS = os.environ.get('TRASSIR_PASS', '')
SDK = os.environ.get('TRASSIR_SDK', '')
if not HOST or not USER:
    sys.exit('Задайте TRASSIR_HOST / TRASSIR_USER / TRASSIR_PASS / TRASSIR_SDK')
ОПИСАНИЕ = {
    'id': 'trassir', 'vendor': 'trassir', 'host': HOST, 'scheme': 'https',
    'port': PORT, 'rtspPort': RTSP, 'login': '/login',
    'loginParams': {'username': '{user}', 'password': '{pass}'},
    'sessionField': 'sid', 'streamPath': '/get_video',
    'streamParams': {'channel': '{camera}', 'stream': '{quality}', 'container': 'rtsp'},
    'streamField': 'token', 'streamUrl': 'rtsp://{host}:{rtspPort}/{token}',
    'sessionTtl': 600, 'sessionExpired': 'no session',
}
итог = []
def факт(имя, ok, деталь=''):
    итог.append((ok, имя, деталь))
    print(('  ✓ ' if ok else '  ✗ ') + имя + (f' — {деталь}' if деталь else ''))

async def потребитель(token, секунд=25):
    """go2rtc в доме читает поток; здесь — тот же читатель, но простой."""
    s = aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False))
    async def качать():
        try:
            async with s.get(f'http://{HOST}:{RTSP}/{token}',
                             timeout=aiohttp.ClientTimeout(total=секунд)) as r:
                await r.content.read(200_000)
        except Exception:
            pass
        finally:
            await s.close()
    return asyncio.create_task(качать())

async def main():
    session = aiohttp.ClientSession()
    client = TC.TrassirClient(session, HOST, PORT, USER, PASS, SDK)
    door = RC.RecorderCall(sid_provider=lambda fresh=False: client.async_sid(fresh=fresh))
    door.apply([ОПИСАНИЕ])
    try:
        каналы = await client.async_channels()
        факт('channels драйвером', len(каналы) > 0, f'{len(каналы)} камер')
        guid = каналы[0]['guid']

        token = await client.async_get_video(guid, 'archive_main', 'rtsp')
        факт('get_video архива', bool(token), token)
        задача = await потребитель(token)
        await asyncio.sleep(2)

        # Дверь той же сессией — то, ради чего 0.2.49 и делалась.
        st, _, тело = await door.call(None, 'GET', '/archive_status', {'type': 'calendar'})
        дни = [r for r in json.loads(тело) if r.get('token') == token]
        факт('дверь видит КАЛЕНДАРЬ потока драйвера',
             bool(дни) and len(дни[0].get('calendar', [])) > 0,
             f'{len(дни[0]["calendar"]) if дни else 0} дней, {len(тело)} байт ответа')

        день = sorted(дни[0]['calendar'])[-1] if дни else ''
        import calendar as cal
        нач = cal.timegm(time.strptime(день + ' 00:00:00', '%Y-%m-%d %H:%M:%S')) * 1_000_000
        ответ = await client.async_archive_command(
            token, command='play', start=нач, stop=нач + 86_400_000_000, speed=1)
        факт('play суточным окном', ответ.get('success') in (1, '1'),
             ответ.get('first_frame_ts', ''))
        await asyncio.sleep(1)

        st, _, тело = await door.call(None, 'GET', '/archive_status', {'type': 'timeline'})
        линия = [r for r in json.loads(тело) if r.get('token') == token]
        факт('дверь видит ШКАЛУ суток', bool(линия) and len(линия[0].get('timeline', [])) > 0,
             f'{len(линия[0]["timeline"]) if линия else 0} участков, день {линия[0]["day_start"] if линия else "—"}')

        # Перемотка — тем же путём, что у бандла: описанный вызов через дверь.
        st, _, тело = await door.call(None, 'GET', '/archive_command',
                                      {'command': 'seek', 'timestamp': нач + 43_200_000_000,
                                       'direction': 1}, None, {'token': token})
        факт('seek через дверь', json.loads(тело).get('success') in (1, '1'))

        st, _, тело = await door.call(None, 'GET', '/archive_status', {'type': 'state'})
        сост = [r for r in json.loads(тело) if r.get('token') == token]
        факт('дверь видит СОСТОЯНИЕ', bool(сост), f'state={сост[0]["state"] if сост else "—"}')

        # Протухшая сессия: дверь обязана перевойти сама.
        client._sids['user'] = ('мертвец', time.monotonic() + 999)
        st, _, тело = await door.call(None, 'GET', '/archive_status', {'type': 'calendar'})
        факт('дверь перевходит на мёртвой сессии', b'no session' not in тело,
             тело[:60].decode('utf-8', 'replace'))

        # Политика: запрет обходом через точки.
        try:
            await door.call(None, 'GET', '/a/../settings/webserver/')
            факт('обход политики закрыт', False, 'ПРОПУСТИЛА')
        except RC.RecorderDenied:
            факт('обход политики закрыт', True)

        await client.async_ping(token)
        факт('ping токена', True)
        задача.cancel()
    finally:
        await door.async_close()
        await session.close()
    плохо = [i for i in итог if not i[0]]
    print(f"\nИТОГ: {len(итог) - len(плохо)}/{len(итог)} прошло")
    sys.exit(1 if плохо else 0)

asyncio.run(main())
