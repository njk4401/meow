import os
import asyncio

from aiohttp import web


API_KEY = os.getenv('BOT_CONTROL_API_KEY', 'local-dev-key')


def require_key(request):
    key = request.headers.get('Authorization', '')
    if not key.startswith('Bearer ') or key.split(' ', 1)[1] != API_KEY:
        raise web.HTTPUnauthorized(text='missing or bad api key')


def create_control_app(bot):
    routes = web.RouteTableDef()

    @routes.get('/control/stats')
    async def stats(request):
        require_key(request)
        return web.json_response(dict(
            user=str(bot.user),
            guild_count=len(bot.guilds),
            latency_ms=round(bot.latency*1000, 1)
        ))

    @routes.post('/control/send_message')
    async def send_message(request):
        require_key(request)
        data = await request.json()
        channel_id = int(data.get('channel_id'))
        content = data.get('content', '')
        channel = bot.get_channel(channel_id)
        if channel is None:
            return web.json_response(dict(
                error='channel not found'
            ), status=404)
        await channel.send(content)
        return web.json_response(dict(
            status='sent'
        ))

    @routes.post('/control/shutdown')
    async def shutdown(request):
        require_key(request)
        asyncio.create_task(bot.close())
        return web.json_response(dict(
            status='shutting down'
        ))

    app = web.Application()
    app.add_routes(routes)
    return app


async def start_control_server(bot, host='127.0.0.1', port=8080):
    app = create_control_app(bot)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host, port)
    await site.start()
    print(f'[Control Api] Running at http://{host}:{port}')
