import os
import asyncio

import httpx
from textual.app import App, ComposeResult
from textual.widgets import Header, Footer, Log, Input, Button


API_URL = os.getenv('BOT_CONTROL_URL', 'http://127.0.0.1:8080')
API_KEY = os.getenv('BOT_CONTROL_API_KEY', 'local-dev-key')


class BotTUI(App):
    CSS = """
    Screen { layout: vertical; }
    #logview { height: 1fr; }
    #input { height: 3; }
    """

    def compose(self) -> ComposeResult:
        yield Header()
        yield Log(id='logview')
        yield Input(placeholder='channel_id message', id='input')
        yield Footer()

    async def on_mount(self) -> None:
        self.log_view = self.query_one('#logview', Log)
        self.input = self.query_one('#input', Input)
        self.client = httpx.AsyncClient(timeout=5)
        #asyncio.create_task(self.refresh_stats_loop())

    async def refresh_stats_loop(self) -> None:
        while True:
            try:
                r = await self.client.get(
                    f'{API_URL}/control/stats',
                    headers=dict(
                        Authorization=f'Bearer {API_KEY}'
                    )
                )
                if r.status_code == 200:
                    stats = r.json()
                    self.log_view.clear()
                    self.log_view.write_line(
                        f'Bot: {stats['user']} | '
                        f'Guilds: {stats['guild_count']} | '
                        f'Latency: {stats['latency_ms']} ms'
                    )
                else:
                    self.log_view.write_line(f'[Error] Status {r.status_code}')
            except Exception as e:
                self.log_view.write_line(f'[Error] {e}')
            await asyncio.sleep(5)

    async def on_input_submitted(self, event: Input.Submitted) -> None:
        if not (text := event.value.strip()):
            return

        parts = text.split(' ', 1)
        if len(parts) != 2:
            self.log_view.write_line('Usage: <channel_id> <content>')
            self.input.value = ''
            return

        channel_id, msg = parts
        try:
            payload = dict(channel_id=int(channel_id), content=msg)
        except ValueError:
            self.log_view.write_line('channel_id must be an integer')
            self.input.value = ''
            return

        try:
            r = await self.client.post(
                f'{API_URL}/control/send_message',
                json=payload,
                headers=dict(
                    Authorization=f'Bearer {API_KEY}'
                )
            )
            if r.status_code == 200:
                self.log_view.write_line('Message sent')
            else:
                self.log_view.write_line(f'[Error] {r.status_code} {r.text}')
        except Exception as e:
            self.log_view.write_line(f'[Error] {e}')
        self.input.value = ''


if __name__ == '__main__':
    BotTUI().run()
