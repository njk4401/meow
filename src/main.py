import os
import time
import asyncio
from traceback import print_exception

from dotenv import load_dotenv
from nextcord import Intents, Interaction
from nextcord.ext import commands

from src.control_server import start_control_server


load_dotenv()

intents = Intents.default()
intents.members = True
intents.guilds = True

bot = commands.Bot(intents=intents)


def load_cmd() -> None:
    for file in os.listdir('src/cmd'):
        if not file.endswith('.py') or file.startswith('_'):
            continue

        rel_path = f'src.cmd.{os.path.splitext(os.path.basename(file))[0]}'
        try:
            bot.load_extension(rel_path)
            print(f'Loaded command module: {rel_path}')
        except Exception as e:
            print(f'Failed to load command module: {rel_path}\nError: {e}')


@bot.event
async def on_ready() -> None:
    asyncio.create_task(start_control_server(bot))
    print('online')


@bot.event
async def on_application_command_error(interaction: Interaction, e: Exception) -> None:
    print(f'Error running /{interaction.application_command.name}:')
    print_exception(type(e), e, e.__traceback__)
    if interaction.response.is_done():
        await interaction.followup.send(
            'Error - Check log for details', ephemeral=True
        )
    else:
        await interaction.response.send_message(
            'Error - Check log for details', ephemeral=True
        )


@bot.event
async def on_application_command(interaction: Interaction) -> None:
    user = interaction.user
    command = interaction.application_command
    print(f'[{time.asctime()}] {user.name} ran the command /{command.name}')
    if command.options:
        print(*[f'  {arg}={val}\n' for arg, val in command.options.items()])

if __name__ == '__main__':
    TOKEN = os.getenv('BOT_TOKEN')
    if not TOKEN:
        print('No token found in environment variables')
        exit(1)

    load_cmd()
    bot.run(TOKEN)
