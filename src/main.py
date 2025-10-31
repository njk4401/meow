import os
import asyncio
import logging

from dotenv import load_dotenv
from nextcord import Intents, Interaction, InteractionType
from nextcord.ext import commands

from src.control_server import start_control_server


logging.basicConfig(level=logging.INFO, format='%(levelname)s::%(message)s')


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
            logging.info(f'Loaded command module: {rel_path}')
        except Exception:
            logging.exception(f'Failed to load command module: {rel_path}')


@bot.event
async def on_ready() -> None:
    asyncio.create_task(start_control_server(bot))
    logging.info(f'Logged in as {bot.user}')


@bot.event
async def on_application_command_error(
    interaction: Interaction, e: Exception
) -> None:
    logging.execption(
        f'Error running /{interaction.application_command.name}:'
    )
    if interaction.response.is_done():
        await interaction.followup.send(
            'Error - Check log for details', ephemeral=True
        )
    else:
        await interaction.response.send_message(
            'Error - Check log for details', ephemeral=True
        )


# @bot.event
# async def on_interaction(interaction: Interaction) -> None:
#     if interaction.type == InteractionType.application_command:
#         user = interaction.user
#         command = interaction.data.get('name')
#         print(f'[{time.asctime()}] {user} ran the command /{command}')
#         for opt in interaction.data.get('options', {}):
#             print(f'  {opt.get('name')} = {opt.get('value')}')
#
#         if not interaction.response.is_done():
#             await interaction.response.defer()


if __name__ == '__main__':
    TOKEN = os.getenv('BOT_TOKEN')
    if not TOKEN:
        print('No token found in environment variables')
        exit(1)

    load_cmd()
    bot.run(TOKEN)
