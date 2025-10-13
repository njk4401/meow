import os
import json
from random import randint

import pandas as pd
from nextcord import File, Interaction, SlashOption, slash_command
from nextcord.ext import commands

from lib.imdb import main as ss_maker

GENRES = set()
DATA = pd.read_excel('main.xlsx')
for item in DATA:
    GENRES.update(set(DATA['Genres'].split(',')))

print(GENRES)

class IMDb(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @slash_command(description='create a spreadsheeet')
    async def spreadsheet(self, interaction: Interaction,
        min_votes: int = SlashOption(
            description='minimum number of votes title must have',
            required=False
        ),
        min_rating: float = SlashOption(
            description='minimum rating title must have',
            required=False
        )
    ) -> None:
        await interaction.response.defer(ephemeral=True)

        if str(interaction.user.id) not in {os.getenv('MY_ID'), os.getenv('JILL_ID')}:
            await interaction.followup.send(
                content='You do not have permission to perform this command',
                ephemeral=True
            )
            return

        min_votes = min_votes or 1000
        min_rating = min_rating or 1

        if min_votes < 1000:
            await interaction.followup.send(
                content='Minimum number of votes is 1000',
                ephemeral=True
            )
            return

        if not (1 <= min_rating <= 10):
            await interaction.followup.send(
                content='Valid for minimum ranking: [1, 10]',
                ephemeral=True
            )
            return

        ss_maker(
            base_filters=dict(
                titles={'movie'},
                min_votes=min_votes,
                ratings=(min_rating, 10))
        )


        with open('imdb.xlsx', 'rb') as f:
            await interaction.followup.send(
                file=File(f, 'imdb.xlsx')
            )

    # @slash_command(description='Generate a random movie')
    async def pickmovie(self, interaction: Interaction) -> None:
        interaction.response.defer()



def setup(bot: commands.Bot):
    bot.add_cog(IMDb(bot))
