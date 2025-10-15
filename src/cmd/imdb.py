import os
import json
from random import randint

import pandas as pd
from nextcord import Embed, File, Interaction, SlashOption, slash_command
from nextcord.ext import commands

from lib.imdb import fetch
from lib.imdb import main as ss_maker


API = 'https://api.imdbapi.dev'

DATA = pd.read_excel('main.xlsx')
GENRES = set(
    genre.strip()
    for entry in DATA['Genres'].dropna()
    for genre in entry.split(',')
)
COUNTRIES = set(
    country.strip()
    for country in DATA['Country'].dropna()
)


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

    @slash_command(description='Generate a random movie')
    async def pickmovie(self, interaction: Interaction,
        genre: str = SlashOption(
            description='Limit movies to a specific genre',
            required=False, autocomplete=True
        ),
        country: str = SlashOption(
            description='Limit movies released in a specific country',
            required=False, autocomplete=True
        ),
        year: int = SlashOption(
            description='Limit movies to a specific year',
            required=False
        ),
        year_min: int = SlashOption(
            description='Limit movies released after or during the given year',
            required=False, default=0
        ),
        year_max: int = SlashOption(
            description='Limit movies released before or during the given year',
            required=False, default=3000
        ),
        rating: int = SlashOption(
            description='Limit movies to a specific rating',
            required=False, choices=range(1, 11)
        ),
        rating_min: float = SlashOption(
            description='Limit movies with a rating of at least the given rating',
            required=False, default=1
        ),
        rating_max: float = SlashOption(
            description='Limit movies with a rating of at most the given rating',
            required=False, default=10
        )
    ) -> None:
        await interaction.response.defer()

        df = DATA.copy()
        if genre:
            if genre.lower() not in {g.lower() for g in GENRES}:
                await interaction.followup.send(f'Unrecognized genre: "{genre}"')
                return
            df = df[df['Genres'].str.contains(genre, case=False, na=False)]
        if country:
            if country not in COUNTRIES:
                await interaction.followup.send(f'Unrecognized country: "{country}"')
                return
            df = df[df['Country'] == country]
        if year:
            df = df[df['Year'] == year]
        if rating:
            df = df[df['Rating'].between(rating, rating+0.9)]

        df = df[df['Year'].between(year_min, year_max)]
        df = df[df['Rating'].between(rating_min, rating_max)]

        if df.empty:
            await interaction.followup.send('No titles found with the given filter')
            return

        pick = df.sample(1)

        resp = fetch(f'{API}/titles/{pick['tconst'].values[0]}')

        if resp is None:
            await interaction.followup.send('Fetch failed... Try again')
            return

        genres = [s['name'] for s in resp['interests'] if 'isSubgenre' not in s]
        interests = [s['name'] for s in resp['interests'] if 'isSubgenre' in s]
        if not genres:
            genres = ['N/A']
        if not interests:
            interests = ['N/A']

        embed = Embed(
            title=resp['primaryTitle'],
            url=f'https://www.imdb.com/title/{resp['id']}',
            description=resp['plot']
        )
        embed.set_image(resp['primaryImage']['url'])
        embed.add_field(name='Released', value=resp.get('startYear', 'N/A'))
        embed.add_field(name='Runtime', value=timestr(resp.get('runtimeSeconds', 0)))
        embed.add_field(name='Rating', value=f'{pick['Rating'].values[0]}/10')
        embed.add_field(name='Country',
            value=f':flag_{resp['originCountries'][0]['code'].lower()}: '+pick['Country'].values[0]
        )
        embed.add_field(name='Genres', value=', '.join(genres))
        embed.add_field(name='Interests', value=', '.join(interests))

        await interaction.followup.send(embed=embed)

    @pickmovie.on_autocomplete('genre')
    async def genre_autocomplete(self, interaction: Interaction, curr: str):
        if not curr:
            matches = tuple(GENRES)[:25]
        else:
            matches = tuple(g for g in GENRES if curr.lower() in g.lower())

        choices = dict(zip(matches, matches))
        await interaction.response.send_autocomplete(choices)

    @pickmovie.on_autocomplete('country')
    async def country_autocomplete(self, interaction: Interaction, curr: str):
        if not curr:
            matches = tuple(COUNTRIES)[:25]
        else:
            matches = tuple(c for c in COUNTRIES if curr.lower() in c.lower())

        choices = dict(zip(matches, matches))
        await interaction.response.send_autocomplete(choices)


def timestr(sec: int) -> str:
    secs = sec % 60
    mins = (sec // 60) % 60
    hours = sec // 3600
    return f'{hours}:{mins:02}:{secs:02}'


def setup(bot: commands.Bot):
    bot.add_cog(IMDb(bot))
