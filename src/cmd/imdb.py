import os
import json
from random import randint

import pandas as pd
from nextcord import Embed, File, Interaction, SlashOption, slash_command
from nextcord.ext import commands

from src.sql import IMDbCache
from src.util import fetch
from lib.imdb import main as ss_maker


API = 'https://api.imdbapi.dev'

with IMDbCache() as cache:
    DATA = pd.DataFrame(cache.query())

TITLES = {t.strip() for t in DATA['primaryTitle'].dropna()}
GENRES = {g.strip() for genre in DATA['genres'].dropna() for g in genre}
COUNTRIES = {c[0].get('name', 'N/A').split('(')[0].strip() for c in DATA['originCountries'].dropna()}

class IMDb(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @slash_command(description='update local cache with new entries')
    async def update(self, interaction: Interaction) -> None:
        global DATA, TITLES, GENRES, COUNTRIES

        await interaction.response.defer()

        if str(interaction.user.id) not in {os.getenv('MY_ID'), os.getenv('JILL_ID')}:
            await interaction.followup.send(
                content='You do not have permission to perform this command'
            )
            return

        before = len(DATA)
        with IMDbCache() as cache:
            DATA = pd.DataFrame(cache.query())

        TITLES = {t.strip() for t in DATA['primaryTitle'].dropna()}
        GENRES = {g.strip() for genre in DATA['genres'].dropna() for g in genre}
        COUNTRIES = {c[0].get('name', 'N/A').split('(')[0].strip() for c in DATA['originCountries'].dropna()}

        await interaction.followup.send(
            content=f'Cache updated (+{len(DATA)-before} Entries)'
        )

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

    @slash_command(description='Load info for a movie')
    async def info(self, interaction: Interaction,
        title: str = SlashOption(
            description='Movie title',
            autocomplete=True
        )
    ) -> None:
        await interaction.response.defer()

        with IMDbCache() as cache:
            data = (cache.query(('primaryTitle', title)))

        if not data:
            await interaction.followup.send(f'No matches for "{title}"')
            return

        pick = data[0]

        genres = pick.get('genres', [])
        interests = [s['name'] for s in pick.get('interests', {}) if 'isSubgenre' in s]
        if not genres:
            genres = ['N/A']
        if not interests:
            interests = ['N/A']

        embed = Embed(
            title=pick['primaryTitle'],
            url=f'https://www.imdb.com/title/{pick['id']}',
            description=pick.get('plot', 'N/A')
        )
        embed.set_image(pick['primaryImage']['url'])
        embed.add_field(name='Released', value=pick.get('startYear', 'N/A'))
        embed.add_field(name='Runtime', value=timestr(pick.get('runtimeSeconds', 'N/A')))
        embed.add_field(name='Rating', value=f'{pick['rating']['aggregateRating']}/10')
        embed.add_field(name='Country',
            value=f':flag_{pick['originCountries'][0]['code'].lower()}: '
                  f'{pick['originCountries'][0]['name'].split('(')[0].strip()}'
        )
        embed.add_field(name='Genres', value=', '.join(genres))
        embed.add_field(name='Interests', value=', '.join(interests))

        await interaction.followup.send(embed=embed)

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

        with IMDbCache() as cache:
            data = cache.query(
                ('genres[*]', genre),
                ('originCountry[*].name', country),
                ('startYear', year),
                ('startYear', (year_min, year_max)),
                ('rating.aggregateRating', rating),
                ('rating.aggregateRating', (rating_min, rating_max))
            )

        if not data:
            await interaction.followup.send('No matches for the given filter')
            return

        pick = pd.DataFrame(data).sample(1).to_dict()

        genres = pick.get('genres', [])
        interests = [s['name'] for s in pick.get('interests', {}) if 'isSubgenre' in s]
        if not genres:
            genres = ['N/A']
        if not interests:
            interests = ['N/A']

        embed = Embed(
            title=pick['primaryTitle'],
            url=f'https://www.imdb.com/title/{pick['id']}',
            description=pick.get('plot', 'N/A')
        )
        embed.set_image(pick['primaryImage']['url'])
        embed.add_field(name='Released', value=pick.get('startYear', 'N/A'))
        embed.add_field(name='Runtime', value=timestr(pick.get('runtimeSeconds', 'N/A')))
        embed.add_field(name='Rating', value=f'{pick['rating']['aggregateRating']}/10')
        embed.add_field(name='Country',
            value=f':flag_{pick['originCountries'][0]['code'].lower()}: '
                  f'{pick['originCountries'][0]['name'].split('(')[0].strip()}'
        )
        embed.add_field(name='Genres', value=', '.join(genres))
        embed.add_field(name='Interests', value=', '.join(interests))

        await interaction.followup.send(embed=embed)

    @info.on_autocomplete('title')
    async def title_autocomplete(self, interaction: Interaction, curr: str):
        if not curr:
            matches = sorted(TITLES)
        else:
            matches = sorted(t for t in TITLES if curr.lower() in t.lower())

        choices = dict(zip(matches[:25], matches[:25]))
        await interaction.response.send_autocomplete(choices)

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


def timestr(sec: float) -> str:
    if not isinstance(sec, (int, float)):
        return str(sec)

    secs = sec % 60
    mins = (sec // 60) % 60
    hours = sec // 3600
    return f'{hours}:{mins:02}:{secs:02}'


def setup(bot: commands.Bot):
    bot.add_cog(IMDb(bot))
