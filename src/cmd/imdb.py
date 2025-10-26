from random import randint
from typing import Any

import pandas as pd
from nextcord import Embed, File, Interaction, SlashOption, slash_command
from nextcord.ext import commands

import src.md as md
from src.sql import IMDbCache
from src.util import autocomplete
from src.permissions import MEDIUM_CLEARANCE, check_perms
from lib.imdb import main as ss_maker


API = 'https://api.imdbapi.dev'


class IMDbCog(commands.Cog):
    """Cog that handles IMDb related tasks."""

    def __init__(self, bot: commands.Bot) -> None:
        self.bot = bot
        self._load_cache()

    #==========================================================================
    # Internal
    #==========================================================================
    def _load_cache(self) -> None:
        """Load local cache from SQL database."""
        with IMDbCache() as cache:
            data = pd.DataFrame(cache.query())

        self.data = data
        self.titles = {t.strip() for t in data['primaryTitle'].dropna()}
        self.genres = {g.strip() for genre in data['genres'].dropna()
                                 for g in genre}
        self.countries = {c[0].get('name', 'N/A').split('(')[0].strip()
                          for c in data['originCountries'].dropna()}

    def _reload_cache(self) -> str:
        """Reload local cache and return a diff summary."""
        before = len(self.data)
        self._load_cache()
        return (
            'Cache Successfully Reloaded\n'
            f'Entries: {len(self.data)-before:+}\n'
            f'Titles: {len(self.titles)}\n'
            f'Genres: {len(self.genres)}\n'
            f'Countries: {len(self.countries)}'
        )

    #==========================================================================
    # Slash Commands
    #==========================================================================
    @slash_command(description='reload local cache with updated entries')
    async def reload(self, interaction: Interaction) -> None:
        await interaction.response.defer()
        if not await check_perms(str(interaction.user.id), MEDIUM_CLEARANCE):
            await interaction.followup.send(
                'You do not have permission to perform this command'
            )
            return

        summary = self._reload_cache()
        await interaction.followup.send(md.mono(summary))

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

        if not await check_perms(str(interaction.user.id), MEDIUM_CLEARANCE):
            await interaction.followup.send(
                'You do not have permission to perform this command',
            )
            return

        min_votes = min_votes or 1000
        min_rating = min_rating or 1

        if min_votes < 1000:
            await interaction.followup.send('Minimum number of votes is 1000')
            return

        if not (1 <= min_rating <= 10):
            await interaction.followup.send('Valid minimum ranking: [1, 10]')
            return

        ss_maker(
            base_filters=dict(
                titles={'movie'},
                min_votes=min_votes,
                ratings=(min_rating, 10)
            )
        )

        with open('imdb.xlsx', 'rb') as f:
            await interaction.followup.send(file=File(f, 'imdb.xlsx'))

    @slash_command(description='get info for a movie')
    async def info(self, interaction: Interaction,
        title: str = SlashOption(
            description='movie title',
            autocomplete=True
        )
    ) -> None:
        await interaction.response.defer()

        with IMDbCache() as cache:
            data = cache.query(('primaryTitle', title))

        if not data:
            await interaction.followup.send(md.mono('No matches'))
            return

        await interaction.followup.send(embed=make_embed(data[0]))

    @slash_command(description='pick a random movie based on filters')
    async def pickmovie(self, interaction: Interaction,
        genre: str = SlashOption(
            description='limit movies to a specific genre',
            required=False, autocomplete=True
        ),
        country: str = SlashOption(
            description='limit movies released in a specific country',
            required=False, autocomplete=True
        ),
        year: int = SlashOption(
            description='limit movies to a specific year',
            required=False
        ),
        year_min: int = SlashOption(
            description='limit movies released after the given year',
            required=False, default=0
        ),
        year_max: int = SlashOption(
            description='limit movies released before the given year',
            required=False, default=3000
        ),
        rating: int = SlashOption(
            description='limit movies to a specific rating',
            required=False, choices=range(1, 11)
        ),
        rating_min: float = SlashOption(
            description='limit movies to a minimum rating',
            required=False, default=1
        ),
        rating_max: float = SlashOption(
            description='limit movies to a maximum rating',
            required=False, default=10
        )
    ) -> None:
        await interaction.response.defer()

        with IMDbCache() as cache:
            data = cache.query(
                ('genres[*]', genre),
                ('originCountries[*].name', country),
                ('startYear', year),
                ('startYear', (year_min, year_max)),
                ('rating.aggregateRating', rating),
                ('rating.aggregateRating', (rating_min, rating_max))
            )

        if not data:
            await interaction.followup.send('No matches for the given filter')
            return

        pick = data[randint(0, len(data)-1)]
        await interaction.followup.send(embed=make_embed(pick))

    #==========================================================================
    # Autocompletes
    #==========================================================================
    @info.on_autocomplete('title')
    async def title_ac(self, interaction: Interaction, query: str) -> None:
        """Autocompletion for title parameters."""
        choices = autocomplete(tuple(sorted(self.titles)), query)
        await interaction.response.send_autocomplete(choices)

    @pickmovie.on_autocomplete('genre')
    async def genre_ac(self, interaction: Interaction, query: str) -> None:
        """Autocompletion for genre parameters."""
        choices = autocomplete(tuple(sorted(self.genres)), query)
        await interaction.response.send_autocomplete(choices)

    @pickmovie.on_autocomplete('country')
    async def country_ac(self, interaction: Interaction, query: str) -> None:
        """Autocompletion for country parameters."""
        choices = autocomplete(tuple(sorted(self.countries)), query)
        await interaction.response.send_autocomplete(choices)


def make_embed(entry: dict[str, Any]) -> Embed:
    """Generate an IMDb breakdown embed for an entry in the SQL database."""
    if (tconst := entry.get('id')) is not None:
        imdb_link = f'https://www.imdb.com/title/{tconst}'
    else:
        imdb_link = None

    embed = Embed(
        title=entry.get('primaryTitle', 'Unknown'),
        url=imdb_link,
        description=entry.get('plot', 'N/A')
    )

    image = entry.get('primaryImage', {}).get('url')
    if image is not None:
        embed.set_image(entry.get('primaryImage', {}).get('url'))

    embed.add_field(name='Released', value=str(entry.get('startYear', 'N/A')))

    genres = entry.get('genres', [])
    interests = [s['name'] for s in entry.get('interests', {}) if 'isSubgenre' in s]
    if not genres:
        genres = ['N/A']
    if not interests:
        interests = ['N/A']

    embed.add_field(name='Runtime', value=timestr(entry.get('runtimeSeconds', 'N/A')))
    embed.add_field(name='Rating', value=f'{entry['rating']['aggregateRating']}/10')
    embed.add_field(name='Country',
        value=f':flag_{entry['originCountries'][0]['code'].lower()}: '
                f'{entry['originCountries'][0]['name'].split('(')[0].strip()}'
    )
    embed.add_field(name='Genres', value=', '.join(genres))
    embed.add_field(name='Interests', value=', '.join(interests))

    return embed


def timestr(sec: float) -> str:
    if not isinstance(sec, (int, float)):
        return str(sec)

    secs = sec % 60
    mins = (sec // 60) % 60
    hours = sec // 3600
    return f'{hours}:{mins:02}:{secs:02}'


def setup(bot: commands.Bot):
    bot.add_cog(IMDbCog(bot))
