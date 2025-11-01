import random
import logging
import functools
from typing import Any

import aiofiles
from nextcord import Embed, File, Interaction, SlashOption, slash_command
from nextcord.ext import commands

import src.md as md
from src.sql import IMDbCache
from src.util import autocomplete
from src.permissions import MEDIUM_CLEARANCE, need_clearance
from lib.imdb import main as ss_maker


API = 'https://api.imdbapi.dev'


class IMDbCog(commands.Cog):
    """Cog that handles IMDb related tasks."""

    def __init__(self, bot: commands.Bot) -> None:
        self.bot = bot
        self.bot.loop.create_task(self._load_autocompletions())

    #==========================================================================
    # Internal
    #==========================================================================
    async def _exe(self, func, *args, **kwargs):
        """Execute blocking function calls in a separate thread."""
        partial = functools.partial(func, *args, **kwargs)
        return await self.bot.loop.run_in_executor(None, partial)

    async def _load_autocompletions(self) -> None:
        """Fetch all unique genres/coutries once on startup."""
        try:
            logging.info('Caching autocompletions for genres and countries...')
            cache = IMDbCache()

            genres = await cache.autocomplete('', 'genres[*]', n=100)
            self.genres = tuple(sorted(genres))

            countries = await cache.autocomplete(
                '', 'originCountries[0].name',
                post_proc=lambda s: s.split('(')[0].strip()
            )
            self.countries = tuple(sorted(countries))

            logging.info(
                '  Autocomplete cache loaded: '
                f'{len(self.genres)} genres, {len(self.countries)} countries'
            )
        except Exception:
            logging.exception(f'Failed to cache autocompletions.')

    #==========================================================================
    # Slash Commands
    #==========================================================================
    @slash_command(description='reload cached autocompletions')
    @need_clearance(MEDIUM_CLEARANCE)
    async def reload(self, interaction: Interaction) -> None:
        """Cache reload slash command."""
        await interaction.response.defer()

        count = await IMDbCache().count()
        await self._load_autocompletions()

        summary = (
            f'Total Entries: {count}\n'
            f'  {len(self.genres)} genres\n'
            f'  {len(self.countries)} countries'
        )
        await interaction.followup.send(md.mono(summary))

    @slash_command(description='create a spreadsheeet')
    @need_clearance(MEDIUM_CLEARANCE)
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
        """Spreadsheet slash command."""
        await interaction.response.defer(ephemeral=True)

        min_votes = min_votes or 1000
        min_rating = min_rating or 1

        if min_votes < 1000:
            await interaction.followup.send('Minimum number of votes is 1000')
            return

        if not (1 <= min_rating <= 10):
            await interaction.followup.send('Valid minimum ranking: [1, 10]')
            return

        await self._exe(ss_maker, dict(
            base_filters=dict(
                titles={'movie'},
                min_votes=min_votes,
                ratings=(min_rating, 10)
            )
        ))

        try:
            async with aiofiles.open('imdb.xlsx', 'rb') as f:
                await interaction.followup.send(file=File(f, 'imdb.xlsx'))
        except FileNotFoundError:
            await interaction.followup.send(
                'Error: Could not find the generated spreadsheet'
            )
        except Exception as e:
            await interaction.followup.send(f'An error occurred: {e}')

    @slash_command(description='get info for a movie')
    async def info(self, interaction: Interaction,
        title: str = SlashOption(
            description='movie title',
            autocomplete=True
        )
    ) -> None:
        """Info slash command."""
        await interaction.response.defer()

        data = await IMDbCache().query(('primaryTitle', title))

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
        """Pickmovie slash command."""
        await interaction.response.defer()

        data = await IMDbCache().query(
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

        pick = random.sample(data, 1)
        await interaction.followup.send(embed=make_embed(pick[0]))

    #==========================================================================
    # Autocompletes
    #==========================================================================
    @info.on_autocomplete('title')
    async def title_ac(self, interaction: Interaction, query: str) -> None:
        """Autocompletion for title parameters."""
        choices = await IMDbCache().autocomplete(query, 'primaryTitle')
        # Pass through lru_cached function
        choices = autocomplete(tuple(choices), query)
        await interaction.response.send_autocomplete(choices)

    @pickmovie.on_autocomplete('genre')
    async def genre_ac(self, interaction: Interaction, query: str) -> None:
        """Autocompletion for genre parameters."""
        choices = autocomplete(self.genres, query)
        await interaction.response.send_autocomplete(choices)

    @pickmovie.on_autocomplete('country')
    async def country_ac(self, interaction: Interaction, query: str) -> None:
        """Autocompletion for country parameters."""
        choices = autocomplete(self.countries, query)
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
