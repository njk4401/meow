import os
import time
import asyncio
import logging
from functools import lru_cache
from itertools import islice
from typing import Any, Generator, Iterable, Sequence

import aiohttp
import requests


#==============================================================================
# Misc. utility
#==============================================================================
def chunks(it: Iterable, n: int) -> Generator[list, None, None]:
    """Separate an iterable into chunks of `n` items."""
    if n <= 0:
        raise ValueError('chunksize must be positive')

    it = iter(it)
    while chunk := list(islice(it, n)):
        yield chunk

#==============================================================================
# File management
#==============================================================================
def is_fresh(file: os.PathLike, max_age: float) -> bool:
    """Return True if a file exists and is fresh.

    Parameters:
        file (PathLike):
            File path string.
        max_age (float):
            The maximum age of `file` to be considered "fresh" in seconds.
    """
    if not os.path.exists(file):
        return False
    age = time.time() - os.path.getmtime(file)
    return age < max_age

def download_imdb_data(files: Sequence[str]) -> tuple[bool]:
    """Download IMDb datasets if missing or over a day old.

    Parameters:
        files (Sequence):
            Sequence of IMDB dataset file names to download.

    Returns:
        downloaded (tuple):
            Tuple of booleans corresponding to their index in files on
            whether the file was downloaded.
    """
    FULL_DAY = 24*60*60
    IMDB_DATA_URL = 'https://datasets.imdbws.com/'
    VALID_IMDB_DATA_FILES = {
        'name.basics.tsv.gz',
        'title.akas.tsv.gz',
        'title.basics.tsv.gz',
        'title.crew.tsv.gz',
        'title.episode.tsv.gz',
        'title.principals.tsv.gz',
        'title.ratings.tsv.gz'
    }
    downloaded = []
    for file in files:
        if file not in VALID_IMDB_DATA_FILES:
            raise ValueError(
                f'File "{file}" not a valid IMDb database file\n'
                f'Valid: {', '.join(f'"{f}"' for f in VALID_IMDB_DATA_FILES)}'
            )

        # Skip if the file is less than a day old
        if is_fresh(file, max_age=FULL_DAY):
            downloaded.append(False)
            continue

        url = IMDB_DATA_URL+file
        response = requests.get(url, stream=True)
        response.raise_for_status()

        with open(file, 'wb') as f:
            for chunk in response.iter_content(8192):
                f.write(chunk)

        downloaded.append(True)

    return tuple(downloaded)

#==============================================================================
# API related
#==============================================================================
type JSON = JSONObject | JSONArray
type JSONObject = dict[str, JSONArray | JSONObject | JSONPrimitive]
type JSONArray = list[JSONArray | JSONObject | JSONPrimitive]
type JSONPrimitive = bool | float | str

async def fetch(
    url: str, timeout: float = 15, retries: int = 5
) -> JSON | None:
    """Fetch a JSON url endpoint asynchronously.

    Parameters:
        url (str):
            URL to fetch.
        timeout (float):
            Fetch timeout in seconds.
        retries (int):
            Number of fetch attempts if request returns error.

    Returns:
        response (JSON | None):
            JSON response if fetch was successful, otherwise None.
    """
    async with aiohttp.ClientSession() as session:
        for attempt in range(retries):
            try:
                async with session.get(url, timeout=timeout) as resp:
                    resp.raise_for_status()
                    return await resp.json()
            # If error raised, wait and retry
            except aiohttp.ClientError as e:
                logging.warning(
                    f'Fetch attempt {attempt+1} failed for {url}: {e}'
                )
                # Non-blocking delay
                await asyncio.sleep(2**(attempt+1))
            except asyncio.TimeoutError:
                logging.warning(
                    f'Fetch attempt {attempt+1} timed out for {url}'
                )

    logging.error(f'Failed to fetch {url} after {retries} attempts')
    return None

#==============================================================================
# Discord related
#==============================================================================
@lru_cache
def autocomplete(vals: dict[str, Any] | Sequence[str],
                 query: str) -> dict[str, Any]:
    """Generate a dictionary of autocompletions based on a query.

    Parameters:
        vals (dict | Sequence):
            If given as a dict, will use the keys as autocompletions
            and values as what will be sent to Discord.
            If given as a sequence, will use the elements as autocompletions
            and also as what will be sent to Discord.
            Can not be given an unhashable type.
        query (str):
            The current query.

    Returns:
        matches (dict):
            Dictionary derived from `vals` where query was
            a substring in any of the keys or elements.
    """
    # Cast to dict if given as a sequence
    if not isinstance(vals, dict):
        vals = dict(zip(vals, vals))

    if not query:
        matches = sorted(vals.keys())
    else:
        matches = sorted(v for v in vals if query.lower() in v.lower())

    # Discord has a limit of 25 autocompletions
    return {key: vals[key] for key in matches[:25]}
