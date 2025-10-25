import os
import json
import time
import sqlite3
from itertools import islice
from typing import Generator, Iterable, Sequence

import requests


#==============================================================================
# Misc. utility
#==============================================================================
def chunks(it: Iterable, n: int) -> Generator[list]:
    """Separate an iterable into chunks of `n` items.
    ```
    # Example:
    for batch in chunks(data, 5):
        ...
    ```
    """
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

def fetch(url: str, timeout: float = 15, retries: int = 5) -> JSON | None:
    """Fetch a JSON url endpoint.

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
    for attempt in range(retries):
        try:
            resp = requests.get(url, timeout=timeout)
            resp.raise_for_status()
            return resp.json()
        # If error raised, wait and retry
        except requests.RequestException:
            time.sleep(2**(attempt+1))
    return None
