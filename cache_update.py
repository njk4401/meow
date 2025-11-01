import time

import pandas as pd

from lib.imdb import BASICS_COLS
from src.sql import IMDbCache
from src.util import chunks, download_imdb_data

print('\x1b[?25l', end='', flush=True)
ids = set()
cache = IMDbCache()
while True:
    # Update IDs if needed
    if any(download_imdb_data({
        'title.basics.tsv.gz',
        'title.ratings.tsv.gz'}
        )) or not ids:
        print('\r\x1b[0KReloading IDs...', end='', flush=True)
        basics_list = []
        sections = pd.read_csv('title.basics.tsv.gz', sep='\t',
                               na_values='\\N', compression='gzip',
                               usecols=['tconst', 'titleType'],
                               chunksize=50_000)
        for chunk in sections:
            chunk = chunk[chunk['titleType'] == 'movie']
            basics_list.append(chunk)
        basics = pd.concat(basics_list, ignore_index=True)
        ratings = pd.read_csv('title.ratings.tsv.gz', sep='\t',
                              na_values='\\N', compression='gzip',
                              usecols=['tconst'])
        df = basics.merge(ratings, on='tconst', how='inner')
        df = df[df['titleType'].isin({'movie'})]
        ids.update(df['tconst'])

    # Cache in batches of 5
    for i, batch in enumerate(chunks(sorted(ids), 5)):
        print(f'\r\x1b[0KCache Count: {await cache.count()} ({((i*5)/len(ids))*100:.2f}%)', end='', flush=True)
        try:
            await cache.add(*batch)
        except Exception as e:
            print(f'[{time.asctime()}] Failed to cache: {e}')

    time.sleep(60)
