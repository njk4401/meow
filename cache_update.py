import time

import pandas as pd

from src.sql import IMDbCache
from src.util import chunks, download_imdb_data


ids = set()
with IMDbCache() as cache:
    while True:
        # Update IDs if needed
        if any(download_imdb_data({'title.ratings.tsv.gz'})) or not ids:
            ids.update(
                pd.read_csv('title.ratings.tsv.gz', sep='\t',
                            na_values='\\N', compression='gzip')['tconst']
            )

        # Cache in batches of 100
        for batch in chunks(ids, 100):
            print(f'Caching {', '.join(batch)}')
            try:
                cache.add(*batch)
            except Exception as e:
                print(f'[{time.asctime()}] Failed to cache: {e}')

        time.sleep(60)
