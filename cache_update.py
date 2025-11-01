import time
import asyncio
import logging
import functools

import pandas as pd

from src.sql import IMDbCache
from src.util import chunks, download_imdb_data, timestr


logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [%(levelname)s] %(message)s',
    handlers=(
        logging.FileHandler('updater.log'),
        logging.StreamHandler()
    )
)


ONE_HOUR = 60*60
FIVE_MIN = 5*60
movie_ids = set()


def id_update_task() -> None:
    """Task to be ran by executor to download and process TSV files."""
    global movie_ids
    try:
        logging.info('Checking for new IMDb data files...')
        files_downloaded = download_imdb_data(
            ('title.basics.tsv.gz', 'title.ratings.tsv.gz')
        )

        if not any(files_downloaded) and movie_ids:
            logging.info('  No new files')
            return

        logging.info('  Fresh data found. Reloading database on disk...')

        basics = []
        sections = pd.read_csv(
            'title.basics.tsv.gz', sep='\t', na_values='\\N', chunksize=50_000,
            compression='gzip', usecols=('tconst', 'titleType')
        )
        # Filter only movies to lower RAM usage
        for chunk in sections:
            chunk = chunk[chunk['titleType'] == 'movie']
            basics.append(chunk)
        basics = pd.concat(basics, ignore_index=True)

        ratings = pd.read_csv(
            'title.ratings.tsv.gz', sep='\t', na_values='\\N',
            compression='gzip', usecols=('tconst',)
        )
        # Filter only movies with ratings
        basics = basics.merge(ratings, on='tconst', how='inner')

        logging.info('    Database reloaded')
        movie_ids.update(basics['tconst'])
        logging.info(f'Total Entries: {len(movie_ids)}')
    except Exception:
        logging.exception('Error updating IDs')


async def main() -> None:
    """Main async task loop."""
    loop = asyncio.get_event_loop()
    cache = IMDbCache()
    while True:
        try:
            await loop.run_in_executor(None, id_update_task)

            if not movie_ids:
                logging.warning('No IDs loaded. Skipping cache add loop.')
                await asyncio.sleep(FIVE_MIN)
                continue

            total_ids = len(movie_ids)
            start_time = time.time()

            for i, batch in enumerate(chunks(sorted(movie_ids), 5)):
                try:
                    await cache.add(*batch)
                except Exception as e:
                    logging.error(f'Failed to cache batch: {batch} - {e}')

                if i > 0 and i % 1000 == 0:
                    elapsed = timestr(time.time() - start_time)
                    progress = ((i*5)/total_ids) * 100
                    print(
                        f'\r\x1b[0KProgress: {progress:.2f}% '
                        f'(Elapsed: {elapsed})',
                        end='', flush=True
                    )

            elapsed = timestr(time.time() - start_time)
            logging.info(f'Refresh finished in {elapsed}. Sleeping...')
            await asyncio.sleep(ONE_HOUR)
        except Exception:
            logging.exception('Unhandled Error')


if __name__ == '__main__':
    try:
        print('\x1b[?25l', end='', flush=True)
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info('Process interrupted')
    finally:
        print('\x1b[?25h', end='', flush=True)
