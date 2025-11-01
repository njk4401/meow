import json
import time
import asyncio
import logging
import sqlite3
import functools
from os import PathLike
from typing import Any, Callable

from src.util import chunks, fetch_sync


API = 'https://api.imdbapi.dev'
MAIN_DB = 'imdb.db'
FULL_DAY = 24*60*60


class IMDbCache:
    """Manages asynchronous access to the SQLite IMDb cache."""

    def __init__(self, db: PathLike = MAIN_DB, ttl: float = FULL_DAY) -> None:
        """Create a new IMDb SQLite cache instance.

        Parameters:
            db (PathLike):
                Path to the SQLite database.
            ttl (float):
                Time-to-live for cached entries in seconds.
        """
        self.db = db
        self.ttl = ttl
        self.loop = asyncio.get_event_loop()

    async def _exe(self, func, *args, **kwargs):
        """Execute blocking function calls in a separate thread."""
        partial = functools.partial(func, *args, **kwargs)
        return await self.loop.run_in_executor(None, partial)

    def _sql_setup(self) -> tuple[sqlite3.Connection, sqlite3.Cursor]:
        """Ensure the database schema exists and set up WAL."""
        conn = sqlite3.connect(self.db, timeout=10)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        conn.execute('PRAGMA journal_mode=WAL;')
        conn.execute('PRAGMA synchronous=NORMAL;')
        cur.execute("""
            CREATE TABLE IF NOT EXISTS titles (
                id TEXT PRIMARY KEY,
                data TEXT NOT NULL,
                last_updated REAL NOT NULL
            )
        """)
        return conn, cur

    def _get_sql_condition(self, key: str, value: Any) -> tuple[str, tuple]:
        """Build a SQL condition string and parameters for a given value."""
        # Substring matching
        if isinstance(value, str):
            return f'{key} LIKE ?', (f'%{value}%',)
        # Exact numeric matching
        if isinstance(value, (int, float)):
            return f'{key} = ?', (value,)
        # Numeric range matching
        if isinstance(value, tuple) and len(value) == 2:
            return f'{key} BETWEEN ? and ?', (value[0], value[1])
        return '1=0', ()

    #==========================================================================
    # Internal Thread-Safe Task Methods
    #==========================================================================
    def _add_task(self, *imdb_ids: str) -> None:
        """Task to be ran in executor for adding IDs to cache."""
        conn, cur = self._sql_setup()
        try:
            # Endpoint can handle max of 5 concurrent queries
            for batch in chunks(imdb_ids, 5):
                now = time.time()
                needs_update = []
                for i in batch:
                    cur.execute(
                        'SELECT data, last_updated FROM titles WHERE id=?',
                        (i,)
                    )
                    entry = cur.fetchone()

                    if not entry or now - entry['last_updated'] >= self.ttl:
                        needs_update.append(i)

                if not needs_update:
                    continue

                params = '&'.join(f'titleIds={i}' for i in needs_update)

                url = f'{API}/titles:batchGet?{params}'
                data = fetch_sync(url)
                if not data or 'titles' not in data:
                    logging.error(f'Malformed response from {url}')
                    continue

                for title in data.get('titles', []):
                    # Only want movies
                    if title['type'] != 'movie':
                        continue
                    cur.execute("""
                        INSERT INTO titles (id, data, last_updated)
                        VALUES (?, ?, ?)
                        ON CONFLICT(id) DO UPDATE SET
                            data=excluded.data,
                            last_updated=excluded.last_updated
                    """, (title['id'], json.dumps(title), time.time()))
                conn.commit()
        except Exception:
            logging.exception('Rolling back database due to error')
            conn.rollback()
        finally:
            conn.close()

    def _query_task(self, *queries: tuple[str, Any],
                    union: bool = False) -> list[dict[str, Any]]:
        """Task to be ran by executor for querying database."""
        conn, cur = self._sql_setup()
        try:
            where = []
            params = []
            for key, value in queries:
                # Skip if value explicitly given as None
                if value is None:
                    continue

                # Convert [*] to json_each paths
                if '[*]' in key:
                    array_path, sub_path = key.split('[*]', maxsplit=1)
                    array_path = f'$.{array_path}'

                    if sub_path:
                        sub_path = f'${sub_path}'
                        value_check = f"json_extract(value, '{sub_path}')"
                    else:
                        value_check = 'value'

                    condition = self._get_sql_condition(value_check, value)
                    sub_query = f"""
                        id IN (
                            SELECT t.id
                            FROM titles t, json_each(t.data, ?)
                            WHERE {condition[0]}
                        )
                    """
                    where.append(sub_query)
                    params.append(array_path)
                    params.extend(condition[1])
                else:
                    key_path = f'$.{key}'
                    json_path = 'json_extract(data, ?)'
                    condition = self._get_sql_condition(json_path, value)
                    where.append(condition[0])
                    params.append(key_path)
                    params.extend(condition[1])

            if not where:
                cur.execute('SELECT data FROM titles')
                all_entries = cur.fetchall()
                return [json.loads(entry['data']) for entry in all_entries]

            sep = ' OR ' if union else ' AND '
            sql = f'SELECT data FROM titles WHERE {sep.join(where)}'

            cur.execute(sql, tuple(params))
            results = cur.fetchall()
            return [json.loads(entry['data']) for entry in results]
        except Exception:
            logging.exception('Rolling back database due to error')
            conn.rollback()
        finally:
            conn.close()

    def _count_task(self) -> int:
        """Task to be ran by executor for counting entries in database."""
        conn, cur = self._sql_setup()
        try:
            cur.execute('SELECT COUNT(*) FROM titles')
            return cur.fetchone()[0]
        except Exception:
            logging.exception('Rolling back database due to error')
            conn.rollback()
        finally:
            conn.close()

    def _autocomplete_task(
            self, query: str, key: str, *, n: int = 25,
            post_proc: Callable[[str], str] = None,
            sort_key: Callable[[Any], Any] = None
    ) -> dict[str, str]:
        """Task to be ran by executor for generating autocompletion results."""
        conn, cur = self._sql_setup()
        try:
            if '[*]' in key:
                array_path, sub_path = key.split('[*]', maxsplit=1)
                array_path = f'$.{array_path}'

                if sub_path:
                    sub_path = f'${sub_path}'
                    value_check = f"json_extract(value, '{sub_path}')"
                else:
                    value_check = 'value'

                sql = f"""
                    SELECT DISTINCT {value_check} as value
                    FROM titles, json_each(data, ?)
                    WHERE value LIKE ?
                    ORDER BY value
                    LIMIT {n}
                """
                params = (array_path, f'%{query}%')
            else:
                key_path = f'$.{key}'
                sql = f"""
                    SELECT DISTINCT json_extract(data, ?) as value
                    FROM titles
                    WHERE value LIKE ?
                    ORDER BY value
                    LIMIT {n}
                """
                params = (key_path, f'%{query}%')

            results = cur.execute(sql, params)
            rows = results.fetchall()

            if sort_key is not None:
                rows = sorted(rows, key=sort_key)

            values = [row['value'] for row in rows if row['value'] is not None]

            if post_proc is not None:
                values = {post_proc(v) for v in values}

            cleaned = sorted(values)
            return dict(zip(cleaned, cleaned))
        except Exception:
            logging.exception('Rolling back database due to error')
            conn.rollback()
        finally:
            conn.close()

    #==========================================================================
    # Public Async Methods
    #==========================================================================
    async def add(self, *imdb_ids: str) -> None:
        """Add a collection of IMDb titles to the database by their ID."""
        await self._exe(self._add_task, *imdb_ids)

    async def query(self, *queries: tuple[str, Any],
                    union: bool = False) -> list[dict[str, Any]]:
        """Query by JSON key.

        Parameters:
            *queries (key, value):
                - key: JSON key path (supports '[*]' for lists)
                - value: substring, number, or range tuple
            union (bool, optional):
                If True, return entries matching any query.
                If False (default), return entries matching all queries.

        Examples:
        ```
            # Return all entries which are movies
            query(('type', 'movie'))

            # Return all entries with a rating between 9 and 10
            query(('rating.aggregateRating', (9, 10)))

            # Return the union of entries where any of the directors names
            # contain "Chris" and any genre contain "Comedy"
            query(
                ('directors[*].displayName', 'Chris'),
                ('genres[*]', 'Comedy')
            )
        ```
        """
        return await self._exe(self._query_task, *queries, union=union)

    async def count(self) -> int:
        """Return number of cached entries."""
        return await self._exe(self._count_task)

    async def autocomplete(
            self, query: str, key: str, *, n: int = 25,
            post_proc: Callable[[str], str] = None,
            sort_key: Callable[[Any], Any] = None
    ) -> dict[str, str]:
        """Get top `n` matching values for a given key.

        Parameters:
            query (str):
                Search query to match against.
            key (str):
                JSON key path to search through.
            n (int):
                Limit on the length of the return value.
            post_proc (Callable, optional):
                Post processing function to clean up results.
            sort_key (Callable, optional):
                Sorting key to organize results.
        """
        return await self._exe(
            self._autocomplete_task, query, key,
            n=n, post_proc=post_proc, sort_key=sort_key
        )
