import json
import time
import asyncio
import logging
import sqlite3
from os import PathLike
from typing import Any, Callable, Self

from src.util import chunks, fetch


API = 'https://api.imdbapi.dev'
MAIN_DB = 'imdb.db'
FULL_DAY = 24*60*60


class IMDbCache:
    """Asynchronous IMDb cache context manager with local SQLite storage."""

    def __init__(self, db: PathLike = MAIN_DB, ttl: float = FULL_DAY) -> None:
        """Create a new IMDb SQLite cache instance.

        Parameters:
            db (PathLike):
                Path to the SQLite database.
            ttl (float):
                Time-to-live for cached entries in seconds.

        Example:
        ```
            async with IMDbCache() as cache:
                await cache.add('tt003234', 'tt1234567')
                entries = await cache.query('startYear', 1996)
        ```
        """
        self.db = db
        self.ttl = ttl
        self.loop = asyncio.get_event_loop()
        self.conn: sqlite3.Connection = None
        self.cur: sqlite3.Cursor = None

    async def __aenter__(self) -> Self:
        """Async context manager entry."""
        self.conn = await self._exe(sqlite3.connect, self.db, timeout=10)
        self.conn.row_factory = sqlite3.Row
        self.cur = await self._exe(self.conn.cursor)
        await self._sql_setup()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Async context manager exit."""
        await self.close()

    async def _exe(self, func, *args):
        """Execute blocking function calls in a separate thread."""
        return await self.loop.run_in_executor(None, func, *args)

    async def _sql_setup(self) -> None:
        """Ensure the database schema exists and set up WAL."""
        await self._exe(self.conn.execute, 'PRAGMA journal_mode=WAL;')
        await self._exe(self.conn.execute, 'PRAGMA synchronous=NORMAL;')
        await self._exe(self.cur.execute, """
            CREATE TABLE IF NOT EXISTS titles (
                id TEXT PRIMARY KEY,
                data TEXT NOT NULL,
                last_updated REAL NOT NULL
            )
        """)
        await self._exe(self.conn.commit)

    async def add(self, *imdb_ids: str) -> None:
        """Add a collection of IMDb titles to the database by their ID."""
        # Endpoint can handle max of 5 concurrent queries
        for batch in chunks(imdb_ids, 5):
            now = time.time()
            needs_update = []
            for i in batch:
                await self._exe(self.cur.execute,
                    'SELECT data, last_updated FROM titles WHERE id=?', (i,)
                )
                entry = await self._exe(self.cur.fetchone)

                if not entry or now - entry['last_updated'] >= self.ttl:
                    needs_update.append(i)

            if not needs_update:
                continue

            params = '&'.join(f'titleIds={i}' for i in needs_update)
            url = f'{API}/titles:batchGet?{params}'
            data = await fetch(url)
            if not data or 'titles' not in data:
                logging.error(f'Malformed response from {url}')
                continue

            for title in data.get('titles', []):
                # Only want movies
                if title['type'] != 'movie':
                    continue
                await self._exe(self.cur.execute, """
                    INSERT INTO titles (id, data, last_updated)
                    VALUES (?, ?, ?)
                    ON CONFLICT(id) DO UPDATE SET
                        data=excluded.data,
                        last_updated=excluded.last_updated
                """, (title['id'], json.dumps(title), time.time()))
            await self._exe(self.conn.commit)

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
        if not queries:
            await self._exe(self.cur.execute, 'SELECT data FROM titles')
            all_entries = await self._exe(self.cur.fetchall)
            return [json.loads(entry['data']) for entry in all_entries]

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
                    sub_path = f'$.{sub_path.lstrip('.')}'
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
            return await self.query()  # Return all

        sep = ' OR ' if union else ' AND '
        sql = f'SELECT data FROM titles WHERE {sep.join(where)}'

        await self._exe(self.cur.execute, sql, tuple(params))
        results = await self._exe(self.cur.fetchall)
        return [json.loads(entry['data']) for entry in results]

    async def count(self) -> int:
        """Return number of cached entries."""
        await self._exe(self.cur.execute, 'SELECT COUNT(*) FROM titles')
        return await self._exe(self.cur.fetchone)[0]

    async def matching(
            self, query: str, key: str, n: int = 25, *,
            post_proc: Callable[[str], str] = None
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
        """
        if '[*]' in key:
            array_path, sub_path = key.split('[*]', maxsplit=1)
            array_path = f'$.{array_path}'

            if sub_path:
                sub_path = f'$.{sub_path}'
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

        results = await self._exe(self.cur.execute, sql, params)
        rows = await self._exe(results.fetchall)

        values = [row['value'] for row in rows if row['value'] is not None]

        if post_proc is not None:
            values = {post_proc(v) for v in values}

        cleaned = sorted(values)
        return dict(zip(cleaned, cleaned))

    async def close(self):
        """Close the database connection."""
        await self._exe(self.conn.commit)
        await self._exe(self.conn.close)
