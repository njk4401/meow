import json
import time
import asyncio
import logging
import sqlite3
from os import PathLike
from typing import Any, Self

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
                cache.add('tt003234', 'tt1234567')
                entries = cache.query('startYear', 1996)
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
        base_sql = 'SELECT data FROM titles'
        where = []
        params = []

        for key, value in queries:
            if value is None:
                continue

            # Build key path for json_extract call
            key_path = '$.'+key.replace('[*]', '')

            # Convert [*] to json_each paths
            if '[*]' in key:
                array, *rest = key.split('[*]')
                array_path = '$.'+array

                sub_path = None
                if rest and rest[0]:
                    sub_path = '$.'+rest[0].lstrip('.')

                json_col = f'json_extract(value,)'

                self.cur.execute("""
                    SELECT DISTINCT t.data
                    FROM titles t, json_each(t.data, ?)
                """, (f'$.{array}',)
                )
                for entry in self.cur.fetchall():
                    data = json.loads(entry['data'])
                    entries.extend(self._match_path(data, key, val))
            else:
                self.cur.execute('SELECT data FROM titles')
                for entry in self.cur.fetchall():
                    data = json.loads(entry['data'])
                    entries.extend(self._match_path(data, key, val))

            # Remove duplicate entries
            entries_by_id.update({entry['id']: entry for entry in entries})
            query_results.append({entry['id'] for entry in entries})

        if union:
            final_entries = set.union(*query_results)
        else:
            final_entries = set.intersection(*query_results)

        return [entries_by_id[i] for i in final_entries]

    def _match_path(self, data: dict[str, Any],
                    key: str, value: Any) -> list[dict[str, Any]]:
        """Recursively traverse data according to key
        and return matching entries. (synchronous)
        """
        matches = []

        def match_leaf(curr):
            # Numeric Range
            if (isinstance(curr, (int, float)) and
                isinstance(value, tuple) and len(value) == 2):
                min_val, max_val = value
                return min_val <= curr <= max_val
            # Exact Value
            if isinstance(curr, (int, float)):
                return curr == value
            # Substring Match
            if isinstance(curr, str):
                return value.lower() in curr.lower()
            return False

        def recurse(curr, rem):
            if curr is None:
                return
            if not rem:
                if value is None:
                    matches.append(data)
                    return
                if isinstance(curr, list):
                    if any(match_leaf(v) for v in curr):
                        matches.append(data)
                elif match_leaf(curr):
                    matches.append(data)
            else:
                part, *rest = rem
                if (part == '[*]') and isinstance(curr, list):
                    for item in curr:
                        recurse(item, rest)
                elif isinstance(curr, dict) and part in curr:
                    recurse(curr[part], rest)

        parts = []
        for part in key.split('.'):
            if '[*]' in part:
                parts.extend([part.replace('[*]', ''), '[*]'])
            else:
                parts.append(part)

        recurse(data, parts)
        return matches

    async def count(self) -> int:
        """Return number of cached entries."""
        await self._exe(self.cur.execute, 'SELECT COUNT(*) FROM titles')
        return await self._exe(self.cur.fetchone)[0]

    async def matching(self, query: str, key: str, n: int = 25) -> tuple[dict]:
        """Get top `n` matching rows for `key` from DB.

        Parameters:
            query (str):
                Query to match against.
            key (str):
                JSON key which to match its value against `query`.
            n (int):
                Limit on the length of the return value.
        """
        results = await self._exe(self.cur.execute, """
            SELECT DISTINCT json_extract(data, '$.?') as entries
            FROM entries
            WHERE entries LIKE ?
            ORDER BY entries
            LIMIT ?
        """, (key, f'%{query}%', n))
        return tuple(self._exe(results.fetchall))

    async def close(self):
        """Close the database connection."""
        await self._exe(self.conn.commit)
        await self._exe(self.conn.close)
