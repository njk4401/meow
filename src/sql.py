import json
import time
import sqlite3
from os import PathLike
from typing import Any, Self

from src.util import chunks, fetch


API = 'https://api.imdbapi.dev'
MAIN_DB = 'imdb.db'
FULL_DAY = 24*60*60


class IMDbCache:
    """IMDb API caching context manager with local SQLite storage."""

    def __init__(self, db: PathLike = MAIN_DB, ttl: float = FULL_DAY) -> None:
        """Create a new IMDb SQLite cache instance.

        Parameters:
            db (PathLike):
                Path to the SQLite database.
            ttl (float):
                Time-to-live for cached entries in seconds.

        Example:
        ```
            with IMDbCache() as cache:
                cache.add('tt003234', 'tt1234567')
                entries = cache.query('startYear', 1996)
        ```
        """
        self.db = db
        self.ttl = ttl
        self.conn = sqlite3.connect(self.db, timeout=10)
        self.conn.row_factory = sqlite3.Row
        self.cur = self.conn.cursor()
        self._sql_setup()

    def __del__(self):
        try:
            self.close()
        except Exception:
            ...  # Ignore any errors if database is already closed

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    def _sql_setup(self) -> None:
        """Ensure the database schema exists and set up WAL."""
        self.conn.execute('PRAGMA journal_mode=WAL;')
        self.conn.execute('PRAGMA synchronous=NORMAL;')
        self.cur.execute("""
            CREATE TABLE IF NOT EXISTS titles (
                id TEXT PRIMARY KEY,
                data TEXT NOT NULL,
                last_updated REAL NOT NULL
            )
        """)
        self.conn.commit()

    def add(self, *imdb_ids: str) -> None:
        """Add a collection of IMDb titles to the database by their ID.

        Parameters:
            *imdb_ids (str):
                IMDb title ID in the format `tt1234567`.
        """
        for batch in chunks(imdb_ids, 5):
            now = time.time()
            needs_update = []
            for i in batch:
                self.cur.execute("""
                    SELECT data, last_updated FROM titles WHERE id=?
                """, (i,))
                entry = self.cur.fetchone()
                if not entry or now - entry['last_updated'] >= self.ttl:
                    needs_update.append(i)

            if not needs_update:
                continue

            params = '&'.join(f'titleIds={i}' for i in needs_update)
            data = fetch(f'{API}/titles:batchGet?{params}')
            for title in data['titles']:
                # Only want movies
                if title['type'] != 'movie':
                    continue
                self.cur.execute("""
                    INSERT INTO titles (id, data, last_updated)
                    VALUES (?, ?, ?)
                    ON CONFLICT(id) DO UPDATE SET
                        data=excluded.data,
                        last_updated=excluded.last_updated
                """, (title['id'], json.dumps(title), time.time()))
            self.conn.commit()

    def query(self, *queries: tuple[str, Any],
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
            self.cur.execute('SELECT data FROM titles')
            return [json.loads(entry['data']) for entry in self.cur.fetchall()]

        query_results = []
        entries_by_id = {}

        for key, val in queries:
            entries = []

            # Convert [*] to json_each paths
            if '[*]' in key:
                array = key.split('[*]')[0]
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
        and return matching entries.
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

    def count(self) -> int:
        """Return number of cached entries."""
        self.cur.execute('SELECT COUNT(*) FROM titles')
        return self.cur.fetchone()[0]

    def close(self):
        """Close the database connection."""
        self.conn.commit()
        self.conn.close()
