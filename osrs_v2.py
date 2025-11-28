#!/usr/bin/env python3
"""
OSRS price collector.

- Fetches item mapping from https://prices.runescape.wiki/api/v1/osrs/mapping
- Periodically pulls /latest prices and stores them in SQLite.
- Designed to be run every minute via cron.

Usage (manual):
    python collector.py
"""

from __future__ import annotations

import logging
import sqlite3
import time
from pathlib import Path
from typing import Any, Dict, Iterable

import requests


LOGGER = logging.getLogger(__name__)

DEFAULT_DB_PATH = Path(__file__).with_name("osrs_prices.db")
MAPPING_URL = "https://prices.runescape.wiki/api/v1/osrs/mapping"
LATEST_URL = "https://prices.runescape.wiki/api/v1/osrs/latest"

DEFAULT_HEADERS: Dict[str, str] = {
    "User-Agent": "osrs-trading-platform/0.1 (contact: rhoades.lorenzo@gmail.com)"
}


class OsrsPriceCollector:
    """
    Collect OSRS price data into a SQLite database.

    Typical usage:

        with OsrsPriceCollector() as collector:
            collector.run_once()
    """

    def __init__(
        self,
        db_path: str | Path = DEFAULT_DB_PATH,
        mapping_url: str = MAPPING_URL,
        latest_url: str = LATEST_URL,
        headers: Dict[str, str] | None = None,
    ) -> None:
        self.db_path = Path(db_path)
        self.mapping_url = mapping_url
        self.latest_url = latest_url
        self.headers = headers or DEFAULT_HEADERS.copy()

        self._conn: sqlite3.Connection | None = None

    # --------------------------------------------------------------------- #
    # Context manager                                                       #
    # --------------------------------------------------------------------- #

    def __enter__(self) -> "OsrsPriceCollector":
        self._conn = self._get_connection()
        self._create_tables()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    # --------------------------------------------------------------------- #
    # DB internals                                                          #
    # --------------------------------------------------------------------- #

    def _get_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        return conn

    @property
    def conn(self) -> sqlite3.Connection:
        if self._conn is None:
            self._conn = self._get_connection()
        return self._conn

    def _create_tables(self) -> None:
        """Create the items and prices tables if they don't exist."""
        cur = self.conn.cursor()

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS items (
                id        INTEGER PRIMARY KEY,
                name      TEXT,
                examine   TEXT,
                members   INTEGER,
                "limit"   INTEGER,
                value     INTEGER,
                icon      TEXT
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS prices (
                scan_ts   INTEGER,      -- when WE scanned (unix seconds)
                item_id   INTEGER,
                high      INTEGER,
                low       INTEGER,
                highTime  INTEGER,
                lowTime   INTEGER,
                PRIMARY KEY (scan_ts, item_id),
                FOREIGN KEY (item_id) REFERENCES items(id)
            )
            """
        )

        self.conn.commit()

    # --------------------------------------------------------------------- #
    # Mapping logic                                                         #
    # --------------------------------------------------------------------- #

    def _is_mapping_seeded(self) -> bool:
        cur = self.conn.cursor()
        cur.execute("SELECT COUNT(*) FROM items")
        (count,) = cur.fetchone()
        return count > 0

    def seed_mapping_if_needed(self) -> None:
        """
        Fetch /mapping once and store it if the items table is empty.
        Safe to call on every run; it only does work if table is empty.
        """
        if self._is_mapping_seeded():
            LOGGER.debug("Mapping is already seeded; skipping.")
            return

        LOGGER.info("Seeding items from /mapping...")
        data = self._fetch_json(self.mapping_url)  # list of dicts

        rows = [
            (
                item.get("id"),
                item.get("name"),
                item.get("examine"),
                int(bool(item.get("members"))),
                item.get("limit"),
                item.get("value"),
                item.get("icon"),
            )
            for item in data
        ]

        cur = self.conn.cursor()
        cur.executemany(
            """
            INSERT OR REPLACE INTO items (id, name, examine, members, "limit", value, icon)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        self.conn.commit()
        LOGGER.info("Inserted/updated %d items.", len(rows))

    # --------------------------------------------------------------------- #
    # Latest snapshot                                                       #
    # --------------------------------------------------------------------- #

    def collect_latest_snapshot(self) -> int:
        """
        Hit /latest and store a one-shot snapshot of all item prices.

        Returns:
            Number of rows inserted into the prices table.
        """
        LOGGER.info("Fetching /latest prices...")
        data = self._fetch_json(self.latest_url)
        latest: Dict[str, Dict[str, Any]] = data["data"]
        scan_ts = int(time.time())

        rows: Iterable[tuple[int, int, int | None, int | None, int | None, int | None]] = (
            (
                scan_ts,
                int(item_id_str),
                payload.get("high"),
                payload.get("low"),
                payload.get("highTime"),
                payload.get("lowTime"),
            )
            for item_id_str, payload in latest.items()
        )

        cur = self.conn.cursor()
        cur.executemany(
            """
            INSERT OR IGNORE INTO prices (scan_ts, item_id, high, low, highTime, lowTime)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        self.conn.commit()

        inserted = cur.rowcount if cur.rowcount is not None else 0
        LOGGER.info("Stored snapshot @ %d (rows inserted: %d).", scan_ts, inserted)
        return inserted

    # --------------------------------------------------------------------- #
    # Public API                                                            #
    # --------------------------------------------------------------------- #

    def run_once(self) -> None:
        """
        High-level entry point:

        - Ensure tables exist.
        - Seed mapping if needed.
        - Store one /latest snapshot.
        """
        _ = self.conn          # ensure connection
        self._create_tables()  # idempotent
        self.seed_mapping_if_needed()
        self.collect_latest_snapshot()

    # --------------------------------------------------------------------- #
    # HTTP helper                                                           #
    # --------------------------------------------------------------------- #

    def _fetch_json(self, url: str) -> Any:
        """GET JSON with basic error handling."""
        response = requests.get(url, headers=self.headers, timeout=10)
        response.raise_for_status()
        return response.json()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    )

    with OsrsPriceCollector() as collector:
        collector.run_once()


if __name__ == "__main__":
    main()
