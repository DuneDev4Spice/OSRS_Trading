#!/usr/bin/env python3
"""
Global flip finder for OSRS.

Reads the latest price snapshot per item from osrs_prices.db and
computes simple flip stats:

- current_high
- current_low
- Margin  (current_high - current_low)
- roi_pct (margin / current_low * 100)

Then shows the top N items by margin.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Optional

import pandas as pd


DEFAULT_DB_PATH = Path(__file__).with_name("osrs_prices.db")


class GlobalFlipFinder:
    """Compute simple flip stats from the latest prices in the DB."""

    def __init__(self, db_path: str | Path = DEFAULT_DB_PATH) -> None:
        self.db_path = Path(db_path)
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row

    # ------------------------------------------------------------------ #
    # Helpers                                                            #
    # ------------------------------------------------------------------ #

    def _get_window_start(self, since_minutes: int) -> Optional[int]:
        """
        Get the earliest scan_ts we care about based on the most recent
        timestamp in the prices table.

        Returns:
            window_start (int) or None if prices table is empty.
        """
        cur = self.conn.cursor()
        cur.execute("SELECT MAX(scan_ts) AS max_ts FROM prices")
        row = cur.fetchone()
        if not row or row["max_ts"] is None:
            return None

        max_ts = row["max_ts"]
        return max_ts - since_minutes * 60

    def load_latest_snapshot(self, since_minutes: int = 240) -> pd.DataFrame:
        """
        Load the latest price row per item within the recent window.

        Returns:
            DataFrame with columns:
              item_id, name, current_high, current_low
        """
        window_start = self._get_window_start(since_minutes)
        if window_start is None:
            return pd.DataFrame()

        # Subquery to pick the latest scan_ts per item in the window
        query = """
            SELECT
                p.item_id,
                p.scan_ts,
                p.high,
                p.low,
                i.name
            FROM prices p
            JOIN (
                SELECT item_id, MAX(scan_ts) AS max_ts
                FROM prices
                WHERE scan_ts >= ?
                GROUP BY item_id
            ) latest
              ON p.item_id = latest.item_id
             AND p.scan_ts = latest.max_ts
            JOIN items i
              ON p.item_id = i.id
            WHERE p.high IS NOT NULL
              AND p.low  IS NOT NULL
        """

        cur = self.conn.cursor()
        cur.execute(query, (window_start,))
        rows = cur.fetchall()
        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame(
            rows,
            columns=["item_id", "scan_ts", "high", "low", "name"],
        )

        df = df.rename(
            columns={
                "high": "current_high",
                "low": "current_low",
            }
        )

        return df

    # ------------------------------------------------------------------ #
    # Flip stats                                                         #
    # ------------------------------------------------------------------ #

    def compute_flip_table(self, since_minutes: int = 240) -> pd.DataFrame:
        """
        Compute simple flip stats for all items:

        - current_high
        - current_low
        - margin
        - roi_pct

        Returns:
            DataFrame sorted by margin descending.
        """
        df = self.load_latest_snapshot(since_minutes=since_minutes)
        if df.empty:
            return pd.DataFrame()

        # Ensure numeric types
        df["current_high"] = pd.to_numeric(df["current_high"], errors="coerce")
        df["current_low"] = pd.to_numeric(df["current_low"], errors="coerce")

        # Drop rows with missing or non-positive lows
        df = df[df["current_low"] > 0]

        # Basic margin
        df["margin"] = df["current_high"] - df["current_low"]

        # Only keep positive margins
        df = df[df["margin"] > 0]

        # ROI as percentage
        df["roi_pct"] = df["margin"] / df["current_low"] * 100.0

        # Sort by margin (biggest absolute GP flips first)
        df = df.sort_values("margin", ascending=False)

        # Final column order
        df = df[
            [
                "name",
                "current_high",
                "current_low",
                "margin",
                "roi_pct",
            ]
        ]

        return df.reset_index(drop=True)

    def close(self) -> None:
        """Close the DB connection."""
        self.conn.close()


# ---------------------------------------------------------------------- #
# Pretty printing                                                        #
# ---------------------------------------------------------------------- #


def format_table_for_print(df: pd.DataFrame) -> str:
    """
    Pretty-print numeric columns with commas and decimal formatting.
    """
    df2 = df.copy()

    int_cols = ["current_high", "current_low", "margin"]
    for col in int_cols:
        if col in df2.columns:
            df2[col] = df2[col].apply(
                lambda x: "" if pd.isna(x) else f"{int(x):,}"
            )

    if "roi_pct" in df2.columns:
        df2["roi_pct"] = df2["roi_pct"].apply(
            lambda x: "" if pd.isna(x) else f"{float(x):.2f}%"
        )

    if "name" in df2.columns:
        df2["name"] = df2["name"].astype(str)

    return df2.to_string(index=False)


# ---------------------------------------------------------------------- #
# CLI                                                                    #
# ---------------------------------------------------------------------- #


def main() -> None:
    finder = GlobalFlipFinder()
    try:
        window_str = input("Minutes to look back (default 240): ").strip()
        if window_str:
            try:
                window = int(window_str)
            except ValueError:
                print("Invalid number; using 240.")
                window = 240
        else:
            window = 240

        top_str = input("How many top flips to show? (default 20): ").strip()
        if top_str:
            try:
                top_n = int(top_str)
            except ValueError:
                print("Invalid number; using 20.")
                top_n = 20
        else:
            top_n = 20

        table = finder.compute_flip_table(since_minutes=window)
        if table.empty:
            print("No data available in the selected window.")
            return

        print(
            f"\n=== Top {top_n} flip candidates "
            f"over last {window} minutes ===\n"
        )
        pretty = format_table_for_print(table.head(top_n))
        print(pretty)
    finally:
        finder.close()


if __name__ == "__main__":
    main()
