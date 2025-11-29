from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Optional

import pandas as pd


DEFAULT_DB_PATH = Path(__file__).with_name("osrs_prices.db")


class GlobalFlipFinder:
    """
    Scan all items and compute flip stats over a recent time window.

    For each item, we compute:
      - current high / low / spread
      - average spread
      - max spread
      - buy_zone  ~ 25% quantile of low
      - sell_zone ~ 75% quantile of high
      - margin    = sell_zone - buy_zone
      - roi       = margin / buy_zone

    Then we sort by margin and show the top N candidates.
    """

    def __init__(self, db_path: str | Path = DEFAULT_DB_PATH) -> None:
        self.db_path = Path(db_path)
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row

    # ------------------------------------------------------------------ #
    # Core data extraction                                               #
    # ------------------------------------------------------------------ #

    def _get_window_bounds(self, since_minutes: int) -> Optional[int]:
        """
        Find the global latest scan_ts and compute window start.

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

    def load_window_dataframe(self, since_minutes: int = 240) -> pd.DataFrame:
        """
        Load all price samples in the recent window, joined with item info.
        """
        window_start = self._get_window_bounds(since_minutes)
        if window_start is None:
            return pd.DataFrame()

        cur = self.conn.cursor()
        cur.execute(
            """
            SELECT
                p.item_id,
                p.scan_ts,
                p.high,
                p.low,
                i.name,
                i.members,
                i."limit",
                i.value
            FROM prices p
            JOIN items i ON p.item_id = i.id
            WHERE p.scan_ts >= ?
              AND p.high IS NOT NULL
              AND p.low  IS NOT NULL
            """,
            (window_start,),
        )
        rows = cur.fetchall()
        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame(rows, columns=["item_id", "scan_ts", "high", "low", "name", "members", "limit", "value"])
        df["spread"] = df["high"] - df["low"]
        return df

    # ------------------------------------------------------------------ #
    # Flip stats per item                                                #
    # ------------------------------------------------------------------ #

    def compute_flip_table(self, since_minutes: int = 240) -> pd.DataFrame:
        """
        Compute flip-oriented stats for all items in the window.

        Returns:
            DataFrame with one row per item, including:
              name, current_high, current_low, current_spread,
              avg_spread, max_spread, buy_zone, sell_zone,
              margin, roi_pct, limit, samples
        """
        df = self.load_window_dataframe(since_minutes)
        if df.empty:
            return pd.DataFrame()

        # --- Current snapshot (latest scan_ts per item) ---
        idx_latest = df.groupby("item_id")["scan_ts"].idxmax()
        current = df.loc[idx_latest, ["item_id", "high", "low", "spread"]].rename(
            columns={
                "high": "current_high",
                "low": "current_low",
                "spread": "current_spread",
            }
        )

        # --- Aggregated stats per item ---
        agg = (
            df.groupby("item_id")
            .agg(
                avg_spread=("spread", "mean"),
                max_spread=("spread", "max"),
                min_low=("low", "min"),
                max_high=("high", "max"),
                samples=("scan_ts", "count"),
            )
        )

        # --- Quantile-based zones (buy/sell) ---
        buy_zone = df.groupby("item_id")["low"].quantile(0.25).rename("buy_zone")
        sell_zone = df.groupby("item_id")["high"].quantile(0.75).rename("sell_zone")

        # --- Basic item metadata (name, limit, etc.) ---
        meta = (
            df.groupby("item_id")
            .agg(
                name=("name", "first"),
                members=("members", "first"),
                limit=("limit", "first"),
                value=("value", "first"),
            )
        )

        # Merge everything together
        out = (
            meta
            .join(agg)
            .join(current.set_index("item_id"))
            .join(buy_zone)
            .join(sell_zone)
        )

        # Compute margins & ROI
        out["margin"] = out["sell_zone"] - out["buy_zone"]
        # Guard against divide-by-zero
        out = out[out["buy_zone"] > 0]
        out["roi_pct"] = out["margin"] / out["buy_zone"] * 100

        # Clean up types / rounding a bit
        for col in ["avg_spread", "max_spread", "buy_zone", "sell_zone", "margin", "current_high", "current_low", "current_spread"]:
            out[col] = out[col].round(1)

        out["roi_pct"] = out["roi_pct"].round(2)

        # Filter out items with non-positive margin or too few samples
        out = out[(out["margin"] > 0) & (out["samples"] >= 5)]

        # Sort by margin (biggest flips at top)
        out = out.sort_values("margin", ascending=False)

        # Move columns into a nice order
        out = out[
            [
                "name",
                "members",
                "limit",
                "samples",
                "current_high",
                "current_low",
                "current_spread",
                "avg_spread",
                "max_spread",
                "buy_zone",
                "sell_zone",
                "margin",
                "roi_pct",
            ]
        ]

        return out.reset_index(drop=True)

    def close(self) -> None:
        self.conn.close()


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

        # Show just the top N
        print(f"\n=== Top {top_n} flip candidates over last {window} minutes ===\n")
        print(table.head(top_n).to_string(index=False))

    finally:
        finder.close()


if __name__ == "__main__":
    main()
