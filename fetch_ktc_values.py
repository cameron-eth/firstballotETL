#!/usr/bin/env python3
"""
KeepTradeCut Dynasty Values Scraper

Scrapes player dynasty values from keeptradecut.com.
KTC embeds a playersArray JavaScript object directly in the HTML which contains
all player values for different scoring formats.

Outputs a CSV to data_output/ and optionally upserts to Supabase.
"""

import re
import sys
import json
import time
import requests
import pandas as pd
from pathlib import Path
from datetime import date
from typing import Optional

sys.path.insert(0, str(Path(__file__).parent))
from config import config

KTC_URL = "https://keeptradecut.com/dynasty/trade-database"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://keeptradecut.com/",
}


def fetch_ktc_html() -> str:
    """Fetch the KTC rankings page HTML."""
    session = requests.Session()
    session.headers.update(HEADERS)
    print(f"Fetching KTC rankings from {KTC_URL} ...")
    resp = session.get(KTC_URL, timeout=30)
    resp.raise_for_status()
    print(f"  Status: {resp.status_code}, size: {len(resp.text):,} bytes")
    return resp.text


def extract_players_array(html: str) -> list:
    """
    Extract the playersArray JavaScript variable from the HTML.
    KTC embeds player data as:  var playersArray = [...];
    """
    # Match the JS variable assignment
    pattern = r"var\s+playersArray\s*=\s*(\[.*?\]);"
    match = re.search(pattern, html, re.DOTALL)
    if not match:
        raise ValueError(
            "Could not find playersArray in KTC HTML. "
            "The site structure may have changed."
        )
    raw_json = match.group(1)
    players = json.loads(raw_json)
    print(f"  Extracted {len(players):,} players from playersArray")
    return players


def parse_players(players: list) -> pd.DataFrame:
    """
    Parse the raw player objects into a flat DataFrame.

    KTC player object keys (may vary):
      playerName, playerID, position, team, age,
      superflexValues  -> {oneQB, sf} (dict with 'value', 'rank', 'trend_week', etc.)
      oneQBValues      -> same structure
      draftkings, fanduels, ... (optional)
    """
    rows = []
    for p in players:
        row = {
            "ktc_player_id": p.get("playerID"),
            "player_name": p.get("playerName"),
            "position": p.get("position"),
            "team": p.get("team"),
            "age": p.get("age"),
            "rookie": p.get("rookie", False),
            "injured": p.get("injured", False),
            # 1QB values
            "value_1qb": _nested(p, "oneQBValues", "value"),
            "rank_1qb": _nested(p, "oneQBValues", "rank"),
            "trend_week_1qb": _nested(p, "oneQBValues", "overallTier"),
            # Superflex values
            "value_sf": _nested(p, "superflexValues", "value"),
            "rank_sf": _nested(p, "superflexValues", "rank"),
            "trend_week_sf": _nested(p, "superflexValues", "overallTier"),
            # Metadata
            "scraped_date": date.today().isoformat(),
        }
        rows.append(row)

    df = pd.DataFrame(rows)
    # Sort by superflex value descending (most valuable first)
    df = df.sort_values("value_sf", ascending=False, na_position="last").reset_index(drop=True)
    return df


def _nested(obj: dict, *keys):
    """Safely navigate nested dict keys, return None if missing."""
    cur = obj
    for k in keys:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(k)
    return cur


def _create_table_sql(table: str) -> str:
    return f"""
CREATE TABLE IF NOT EXISTS {table} (
    id              BIGSERIAL PRIMARY KEY,
    ktc_player_id   INTEGER,
    player_name     TEXT,
    position        TEXT,
    team            TEXT,
    age             NUMERIC,
    rookie          BOOLEAN,
    injured         BOOLEAN,
    value_1qb       INTEGER,
    rank_1qb        INTEGER,
    trend_week_1qb  INTEGER,
    value_sf        INTEGER,
    rank_sf         INTEGER,
    trend_week_sf   INTEGER,
    scraped_date    DATE,
    UNIQUE (ktc_player_id, scraped_date)
);
"""


def save_csv(df: pd.DataFrame) -> Path:
    """Save DataFrame to CSV in data_output/."""
    out_dir = Path(__file__).parent / "data_output"
    out_dir.mkdir(exist_ok=True)
    today = date.today().isoformat()
    out_path = out_dir / f"ktc_values_{today}.csv"
    df.to_csv(out_path, index=False)
    print(f"  Saved CSV -> {out_path}")
    return out_path


def upsert_supabase(df: pd.DataFrame, table: str = "ktc_player_values") -> None:
    """Upsert records into Supabase table."""
    client = config.get_supabase_client()
    if client is None:
        print("  Supabase client not available — skipping database upsert.")
        return

    records = df.to_dict(orient="records")
    batch_size = config.batch_size if hasattr(config, "batch_size") else 200

    print(f"  Upserting {len(records):,} records to '{table}' in batches of {batch_size} ...")
    try:
        for i in range(0, len(records), batch_size):
            batch = records[i : i + batch_size]
            client.table(table).upsert(batch, on_conflict="ktc_player_id,scraped_date").execute()
            print(f"    Upserted rows {i + 1}–{min(i + batch_size, len(records))}")
            time.sleep(0.1)
        print("  Supabase upsert complete.")
    except Exception as e:
        print(f"  Supabase upsert failed: {e}")
        print(f"  Note: Create the '{table}' table first using the SQL below:")
        print(_create_table_sql(table))


def main():
    print("=== KTC Dynasty Values Scraper ===")

    html = fetch_ktc_html()
    players_raw = extract_players_array(html)
    df = parse_players(players_raw)

    print(f"\nTop 10 players by Superflex value:")
    print(
        df[["player_name", "position", "team", "value_sf", "rank_sf", "value_1qb", "rank_1qb"]]
        .head(10)
        .to_string(index=False)
    )

    csv_path = save_csv(df)

    # Upsert to Supabase if configured
    upsert_supabase(df)

    print(f"\nDone! {len(df):,} players scraped.")
    print(f"CSV: {csv_path}")
    return df


if __name__ == "__main__":
    main()
