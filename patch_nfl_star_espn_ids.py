#!/usr/bin/env python3
"""
Set espn_id + NFL headshot_url on dynasty_player_tiers for known stars.

Requires column espn_id on dynasty_player_tiers — run first:
  sql/add_espn_id_dynasty_player_tiers.sql (Supabase SQL Editor)

If espn_id column is missing, this script still updates headshot_url only
(see _update_headshot_only).

ESPN refs:
  Saquon Barkley      https://www.espn.com/nfl/player/_/id/3929630/saquon-barkley
  Patrick Mahomes     https://www.espn.com/nfl/player/_/id/3139477/...
  Christian McCaffrey https://www.espn.com/nfl/player/_/id/3117251/christian-mccaffrey
"""
from __future__ import annotations

from postgrest.exceptions import APIError

from config import config

PATCHES: list[tuple[str, int]] = [
    ("Saquon Barkley", 3929630),
    ("Patrick Mahomes", 3139477),
    ("Christian McCaffrey", 3117251),
]


def nfl_headshot(espn_id: int) -> str:
    return f"https://a.espncdn.com/i/headshots/nfl/players/full/{espn_id}.png"


def main() -> None:
    supabase = config.get_supabase_client()
    if not supabase:
        raise SystemExit("Supabase client not configured (SUPABASE_URL + SUPABASE_SERVICE_KEY).")

    for name, espn_id in PATCHES:
        payload = {"headshot_url": nfl_headshot(espn_id), "espn_id": espn_id}
        try:
            supabase.table("dynasty_player_tiers").update(payload).eq("player_name", name).execute()
        except APIError as e:
            msg = (e.args[0] or {}) if e.args else {}
            if isinstance(msg, dict):
                detail = f"{msg.get('message', '')} {msg.get('hint', '')}"
            else:
                detail = str(msg)
            if "espn_id" in detail.lower():
                supabase.table("dynasty_player_tiers").update(
                    {"headshot_url": payload["headshot_url"]}
                ).eq("player_name", name).execute()
                v = (
                    supabase.table("dynasty_player_tiers")
                    .select("player_name,headshot_url")
                    .eq("player_name", name)
                    .single()
                    .execute()
                )
                print(
                    f"OK headshot only — run sql/add_espn_id_dynasty_player_tiers.sql then re-run script: {v.data}"
                )
                continue
            raise
        verify = (
            supabase.table("dynasty_player_tiers")
            .select("player_name,espn_id,headshot_url")
            .eq("player_name", name)
            .single()
            .execute()
        )
        print(f"OK {verify.data}")


if __name__ == "__main__":
    main()
