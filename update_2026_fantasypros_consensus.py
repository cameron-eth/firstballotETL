#!/usr/bin/env python3
"""Write the current 2026 FantasyPros superflex consensus inputs to Supabase."""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from config import config
from fantasypros_2026_consensus import (
    build_consensus_lookup,
    normalize_consensus_name,
    parse_2026_fantasypros_superflex,
)


def update_2026_fantasypros_consensus() -> bool:
    supabase = config.get_supabase_client()
    if not supabase:
        print("❌ Failed to get Supabase client")
        return False

    rankings = parse_2026_fantasypros_superflex()
    lookup = build_consensus_lookup()
    timestamp = datetime.now(timezone.utc).isoformat()

    result = (
        supabase.table("dynasty_prospects")
        .select("id,name,position,draft_year")
        .eq("draft_year", 2026)
        .execute()
    )
    prospects = result.data or []
    if not prospects:
        print("❌ No 2026 dynasty_prospects rows found")
        return False

    matched = 0
    unmatched_db = []

    for prospect in prospects:
        name = prospect.get("name")
        position = prospect.get("position")
        lookup_key = (normalize_consensus_name(name), position)
        consensus = lookup.get(lookup_key)
        if not consensus:
            unmatched_db.append(f"{name} ({position})")
            continue

        update = {
            "consensus_rank": consensus["consensus_rank"],
            "consensus_position_rank": consensus["consensus_position_rank"],
            "consensus_avg_rank": consensus["consensus_avg_rank"],
            "consensus_rank_stddev": consensus["consensus_rank_stddev"],
            "consensus_best_rank": consensus["consensus_best_rank"],
            "consensus_worst_rank": consensus["consensus_worst_rank"],
            "consensus_source": consensus["consensus_source"],
            "consensus_updated_at": timestamp,
        }
        supabase.table("dynasty_prospects").update(update).eq("id", prospect["id"]).execute()
        matched += 1

    db_key_set = {
        (
            normalize_consensus_name(p.get("name")),
            p.get("position"),
        )
        for p in prospects
    }
    unmatched_consensus = [
        f"{row['name']} ({row['position']})"
        for row in rankings
        if (row["name_key"], row["position"]) not in db_key_set
    ]

    print(f"✅ Updated consensus inputs for {matched} 2026 prospects")
    if unmatched_db:
        print(f"⚠ {len(unmatched_db)} database prospects had no FantasyPros match")
        for item in unmatched_db[:20]:
            print(f"   - {item}")
    if unmatched_consensus:
        print(f"⚠ {len(unmatched_consensus)} FantasyPros entries had no dynasty_prospects match")
        for item in unmatched_consensus[:20]:
            print(f"   - {item}")

    return True


if __name__ == "__main__":
    success = update_2026_fantasypros_consensus()
    sys.exit(0 if success else 1)
