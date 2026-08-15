#!/usr/bin/env python3
"""
Fill ``nfl_comparisons`` for the 2027 / 2028 prospects that don't have them.

Reuses the comparison engine from ``college_ranking_pipeline`` but skips every
CFBD call — the import script already populated stats and measurables, so this
only needs ``master_player_stats`` and the prospect rows themselves.

Run:
    python backfill_comps_2027_2028.py --dry-run
    python backfill_comps_2027_2028.py
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent))

from config import config
from college_ranking_pipeline import CollegeRankingPipeline

TARGET_YEARS = (2027, 2028)
SKILL_POSITIONS = ("QB", "RB", "WR", "TE")


def load_nfl_stats(supabase) -> pd.DataFrame:
    """NFL production rows, enriched with prospect-era size for size-aware comps."""
    result = (
        supabase.from_("master_player_stats")
        .select("player_display_name, position, season, fantasy_ppg, games_played")
        .in_("position", list(SKILL_POSITIONS))
        .gte("games_played", 1)
        .execute()
    )
    if not result.data:
        return pd.DataFrame()

    df = pd.DataFrame(result.data)
    pipeline_helper = CollegeRankingPipeline.__new__(CollegeRankingPipeline)

    sizes = (
        supabase.from_("dynasty_prospects")
        .select("name,height,weight")
        .in_("position", list(SKILL_POSITIONS))
        .not_.is_("height", "null")
        .not_.is_("weight", "null")
        .execute()
    )
    size_map: Dict[str, Tuple[float, float]] = {}
    for row in sizes.data or []:
        key = pipeline_helper._normalize_person_name(row.get("name"))
        if key:
            size_map[key] = (
                pipeline_helper._safe_float(row.get("height")),
                pipeline_helper._safe_float(row.get("weight")),
            )

    norm = df["player_display_name"].map(pipeline_helper._normalize_person_name)
    df["height"] = norm.map(lambda n: size_map.get(n, (0.0, 0.0))[0])
    df["weight"] = norm.map(lambda n: size_map.get(n, (0.0, 0.0))[1])
    return df


def coerce_stats(raw) -> Optional[Dict]:
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except (TypeError, ValueError):
            return None
    return raw if isinstance(raw, dict) and raw else None


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--all", action="store_true", help="Also overwrite existing comps")
    args = parser.parse_args()

    supabase = config.get_supabase_client()
    if not supabase:
        print("ERROR: Supabase client unavailable")
        return 1

    # The comparison methods never touch the CFBD client, so bypass __init__
    # (which requires an API key) rather than demanding a key we won't use.
    pipeline = CollegeRankingPipeline.__new__(CollegeRankingPipeline)
    pipeline.skill_positions = list(SKILL_POSITIONS)

    print("Loading NFL production...")
    nfl_df = load_nfl_stats(supabase)
    if nfl_df.empty:
        print("ERROR: no NFL stats available for comparisons")
        return 1
    print(f"   {len(nfl_df)} NFL season rows")

    rows = (
        supabase.table("dynasty_prospects")
        .select("id,name,position,rank,draft_year,tier,overall_grade,valuation,"
                "height,weight,college_stats,nfl_comparisons")
        .in_("draft_year", list(TARGET_YEARS))
        .in_("position", list(SKILL_POSITIONS))
        .order("draft_year")
        .order("rank")
        .execute()
        .data
        or []
    )
    targets = rows if args.all else [r for r in rows if not r.get("nfl_comparisons")]
    print(f"   {len(targets)} of {len(rows)} prospects need comps")

    updated = 0
    for row in targets:
        position = row["position"]
        tier = row.get("tier") or "Tier 4"
        stats = coerce_stats(row.get("college_stats"))
        profile = {
            "overall_grade": row.get("overall_grade"),
            "rank": row.get("rank"),
            "height": row.get("height"),
            "weight": row.get("weight"),
            "valuation": row.get("valuation"),
        }

        if stats:
            comps = pipeline.find_nfl_comparisons(
                row["name"], position, stats, tier, nfl_df, prospect_profile=profile
            )
        else:
            comps = []
        if not comps:
            comps = pipeline.find_tier_based_comps(
                position, tier, nfl_df, player_name=row["name"], prospect_profile=profile
            )

        if not comps:
            print(f"   - {row['name']:<26} no comps found")
            continue

        joined = ", ".join(comps)
        print(f"   ✓ {row['name']:<26} {joined}")
        if not args.dry_run:
            supabase.table("dynasty_prospects").update({"nfl_comparisons": joined}).eq(
                "id", row["id"]
            ).execute()
        updated += 1

    print(f"\n{'[DRY RUN] would update' if args.dry_run else 'Updated'} {updated} prospects")
    return 0


if __name__ == "__main__":
    sys.exit(main())
