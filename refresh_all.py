#!/usr/bin/env python3
"""
Unified Refresh Pipeline
Runs the full FirstBallot data refresh in order:

  Step 1: Run FirstBallot tiering model  → dynasty_player_tiers
  Step 2: Re-grade all prospects         → dynasty_prospects

Run from firstballotETL/:
    python refresh_all.py

Flags:
    --skip-model        Skip step 1 (player tiers model)
    --skip-grades       Skip step 2 (prospect grading)
    --dry-run           Run everything but skip all DB writes
    --season YYYY       Override season year (default: 2025)

Tables written:
    dynasty_player_tiers  — upserted on player_id
    dynasty_prospects     — upserted on id
"""

import sys
import argparse
from datetime import datetime
from pathlib import Path

# ── path setup so we can import from both ETL and Model ──────────────────────
ETL_DIR = Path(__file__).parent
MODEL_SRC = ETL_DIR.parent / "FirstBallotModel" / "src"
sys.path.insert(0, str(ETL_DIR))
sys.path.insert(0, str(MODEL_SRC))

from config import config


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def header(title: str):
    width = 72
    print()
    print("=" * width)
    print(f"  {title}")
    print("=" * width)


def result_line(label: str, ok: bool, detail: str = ""):
    icon = "✓" if ok else "✗"
    suffix = f"  ({detail})" if detail else ""
    print(f"  {icon} {label}{suffix}")


# ─────────────────────────────────────────────────────────────────────────────
# Step 1 — Tiering model → dynasty_player_tiers
# ─────────────────────────────────────────────────────────────────────────────

def step_model(season: int, dry_run: bool) -> bool:
    header(f"STEP 1 — Tiering Model (season={season})  →  dynasty_player_tiers")

    try:
        from firstballot_model.tiering import DynastySFTieringModel
        from firstballot_model.exporter import Exporter

        model = DynastySFTieringModel(current_season=season)

        print("  Loading NFL data...")
        model.load_data()

        print("  Calculating player scores...")
        model.calculate_player_scores()

        player_count = len(model.scores_df) if model.scores_df is not None else 0

        # Always write CSV as a local backup
        output_dir = ETL_DIR.parent / "FirstBallotModel" / "output"
        output_dir.mkdir(parents=True, exist_ok=True)
        model.export_tiers(str(output_dir / "player_tiers.csv"))
        model.export_draft_picks(str(output_dir / "draft_pick_values.csv"))
        print(f"  Local CSV written → {output_dir}")

        if dry_run:
            print("  [DRY RUN] Skipping Supabase write")
        else:
            exporter = Exporter(model)
            exporter.to_supabase()

        result_line("Player tiers updated", True, f"{player_count} players")
        return True

    except Exception as e:
        result_line("Player tiers update FAILED", False, str(e))
        import traceback
        traceback.print_exc()
        return False


# ─────────────────────────────────────────────────────────────────────────────
# Step 2 — Prospect grades → dynasty_prospects
# ─────────────────────────────────────────────────────────────────────────────

def step_grades(dry_run: bool) -> bool:
    header("STEP 2 — Prospect Grades  →  dynasty_prospects")

    try:
        if dry_run:
            supabase = config.get_supabase_client()
            if supabase:
                result = supabase.table("dynasty_prospects").select("id", count="exact").execute()
                count = result.count or 0
                print(f"  [DRY RUN] Would grade {count} prospects — skipping writes")
                result_line("Prospect grades (dry run)", True, f"{count} prospects found")
            else:
                print("  [DRY RUN] Supabase not configured — skipping")
                result_line("Prospect grades (dry run)", True, "no DB connection")
            return True

        from grade_all_prospects import main as grade_main
        ok = grade_main()
        result_line("Prospect grades updated", ok)
        return ok

    except Exception as e:
        result_line("Prospect grades FAILED", False, str(e))
        import traceback
        traceback.print_exc()
        return False


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Full FirstBallot data refresh pipeline"
    )
    parser.add_argument("--skip-model",  action="store_true", help="Skip step 1 (player tiers)")
    parser.add_argument("--skip-grades", action="store_true", help="Skip step 2 (prospect grades)")
    parser.add_argument("--dry-run",     action="store_true", help="No DB writes")
    parser.add_argument("--season", type=int, default=2025,   help="NFL season year")
    args = parser.parse_args()

    started = datetime.now()

    print()
    print("=" * 72)
    print("  FIRSTBALLOT  —  FULL REFRESH PIPELINE")
    print("=" * 72)
    print(f"  Started : {started.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Season  : {args.season}")
    print(f"  Dry run : {args.dry_run}")

    steps_desc = []
    if not args.skip_model:
        steps_desc.append("model → dynasty_player_tiers")
    if not args.skip_grades:
        steps_desc.append("grades → dynasty_prospects")
    print(f"  Steps   : {',  '.join(steps_desc) if steps_desc else '[all skipped]'}")

    results = {}

    # Step 1 — Player tiers
    if args.skip_model:
        print("\n  [Skipping Step 1 — player tiers model]")
        results["model"] = None
    else:
        results["model"] = step_model(season=args.season, dry_run=args.dry_run)

    # Step 2 — Prospect grades
    if args.skip_grades:
        print("\n  [Skipping Step 2 — prospect grades]")
        results["grades"] = None
    else:
        results["grades"] = step_grades(dry_run=args.dry_run)

    # Summary
    elapsed = (datetime.now() - started).total_seconds()
    header("REFRESH SUMMARY")

    labels = {
        "model":  "Player tiers  →  dynasty_player_tiers",
        "grades": "Prospect grades  →  dynasty_prospects",
    }
    all_ok = True
    for key, label in labels.items():
        val = results[key]
        if val is None:
            print(f"  -  {label}  [skipped]")
        elif val:
            print(f"  ✓  {label}")
        else:
            print(f"  ✗  {label}  ← FAILED")
            all_ok = False

    print()
    print(f"  Elapsed : {elapsed:.1f}s")
    if args.dry_run:
        print("  Mode    : DRY RUN — no data was written to the database")

    if all_ok:
        print("\n  ✅  ALL STEPS COMPLETE")
    else:
        print("\n  ⚠️   SOME STEPS FAILED — check output above")

    print()
    return 0 if all_ok else 1


if __name__ == "__main__":
    sys.exit(main())
