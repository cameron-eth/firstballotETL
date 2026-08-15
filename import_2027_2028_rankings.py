#!/usr/bin/env python3
"""
Import the 2027 Superflex Dynasty Rookie Top 75 and the 2028 Top 25 into
``dynasty_prospects``.

The supplied rankings are the source of truth for *who* is in each class and
for the overall class ordering.  Everything else is pulled from the College
Football Data API:

  * ``/player/search``       -> cfbd_id / espn athlete id, height, weight,
                               jersey, hometown, team color
  * ``/recruiting/players``  -> HS stars / rating / national ranking / HS school
  * ``/stats/player/season`` -> counting stats aggregated into ``college_stats``

Grades are produced by the existing grading model (``grade_all_prospects``),
seeded with the *overall* class rank.  ``rank`` is stored as that overall rank;
``/api/prospects`` derives the positional rank it displays by ordering ``rank``
within (draft_year, position).

Run:
    python import_2027_2028_rankings.py --dry-run
    python import_2027_2028_rankings.py
    python import_2027_2028_rankings.py --only-year 2028
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import requests

sys.path.insert(0, str(Path(__file__).parent))

from config import config
from grade_all_prospects import grade_prospect
from valuation import calculate_prospect_value

CFBD_BASE = "https://api.collegefootballdata.com"
SKILL_POSITIONS = ("QB", "RB", "WR", "TE")

# College seasons that can contribute production for these classes.
# 2026 has not been played yet as of this import.
STAT_SEASONS = (2023, 2024, 2025)
# HS classes that feed the 2027 / 2028 draft classes.
RECRUITING_YEARS = (2022, 2023, 2024, 2025, 2026)

POS_CATEGORIES = {
    "QB": ("passing", "rushing"),
    "RB": ("rushing", "receiving"),
    "WR": ("receiving", "rushing"),
    "TE": ("receiving", "rushing"),
}

STAT_KEY_MAP = {
    "passing": {
        "YDS": "pass_yds",
        "TD": "pass_tds",
        "INT": "pass_int",
        "ATT": "pass_att",
        "COMPLETIONS": "pass_comp",
    },
    "rushing": {
        "YDS": "rush_yds",
        "TD": "rush_tds",
        "CAR": "rush_att",
    },
    "receiving": {
        "YDS": "rec_yds",
        "TD": "rec_tds",
        "REC": "rec",
    },
}

# Ranking-list school -> CFBD team name (only where they differ).
SCHOOL_TO_CFBD_TEAM = {
    "Ole Miss": "Ole Miss",
    "Mississippi": "Ole Miss",
    "Cal": "California",
    "Miami": "Miami",
    "USC": "USC",
    "BYU": "BYU",
    "UCLA": "UCLA",
    "LSU": "LSU",
    "NC State": "NC State",
    "Transfer": None,
}

# (overall_rank, name, position, school)
RANKINGS_2027: List[Tuple[int, str, str, str]] = [
    (1, "Jeremiah Smith", "WR", "Ohio State"),
    (2, "Arch Manning", "QB", "Texas"),
    (3, "Dante Moore", "QB", "Oregon"),
    (4, "Cam Coleman", "WR", "Texas"),
    (5, "Julian Sayin", "QB", "Ohio State"),
    (6, "Bryant Wesco", "WR", "Clemson"),
    (7, "Kewan Lacy", "RB", "Ole Miss"),
    (8, "Jadan Baugh", "RB", "Florida"),
    (9, "Charlie Becker", "WR", "Indiana"),
    (10, "Nick Marsh", "WR", "Indiana"),
    (11, "Ahmad Hardy", "RB", "Missouri"),
    (12, "LaNorris Sellers", "QB", "South Carolina"),
    (13, "Darian Mensah", "QB", "Miami"),
    (14, "CJ Carr", "QB", "Notre Dame"),
    (15, "Trey'Dez Green", "TE", "LSU"),
    (16, "Ryan Coleman-Williams", "WR", "Alabama"),
    (17, "TJ Moore", "WR", "Clemson"),
    (18, "Isaac Brown", "RB", "Louisville"),
    (19, "Mark Fletcher", "RB", "Miami"),
    (20, "KJ Duff", "WR", "Rutgers"),
    (21, "Mario Craver", "WR", "Texas A&M"),
    (22, "Jamari Johnson", "TE", "Oregon"),
    (23, "Isaiah Sategna III", "WR", "Oklahoma"),
    (24, "Brendan Sorsby", "QB", "Texas Tech"),
    (25, "Hollywood Smothers", "RB", "Texas"),
    (26, "Trinidad Chambliss", "QB", "Mississippi"),
    (27, "Ryan Wingo", "WR", "Texas"),
    (28, "Eugene Wilson III", "WR", "LSU"),
    (29, "Justice Haynes", "RB", "Transfer"),
    (30, "Drew Mestemaker", "QB", "Oklahoma State"),
    (31, "Jayce Brown", "WR", "LSU"),
    (32, "Duce Robinson", "WR", "Florida State"),
    (33, "Terrance Carter", "TE", "Texas Tech"),
    (34, "LJ Martin", "RB", "BYU"),
    (35, "Antwan Raymond", "RB", "Rutgers"),
    (36, "Josh Hoover", "QB", "Indiana"),
    (37, "Nate Frazier", "RB", "Georgia"),
    (38, "Jayden Maiava", "QB", "USC"),
    (39, "Cooper Barkate", "WR", "Miami"),
    (40, "Cameron Dickey", "RB", "Texas Tech"),
    (41, "DJ Vonnahme", "TE", "Iowa"),
    (42, "Nyck Harbor", "WR", "South Carolina"),
    (43, "Cam Cook", "RB", "West Virginia"),
    (44, "DeSean Bishop", "RB", "Tennessee"),
    (45, "Danny Scudero", "WR", "Colorado"),
    (46, "Isaiah Horton", "WR", "Texas A&M"),
    (47, "Sam Leavitt", "QB", "LSU"),
    (48, "Wayne Knight", "RB", "UCLA"),
    (49, "Eric Singleton Jr.", "WR", "Florida"),
    (50, "CJ Bailey", "QB", "NC State"),
    (51, "Nic Anderson", "WR", "LSU"),
    (52, "Dorian Thomas", "TE", "Cal"),
    (53, "Gunner Stockton", "QB", "Georgia"),
    (54, "Cam Edwards", "RB", "Michigan State"),
    (55, "Raleek Brown", "RB", "Texas"),
    (56, "John Mateer", "QB", "Oklahoma"),
    (57, "Tre Wisner", "RB", "Florida State"),
    (58, "Aneyas Williams", "RB", "Notre Dame"),
    (59, "Waymond Jordan", "RB", "USC"),
    (60, "Brandon Inniss", "WR", "Ohio State"),
    (61, "Caden Durham", "RB", "LSU"),
    (62, "Kenny Johnson", "WR", "Texas Tech"),
    (63, "Kaden Feagin", "RB", "Illinois"),
    (64, "Amare Thomas", "WR", "Houston"),
    (65, "CJ Baxter", "RB", "Kentucky"),
    (66, "Jaden Greathouse", "WR", "Notre Dame"),
    (67, "Darius Taylor", "RB", "Minnesota"),
    (68, "Benjamin Brahmer", "TE", "Penn State"),
    (69, "Jackson Harris", "WR", "LSU"),
    (70, "Ian Strong", "WR", "Cal"),
    (71, "Luke Reynolds", "TE", "Virginia Tech"),
    (72, "Luke Hasz", "TE", "Ole Miss"),
    (73, "Omarion Miller", "WR", "Arizona State"),
    (74, "Decker DeGraaf", "TE", "Washington"),
    (75, "Braylon Staley", "WR", "Tennessee"),
]

RANKINGS_2028: List[Tuple[int, str, str, str]] = [
    (1, "Malachi Toney", "WR", "Miami"),
    (2, "Dakorien Moore", "WR", "Oregon"),
    (3, "Bo Jackson", "RB", "Ohio State"),
    (4, "Andrew Marsh", "WR", "Michigan"),
    (5, "Dallas Wilson", "WR", "Florida"),
    (6, "Caleb Hawkins", "RB", "Oklahoma State"),
    (7, "Vernell Brown III", "WR", "Florida"),
    (8, "Jordon Davison", "RB", "Oregon"),
    (9, "Nate Shepard", "RB", "Duke"),
    (10, "Lotzeir Brooks", "WR", "Alabama"),
    (11, "Bryce Underwood", "QB", "Michigan"),
    (12, "Dierre Hill Jr.", "RB", "Oregon"),
    (13, "Harlem Berry", "RB", "LSU"),
    (14, "Jaron-Keawe Sagapolutele", "QB", "Cal"),
    (15, "Keelon Russell", "QB", "Alabama"),
    (16, "Quentin Gibson", "WR", "Colorado"),
    (17, "Gideon Davidson", "RB", "Clemson"),
    (18, "Linkon Cure", "TE", "Kansas State"),
    (19, "Donovan Olugbode", "WR", "Missouri"),
    (20, "Cortez Mills", "WR", "Nebraska"),
    (21, "Girard Pringle Jr.", "RB", "Miami"),
    (22, "Kaliq Lockett", "WR", "Texas"),
    (23, "Talyn Taylor", "WR", "Georgia"),
    (24, "AK Dear", "RB", "Alabama"),
    (25, "Elyiss Williams", "TE", "Georgia"),
]

RANKINGS = {2027: RANKINGS_2027, 2028: RANKINGS_2028}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

SUFFIXES = {"jr", "sr", "ii", "iii", "iv", "v"}

# Players CFBD lists under a different name than the ranking list uses.
NAME_ALIASES = {
    "Hollywood Smothers": "Daylan Smothers",
    "Ryan Coleman-Williams": "Ryan Williams",
}


def normalize_name(name: Optional[str]) -> str:
    """Lowercase, drop punctuation and generational suffixes.

    Periods are removed rather than replaced so ``C.J. Carr`` and ``CJ Carr``
    normalize to the same token; hyphens become spaces so ``Coleman-Williams``
    stays two words.
    """
    s = (name or "").lower()
    s = re.sub(r"[.'`’‘]", "", s)
    s = re.sub(r"[,\-]", " ", s)
    parts = [p for p in re.split(r"\s+", s) if p and p not in SUFFIXES]
    return " ".join(parts)


def search_terms_for(name: str) -> List[str]:
    """CFBD's searchTerm is a literal contains match, so try a few spellings."""
    terms: List[str] = []

    def add(term: str) -> None:
        term = term.strip()
        if term and term not in terms:
            terms.append(term)

    alias = NAME_ALIASES.get(name)
    if alias:
        add(alias)
    add(name)

    # Drop a generational suffix: "Eugene Wilson III" -> "Eugene Wilson".
    tokens = [t for t in (alias or name).split() if t.strip(".").lower() not in SUFFIXES]
    add(" ".join(tokens))

    # Punctuate bare initials: "CJ Carr" -> "C.J. Carr".
    dotted = [
        f"{t[0]}.{t[1]}." if len(t) == 2 and t.isalpha() and t.isupper() else t
        for t in tokens
    ]
    add(" ".join(dotted))

    # Last resort: surname only, which the scorer still has to agree with.
    if len(tokens) >= 2:
        add(tokens[-1])
    return terms


def parse_name(full_name: str) -> Tuple[str, str]:
    parts = full_name.strip().split()
    if len(parts) >= 2:
        return parts[0], " ".join(parts[1:])
    return (parts[0] if parts else ""), ""


def cfbd_team_for(school: str) -> Optional[str]:
    if school in SCHOOL_TO_CFBD_TEAM:
        return SCHOOL_TO_CFBD_TEAM[school]
    return school


def to_float(value) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def to_int(value) -> Optional[int]:
    f = to_float(value)
    return int(f) if f is not None else None


class CFBD:
    """Thin CFBD client with rate limiting and simple retries."""

    def __init__(self, api_key: str, delay: float = 0.15):
        self.headers = {"Authorization": f"Bearer {api_key}", "Accept": "application/json"}
        self.delay = delay

    def get(self, path: str, params: Dict, timeout: int = 60) -> Optional[list]:
        for attempt in range(3):
            time.sleep(self.delay)
            try:
                r = requests.get(
                    f"{CFBD_BASE}{path}", headers=self.headers, params=params, timeout=timeout
                )
            except requests.RequestException as exc:
                print(f"    ! {path} {params} -> {exc}")
                continue
            if r.status_code == 200:
                return r.json()
            if r.status_code == 429:
                print("    ! rate limited, sleeping 30s")
                time.sleep(30)
                continue
            print(f"    ! {r.status_code} {path} {params}")
            return None
        return None


# ---------------------------------------------------------------------------
# Phase 1 — resolve players against CFBD
# ---------------------------------------------------------------------------


def score_candidate(candidate: Dict, name: str, position: str, school: str) -> int:
    """Higher is better, -1 for "not this player".

    An exact normalized-name match scores 100; a token-subset match (a name the
    list abbreviates or extends) only scores 60 and therefore needs a team
    agreement to clear the acceptance bar.
    """
    # CFBD's display `name` and its firstName/lastName can disagree — Tre Wisner
    # is listed as firstName "Quintrevion" — so both spellings get a shot.
    api_variants = {
        normalize_name(candidate.get("name")),
        normalize_name(
            f"{candidate.get('firstName') or ''} {candidate.get('lastName') or ''}".strip()
        ),
    }
    api_variants.discard("")
    if not api_variants:
        return -1

    wanted = {normalize_name(name)}
    alias = NAME_ALIASES.get(name)
    if alias:
        wanted.add(normalize_name(alias))

    if api_variants & wanted:
        score = 100
    elif any(
        set(a.split()) <= set(w.split()) or set(w.split()) <= set(a.split())
        for a in api_variants
        for w in wanted
    ):
        # Token-subset, not substring: "ryan williams" must not match "bryan williams".
        score = 60
    else:
        return -1

    if (candidate.get("position") or "").upper() == position.upper():
        score += 25
    team = (candidate.get("team") or "").lower()
    if school and team and (school.lower() in team or team in school.lower()):
        score += 15
    # Prefer the still-active, fully-populated record when CFBD carries
    # duplicate athlete rows for the same person (it does, e.g. Isaiah Sategna).
    active_end = to_int(candidate.get("activeEndYear")) or 0
    if active_end >= 2026:
        score += 10
    elif active_end >= 2025:
        score += 5
    if to_float(candidate.get("height")) and to_float(candidate.get("weight")):
        score += 8
    return score


# Anything below this is treated as "not confident enough to attach".
# Exact name + position clears it; a looser name match needs the team to agree.
MIN_MATCH_SCORE = 110


def resolve_player(
    client: CFBD,
    name: str,
    position: str,
    school: str,
    fbs_teams: Optional[set] = None,
) -> Optional[Dict]:
    """Search CFBD for a player.

    Every spelling variant is tried with the team filter before any of them is
    tried without it, so a same-name player at another school cannot win just
    because the list spelled the name without periods.
    """
    team = cfbd_team_for(school)
    terms = search_terms_for(name)
    tiers: List[List[Dict]] = []
    if team:
        tiers.append([{"searchTerm": t, "position": position, "team": team} for t in terms])
    tiers.append([{"searchTerm": t, "position": position} for t in terms])
    tiers.append([{"searchTerm": t} for t in terms])

    best = None
    for tier in tiers:
        candidates: Dict[str, Dict] = {}
        for params in tier:
            for candidate in client.get("/player/search", params, timeout=30) or []:
                if fbs_teams and (candidate.get("team") or "") not in fbs_teams:
                    # These classes are all FBS players; an FCS namesake is noise.
                    continue
                candidates[str(candidate.get("id"))] = candidate
        scored = [
            (score_candidate(c, name, position, school), c) for c in candidates.values()
        ]
        scored = [pair for pair in scored if pair[0] >= MIN_MATCH_SCORE]
        if scored:
            scored.sort(key=lambda pair: pair[0], reverse=True)
            best = scored[0][1]
            break

    if best is None:
        return None

    jersey = to_int(best.get("jersey"))
    hometown = (best.get("hometown") or "").strip()
    return {
        "cfbd_id": to_int(best.get("id")),
        "cfbd_team": best.get("team"),
        "cfbd_position": best.get("position"),
        "height": to_float(best.get("height")),
        "weight": to_float(best.get("weight")),
        # CFBD uses 0/-1 as "unknown" jersey sentinels.
        "jersey": jersey if jersey and jersey > 0 else None,
        "hometown": hometown or None,
        "team_color": best.get("teamColor"),
    }


# ---------------------------------------------------------------------------
# Phase 2 — HS recruiting profiles
# ---------------------------------------------------------------------------


def build_fbs_team_set(client: CFBD) -> set:
    """CFBD team names for every FBS program (a few seasons, to cover moves)."""
    teams = set()
    for year in (2025, 2026):
        rows = client.get("/teams/fbs", {"year": year}, timeout=60) or []
        for row in rows:
            if row.get("school"):
                teams.add(row["school"])
    return teams


def build_recruiting_index(client: CFBD) -> Tuple[Dict[int, Dict], Dict[str, Dict]]:
    """Return (by_athlete_id, by_normalized_name) recruiting profiles."""
    by_id: Dict[int, Dict] = {}
    by_name: Dict[str, Dict] = {}
    for year in RECRUITING_YEARS:
        rows = client.get(
            "/recruiting/players", {"year": year, "classification": "HighSchool"}, timeout=120
        )
        if not rows:
            print(f"   recruiting {year}: no data")
            continue
        print(f"   recruiting {year}: {len(rows)} recruits")
        for row in rows:
            if (row.get("position") or "").upper() not in (
                "QB", "RB", "WR", "TE", "APB", "ATH", "PRO", "DUAL",
            ):
                continue
            profile = {
                "hs_stars": to_int(row.get("stars")),
                "hs_rating": to_float(row.get("rating")),
                "hs_rank": to_int(row.get("ranking")),
                "hs_school": row.get("school"),
                "hs_state": row.get("stateProvince"),
                "recruit_year": year,
            }
            athlete_id = to_int(row.get("athleteId"))
            if athlete_id:
                # Keep the highest-rated entry if a player appears twice.
                prev = by_id.get(athlete_id)
                if not prev or (profile["hs_rating"] or 0) > (prev["hs_rating"] or 0):
                    by_id[athlete_id] = profile
            key = normalize_name(row.get("name"))
            if key:
                prev = by_name.get(key)
                if not prev or (profile["hs_rating"] or 0) > (prev["hs_rating"] or 0):
                    by_name[key] = profile
    return by_id, by_name


# ---------------------------------------------------------------------------
# Phase 3 — college production
# ---------------------------------------------------------------------------


def build_stats_index(client: CFBD) -> Dict[Tuple[int, str], list]:
    cache: Dict[Tuple[int, str], list] = {}
    for year in STAT_SEASONS:
        for category in ("passing", "rushing", "receiving"):
            rows = client.get(
                "/stats/player/season", {"year": year, "category": category}, timeout=180
            )
            cache[(year, category)] = rows or []
            print(f"   stats {year}/{category}: {len(rows or [])} rows")
    return cache


def aggregate_stats(
    cache: Dict[Tuple[int, str], list],
    name: str,
    position: str,
    cfbd_id: Optional[int],
) -> Dict:
    """Aggregate counting stats across seasons, matching on CFBD id then name."""
    want_norm = normalize_name(name)
    want_id = str(cfbd_id) if cfbd_id else None
    agg: Dict[str, float] = {}
    seasons: Dict[int, bool] = {}
    teams = set()

    for category in POS_CATEGORIES.get(position, ("receiving", "rushing")):
        key_map = STAT_KEY_MAP[category]
        for year in STAT_SEASONS:
            for row in cache.get((year, category), []):
                row_id = str(row.get("playerId") or "")
                if want_id:
                    if row_id != want_id:
                        continue
                elif normalize_name(row.get("player")) != want_norm:
                    continue
                stat_type = row.get("statType")
                if stat_type not in key_map:
                    continue
                value = to_float(row.get("stat"))
                if value is None:
                    continue
                agg[key_map[stat_type]] = agg.get(key_map[stat_type], 0.0) + value
                seasons[year] = True
                if row.get("team"):
                    teams.add(row["team"])

    if not agg:
        return {}

    for key, value in list(agg.items()):
        agg[key] = int(round(value))

    if agg.get("pass_att"):
        agg["comp_pct"] = round(100.0 * agg.get("pass_comp", 0) / agg["pass_att"], 1)

    agg["seasons"] = len(seasons)
    agg["seasons_played"] = sorted(seasons)
    agg["total_games"] = 13 * len(seasons)
    if cfbd_id:
        agg["cfbd_player_id"] = cfbd_id
    if teams:
        agg["college_teams"] = sorted(teams)
        agg["transfer"] = len(teams) > 1
    agg["cfbd_stats_years_scanned"] = list(STAT_SEASONS)
    return agg


# ---------------------------------------------------------------------------
# Phase 4 — database sync
# ---------------------------------------------------------------------------


def load_existing(supabase) -> Tuple[Dict[str, Dict], Dict[int, List[Dict]]]:
    """Return (all rows keyed by normalized name, rows grouped by draft_year)."""
    rows = (
        supabase.table("dynasty_prospects")
        .select("id,name,position,school,draft_year,hs_stars,hs_rank,hs_rating,"
                "hs_school,hs_state,nfl_comparisons,college_stats,height,weight,"
                "espn_id,cfbd_id,headshot_url,hometown,jersey,team_color,class")
        .execute()
        .data
        or []
    )
    by_name: Dict[str, Dict] = {}
    by_year: Dict[int, List[Dict]] = defaultdict(list)
    for row in rows:
        by_year[row.get("draft_year")].append(row)
        key = normalize_name(row.get("name"))
        # Prefer the most recent class when a name appears more than once.
        prev = by_name.get(key)
        if not prev or (row.get("draft_year") or 0) > (prev.get("draft_year") or 0):
            by_name[key] = row
    return by_name, by_year


def headshot_for(espn_id: Optional[int]) -> Optional[str]:
    if not espn_id:
        return None
    return f"https://a.espncdn.com/i/headshots/college-football/players/full/{espn_id}.png"


def merge_college_stats(existing, fetched: Dict) -> Dict:
    if isinstance(existing, str):
        try:
            existing = json.loads(existing)
        except (TypeError, ValueError):
            existing = {}
    if not isinstance(existing, dict):
        existing = {}
    merged = dict(existing)
    merged.update(fetched)
    return merged


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def build_records(
    year: int,
    entries: Iterable[Tuple[int, str, str, str]],
    client: CFBD,
    recruit_by_id: Dict[int, Dict],
    recruit_by_name: Dict[str, Dict],
    stats_cache: Dict[Tuple[int, str], list],
    existing_by_name: Dict[str, Dict],
    fbs_teams: set,
) -> List[Dict]:
    records = []
    for overall_rank, name, position, school in entries:
        print(f"  [{year}] {overall_rank:>2}. {name} ({position}, {school})")
        existing = existing_by_name.get(normalize_name(name))

        resolved = resolve_player(client, name, position, school, fbs_teams) or {}
        if not resolved:
            print("       ! CFBD: no match")

        cfbd_id = resolved.get("cfbd_id") or (existing or {}).get("cfbd_id")
        # CFBD player ids are ESPN athlete ids for the modern player universe.
        espn_id = cfbd_id or (existing or {}).get("espn_id")

        lookup_name = NAME_ALIASES.get(name, name)
        recruit = {}
        if cfbd_id and cfbd_id in recruit_by_id:
            recruit = recruit_by_id[cfbd_id]
        else:
            for key in (normalize_name(lookup_name), normalize_name(name)):
                if key in recruit_by_name:
                    recruit = recruit_by_name[key]
                    break

        stats = aggregate_stats(stats_cache, lookup_name, position, cfbd_id)
        if not stats:
            print("       ! CFBD: no season stats")

        first_name, last_name = parse_name(name)
        cfbd_team = resolved.get("cfbd_team")
        if cfbd_team and school not in ("Transfer",) and cfbd_team.lower() != school.lower():
            print(f"       ~ school differs: list={school} cfbd={cfbd_team}")
        display_school = school if school != "Transfer" else (cfbd_team or "Transfer")

        record = {
            "name": name,
            # Derived from the ranking-list name so it matches what is displayed
            # (CFBD sometimes carries a legal name, e.g. Quintrevion "Tre" Wisner).
            "first_name": first_name,
            "last_name": last_name,
            "position": position,
            "school": display_school,
            "draft_year": year,
            "cfbd_id": cfbd_id,
            "espn_id": espn_id,
            "headshot_url": headshot_for(espn_id) or (existing or {}).get("headshot_url"),
            "height": resolved.get("height") or (existing or {}).get("height"),
            "weight": resolved.get("weight") or (existing or {}).get("weight"),
            "jersey": resolved.get("jersey") or (existing or {}).get("jersey"),
            "hometown": resolved.get("hometown") or (existing or {}).get("hometown"),
            "team_color": resolved.get("team_color") or (existing or {}).get("team_color"),
            "hs_stars": recruit.get("hs_stars") or (existing or {}).get("hs_stars"),
            "hs_rank": recruit.get("hs_rank") or (existing or {}).get("hs_rank"),
            "hs_rating": recruit.get("hs_rating") or (existing or {}).get("hs_rating"),
            "hs_school": recruit.get("hs_school") or (existing or {}).get("hs_school"),
            "hs_state": recruit.get("hs_state") or (existing or {}).get("hs_state"),
            "college_stats": merge_college_stats((existing or {}).get("college_stats"), stats),
            "college_games": stats.get("total_games") or (existing or {}).get("college_games"),
            "updated_at": datetime.now().isoformat(),
        }
        # Carry existing comps forward; the comps pipeline fills the rest.
        if existing and existing.get("nfl_comparisons"):
            record["nfl_comparisons"] = existing["nfl_comparisons"]

        records.append(
            {
                "overall_rank": overall_rank,
                "existing_id": (existing or {}).get("id"),
                "existing_year": (existing or {}).get("draft_year"),
                "record": record,
            }
        )
    return records


def apply_grades(items: List[Dict], use_real_games: bool) -> None:
    """Grade in-memory, seeding the model with the overall class rank."""
    for item in items:
        record = item["record"]
        payload = dict(record)
        payload["rank"] = item["overall_rank"]
        payload["consensus_rank"] = item["overall_rank"]
        if not use_real_games:
            # Match the rest of the table, which grades without a games denominator.
            payload["college_games"] = None
        grades = grade_prospect(payload)
        grades.pop("draft_year", None)
        record.update(grades)
        record["valuation"] = calculate_prospect_value(
            item["overall_rank"], record.get("position")
        )


def assign_ranks(items: List[Dict]) -> None:
    """Store the overall class rank.

    ``/api/prospects`` derives the positional rank it displays by ordering
    ``rank`` within (draft_year, position), so keeping the overall rank here
    preserves the published class ordering *and* still yields the right
    positional rank downstream.
    """
    for item in items:
        item["record"]["rank"] = item["overall_rank"]
        # Positional rank is derived, but keep it handy for the console report.
        item["positional_rank"] = 0
    by_position: Dict[str, List[Dict]] = defaultdict(list)
    for item in items:
        by_position[item["record"]["position"]].append(item)
    for group in by_position.values():
        group.sort(key=lambda i: i["overall_rank"])
        for index, item in enumerate(group, start=1):
            item["positional_rank"] = index


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="Do not write to the database")
    parser.add_argument("--only-year", type=int, choices=(2027, 2028))
    parser.add_argument(
        "--real-games",
        action="store_true",
        help="Grade production with an actual games-played denominator "
             "(more accurate, but not comparable to the legacy classes)",
    )
    parser.add_argument("--api-key", help="CFBD API key override")
    args = parser.parse_args()

    api_key = args.api_key or os.getenv("CFBD_API_KEY")
    if not api_key:
        print("ERROR: CFBD_API_KEY not set")
        return 1

    supabase = config.get_supabase_client()
    if not supabase:
        print("ERROR: Supabase client unavailable")
        return 1

    client = CFBD(api_key)
    years = [args.only_year] if args.only_year else sorted(RANKINGS)

    print("=" * 78)
    print("IMPORT 2027 / 2028 DYNASTY ROOKIE RANKINGS")
    print("=" * 78)

    print("\nLoading existing prospects...")
    existing_by_name, existing_by_year = load_existing(supabase)
    print(f"   {sum(len(v) for v in existing_by_year.values())} rows in dynasty_prospects")

    print("\nLoading FBS team list...")
    fbs_teams = build_fbs_team_set(client)
    print(f"   {len(fbs_teams)} FBS programs")

    print("\nBuilding HS recruiting index...")
    recruit_by_id, recruit_by_name = build_recruiting_index(client)
    print(f"   {len(recruit_by_id)} recruits indexed by athlete id")

    print("\nBuilding college season stat index...")
    stats_cache = build_stats_index(client)

    all_items: Dict[int, List[Dict]] = {}
    for year in years:
        print(f"\nResolving {year} class...")
        items = build_records(
            year,
            RANKINGS[year],
            client,
            recruit_by_id,
            recruit_by_name,
            stats_cache,
            existing_by_name,
            fbs_teams,
        )
        apply_grades(items, args.real_games)
        assign_ranks(items)
        all_items[year] = items

    # ---- report -----------------------------------------------------------
    for year, items in all_items.items():
        print(f"\n{'=' * 78}\n{year} CLASS ({len(items)})\n{'=' * 78}")
        for item in sorted(items, key=lambda i: i["overall_rank"]):
            r = item["record"]
            stats = r.get("college_stats") or {}
            flags = []
            if not r.get("cfbd_id"):
                flags.append("no-cfbd-id")
            if not stats.get("seasons"):
                flags.append("no-stats")
            if not r.get("hs_stars"):
                flags.append("no-hs")
            if not r.get("height"):
                flags.append("no-size")
            print(
                f"{item['overall_rank']:>3}. {r['name']:<26} {r['position']:<3} "
                f"{str(r['school'])[:16]:<16} {r['position']}{item['positional_rank']:<3} "
                f"grade={r.get('overall_grade'):<6} {str(r.get('grade_tier')):<12} "
                f"{' '.join(flags)}"
            )

    if args.dry_run:
        print("\n[DRY RUN] no database writes")
        return 0

    # ---- write ------------------------------------------------------------
    inserted = updated = deleted = 0
    for year, items in all_items.items():
        keep_ids = set()
        for item in items:
            record = item["record"]
            if item["existing_id"]:
                supabase.table("dynasty_prospects").update(record).eq(
                    "id", item["existing_id"]
                ).execute()
                keep_ids.add(item["existing_id"])
                updated += 1
            else:
                created = (
                    supabase.table("dynasty_prospects").insert(record).execute().data or []
                )
                if created:
                    keep_ids.add(created[0]["id"])
                inserted += 1

        stale = [
            row["id"]
            for row in existing_by_year.get(year, [])
            if row["id"] not in keep_ids
        ]
        if stale:
            supabase.table("dynasty_prospects").delete().in_("id", stale).execute()
            deleted += len(stale)
            print(f"\nRemoved {len(stale)} stale {year} rows")

    print(f"\nDone. inserted={inserted} updated={updated} deleted={deleted}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
