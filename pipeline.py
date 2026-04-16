"""
foot-etl · pandas pipeline → SQLite

Star schema (from ERD):
  CITY     (id_city, city_name)
  STADIUM  (id_stadium, stadium_name, id_city FK)
  TEAM     (id_team, team_name)
  EDITION  (id_edition, edition_name, year)
  MATCH    (id_match, date, round, home_result, away_result, result,
            id_home_team FK, id_away_team FK, id_stadium FK, id_edition FK)
"""

import json
import re
import sqlite3
from pathlib import Path

import pandas as pd

DB_PATH  = Path("foot_etl.sqlite")
DATA_DIR = Path("data_raw")

# ── Round label normalisation ────────────────────────────────
ROUND_MAP = {
    "GROUP_STAGE":        "Group stage",
    "FIRST":              "Group stage",
    "FINAL_ROUND":        "Group stage",
    "SEMIFINAL_STAGE":    "Semi-final",
    "QUARTERFINAL_STAGE": "Quarter-final",
    "1/8_FINAL":          "Round of 16",
    "1/4_FINAL":          "Quarter-final",
    "1/2_FINAL":          "Semi-final",
    "_FINAL":             "Final",
    "PLACES_3&4":         "Third place",
}


def _norm_round(raw: str) -> str:
    if pd.isna(raw):
        return "Unknown"
    r = str(raw).strip()
    if r in ROUND_MAP:
        return ROUND_MAP[r]
    if r.startswith("PRELIMINARY"):
        return "Preliminary"
    return r.replace("_", " ").strip()


def _result(home: float, away: float) -> str:
    if pd.isna(home) or pd.isna(away):
        return "unknown"
    if home > away:
        return "home"
    if away > home:
        return "away"
    return "draw"


def _clean_team(name: str) -> str:
    return re.sub(r"\s*\(.*?\)", "", str(name)).strip()


# ─────────────────────────────────────────────────────────────
# Extract
# ─────────────────────────────────────────────────────────────

def extract() -> dict[str, pd.DataFrame]:
    matches = pd.read_csv(DATA_DIR / "matches_19302010.csv")
    wc2014  = pd.read_csv(DATA_DIR / "WorldCupMatches2014.csv", delimiter=";")
    with open(DATA_DIR / "worldcup_2022.json") as f:
        wc2022 = pd.DataFrame(json.load(f)["matches"])
    return {"matches": matches, "wc2014": wc2014, "wc2022": wc2022}


# ─────────────────────────────────────────────────────────────
# Flatten each source to a common intermediate format
# ─────────────────────────────────────────────────────────────
# Intermediate columns:
#   home_team, away_team, home_result, away_result, result,
#   date, round, stadium_name, city_name, edition_year, edition_name

def _flat_matches(df: pd.DataFrame) -> pd.DataFrame:
    """matches_19302010 — WC finals only, no qualifiers, no 2014+."""
    out = df[
        (df["year"] < 2014) & (~df["round"].str.startswith("PRELIMINARY"))
    ].copy()

    out["home_team"] = out["team1"].apply(_clean_team)
    out["away_team"] = out["team2"].apply(_clean_team)

    goals = out["score"].str.extract(r"(\d+)-(\d+)").astype(float)
    out["home_result"] = goals[0]
    out["away_result"] = goals[1]
    out["result"]      = out.apply(lambda r: _result(r["home_result"], r["away_result"]), axis=1)

    out["date"]         = pd.to_datetime(out["year"].astype(str), format="%Y").dt.strftime("%Y-01-01")
    out["round"]        = out["round"].apply(_norm_round)
    # venue = "Montevideo." → use as stadium_name; no separate city available
    out["stadium_name"] = out["venue"].str.rstrip(".").str.strip()
    out["city_name"]    = out["venue"].str.rstrip(".").str.strip()   # best approximation
    out["edition_year"] = out["year"].astype(int)
    out["edition_name"] = out["edition"].str.split("-").str[1:].str.join(" ").str.title()

    out.drop_duplicates(subset=["home_team", "away_team", "date", "home_result", "away_result"],
                        inplace=True)
    return out[["home_team","away_team","home_result","away_result","result",
                "date","round","stadium_name","city_name","edition_year","edition_name"]]


def _flat_wc2014(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    str_cols = out.select_dtypes(include="str").columns
    out[str_cols] = out[str_cols].apply(lambda c: c.str.strip())

    out["home_team"]   = out["Home Team Name"].str.title()
    out["away_team"]   = out["Away Team Name"].str.title()
    out["home_result"] = out["Home Team Goals"].astype(float)
    out["away_result"] = out["Away Team Goals"].astype(float)
    out["result"]      = out.apply(lambda r: _result(r["home_result"], r["away_result"]), axis=1)

    out["date"]         = pd.to_datetime(out["Datetime"].str.strip(),
                                         format="%d %b %Y - %H:%M").dt.strftime("%Y-%m-%d")
    out["round"]        = out["Stage"].apply(_norm_round)
    out["stadium_name"] = out["Stadium"].str.strip()
    out["city_name"]    = out["City"].str.title()
    out["edition_year"] = 2014
    out["edition_name"] = "Brazil"

    out = out.dropna(subset=["home_result", "away_result"])
    out.drop_duplicates(subset=["home_team", "away_team", "date"], inplace=True)
    return out[["home_team","away_team","home_result","away_result","result",
                "date","round","stadium_name","city_name","edition_year","edition_name"]]


def _flat_wc2022(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    out["home_team"]   = out["team1"].str.strip()
    out["away_team"]   = out["team2"].str.strip()
    out["home_result"] = out["score"].apply(lambda s: float(s["ft"][0]))
    out["away_result"] = out["score"].apply(lambda s: float(s["ft"][1]))
    out["result"]      = out.apply(lambda r: _result(r["home_result"], r["away_result"]), axis=1)

    out["date"]  = pd.to_datetime(out["date"]).dt.strftime("%Y-%m-%d")
    out["round"] = out.apply(
        lambda r: _norm_round(r["group"] if pd.notna(r.get("group")) else r["round"]), axis=1
    )
    # ground = "Al Bayt Stadium, Al Khor"
    out["stadium_name"] = out["ground"].str.split(",").str[0].str.strip()
    out["city_name"]    = out["ground"].str.split(",").str[-1].str.strip()
    out["edition_year"] = 2022
    out["edition_name"] = "Qatar"

    out.drop_duplicates(subset=["home_team", "away_team", "date"], inplace=True)
    return out[["home_team","away_team","home_result","away_result","result",
                "date","round","stadium_name","city_name","edition_year","edition_name"]]


# ─────────────────────────────────────────────────────────────
# Build dimension tables + fact table
# ─────────────────────────────────────────────────────────────

def build_schema(flat: pd.DataFrame) -> dict[str, pd.DataFrame]:
    # ── CITY ────────────────────────────────────────────────
    cities = (
        flat["city_name"].dropna().drop_duplicates()
        .reset_index(drop=True)
        .rename("city_name")
        .to_frame()
    )
    cities.insert(0, "id_city", range(1, len(cities) + 1))

    # ── STADIUM ─────────────────────────────────────────────
    stadiums = (
        flat[["stadium_name", "city_name"]].dropna(subset=["stadium_name"])
        .drop_duplicates(subset=["stadium_name"])
        .reset_index(drop=True)
        .merge(cities, on="city_name", how="left")
        [["stadium_name", "id_city"]]
    )
    stadiums.insert(0, "id_stadium", range(1, len(stadiums) + 1))

    # ── TEAM ────────────────────────────────────────────────
    all_teams = pd.concat([flat["home_team"], flat["away_team"]]).dropna().drop_duplicates()
    teams = all_teams.sort_values().reset_index(drop=True).rename("team_name").to_frame()
    teams.insert(0, "id_team", range(1, len(teams) + 1))

    # ── EDITION ─────────────────────────────────────────────
    editions = (
        flat[["edition_year", "edition_name"]].drop_duplicates()
        .sort_values("edition_year")
        .reset_index(drop=True)
        .rename(columns={"edition_year": "year", "edition_name": "edition_name"})
    )
    editions.insert(0, "id_edition", range(1, len(editions) + 1))

    # ── MATCH (fact) ─────────────────────────────────────────
    team_map    = teams.set_index("team_name")["id_team"]
    stadium_map = stadiums.set_index("stadium_name")["id_stadium"]
    edition_map = editions.set_index("year")["id_edition"]

    matches = flat.copy()
    matches["id_home_team"] = matches["home_team"].map(team_map)
    matches["id_away_team"] = matches["away_team"].map(team_map)
    matches["id_stadium"]   = matches["stadium_name"].map(stadium_map)
    matches["id_edition"]   = matches["edition_year"].map(edition_map)

    matches = matches[["date","round","home_result","away_result","result",
                        "id_home_team","id_away_team","id_stadium","id_edition"]]
    matches = matches.reset_index(drop=True)
    matches.insert(0, "id_match", range(1, len(matches) + 1))

    return {
        "city":    cities,
        "stadium": stadiums,
        "team":    teams,
        "edition": editions,
        "match":   matches,
    }


# ─────────────────────────────────────────────────────────────
# Transform
# ─────────────────────────────────────────────────────────────

def transform(dfs: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    flat_hist = _flat_matches(dfs["matches"])
    flat_2014 = _flat_wc2014(dfs["wc2014"])
    flat_2022 = _flat_wc2022(dfs["wc2022"])

    print(f"matches (1930‑2010): {len(flat_hist)} rows")
    print(f"wc2014:              {len(flat_2014)} rows")
    print(f"wc2022:              {len(flat_2022)} rows")

    flat = (
        pd.concat([flat_hist, flat_2014, flat_2022], ignore_index=True)
        .sort_values(["edition_year", "date"])
        .reset_index(drop=True)
    )
    print(f"unified flat:        {len(flat)} rows total")

    return build_schema(flat)


# ─────────────────────────────────────────────────────────────
# Load
# ─────────────────────────────────────────────────────────────

def load(dfs: dict[str, pd.DataFrame], db_path: Path = DB_PATH) -> None:
    with sqlite3.connect(db_path) as con:
        # Drop all existing tables first for a clean rebuild
        cur = con.cursor()
        cur.executescript("""
            PRAGMA foreign_keys = OFF;
            DROP TABLE IF EXISTS match;
            DROP TABLE IF EXISTS stadium;
            DROP TABLE IF EXISTS city;
            DROP TABLE IF EXISTS team;
            DROP TABLE IF EXISTS edition;
            DROP TABLE IF EXISTS matches;
            DROP TABLE IF EXISTS stadiums;
            DROP TABLE IF EXISTS teams;
            DROP TABLE IF EXISTS tv_channels;
            DROP TABLE IF EXISTS wc2014;
            DROP TABLE IF EXISTS wc2022;
            PRAGMA foreign_keys = ON;
        """)

        # Create tables with FK constraints
        con.executescript("""
            CREATE TABLE city (
                id_city   INTEGER PRIMARY KEY,
                city_name TEXT NOT NULL
            );
            CREATE TABLE stadium (
                id_stadium   INTEGER PRIMARY KEY,
                stadium_name TEXT NOT NULL,
                id_city      INTEGER REFERENCES city(id_city)
            );
            CREATE TABLE team (
                id_team   INTEGER PRIMARY KEY,
                team_name TEXT NOT NULL
            );
            CREATE TABLE edition (
                id_edition   INTEGER PRIMARY KEY,
                edition_name TEXT NOT NULL,
                year         INTEGER NOT NULL
            );
            CREATE TABLE match (
                id_match     INTEGER PRIMARY KEY,
                date         TEXT,
                round        TEXT,
                home_result  REAL,
                away_result  REAL,
                result       TEXT,
                id_home_team INTEGER REFERENCES team(id_team),
                id_away_team INTEGER REFERENCES team(id_team),
                id_stadium   INTEGER REFERENCES stadium(id_stadium),
                id_edition   INTEGER REFERENCES edition(id_edition)
            );
        """)

        # Insert data
        for table_name, df in dfs.items():
            df.to_sql(table_name, con, if_exists="append", index=False)
            print(f"  ✓ {table_name:12s} — {len(df):>5} rows → {db_path.name}")


# ─────────────────────────────────────────────────────────────
# Pipeline
# ─────────────────────────────────────────────────────────────

def main() -> None:
    print("── foot-etl pandas pipeline ────────────────────────────────")

    print("\n[1/3] Extracting…")
    raw = extract()

    print("\n[2/3] Transforming & normalising…")
    clean = transform(raw)

    print("\n[3/3] Loading into SQLite…")
    load(clean)

    print(f"\n✓ Done. Database: {DB_PATH.resolve()}")


if __name__ == "__main__":
    main()
