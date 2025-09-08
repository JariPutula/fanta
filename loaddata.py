# -*- coding: utf-8 -*-
"""
Created on Sat Sep  6 17:18:39 2025

@author: jarip
"""

#!/usr/bin/env python3
"""
load_fantacalcio_to_postgres.py

Reads the output Excel from merge_fantacalcio_batch.py and loads it into PostgreSQL:
- fantacalcio.stg_merged_all       (staging; replaced each run)
- fantacalcio.dim_round            (season/round per round_code; upsert)
- fantacalcio.dim_player           (player canonical key + display; upsert)
- fantacalcio.fact_observations    (long table of votes/metrics by source; per-run rounds reloaded)

Requirements:  pip install pandas sqlalchemy psycopg2-binary openpyxl
"""

import os
import re
from pathlib import Path
from typing import List, Dict, Set

import pandas as pd
from sqlalchemy import create_engine, text

# ======= CONFIG: point this to the Excel produced by merge_fantacalcio_batch.py =======
EXCEL_PATH = Path(r"C:\virtual\projects\fanta\data\fantacalcio_merged_all_rounds.xlsx")
SCHEMA = "fantacalcio"

# Base columns we keep as metadata (do not unpivot)
META_COLS = {
    "season", "round", "round_code",
    "match_flag", "match_method", "similarity",
    "giocatore", "giocatore_left", "giocatore_right",
    "__giocatore_norm", "__source_file_left", "__source_file_right"
}

def get_engine():
    # Prefer DATABASE_URL; else build from PG* env vars
    url = os.getenv("DATABASE_URL")
    if not url:
        host = os.getenv("PGHOST", "localhost")
        port = os.getenv("PGPORT", "5433")
        db   = os.getenv("PGDATABASE", "fantacalcio")
        user = os.getenv("PGUSER", "postgres")
        pw   = os.getenv("PGPASSWORD", "admin")
        url = f"postgresql+psycopg2://{user}:{pw}@{host}:{port}/{db}"
    return create_engine(url, future=True)

def coerce_numeric(x):
    if x is None or (isinstance(x, float) and pd.isna(x)) or (isinstance(x, str) and x.strip() == ""):
        return None
    try:
        if isinstance(x, (int, float)):
            return float(x)
        s = str(x).strip().replace(",", ".")  # handle decimal comma
        return float(s)
    except Exception:
        return None

def load_excel_sheet(path: Path) -> pd.DataFrame:
    # Prefer "merged_all" (batch output); fallback to "merged" (single-round)
    x = pd.ExcelFile(path)
    sheet = "merged_all" if "merged_all" in x.sheet_names else "merged"
    df = pd.read_excel(path, sheet_name=sheet)
    # Ensure key columns exist
    for col in ["round_code", "season", "round", "__giocatore_norm"]:
        if col not in df.columns:
            raise RuntimeError(f"Expected column '{col}' not found in sheet '{sheet}'.")
    return df

def detect_extra_slugs(columns: List[str]) -> Set[str]:
    # Slugs come from columns named 'giocatore_<slug>'
    slugs = set()
    for c in columns:
        if c.startswith("giocatore_") and c != "giocatore_right" and c != "giocatore_left":
            parts = c.split("_", 1)
            if len(parts) == 2 and parts[1]:
                slugs.add(parts[1])
    return slugs

def build_observations_long(df: pd.DataFrame) -> pd.DataFrame:
    """
    Turn wide columns into long observations:
      - source = 'right' for columns ending with '_right'
      - source = <slug> for columns ending with f'_{slug}' where 'giocatore_<slug>' exists
      - metric = column name without the suffix
    Only non-null values are emitted.
    """
    cols = list(df.columns)
    slugs = detect_extra_slugs(cols)

    base = df[["round_code", "__giocatore_norm"]].copy()
    obs_frames = []

    # RIGHT (main votes) — columns ending with '_right'
    right_cols = [c for c in cols if c.endswith("_right") and c not in {"__source_file_right"}]
    if right_cols:
        tmp = pd.concat([base, df[right_cols]], axis=1)
        melted = tmp.melt(id_vars=["round_code", "__giocatore_norm"], var_name="col", value_name="value")
        melted = melted[melted["value"].notna()]
        melted["source"] = "right"
        melted["metric"] = melted["col"].str[:-6]  # strip '_right'
        melted = melted.drop(columns=["col"])
        obs_frames.append(melted)

    # EXTRA sheets per slug — columns ending with f'_{slug}' and not the 'giocatore_<slug>'
    for slug in slugs:
        suffix = f"_{slug}"
        slug_cols = [c for c in cols if c.endswith(suffix) and c != f"giocatore_{slug}"]
        if not slug_cols:
            continue
        tmp = pd.concat([base, df[slug_cols]], axis=1)
        melted = tmp.melt(id_vars=["round_code", "__giocatore_norm"], var_name="col", value_name="value")
        melted = melted[melted["value"].notna()]
        melted["source"] = slug
        melted["metric"] = melted["col"].str[:-len(suffix)]
        melted = melted.drop(columns=["col"])
        obs_frames.append(melted)

    if not obs_frames:
        return pd.DataFrame(columns=["round_code","__giocatore_norm","source","metric","value_text","value_numeric"])

    obs = pd.concat(obs_frames, ignore_index=True)
    # Store numeric + text
    obs["value_numeric"] = obs["value"].map(coerce_numeric)
    obs["value_text"] = obs["value"].astype(str)
    obs = obs.drop(columns=["value"])
    # Deduplicate (some merges can produce duplicates)
    obs = obs.drop_duplicates(subset=["round_code", "__giocatore_norm", "source", "metric", "value_text", "value_numeric"])
    return obs

def main():
    if not EXCEL_PATH.exists():
        raise FileNotFoundError(f"Excel not found: {EXCEL_PATH}")

    df = load_excel_sheet(EXCEL_PATH)

    eng = get_engine()
    with eng.begin() as conn:
        # Ensure schema
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}"))

        # Stage raw merged_all
        df.to_sql("stg_merged_all", conn, schema=SCHEMA, if_exists="replace", index=False)

        # Core dims
        conn.execute(text(f"""
        CREATE TABLE IF NOT EXISTS {SCHEMA}.dim_round (
          round_code TEXT PRIMARY KEY,
          season     INT NOT NULL,
          round      INT NOT NULL
        );"""))

        conn.execute(text(f"""
        CREATE TABLE IF NOT EXISTS {SCHEMA}.dim_player (
          __giocatore_norm TEXT PRIMARY KEY,
          giocatore        TEXT
        );"""))

        # Observations fact (long)
        conn.execute(text(f"""
        CREATE TABLE IF NOT EXISTS {SCHEMA}.fact_observations (
          round_code        TEXT NOT NULL REFERENCES {SCHEMA}.dim_round(round_code) ON DELETE CASCADE,
          __giocatore_norm  TEXT NOT NULL REFERENCES {SCHEMA}.dim_player(__giocatore_norm) ON DELETE CASCADE,
          source            TEXT NOT NULL,
          metric            TEXT NOT NULL,
          value_numeric     DOUBLE PRECISION NULL,
          value_text        TEXT NULL,
          created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          CONSTRAINT fact_observations_pk PRIMARY KEY (round_code, __giocatore_norm, source, metric)
        );"""))

        # Upsert rounds
        conn.execute(text(f"""
        INSERT INTO {SCHEMA}.dim_round (round_code, season, round)
        SELECT DISTINCT round_code::TEXT, season::INT, round::INT
        FROM {SCHEMA}.stg_merged_all
        WHERE round_code IS NOT NULL
        ON CONFLICT (round_code)
        DO UPDATE SET season = EXCLUDED.season, round = EXCLUDED.round;"""))

        # Upsert players (prefer any non-null display name)
        conn.execute(text(f"""
        INSERT INTO {SCHEMA}.dim_player (__giocatore_norm, giocatore)
        SELECT DISTINCT __giocatore_norm::TEXT,
               NULLIF(TRIM(COALESCE(giocatore, giocatore_left, giocatore_right, '')),'') AS giocatore
        FROM {SCHEMA}.stg_merged_all
        WHERE __giocatore_norm IS NOT NULL
        ON CONFLICT (__giocatore_norm)
        DO UPDATE SET giocatore = COALESCE(EXCLUDED.giocatore, {SCHEMA}.dim_player.giocatore);"""))

    # Build observations in Python (dynamic sources/metrics)
    obs = build_observations_long(df)

    with eng.begin() as conn:
        # Delete facts for the rounds we’re reloading (idempotent)
        round_codes = df["round_code"].dropna().astype(str).unique().tolist()
        if round_codes:
            conn.execute(text(f"DELETE FROM {SCHEMA}.fact_observations WHERE round_code = ANY(:codes)"),
                         {"codes": round_codes})

        if not obs.empty:
            # Stage then upsert to handle big inserts safely
            obs.to_sql("stg_observations", conn, schema=SCHEMA, if_exists="replace", index=False)
            conn.execute(text(f"""
            INSERT INTO {SCHEMA}.fact_observations
              (round_code, __giocatore_norm, source, metric, value_numeric, value_text)
            SELECT round_code::TEXT, __giocatore_norm::TEXT, source::TEXT, metric::TEXT,
                   NULLIF(value_numeric::TEXT,'')::DOUBLE PRECISION AS value_numeric,
                   NULLIF(value_text,'')::TEXT AS value_text
            FROM {SCHEMA}.stg_observations
            ON CONFLICT (round_code, __giocatore_norm, source, metric)
            DO UPDATE SET
              value_numeric = EXCLUDED.value_numeric,
              value_text    = EXCLUDED.value_text,
              updated_at    = NOW();"""))

            # Optional indexes (speed up queries)
            conn.execute(text(f"CREATE INDEX IF NOT EXISTS fact_obs_round ON {SCHEMA}.fact_observations(round_code);"))
            conn.execute(text(f"CREATE INDEX IF NOT EXISTS fact_obs_player ON {SCHEMA}.fact_observations(__giocatore_norm);"))
            conn.execute(text(f"CREATE INDEX IF NOT EXISTS fact_obs_source ON {SCHEMA}.fact_observations(source);"))

    print("Load complete.")

if __name__ == "__main__":
    main()
