# -*- coding: utf-8 -*-
"""
Created on Mon Aug 25 19:16:56 2025

@author: jarip
"""

import re
import pandas as pd
from pathlib import Path

# =========================================================
# === CONFIG: set your file paths here ====================
# =========================================================
fanta_file = Path(r"C:\virtual\projects\fanta\fanta-26-alku.xlsx")
voti_file  = Path(r"C:\virtual\projects\fanta\VotiExcelUfficiali202501.xlsx")
outdir     = Path(r"C:\virtual\projects\fanta\out")  # output folder for CSVs
outdir.mkdir(parents=True, exist_ok=True)

# If you know the exact numeric column name in the Voti file, set it here (case-sensitive).
# Example: VOTI_NUM_COL = "Num"
VOTI_NUM_COL = None  # leave None to auto-detect a numeric id column


# =========================================================
# === Helpers =============================================
# =========================================================

def extract_first_int(x):
    """
    Return the first integer found in a cell as int, else <NA>.
    Works for '105 Carnesecchi' -> 105, '105' -> 105, otherwise <NA>.
    """
    if pd.isna(x):
        return pd.NA
    s = str(x)
    m = re.search(r"\d+", s)
    if m:
        try:
            return int(m.group(0))
        except Exception:
            return pd.NA
    return pd.NA

def find_fanta_key_col(df: pd.DataFrame) -> str:
    """
    Use the first column of the Fanta file as the source for the numeric join key.
    """
    return df.columns[0]

def pick_numeric_voti_col(df: pd.DataFrame, preferred: str | None = None) -> str:
    """
    Choose a numeric 'id' column from the Voti sheet:
    1) If 'preferred' provided and exists, use it.
    2) Try common names (case-insensitive).
    3) Fallback: scan all columns and pick the one with the most integer-like entries.
    """
    if preferred and preferred in df.columns:
        return preferred

    # Try common candidates (case-insensitive)
    common = ["num", "numero", "id", "cod", "codice", "n", "#", "fgid", "player_id", "code", "value", "valore"]
    lower_map = {c.lower(): c for c in df.columns}
    for c in common:
        if c in lower_map:
            return lower_map[c]

    # Fallback: scan all columns
    best_col, best_count = None, -1
    for c in df.columns:
        ints = df[c].apply(extract_first_int)
        cnt = ints.notna().sum()
        if cnt > best_count:
            best_col, best_count = c, cnt
    return best_col


# =========================================================
# === Load =================================================
# =========================================================

# Read first sheet from each workbook
df_fanta = pd.read_excel(fanta_file, engine="openpyxl")
df_voti  = pd.read_excel(voti_file,  engine="openpyxl")

# =========================================================
# === Build numeric keys ==================================
# =========================================================

# Fanta: use first column as source for numeric id
fanta_key_col = find_fanta_key_col(df_fanta)
df_fanta["_num_key"] = df_fanta[fanta_key_col].apply(extract_first_int)

# Find 'org' column (case-insensitive)
org_col = None
for c in df_fanta.columns:
    if str(c).strip().lower() == "org":
        org_col = c
        break

# Voti: pick numeric id column
voti_num_col = pick_numeric_voti_col(df_voti, preferred=VOTI_NUM_COL)
df_voti["_num_key"] = df_voti[voti_num_col].apply(extract_first_int)

# =========================================================
# === Merge (full outer on _num_key) ======================
# =========================================================

# Keep only first-column + org + numeric key from Fanta
fanta_keep = [fanta_key_col, "_num_key"]
if org_col:
    fanta_keep.append(org_col)
df_fanta_slim = df_fanta[fanta_keep].copy()

# Keep ALL Voti columns (plus numeric key)
df_voti_slim = df_voti.copy()

merged = pd.merge(
    df_fanta_slim, df_voti_slim,
    on="_num_key", how="outer", suffixes=("_fanta", "_voti")
)

# Reorder columns: keys & org up front for readability
front_cols = ["_num_key"]
if org_col and org_col in merged.columns:
    front_cols.append(org_col)
if fanta_key_col in merged.columns:
    front_cols.append(fanta_key_col)
if voti_num_col in merged.columns:
    front_cols.append(voti_num_col)

other_cols = [c for c in merged.columns if c not in front_cols]
merged = merged[front_cols + other_cols]

# =========================================================
# === Unmatched diagnostics ===============================
# =========================================================

# Rows present only in Fanta (no Voti id)
if voti_num_col in merged.columns:
    fanta_only = merged[merged[voti_num_col].isna()].copy()
else:
    # fallback: rows with all original Voti columns missing
    voti_original_cols = [c for c in df_voti_slim.columns if c != "_num_key"]
    fanta_only = merged[merged[voti_original_cols].isna().all(axis=1)].copy()

# Rows present only in Voti (no Fanta first col)
if fanta_key_col in merged.columns:
    voti_only = merged[merged[fanta_key_col].isna()].copy()
else:
    base_cols = [org_col] if org_col else []
    voti_only = merged[merged[base_cols].isna().all(axis=1)].copy()

# =========================================================
# === Save outputs ========================================
# =========================================================

merged.to_csv(outdir / "combined_full_outer.csv", index=False)
fanta_only.to_csv(outdir / "unmatched_from_fanta.csv", index=False)
voti_only.to_csv(outdir / "unmatched_from_voti.csv", index=False)

print("✓ Done.")
print("Fanta key column   :", fanta_key_col)
print("Voti numeric column:", voti_num_col)
print("Rows in merged     :", len(merged))
print("Unmatched (fanta)  :", len(fanta_only))
print("Unmatched (voti)   :", len(voti_only))

# After running in Spyder, you can inspect:
# - merged
# - fanta_only
# - voti_only

