import re
import pandas as pd
from pathlib import Path
from typing import Any, Optional, List, Dict
from rapidfuzz import process, fuzz

# ==============================
# CONFIG: set your paths here
# ==============================
combined_csv = Path(r"C:\virtual\projects\fanta\out\combined_full_outer.csv")   # <-- your earlier combined
new_excel    = Path(r"C:\virtual\projects\fanta\Voti_Fantacalcio_Stagione_2025_26_Giornata_1(1).xlsx")
outdir       = Path(r"C:\virtual\projects\fanta\out")
outdir.mkdir(parents=True, exist_ok=True)

# Fuzzy match acceptance threshold (0..100). 85 is usually good.
MATCH_THRESHOLD = 85

# OPTIONAL: if you know the name column in your old combined, set it.
# Otherwise auto-detect tries: Giocatore/Nome/Player/Calciatore/Name.
COMBINED_NAME_COL = None  # e.g. "Giocatore"

# ==============================
# Helpers
# ==============================
# Lightweight accent/cleanup (works without extra packages)
ACCENT_MAP = str.maketrans(
    "àáâãäåèéêëìíîïòóôõöùúûüçñýÿÀÁÂÃÄÅÈÉÊËÌÍÎÏÒÓÔÕÖÙÚÛÜÇÑÝ",
    "aaaaaaeeeeiiiiooooouuuucnyyAAAAAAEEEEIIIIOOOOOUUUUCNY"
)
def strip_accents_basic(s: str) -> str:
    return s.translate(ACCENT_MAP)

def normalize_name(s: Any) -> str:
    if pd.isna(s):
        return ""
    s = str(s)
    s = strip_accents_basic(s).lower()
    s = re.sub(r"[^a-z\s]", " ", s)  # keep letters/spaces
    s = " ".join(s.split())
    return s

def looks_like_header(row_vals: List[Any]) -> bool:
    """Header row if any cell equals 'nome' after normalization."""
    for v in row_vals:
        if isinstance(v, str) and normalize_name(v) == "nome":
            return True
    return False

def looks_like_team_row(row_vals: List[Any]) -> bool:
    """Team title row: exactly one non-empty cell (alphabetic), e.g. 'Atalanta'."""
    non_null = [v for v in row_vals if (pd.notna(v) and str(v).strip() != "")]
    if len(non_null) == 1:
        v = str(non_null[0]).strip()
        if v and v.replace(" ", "").isalpha() and normalize_name(v) != "nome":
            return True
    return False

def parse_sheet(sheet_df: pd.DataFrame) -> pd.DataFrame:
    """
    sheet_df is read with header=None and skiprows=4.
    Scan for header rows (with 'nome'), then collect records until next team/header.
    """
    records: List[Dict[str, Any]] = []
    current_cols: Optional[List[str]] = None

    for idx in range(sheet_df.shape[0]):
        row = sheet_df.iloc[idx].tolist()
        if all(pd.isna(x) or str(x).strip()=="" for x in row):
            continue

        if looks_like_header(row):
            # Build column names from this row
            cols = [str(v).strip() if pd.notna(v) else "" for v in row]
            current_cols = [re.sub(r"\s+", "_", strip_accents_basic(c).strip().lower()) for c in cols]
            continue

        if looks_like_team_row(row):
            # boundary between teams; just skip the title row
            continue

        if current_cols is not None:
            vals = row[:len(current_cols)] + [""] * max(0, len(current_cols)-len(row))
            rec = dict(zip(current_cols, vals))
            records.append(rec)

    df = pd.DataFrame.from_records(records)
    # Find the 'nome' column among normalized columns
    nome_col = None
    for c in df.columns:
        if c == "nome":
            nome_col = c
            break
    if nome_col is None:
        for c in df.columns:
            if normalize_name(c) == "nome":
                nome_col = c
                break
    if nome_col is None:
        # no usable 'nome' found in this sheet
        return pd.DataFrame(columns=["nome"])

    # Drop rows with empty nome; enforce exact column name 'nome'
    df = df[df[nome_col].astype(str).str.strip() != ""]
    if nome_col != "nome":
        df = df.rename(columns={nome_col: "nome"})
    return df

def autodetect_name_col(df: pd.DataFrame) -> Optional[str]:
    if COMBINED_NAME_COL and COMBINED_NAME_COL in df.columns:
        return COMBINED_NAME_COL
    candidates = ["Giocatore","giocatore","Nome","nome","Player","player","Calciatore","calciatore","Name","name"]
    for c in candidates:
        if c in df.columns:
            return c
    # fallback: strip leading numbers from first column if it looks like "105 Carnesecchi"
    first = df.columns[0]
    if df[first].dtype == object:
        tmp = df[first].astype(str).str.replace(r"^\s*\d+\s*", "", regex=True)
        if (tmp.str.len() > 0).mean() > 0.5:
            df["__derived_name__"] = tmp
            return "__derived_name__"
    return None

# ==============================
# Parse all three sheets (skip first 4 rows)
# ==============================
xls = pd.ExcelFile(new_excel)
parsed_sheets = []
for sheet_name in xls.sheet_names:
    raw = pd.read_excel(new_excel, sheet_name=sheet_name, header=None, skiprows=4)
    parsed = parse_sheet(raw)
    if not parsed.empty:
        # suffix non-nome columns with the sheet name to keep them distinct
        rename_map = {c: f"{c}__{sheet_name}" for c in parsed.columns if c != "nome"}
        parsed = parsed.rename(columns=rename_map)
        parsed_sheets.append(parsed)

if not parsed_sheets:
    raise ValueError("No usable rows with a 'nome' column were found after skipping headers and team titles.")

from functools import reduce
votes_by_nome = reduce(lambda L, R: pd.merge(L, R, on="nome", how="outer"), parsed_sheets)

# ==============================
# Load earlier combined & fuzzy match by name
# ==============================
df_old = pd.read_csv(combined_csv)
name_col_old = autodetect_name_col(df_old)
if name_col_old is None:
    raise ValueError("Could not detect a name column in the previous combined CSV. Set COMBINED_NAME_COL.")

df_old["_name_norm_old"] = df_old[name_col_old].map(normalize_name)
votes_by_nome["_name_norm_new"] = votes_by_nome["nome"].map(normalize_name)

choices = df_old["_name_norm_old"].dropna().unique().tolist()
def best_match(n):
    if not n:
        return (None, 0, None)
    return process.extractOne(n, choices, scorer=fuzz.token_sort_ratio)

mm = votes_by_nome["_name_norm_new"].apply(best_match)
votes_by_nome["matched_norm"] = [t[0] if isinstance(t, tuple) else None for t in mm]
votes_by_nome["match_score"]  = [int(t[1]) if isinstance(t, tuple) else 0 for t in mm]

good = votes_by_nome[votes_by_nome["match_score"] >= MATCH_THRESHOLD].copy()
weak = votes_by_nome[votes_by_nome["match_score"] < MATCH_THRESHOLD].copy()

# Merge good matches
merged_good = pd.merge(
    df_old,
    good.drop(columns=["_name_norm_new"]),
    left_on="_name_norm_old", right_on="matched_norm",
    how="left"
)

# Append weak/unmatched new rows (keep their vote columns, old cols remain NaN)
merged_cols = merged_good.columns.tolist()
common_new_cols = [c for c in weak.columns if c in merged_cols]
new_only_rows = []
for _, r in weak.iterrows():
    row = {c: pd.NA for c in merged_cols}
    for c in common_new_cols:
        row[c] = r[c]
    if "_name_norm_old" in row:
        row["_name_norm_old"] = pd.NA
    new_only_rows.append(row)
stub = pd.DataFrame(new_only_rows, columns=merged_cols) if new_only_rows else pd.DataFrame(columns=merged_cols)
merged_final = pd.concat([merged_good, stub], ignore_index=True)

# ==============================
# Save
# ==============================
out_combined = outdir / "combined_with_new_votes.csv"
out_unmatched_new = outdir / "unmatched_from_new_names.csv"
out_unmatched_old = outdir / "old_without_newmatch.csv"

merged_final.to_csv(out_combined, index=False)
weak.to_csv(out_unmatched_new, index=False)
old_without = df_old[~df_old["_name_norm_old"].isin(good["matched_norm"])]
old_without.to_csv(out_unmatched_old, index=False)

print("✓ Done.")
print("Old name column:", name_col_old)
print("New rows:", votes_by_nome.shape[0], "Good matches:", len(good), "Weak/no matches:", len(weak))
print("Merged rows:", merged_final.shape[0])
print("Saved to:", out_combined)

# (Optional) Quick previews in Spyder: examine variables:
# votes_by_nome, merged_final, weak, old_without





