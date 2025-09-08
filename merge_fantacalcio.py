#!/usr/bin/env python3
"""
merge_fantacalcio.py

Merge two Excel files using the 'giocatore' (player name) key with robust normalization,
perform a full outer join (keep all rows), and produce helpful stats and fuzzy
match suggestions for near-miss names.

New in this version:
- Smarter normalization: removes leading/trailing initials like "ANGELINO J." -> "angelino"
- Optional auto-fuzzy merge to join near-identical names (configurable cutoff/margin)

OUTPUT sheets:
- merged
- stats
- match_methods (exact vs fuzzy counts)
- unmatched_from_left
- unmatched_from_right
- fuzzy_suggestions (for any still-unmatched left names)

Run from IDE (uses defaults), or CLI (overrides defaults).
Requirements:  pip install pandas openpyxl xlsxwriter
"""

from __future__ import annotations
from pathlib import Path
import pandas as pd
import unicodedata
import re
from difflib import SequenceMatcher, get_close_matches
import argparse
import sys

# ===============================
# HARD-CODED DEFAULTS (edit here)
# ===============================
DEFAULTS = {
    "left": Path(r"C:\virtual\projects\fanta\data\fanta-26-alku.xlsx"),
    "right": Path(r"C:\virtual\projects\fanta\data\VotiExcelUfficiali202501.xlsx"),
    "left_sheet": None,
    "right_sheet": None,
    "output": Path(r"C:\virtual\projects\fanta\data\fantacalcio_merged_report.xlsx"),

    # Suggestions (shown on a separate sheet)
    "cutoff": 0.8,
    "max_suggestions": 2,

    # Auto merge remaining near-misses (set False to disable)
    "auto_fuzzy_merge": True,
    "auto_cutoff": 0.92,   # require ≥ this similarity to auto-merge
    "auto_margin": 0.02,   # best must beat 2nd-best by this margin
}

POSSIBLE_PLAYER_COLS = ["giocatore", "nome", "player", "calciatore", "giocatori"]

# ------------------ Normalization ------------------
_STOP_TOKENS = {"jr", "sr", "ii", "iii", "iv", "v"}

def _strip_accents(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))

def _clean_to_tokens(s: str) -> list[str]:
    # lower, remove accents, convert punctuation to space, keep alnum words
    s = _strip_accents(s.lower().strip())
    s = re.sub(r"[^\w\s]", " ", s)           # punctuation -> space
    s = re.sub(r"\s+", " ", s).strip()
    return [t for t in s.split(" ") if t]

def _remove_initials(tokens: list[str]) -> list[str]:
    # drop single-letter tokens anywhere (typical initials) and some stop tokens
    return [t for t in tokens if (len(t) > 1 and t not in _STOP_TOKENS)]

def normalize_name(name):
    """Normalize player names so that 'ANGELINO J.' and 'Angelino' both -> 'angelino'."""
    if pd.isna(name):
        return None
    tokens = _clean_to_tokens(str(name))
    if not tokens:
        return None
    tokens = _remove_initials(tokens)
    if not tokens:
        # if all were initials, fall back to original tokens (edge case)
        tokens = _clean_to_tokens(str(name))
    return " ".join(tokens).strip() or None

def detect_header_row(raw_df: pd.DataFrame) -> int | None:
    """Find a header row (Fantacalcio exports often start real headers around row 6)."""
    for idx in range(min(30, len(raw_df))):
        row_vals = [str(v).strip().lower() for v in raw_df.iloc[idx].tolist() if pd.notna(v)]
        if any(h in row_vals for h in POSSIBLE_PLAYER_COLS + ["voto"]):
            return idx
    return None

def load_excel_with_player_column(path: Path, sheet: str | None = None) -> pd.DataFrame:
    """Load Excel ensuring there's a 'giocatore' column; rename 'Nome' etc. to 'giocatore'."""
    # Try simple read
    try:
        df = pd.read_excel(path, sheet_name=sheet) if sheet else pd.read_excel(path)
        lowered = {str(c).strip().lower(): c for c in df.columns}
        player_col = next((lowered[k] for k in POSSIBLE_PLAYER_COLS if k in lowered), None)
        if player_col is not None:
            if player_col != "giocatore":
                df = df.rename(columns={player_col: "giocatore"})
            df["__source_file"] = path.name
            df["__row_id"] = df.index
            df["__giocatore_raw"] = df["giocatore"].astype(str)
            df["__giocatore_norm"] = df["__giocatore_raw"].map(normalize_name)
            return df
    except Exception:
        pass

    # Fallback with header detection
    raw = pd.read_excel(path, sheet_name=sheet, header=None) if sheet else pd.read_excel(path, header=None)
    hdr = detect_header_row(raw)
    if hdr is None:
        raise KeyError(f"Could not detect a header row with a giocatore/Name column in {path.name}")
    df = pd.read_excel(path, sheet_name=sheet, header=hdr) if sheet else pd.read_excel(path, header=hdr)

    # Rename equivalent header to 'giocatore'
    ren = {}
    for c in df.columns:
        if str(c).strip().lower() in POSSIBLE_PLAYER_COLS and str(c).strip().lower() != "giocatore":
            ren[c] = "giocatore"
    if ren:
        df = df.rename(columns=ren)

    if "giocatore" not in [str(c).strip().lower() for c in df.columns]:
        raise KeyError(f"'giocatore' column not found in {path.name} after header detection. Columns: {list(df.columns)}")

    gioc_col = [c for c in df.columns if str(c).strip().lower() == "giocatore"][0]
    if gioc_col != "giocatore":
        df = df.rename(columns={gioc_col: "giocatore"})

    df["__source_file"] = path.name
    df["__row_id"] = df.index
    df["__giocatore_raw"] = df["giocatore"].astype(str)
    df["__giocatore_norm"] = df["__giocatore_raw"].map(normalize_name)
    return df

# ------------------ Fuzzy helpers ------------------
def _best_match(target: str, candidates: list[str]) -> tuple[str | None, float, float]:
    """Return (best_candidate, best_ratio, second_best_ratio)."""
    if not candidates:
        return None, 0.0, 0.0
    best = None; best_r = 0.0; second = 0.0
    for c in candidates:
        r = SequenceMatcher(None, target, c).ratio()
        if r > best_r:
            second = best_r
            best_r = r
            best = c
        elif r > second:
            second = r
    return best, best_r, second

def _auto_pairs(left_norms: list[str], right_norms: list[str], cutoff=0.92, margin=0.02):
    """High-confidence one-to-one pairs between left and right names."""
    right_available = set(right_norms)
    pairs = []
    for ln in left_norms:
        cand = [r for r in right_available]
        best, br, second = _best_match(ln, cand)
        if best is not None and br >= cutoff and (br - second) >= margin:
            pairs.append((ln, best, round(br, 3)))
            right_available.remove(best)
    return pairs

# ------------------ Suggestions for remaining unmatched ------------------
def make_fuzzy_suggestions(unmatched_left: pd.DataFrame, right_df: pd.DataFrame, cutoff: float = 0.8, n: int = 2) -> pd.DataFrame:
    """Suggest likely matches from right_df for left-only unmatched names using difflib."""
    def build_norm_map(series: pd.Series):
        norm_to_example = {}
        for x in series.dropna().astype(str):
            nrm = normalize_name(x)
            if nrm:
                norm_to_example.setdefault(nrm, x)
        return norm_to_example

    left_norms = sorted(set(normalize_name(x) for x in unmatched_left["giocatore"].dropna().astype(str)))
    right_norm_map = build_norm_map(right_df["giocatore"])
    right_norms_list = list(right_norm_map.keys())

    rows = []
    for ln in left_norms:
        if not ln:
            continue
        matches = get_close_matches(ln, right_norms_list, n=n, cutoff=cutoff)
        for m in matches:
            ratio = SequenceMatcher(None, ln, m).ratio()
            left_example = next((orig for orig in unmatched_left["giocatore"].astype(str) if normalize_name(orig) == ln), None)
            rows.append({"left_name": left_example, "suggested_right_name": right_norm_map[m], "similarity": round(ratio, 3)})
    return pd.DataFrame(rows)

# ------------------ CLI with defaults ------------------
def parse_args_with_defaults():
    ap = argparse.ArgumentParser(description="Merge two Excel files by 'giocatore' with full outer join and stats.")
    ap.add_argument("left", nargs="?", default=None, type=Path, help="Path to left Excel file (optional if using defaults)")
    ap.add_argument("right", nargs="?", default=None, type=Path, help="Path to right Excel file (optional if using defaults)")
    ap.add_argument("--left-sheet", type=str, default=None, help="Sheet name for the left file (optional)")
    ap.add_argument("--right-sheet", type=str, default=None, help="Sheet name for the right file (optional)")
    ap.add_argument("--output", type=Path, default=None, help="Output Excel path (optional)")
    ap.add_argument("--cutoff", type=float, default=None, help="Similarity cutoff for fuzzy suggestions (0..1)")
    ap.add_argument("--max-suggestions", type=int, default=None, help="Max suggestions per unmatched name")
    ap.add_argument("--auto-fuzzy-merge", dest="auto_fuzzy_merge", action="store_true", help="Enable auto fuzzy merge of near-miss names")
    ap.add_argument("--no-auto-fuzzy-merge", dest="auto_fuzzy_merge", action="store_false", help="Disable auto fuzzy merge")
    ap.add_argument("--auto-cutoff", type=float, default=None, help="Similarity cutoff for auto fuzzy merge")
    ap.add_argument("--auto-margin", type=float, default=None, help="Min margin over 2nd-best match")
    ap.set_defaults(auto_fuzzy_merge=None)
    args = ap.parse_args()

    left = args.left or DEFAULTS["left"]
    right = args.right or DEFAULTS["right"]
    left_sheet = args.left_sheet if args.left_sheet is not None else DEFAULTS["left_sheet"]
    right_sheet = args.right_sheet if args.right_sheet is not None else DEFAULTS["right_sheet"]
    output = args.output or DEFAULTS["output"]
    cutoff = args.cutoff if args.cutoff is not None else DEFAULTS["cutoff"]
    max_suggestions = args.max_suggestions if args.max_suggestions is not None else DEFAULTS["max_suggestions"]
    auto_fuzzy_merge = DEFAULTS["auto_fuzzy_merge"] if args.auto_fuzzy_merge is None else args.auto_fuzzy_merge
    auto_cutoff = args.auto_cutoff if args.auto_cutoff is not None else DEFAULTS["auto_cutoff"]
    auto_margin = args.auto_margin if args.auto_margin is not None else DEFAULTS["auto_margin"]

    print("=== merge_fantacalcio.py ===")
    print(f"Left file:   {left}")
    print(f"Right file:  {right}")
    print(f"Left sheet:  {left_sheet}")
    print(f"Right sheet: {right_sheet}")
    print(f"Output:      {output}")
    print(f"Suggest cutoff={cutoff}, max_suggestions={max_suggestions}")
    print(f"Auto fuzzy merge: {auto_fuzzy_merge} (cutoff={auto_cutoff}, margin={auto_margin})")
    print("============================")

    if not Path(left).exists():
        print(f"ERROR: Left file does not exist: {left}", file=sys.stderr); sys.exit(1)
    if not Path(right).exists():
        print(f"ERROR: Right file does not exist: {right}", file=sys.stderr); sys.exit(1)

    return left, right, left_sheet, right_sheet, output, cutoff, max_suggestions, auto_fuzzy_merge, auto_cutoff, auto_margin

# ------------------ Main ------------------
def main():
    (left_path, right_path, left_sheet, right_sheet, output_path,
     cutoff, max_suggestions, auto_merge, auto_cutoff, auto_margin) = parse_args_with_defaults()

    # Load inputs
    left_df = load_excel_with_player_column(left_path, sheet=left_sheet)
    right_df = load_excel_with_player_column(right_path, sheet=right_sheet)

    # First pass: exact match on normalized key
    merged_exact = pd.merge(
        left_df, right_df, how="outer", on="__giocatore_norm",
        suffixes=("_left", "_right"), indicator=True
    )
    merged_exact["match_flag"] = merged_exact["_merge"].map({"both": "matched", "left_only": "left_only", "right_only": "right_only"})
    merged_exact["match_method"] = merged_exact["_merge"].map({"both": "exact", "left_only": None, "right_only": None})

    # Optional second pass: high-confidence fuzzy merge of remaining unmatched
    fuzzy_merged = pd.DataFrame()
    if auto_merge:
        left_un = sorted(set(merged_exact.loc[merged_exact["_merge"] == "left_only", "__giocatore_norm"].dropna()))
        right_un = sorted(set(merged_exact.loc[merged_exact["_merge"] == "right_only", "__giocatore_norm"].dropna()))
        pairs = _auto_pairs(left_un, right_un, cutoff=auto_cutoff, margin=auto_margin)

        if pairs:
            # Build one row per pair (take first occurrence per normalized name)
            left_rows, right_rows, sims = [], [], []
            for ln, rn, sim in pairs:
                lpart = left_df[left_df["__giocatore_norm"] == ln].head(1).copy()
                rpart = right_df[right_df["__giocatore_norm"] == rn].head(1).copy()
                lpart["__pair_id"] = len(left_rows); rpart["__pair_id"] = len(right_rows)
                left_rows.append(lpart); right_rows.append(rpart); sims.append(sim)

            if left_rows and right_rows:
                left_part = pd.concat(left_rows, ignore_index=True)
                right_part = pd.concat(right_rows, ignore_index=True)
                fuzzy_merged = pd.merge(left_part, right_part, on="__pair_id", suffixes=("_left", "_right")).drop(columns=["__pair_id"])
                # Choose a single normalized key (prefer left)
                if "__giocatore_norm_left" in fuzzy_merged.columns and "__giocatore_norm_right" in fuzzy_merged.columns:
                    fuzzy_merged["__giocatore_norm"] = fuzzy_merged["__giocatore_norm_left"]
                    fuzzy_merged = fuzzy_merged.drop(columns=["__giocatore_norm_left", "__giocatore_norm_right"])
                fuzzy_merged["_merge"] = "both"
                fuzzy_merged["match_flag"] = "matched"
                fuzzy_merged["match_method"] = "fuzzy"
                fuzzy_merged["similarity"] = sims

            # Drop the paired left/right-only rows from merged_exact
            left_matched = {ln for ln, _, _ in pairs}
            right_matched = {rn for _, rn, _ in pairs}
            keep_left = ~((merged_exact["_merge"] == "left_only") & (merged_exact["__giocatore_norm"].isin(left_matched)))
            keep_right = ~((merged_exact["_merge"] == "right_only") & (merged_exact["__giocatore_norm"].isin(right_matched)))
            merged_exact = merged_exact[keep_left & keep_right].copy()

    # Combine exact matches + fuzzy matches + remaining unmatched
    combined = pd.concat([
        merged_exact[merged_exact["_merge"] == "both"],
        fuzzy_merged,
        merged_exact[merged_exact["_merge"] == "left_only"],
        merged_exact[merged_exact["_merge"] == "right_only"],
    ], ignore_index=True)

    # Unified display name, prefer left then right
    def choose_name(row):
        gl = row.get("giocatore_left")
        return gl if pd.notna(gl) and str(gl).strip() else row.get("giocatore_right")

    combined["giocatore"] = combined.apply(choose_name, axis=1)

    # Stats
    stats_counts = combined["match_flag"].value_counts(dropna=False).rename_axis("status").reset_index(name="count")
    method_counts = (combined.loc[combined["match_flag"] == "matched", "match_method"]
                     .value_counts(dropna=False).rename_axis("match_method").reset_index(name="count"))

    # Unmatched
    unmatched_left = combined[combined["match_flag"] == "left_only"][["giocatore_left"]].rename(columns={"giocatore_left": "giocatore"})
    unmatched_right = combined[combined["match_flag"] == "right_only"][["giocatore_right"]].rename(columns={"giocatore_right": "giocatore"})

    # Suggestions for any remaining left-only
    suggestions = pd.DataFrame()
    if not unmatched_left.empty:
        suggestions = make_fuzzy_suggestions(unmatched_left, right_df, cutoff=cutoff, n=max_suggestions)

    # Column order
    first_cols = [
        "match_flag", "match_method", "similarity",
        "giocatore", "giocatore_left", "giocatore_right",
        "__source_file_left", "__source_file_right",
    ]
    rest = [c for c in combined.columns if c not in first_cols]
    combined = combined[first_cols + rest]

    # Save
    with pd.ExcelWriter(output_path, engine="xlsxwriter") as writer:
        combined.to_excel(writer, index=False, sheet_name="merged")
        stats_counts.to_excel(writer, index=False, sheet_name="stats")
        method_counts.to_excel(writer, index=False, sheet_name="match_methods")
        unmatched_left.to_excel(writer, index=False, sheet_name="unmatched_from_left")
        unmatched_right.to_excel(writer, index=False, sheet_name="unmatched_from_right")
        if not suggestions.empty:
            suggestions.to_excel(writer, index=False, sheet_name="fuzzy_suggestions")

    print(f"Done. Wrote: {output_path}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)
