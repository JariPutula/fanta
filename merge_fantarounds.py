#!/usr/bin/env python3
"""
merge_fantacalcio_batch.py

Scans a directory for ALL rounds of vote files whose names end with a 6-digit round code,
e.g. 202501, 202502, …
Matches patterns:
  - VotiExcelUfficiali{RRRRRR}.xlsx                   (main votes per round)
  - Voti_Fantacalcio_Stagione_*_Giornata{RRRRRR}.xlsx (extra votes per round; multi-sheet)

For each found round it:
- merges your roster (“left”) with the round’s main votes (“right”)
- optionally joins the round’s extra workbook (all sheets), suffixing columns per sheet
- uses robust normalization (case/accents/punctuation/initials)
- can auto-merge near-name matches at high confidence

Finally it concatenates all rounds into one long table and writes:
  * merged_all
  * round_stats
  * match_methods_by_round
  * coverage_by_round (if extras present)

Requirements: pip install pandas openpyxl xlsxwriter
"""

from pathlib import Path
from typing import List, Optional, Tuple, Dict
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
    # Your roster / player list
    "left": Path(r"C:\virtual\projects\fanta\data\fanta-26-alku.xlsx"),
    "left_sheet": None,

    # Single-round (legacy) inputs — only used if --votes-dir is NOT provided
    "right": Path(r"C:\virtual\projects\fanta\data\VotiExcelUfficiali202501.xlsx"),
    "right_sheet": None,
    "extra_workbook": Path(r"C:\virtual\projects\fanta\data\Voti_Fantacalcio_Stagione_2025_26_Giornata202501.xlsx"),
    "extra_sheets": None,  # or e.g. ["Voti Redazione","Voti Live","Voti Statistici"]

    # BATCH MODE: folder to scan for all rounds (edit this to your folder)
    "votes_dir": Path(r"C:\virtual\projects\fanta\data"),

    # How to join extra sheets:
    #   "left"  -> enrich only existing players (default; avoids 'extra_only' rows)
    #   "outer" -> include players only in extra sheets
    "extra_join": "left",
    # If 'outer' join is used, you can still drop rows that are extra-only:
    "drop_extra_only": True,

    # Output file
    "output": Path(r"C:\virtual\projects\fanta\data\fantacalcio_merged_all_rounds.xlsx"),

    # Suggestions (display-only)
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

def _clean_to_tokens(s: str) -> List[str]:
    s = _strip_accents(s.lower().strip())
    s = re.sub(r"[^\w\s]", " ", s)           # punctuation -> space
    s = re.sub(r"\s+", " ", s).strip()
    return [t for t in s.split(" ") if t]

def _remove_initials(tokens: List[str]) -> List[str]:
    # drop single-letter tokens (typical initials) and some stop tokens
    return [t for t in tokens if (len(t) > 1 and t not in _STOP_TOKENS)]

def normalize_name(name: object) -> Optional[str]:
    """Normalize names so that 'ANGELINO J.' and 'Angelino' both -> 'angelino'."""
    if pd.isna(name):
        return None
    tokens = _clean_to_tokens(str(name))
    if not tokens:
        return None
    tokens = _remove_initials(tokens)
    if not tokens:
        tokens = _clean_to_tokens(str(name))
    return " ".join(tokens).strip() or None

def detect_header_row(raw_df: pd.DataFrame) -> Optional[int]:
    """Find a header row (Fantacalcio exports often start real headers around row 6)."""
    for idx in range(min(30, len(raw_df))):
        row_vals = [str(v).strip().lower() for v in raw_df.iloc[idx].tolist() if pd.notna(v)]
        if any(h in row_vals for h in POSSIBLE_PLAYER_COLS + ["voto"]):
            return idx
    return None

def load_excel_with_player_column(path: Path, sheet: Optional[str] = None) -> pd.DataFrame:
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
            # Drop stray 'Nome'/'nome'
            if "Nome" in df.columns: df = df.drop(columns=["Nome"])
            if "nome" in df.columns: df = df.drop(columns=["nome"])
            return df
    except Exception:
        pass

    # Fallback with header detection
    raw = pd.read_excel(path, sheet_name=sheet, header=None) if sheet else pd.read_excel(path, header=None)
    hdr = detect_header_row(raw)
    if hdr is None:
        hdr = 0
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
    dropcand = [c for c in df.columns if str(c).strip().lower() == "nome"]
    if dropcand:
        df = df.drop(columns=dropcand)
    return df

# ------------------ Fuzzy helpers ------------------
def _best_match(target: str, candidates: List[str]) -> Tuple[Optional[str], float, float]:
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

def _auto_pairs(left_norms: List[str], right_norms: List[str], cutoff: float = 0.92, margin: float = 0.02) -> List[Tuple[str, str, float]]:
    """High-confidence one-to-one pairs between left and right names."""
    right_available = set(right_norms)
    pairs: List[Tuple[str, str, float]] = []
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

# ------------------ Extra workbook (multiple sheets) ------------------
def _slugify_sheet(name: str) -> str:
    s = re.sub(r"\s+", "_", str(name).strip())
    s = re.sub(r"[^\w]", "_", s)
    return s.strip("_") or "Sheet"

def load_extra_workbook(path: Path, sheets: Optional[List[str]]) -> List[pd.DataFrame]:
    """Load an extra workbook with multiple sheets. Each sheet gets suffixed columns.
    Returns list of per-sheet DataFrames keyed by '__giocatore_norm' plus renamed columns.
    """
    xl = pd.ExcelFile(path)
    chosen = sheets if sheets else xl.sheet_names
    results: List[pd.DataFrame] = []
    for sh in chosen:
        try:
            df = load_excel_with_player_column(path, sheet=sh)
        except Exception:
            continue  # skip non-data sheets
        slug = _slugify_sheet(sh)
        # Rename 'giocatore' -> 'giocatore_<slug>' to preserve raw per-sheet name
        df = df.rename(columns={"giocatore": f"giocatore_{slug}"})
        # Drop noisy internals except key
        drop_cols = [c for c in df.columns if c.startswith("__") and c != "__giocatore_norm"]
        df = df.drop(columns=drop_cols, errors="ignore")
        # Suffix all non-key columns except the per-sheet giocatore
        new_cols = {}
        for c in df.columns:
            if c in ["__giocatore_norm", f"giocatore_{slug}"]:
                continue
            new_cols[c] = f"{c}_{slug}"
        df = df.rename(columns=new_cols)
        results.append(df)
    return results

# ------------------ Round-file discovery ------------------
RE_MAIN  = re.compile(r"^VotiExcelUfficiali(\d{6})\.xlsx$", re.IGNORECASE)
RE_EXTRA = re.compile(r"^Voti_Fantacalcio_Stagione_.*_Giornata(\d{6})\.xlsx$", re.IGNORECASE)

def parse_round_from_code(code: str) -> Tuple[int, int]:
    season = int(code[:4])
    rnd = int(code[4:])
    return season, rnd

def discover_rounds(votes_dir: Path) -> Dict[str, Dict[str, Path]]:
    rounds: Dict[str, Dict[str, Path]] = {}
    for p in votes_dir.glob("*.xlsx"):
        m1 = RE_MAIN.match(p.name)
        m2 = RE_EXTRA.match(p.name)
        if m1:
            code = m1.group(1)
            bucket = rounds.setdefault(code, {})
            bucket['right'] = p
        elif m2:
            code = m2.group(1)
            bucket = rounds.setdefault(code, {})
            bucket['extra'] = p
    return rounds

# ------------------ CLI with defaults ------------------
def parse_args_with_defaults():
    ap = argparse.ArgumentParser(description="Merge Fantacalcio votes across ALL rounds (directory scan) or one round.")
    ap.add_argument("--votes-dir", type=Path, default=None, help="Directory containing per-round vote files (*.xlsx)")
    ap.add_argument("left", nargs="?", default=None, type=Path, help="(Single-round mode) Path to left Excel file")
    ap.add_argument("right", nargs="?", default=None, type=Path, help="(Single-round mode) Path to main votes Excel")
    ap.add_argument("--left-sheet", type=str, default=None, help="Sheet name for the left file (optional)")
    ap.add_argument("--right-sheet", type=str, default=None, help="Sheet name for the right file (optional)")
    ap.add_argument("--extra-workbook", type=Path, default=None, help="(Single-round mode) Extra votes workbook with multiple sheets")
    ap.add_argument("--extra-sheets", type=str, default=None, help="Comma-separated sheet names (default: autodetect all)")
    ap.add_argument("--extra-join", type=str, choices=["left","outer"], default=None, help="How to join extra sheets (default from DEFAULTS)")
    ap.add_argument("--drop-extra-only", action="store_true", help="Drop rows that have data only from extra sheets")
    ap.add_argument("--keep-extra-only", action="store_true", help="Keep rows that exist only in extra sheets")
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
    extra_workbook = args.extra_workbook or DEFAULTS["extra_workbook"]
    extra_sheets = None
    if args.extra_sheets:
        extra_sheets = [s.strip() for s in args.extra_sheets.split(",") if s.strip()]
    elif DEFAULTS["extra_sheets"]:
        extra_sheets = list(DEFAULTS["extra_sheets"])
    extra_join = args.extra_join or DEFAULTS["extra_join"]
    drop_extra_only = DEFAULTS["drop_extra_only"]
    if args.drop_extra_only:
        drop_extra_only = True
    if args.keep_extra_only:
        drop_extra_only = False
    votes_dir = args.votes_dir or DEFAULTS["votes_dir"]

    print("=== merge_fantacalcio_batch.py ===")
    print(f"Votes dir:        {votes_dir if votes_dir else '(single-round mode)'}")
    print(f"Left file:        {left}")
    if votes_dir is None:
        print(f"Right file:       {right}")
        print(f"Extra workbook:   {extra_workbook}")
    print(f"Extra sheets:     {extra_sheets if extra_sheets else '(auto)'}")
    print(f"Extra join:       {extra_join} (drop_extra_only={drop_extra_only})")
    print(f"Output:           {output}")
    print(f"Left sheet:       {left_sheet}")
    if votes_dir is None:
        print(f"Right sheet:      {right_sheet}")
    print(f"Suggest cutoff={cutoff}, max_suggestions={max_suggestions}")
    print(f"Auto fuzzy merge: {auto_fuzzy_merge} (cutoff={auto_cutoff}, margin={auto_margin})")
    print("============================")

    if not Path(left).exists():
        print(f"ERROR: Left file does not exist: {left}", file=sys.stderr); sys.exit(1)

    if votes_dir is None:
        if not Path(right).exists():
            print(f"ERROR: Right file does not exist: {right}", file=sys.stderr); sys.exit(1)
        if extra_workbook and not Path(extra_workbook).exists():
            print(f"WARNING: Extra workbook not found: {extra_workbook} (continuing without it)", file=sys.stderr)
            extra_workbook = None
    else:
        if not Path(votes_dir).exists():
            print(f"ERROR: votes-dir does not exist: {votes_dir}", file=sys.stderr); sys.exit(1)

    return (left, left_sheet, right, right_sheet, output, cutoff, max_suggestions,
            auto_fuzzy_merge, auto_cutoff, auto_margin, extra_workbook, extra_sheets,
            extra_join, drop_extra_only, votes_dir)

# ------------------ Per-round merge core ------------------
def merge_one_round(left_df: pd.DataFrame,
                    right_path: Optional[Path],
                    right_sheet: Optional[str],
                    extra_workbook: Optional[Path],
                    extra_sheets: Optional[List[str]],
                    extra_join: str,
                    drop_extra_only: bool,
                    cutoff: float,
                    max_suggestions: int,
                    auto_merge: bool,
                    auto_cutoff: float,
                    auto_margin: float) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Return (combined, stats_counts, method_counts, coverage) for one round."""
    # Load main votes if present
    if right_path is None:
        right_df = pd.DataFrame(columns=["__giocatore_norm"])
    else:
        right_df = load_excel_with_player_column(right_path, sheet=right_sheet)

    # First pass: exact merge
    merged_exact = pd.merge(
        left_df, right_df, how="outer", on="__giocatore_norm",
        suffixes=("_left", "_right"), indicator=True
    )
    merged_exact["match_flag"] = merged_exact["_merge"].map({"both": "matched", "left_only": "left_only", "right_only": "right_only"})
    merged_exact["match_method"] = merged_exact["_merge"].map({"both": "exact", "left_only": None, "right_only": None})

    # Second pass: auto-fuzzy
    fuzzy_merged = pd.DataFrame()
    if auto_merge and not right_df.empty:
        left_un = sorted(set(merged_exact.loc[merged_exact["_merge"] == "left_only", "__giocatore_norm"].dropna()))
        right_un = sorted(set(merged_exact.loc[merged_exact["_merge"] == "right_only", "__giocatore_norm"].dropna()))
        pairs = _auto_pairs(left_un, right_un, cutoff=auto_cutoff, margin=auto_margin)

        if pairs:
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
                if "__giocatore_norm_left" in fuzzy_merged.columns and "__giocatore_norm_right" in fuzzy_merged.columns:
                    fuzzy_merged["__giocatore_norm"] = fuzzy_merged["__giocatore_norm_left"]
                    fuzzy_merged = fuzzy_merged.drop(columns=["__giocatore_norm_left", "__giocatore_norm_right"])
                fuzzy_merged["_merge"] = "both"
                fuzzy_merged["match_flag"] = "matched"
                fuzzy_merged["match_method"] = "fuzzy"
                fuzzy_merged["similarity"] = sims

            left_matched = {ln for ln, _, _ in pairs}
            right_matched = {rn for _, rn, _ in pairs}
            keep_left = ~((merged_exact["_merge"] == "left_only") & (merged_exact["__giocatore_norm"].isin(left_matched)))
            keep_right = ~((merged_exact["_merge"] == "right_only") & (merged_exact["__giocatore_norm"].isin(right_matched)))
            merged_exact = merged_exact[keep_left & keep_right].copy()

    # Combine
    combined = pd.concat([
        merged_exact[merged_exact["_merge"] == "both"],
        fuzzy_merged,
        merged_exact[merged_exact["_merge"] == "left_only"],
        merged_exact[merged_exact["_merge"] == "right_only"],
    ], ignore_index=True)

    # Unified name
    def choose_name(row):
        gl = row.get("giocatore_left")
        return gl if pd.notna(gl) and str(gl).strip() else row.get("giocatore_right")
    combined["giocatore"] = combined.apply(choose_name, axis=1)

    # Join extra workbook sheets for the round
    coverage_rows = []
    if extra_workbook is not None and Path(extra_workbook).exists():
        extra_dfs = load_extra_workbook(extra_workbook, sheets=extra_sheets)
        for edf in extra_dfs:
            sheet_slug = [c.split("_", 1)[1] for c in edf.columns if c.startswith("giocatore_")]
            sheet_slug = sheet_slug[0] if sheet_slug else "Sheet"
            # Choose join type for extra sheets
            how_join = "left" if extra_join == "left" else "outer"
            combined = pd.merge(combined, edf, how=how_join, on="__giocatore_norm")
            if how_join == "outer":
                # Mark pure-extra rows
                newly_extra = combined["match_flag"].isna()
                if newly_extra.any():
                    combined.loc[newly_extra, "match_flag"] = "extra_only"
                    combined.loc[newly_extra, "match_method"] = None
            # Coverage: count non-null in a representative column
            rep_cols = [c for c in edf.columns if c.endswith(f"_{sheet_slug}") and not c.startswith("giocatore_")]
            rep_col = rep_cols[0] if rep_cols else None
            if rep_col:
                n_has = combined[rep_col].notna().sum()
                coverage_rows.append({"sheet": sheet_slug, "column": rep_col, "non_null_rows": int(n_has)})

        # Optionally drop rows that are only extra (if created)
        if drop_extra_only and "match_flag" in combined.columns:
            mask_extra_only = combined["match_flag"].eq("extra_only")
            if mask_extra_only.any():
                combined = combined[~mask_extra_only].copy()

    # Final tidy: ensure no stray 'Nome'/'nome' columns remain
    stray = [c for c in combined.columns if str(c).strip().lower() == "nome"]
    if stray:
        combined = combined.drop(columns=stray)

    stats_counts = combined["match_flag"].value_counts(dropna=False).rename_axis("status").reset_index(name="count")
    method_counts = (combined.loc[combined["match_flag"] == "matched", "match_method"]
                     .value_counts(dropna=False).rename_axis("match_method").reset_index(name="count"))
    coverage = pd.DataFrame(coverage_rows)
    return combined, stats_counts, method_counts, coverage

# ------------------ Main ------------------
def main():
    (left_path, left_sheet, right_path, right_sheet, output_path,
     cutoff, max_suggestions, auto_merge, auto_cutoff, auto_margin,
     extra_workbook, extra_sheets, extra_join, drop_extra_only, votes_dir) = parse_args_with_defaults()

    # Load roster once
    left_df = load_excel_with_player_column(left_path, sheet=left_sheet)

    # Batch mode?
    if votes_dir:
        rounds = discover_rounds(Path(votes_dir))
        if not rounds:
            print(f"ERROR: No round files found in {votes_dir}. Expected names like VotiExcelUfficiali202501.xlsx", file=sys.stderr)
            sys.exit(1)

        all_rows = []
        all_stats = []
        all_methods = []
        all_coverage = []

        for code in sorted(rounds.keys()):
            season, rnd = parse_round_from_code(code)
            right = rounds[code].get("right")
            extra = rounds[code].get("extra")

            print(f"--- Processing round {code} (season={season}, round={rnd}) ---")
            combined, stats_counts, method_counts, coverage = merge_one_round(
                left_df=left_df,
                right_path=right,
                right_sheet=right_sheet,
                extra_workbook=extra,
                extra_sheets=extra_sheets,
                extra_join=extra_join,
                drop_extra_only=drop_extra_only,
                cutoff=cutoff,
                max_suggestions=max_suggestions,
                auto_merge=auto_merge,
                auto_cutoff=auto_cutoff,
                auto_margin=auto_margin,
            )
            combined.insert(0, "season", season)
            combined.insert(1, "round", rnd)
            combined.insert(2, "round_code", code)
            all_rows.append(combined)

            stats_counts = stats_counts.assign(season=season, round=rnd, round_code=code)
            method_counts = method_counts.assign(season=season, round=rnd, round_code=code)
            coverage = coverage.assign(season=season, round=rnd, round_code=code)
            all_stats.append(stats_counts)
            all_methods.append(method_counts)
            all_coverage.append(coverage)

        merged_all = pd.concat(all_rows, ignore_index=True)
        round_stats = pd.concat(all_stats, ignore_index=True) if all_stats else pd.DataFrame()
        match_methods_by_round = pd.concat(all_methods, ignore_index=True) if all_methods else pd.DataFrame()
        coverage_by_round = pd.concat(all_coverage, ignore_index=True) if all_coverage else pd.DataFrame()

        with pd.ExcelWriter(output_path, engine="xlsxwriter") as writer:
            merged_all.to_excel(writer, index=False, sheet_name="merged_all")
            if not round_stats.empty:
                round_stats.to_excel(writer, index=False, sheet_name="round_stats")
            if not match_methods_by_round.empty:
                match_methods_by_round.to_excel(writer, index=False, sheet_name="match_methods_by_round")
            if not coverage_by_round.empty:
                coverage_by_round.to_excel(writer, index=False, sheet_name="coverage_by_round")
        print(f"Done. Wrote: {output_path}")
        return

    # Single-round fallback (legacy)
    if right_path is not None and not Path(right_path).exists():
        print(f"ERROR: Right file does not exist: {right_path}", file=sys.stderr); sys.exit(1)
    if extra_workbook and not Path(extra_workbook).exists():
        print(f"WARNING: Extra workbook not found: {extra_workbook} (continuing without it)", file=sys.stderr)
        extra_workbook = None

    combined, stats_counts, method_counts, coverage = merge_one_round(
        left_df=load_excel_with_player_column(left_path, sheet=left_sheet),
        right_path=right_path,
        right_sheet=right_sheet,
        extra_workbook=extra_workbook,
        extra_sheets=extra_sheets,
        extra_join=extra_join,
        drop_extra_only=drop_extra_only,
        cutoff=cutoff,
        max_suggestions=max_suggestions,
        auto_merge=auto_merge,
        auto_cutoff=auto_cutoff,
        auto_margin=auto_margin,
    )
    with pd.ExcelWriter(output_path, engine="xlsxwriter") as writer:
        combined.to_excel(writer, index=False, sheet_name="merged")
        stats_counts.to_excel(writer, index=False, sheet_name="stats")
        method_counts.to_excel(writer, index=False, sheet_name="match_methods")
        if not coverage.empty:
            coverage.to_excel(writer, index=False, sheet_name="coverage")
    print(f"Done. Wrote: {output_path}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)

