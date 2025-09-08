import tkinter as tk
from tkinter import filedialog, messagebox, ttk
import pandas as pd
import re
from pathlib import Path
import chardet


def detect_and_read(path: Path) -> str:
    raw = path.read_bytes()
    enc = chardet.detect(raw).get("encoding") or "utf-8"
    try:
        return raw.decode(enc)
    except Exception:
        return raw.decode(enc, errors="replace")


def to_float(num_str):
    if num_str is None:
        return None
    s = num_str.strip().replace("*", "").replace(",", ".")
    try:
        return float(s)
    except:
        return None


def parse_vote_and_indicators(vote_token, trailing):
    vote_token = vote_token.strip()
    sub_used = False
    if vote_token.startswith(">"):
        sub_used = True
        vote_token = vote_token[1:].strip()

    vote = None
    if vote_token not in ("-", ""):
        try:
            vote = float(vote_token.split()[0].replace(",", "."))
        except:
            pass

    adjustments = None
    indicators = None
    trailing = trailing.strip()
    if trailing:
        m = re.search(r"\(([^)]+)\)", trailing)
        if m:
            adjustments = m.group(1).strip()
            trailing = (trailing[:m.start()] + trailing[m.end():]).strip()
        if trailing:
            indicators = trailing

    return vote, indicators, adjustments, sub_used


def parse_file(lines):
    giornata_re = re.compile(r"^Giornata\s+(\d+)")
    match_re = re.compile(r"^([A-Za-zÄäÖöÅå]+)-([A-Za-zÄäÖöÅå]+)\s+([\d,]+)\s*-\s*([\d,]+)")
    manager_header_re = re.compile(r"^([A-ZÄÖÅA-Za-zÄäÖöÅå]+)\s+([\d,]+(?:\*)?)\s*$")
    dash_line_re = re.compile(r"^-{3,}$")
    player_line_re = re.compile(r"^\s*(\d+)\.?\s+([PDCA])\s+(.+?)\s+([> -]?\s*[-]|\>?\s*\d+(?:,\d+)?)\s*(.*)$")

    players_rows, matches_rows = [], []
    current_giornata = None
    in_manager_block = False
    current_manager = None
    current_team_total = None

    for line in lines:
        line = line.rstrip()
        mg = giornata_re.search(line)
        if mg:
            current_giornata = int(mg.group(1))
            in_manager_block = False
            current_manager = None
            continue

        mm = match_re.match(line)
        if mm and current_giornata is not None:
            home, away, s1, s2 = mm.groups()
            matches_rows.append({
                "giornata": current_giornata,
                "home": home,
                "away": away,
                "home_score": to_float(s1),
                "away_score": to_float(s2),
            })
            continue

        mh = manager_header_re.match(line)
        if mh and current_giornata is not None:
            current_manager = mh.group(1).capitalize()
            current_team_total = to_float(mh.group(2))
            in_manager_block = True
            continue

        if dash_line_re.match(line):
            in_manager_block = False
            current_manager = None
            current_team_total = None
            continue

        if in_manager_block and current_manager and line.strip():
            pm = player_line_re.match(line)
            if pm:
                num_str, role, name, vote_token, trailing = pm.groups()
                clean_name = name.strip().replace("_", " ")
                vote, indicators, adj, sub_used = parse_vote_and_indicators(vote_token, trailing)
                players_rows.append({
                    "giornata": current_giornata,
                    "manager": current_manager,
                    "team_total": current_team_total,
                    "player_num": int(num_str),
                    "role": role,
                    "name": clean_name,
                    "vote": vote,
                    "sub_used": sub_used,
                    "indicators": indicators,
                    "adjustments": adj,
                })

    players_df = pd.DataFrame(players_rows)
    matches_df = pd.DataFrame(matches_rows)

    def result_points(row):
        if pd.isna(row["home_score"]) or pd.isna(row["away_score"]):
            return 0, 0
        if row["home_score"] > row["away_score"]:
            return 2, 0
        if row["home_score"] < row["away_score"]:
            return 0, 2
        return 1, 1

    stand_rows = []
    for _, r in matches_df.iterrows():
        hp, ap = result_points(r)
        stand_rows.append({"giornata": r["giornata"], "manager": r["home"], "points": hp, "score": r["home_score"]})
        stand_rows.append({"giornata": r["giornata"], "manager": r["away"], "points": ap, "score": r["away_score"]})

    stand_df = pd.DataFrame(stand_rows)
    standings = stand_df.groupby("manager").agg(
        games=("giornata", "count"),
        pts=("points", "sum"),
        total_score=("score", "sum"),
    ).reset_index().sort_values(["pts", "total_score"], ascending=[False, False])

    return players_df, matches_df, standings


# --- GUI part ---
def run_gui():
    root = tk.Tk()
    root.title("Fantacalcio Parser")

    def choose_file():
        file_path = filedialog.askopenfilename(filetypes=[("Text Files", "*.txt")])
        entry_file.delete(0, tk.END)
        entry_file.insert(0, file_path)

    def choose_dir():
        dir_path = filedialog.askdirectory()
        entry_dir.delete(0, tk.END)
        entry_dir.insert(0, dir_path)

    def parse_and_save():
        global players_df, matches_df, standings_df
        try:
            infile = Path(entry_file.get())
            outdir = Path(entry_dir.get())
            text = detect_and_read(infile)
            players_df, matches_df, standings_df = parse_file(text.splitlines())
            outdir.mkdir(parents=True, exist_ok=True)
            players_df.to_csv(outdir / "players_parsed.csv", index=False)
            matches_df.to_csv(outdir / "matches_parsed.csv", index=False)
            standings_df.to_csv(outdir / "standings_parsed.csv", index=False)
            messagebox.showinfo("Success", f"Parsed and saved CSVs to {outdir}")
            # Preview standings in Treeview
            for row in tree.get_children():
                tree.delete(row)
            for _, row in standings_df.iterrows():
                tree.insert("", tk.END, values=(row["manager"], row["games"], row["pts"], row["total_score"]))
        except Exception as e:
            messagebox.showerror("Error", str(e))

    # Layout
    frm = tk.Frame(root, padx=10, pady=10)
    frm.pack(fill=tk.BOTH, expand=True)

    tk.Label(frm, text="Input text file:").grid(row=0, column=0, sticky="w")
    entry_file = tk.Entry(frm, width=40)
    entry_file.grid(row=0, column=1, padx=5)
    tk.Button(frm, text="Browse", command=choose_file).grid(row=0, column=2)

    tk.Label(frm, text="Output directory:").grid(row=1, column=0, sticky="w")
    entry_dir = tk.Entry(frm, width=40)
    entry_dir.grid(row=1, column=1, padx=5)
    tk.Button(frm, text="Browse", command=choose_dir).grid(row=1, column=2)

    tk.Button(frm, text="Parse", command=parse_and_save, bg="lightblue").grid(row=2, column=0, columnspan=3, pady=10)

    tk.Label(frm, text="Standings Preview:").grid(row=3, column=0, columnspan=3)

    cols = ("Manager", "Games", "Points", "Total Score")
    tree = ttk.Treeview(frm, columns=cols, show="headings", height=10)
    for c in cols:
        tree.heading(c, text=c)
        tree.column(c, width=100)
    tree.grid(row=4, column=0, columnspan=3, pady=5)

    root.mainloop()


if __name__ == "__main__":
    run_gui()
