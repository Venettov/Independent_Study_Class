#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Convert Census Excel -> clean CSV/JSON + compact PR series (2010–2024).

This version reads two Excel files from the same directory and merges the data.
"""

from __future__ import annotations
import json
import re
from pathlib import Path
from typing import Any, List, Dict, Optional
import pandas as pd
from openpyxl import load_workbook

# ---------- Paths ----------
REPO = Path(__file__).resolve().parents[1]
# Point to the data directory where both files are located
DATA_DIR = REPO / "data" / "population"
XLSX_2024 = DATA_DIR / "prm-est2024-chg.xlsx"
XLSX_2020 = DATA_DIR / "prm-est2020int-pop-72.xlsx"
OUT_CSV = DATA_DIR / "prm-est2010-2024-chg.csv"
OUT_JSON = DATA_DIR / "prm-est2010-2024-chg.json"
OUT_PR = DATA_DIR / "pr_pr_2010_2024.json"

# --- Helper functions (same as original script) ---
def txt(v: Any) -> str:
    return "" if v is None else str(v).strip()

def norm(s: str) -> str:
    s = s.replace("\u2013", "-")
    s = s.replace("\xa0", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s

def uniquify(cols: List[str]) -> List[str]:
    seen: Dict[str, int] = {}
    out: List[str] = []
    for c in cols:
        if c not in seen:
            seen[c] = 1
            out.append(c)
        else:
            out.append(f"{c}__{seen[c]}")
            seen[c] += 1
    return out

_num_keep = re.compile(r"[^0-9.\-]")

def to_int(v: Any) -> Optional[int]:
    if v is None:
        return None
    if isinstance(v, (int,)):
        return int(v)
    s = str(v)
    s = s.replace("\xa0", "")
    s = s.replace(",", "")
    s = _num_keep.sub("", s)
    if s in ("", "-", ".", "-.", ".-"):
        return None
    try:
        return int(float(s))
    except Exception:
        return None

_rm_municipio_suffix = re.compile(r"\s*Municipio,\s*Puerto\s*Rico\s*$", re.IGNORECASE)
def clean_geo_name(value: Any) -> Any:
    if value is None:
        return value
    s = str(value).strip()
    s = _rm_municipio_suffix.sub("", s)
    return s

def process_workbook(xlsx_path: Path, years: List[str]) -> pd.DataFrame:
    """Generic function to read an Excel file and return a cleaned DataFrame."""
    print(f"Reading workbook: {xlsx_path}")
    wb = load_workbook(xlsx_path, data_only=True, read_only=True)
    ws = wb[wb.sheetnames[0]]

    max_rows_scan = min(ws.max_row, 120)
    max_cols_scan = min(ws.max_column, 200)

    YEAR_SET = set(years)
    def is_year_token(cell_val: Any) -> bool:
        return txt(cell_val) in YEAR_SET

    child_hdr_row: Optional[int] = None
    for r in range(1, max_rows_scan + 1):
        hits = sum(1 for c in range(1, max_cols_scan + 1) if is_year_token(ws.cell(r, c).value))
        if hits >= 3:
            child_hdr_row = r
            break

    if child_hdr_row is None:
        raise RuntimeError(f"Couldn't detect child header row in {xlsx_path}")

    parent_hdr_row = max(1, child_hdr_row - 1)
    print(f"Header rows: parent={parent_hdr_row}, child={child_hdr_row}")

    parent_vals_raw = [norm(txt(ws.cell(parent_hdr_row, c).value)) for c in range(1, max_cols_scan + 1)]
    child_vals = [norm(txt(ws.cell(child_hdr_row, c).value)) for c in range(1, max_cols_scan + 1)]

    parent_vals: List[str] = []
    last_parent = ""
    for val in parent_vals_raw:
        if val:
            last_parent = val
            parent_vals.append(val)
        else:
            parent_vals.append(last_parent)

    def compose(parent: str, child: str, idx: int) -> str:
        if parent and child:
            return f"{parent} | {child}"
        if parent:
            return parent
        if child:
            return child
        return f"col_{idx}"

    headers = [compose(parent_vals[i-1], child_vals[i-1], i) for i in range(1, max_cols_scan + 1)]

    def column_is_empty(col_idx: int) -> bool:
        for r in range(child_hdr_row + 1, min(ws.max_row, child_hdr_row + 200) + 1):
            if txt(ws.cell(r, col_idx).value):
                return False
        return True

    while headers and headers[-1].startswith("col_") and column_is_empty(len(headers)):
        headers.pop()

    n_cols = len(headers)
    print(f"Detected {n_cols} header columns.")
    print("Columns sample:", headers[:12])

    data_rows: List[List[Any]] = []
    for r in range(child_hdr_row + 1, ws.max_row + 1):
        row_vals = [ws.cell(r, c).value for c in range(1, n_cols + 1)]
        if all(v in (None, "") for v in row_vals):
            break
        data_rows.append(row_vals)
    if not data_rows:
        raise RuntimeError("No data rows found under headers.")

    df = pd.DataFrame(data_rows, columns=uniquify(headers))
    return df

def main():
    # Process both Excel files
    df_2024 = process_workbook(XLSX_2024, ["2020", "2021", "2022", "2023", "2024"])
    df_2020 = process_workbook(XLSX_2020, ["2010", "2011", "2012", "2013", "2014", "2015", "2016", "2017", "2018", "2019"])

    # Identify and clean geographic column in both DataFrames
    geo_col_2024 = None
    geo_col_2020 = None
    geo_regex = re.compile(r"(geographic.*area|area.*name|geographic.*name|\.geographic area)", re.I)

    for c in df_2024.columns:
        if geo_regex.search(c):
            geo_col_2024 = c
            break
    if geo_col_2024 is None:
        for c in df_2024.columns:
            s = df_2024[c].astype(str).str.strip().str.lower()
            if s.str.contains(r"^puerto rico\b").any():
                geo_col_2024 = c
                break
    if geo_col_2024 is None:
        raise RuntimeError("Could not identify the 'Geographic Area' column in the 2024 file.")
    df_2024[geo_col_2024] = df_2024[geo_col_2024].apply(clean_geo_name)

    for c in df_2020.columns:
        if geo_regex.search(c):
            geo_col_2020 = c
            break
    if geo_col_2020 is None:
        for c in df_2020.columns:
            s = df_2020[c].astype(str).str.strip().str.lower()
            if s.str.contains(r"^puerto rico\b").any():
                geo_col_2020 = c
                break
    if geo_col_2020 is None:
        raise RuntimeError("Could not identify the 'Geographic Area' column in the 2020 file.")
    df_2020[geo_col_2020] = df_2020[geo_col_2020].apply(clean_geo_name)

    # Rename geo columns to merge on a consistent name
    df_2024.rename(columns={geo_col_2024: "Geographic Area"}, inplace=True)
    df_2020.rename(columns={geo_col_2020: "Geographic Area"}, inplace=True)

    # Merge the dataframes on the cleaned 'Geographic Area' column
    df = pd.merge(df_2020, df_2024, on="Geographic Area", how="outer")

    # --- Save full combined table ---
    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUT_CSV, index=False)
    df.to_json(OUT_JSON, orient="records", force_ascii=False)
    print("Wrote combined table CSV and JSON.")

    # --- Build compact JSON series for Puerto Rico ---
    pr_mask = df["Geographic Area"].astype(str).str.strip().str.lower().str.startswith("puerto rico")
    if not pr_mask.any():
        raise RuntimeError("Could not find a row for 'Puerto Rico'.")
    pr_row = df.loc[pr_mask].iloc[0]

    def find_exact(name: str, df_cols) -> Optional[str]:
        for col in df_cols:
            if col.strip() == name:
                return col
        return None

    def find_contains(*parts: str) -> List[str]:
        parts_l = [p.lower() for p in parts]
        out = []
        for col in df.columns:
            cl = col.lower()
            if all(p in cl for p in parts_l):
                out.append(col)
        return out

    def candidate_columns_for_year(year: int) -> List[str]:
        cands = []
        # 1) Exact composed header
        exact = f"Population Estimate (as of July 1) | {year}"
        c = find_exact(exact, df.columns)
        if c: cands.append(c)
        # 2) Contains "Population Estimate" + year
        cands.extend([col for col in find_contains("population estimate", str(year)) if col not in cands])
        # 3) Plain year header (e.g., "2020")
        c = find_exact(str(year), df.columns)
        if c and c not in cands: cands.append(c)
        # 4) Special fallback for 2010 and 2020 base
        if year == 2010:
            base = find_exact("April 1, 2010 Estimates Base", df.columns)
            if not base:
                base_list = find_contains("april 1, 2010", "base")
                base = base_list[0] if base_list else None
            if base and base not in cands: cands.append(base)
        if year == 2020:
            base = find_exact("April 1, 2020 Estimates Base", df.columns)
            if not base:
                base_list = find_contains("april 1, 2020", "base")
                base = base_list[0] if base_list else None
            if base and base not in cands: cands.append(base)
        return cands

    def get_year_value(row: pd.Series, year: int) -> tuple[Optional[int], Optional[str], Any]:
        for col in candidate_columns_for_year(year):
            raw = row.get(col, None)
            val = to_int(raw)
            if val is not None:
                return val, col, raw
        return None, None, None

    series = []
    chosen_map: Dict[int, str] = {}
    for y in range(2010, 2025):
        val, chosen_col, raw = get_year_value(pr_row, y)
        if val is None:
            print(f"[DEBUG] Could not parse a value for {y}. Candidates were: {candidate_columns_for_year(y)}")
            raise RuntimeError(f"Null/invalid value for {y}")
        series.append({"year": y, "population": val})
        chosen_map[y] = chosen_col
        print(f"Year {y}: using column '{chosen_col}' with raw='{raw}' -> {val}")

    OUT_PR.write_text(json.dumps({"name": "Puerto Rico", "series": series}, ensure_ascii=False, indent=2), encoding="utf-8")
    print("Wrote compact series JSON:", OUT_PR)
    print("Chosen columns:", chosen_map)
    print("All done.")

if __name__ == "__main__":
    main()
