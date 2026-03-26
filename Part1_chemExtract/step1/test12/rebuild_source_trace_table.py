import argparse
import json
import re
from pathlib import Path
from typing import Iterable, List, Optional

import pandas as pd

DEFAULT_EXTRACT_COLUMNS = [
    "膜材料",
    "水相单体",
    "油相单体",
    "有机溶剂",
    "基底",
    "添加剂",
    "改性剂",
]

CONTEXT_COLUMNS = [
    "膜类型",
    "样品编号",
    "文件名称",
    "发表年份",
    "DOI",
    "论文题目",
]

NAME_ALIASES = [
    "Original_Name",
    "original_name",
    "Name",
    "name",
    "化学物名称",
    "化学名称",
    "名称",
    "原始名称",
    "物质名称",
]


def read_table(path: str, sheet_name: Optional[str] = None) -> pd.DataFrame:
    p = Path(path)
    suf = p.suffix.lower()
    if suf in {".xlsx", ".xls"}:
        return pd.read_excel(path, sheet_name=sheet_name)
    if suf == ".csv":
        last_err = None
        for enc in ["utf-8", "utf-8-sig", "gbk", "gb18030", "latin1"]:
            try:
                return pd.read_csv(path, encoding=enc)
            except Exception as e:
                last_err = e
        raise last_err
    raise ValueError(f"Unsupported file type: {path}")


NON_VALUE_TOKENS = {"", "nan", "none", "null", "n/a", "na", "-", "--"}


def normalize_cell_text(text: object) -> str:
    s = "" if pd.isna(text) else str(text)
    s = s.replace("\u3000", " ")
    s = s.replace("（", "(").replace("）", ")")
    s = s.replace("【", "[").replace("】", "]")
    s = s.replace("；", ";").replace("，", ",")
    s = re.sub(r"\s+", " ", s).strip()
    return s


def should_skip_value(text: str) -> bool:
    return normalize_match_key(text) in NON_VALUE_TOKENS


SPLIT_SEPARATORS = set([";", "\n", "\r", "|", "｜"])


def split_components(cell_text: str) -> List[str]:
    """Split top-level semicolon/newline separated components.

    Intentionally does NOT split on comma, because many chemical names contain commas.
    Also avoids splitting inside (), [], {}.
    """
    text = normalize_cell_text(cell_text)
    if should_skip_value(text):
        return []

    items = []
    buf = []
    depth_round = depth_square = depth_curly = 0

    for ch in text:
        if ch == "(":
            depth_round += 1
        elif ch == ")":
            depth_round = max(0, depth_round - 1)
        elif ch == "[":
            depth_square += 1
        elif ch == "]":
            depth_square = max(0, depth_square - 1)
        elif ch == "{":
            depth_curly += 1
        elif ch == "}":
            depth_curly = max(0, depth_curly - 1)

        if ch in SPLIT_SEPARATORS and depth_round == 0 and depth_square == 0 and depth_curly == 0:
            item = "".join(buf).strip()
            if item:
                items.append(item)
            buf = []
        else:
            buf.append(ch)

    tail = "".join(buf).strip()
    if tail:
        items.append(tail)

    cleaned = []
    seen = set()
    for item in items:
        item = re.sub(r"^[,;\s]+|[,;\s]+$", "", item).strip()
        key = normalize_match_key(item)
        if key in NON_VALUE_TOKENS or not item:
            continue
        if key not in seen:
            cleaned.append(item)
            seen.add(key)
    return cleaned


EXACT_NORM_REPLACEMENTS = {
    "（": "(",
    "）": ")",
    "；": ";",
    "，": ",",
    "–": "-",
    "—": "-",
    "−": "-",
}


def normalize_match_key(text: object) -> str:
    s = "" if pd.isna(text) else str(text)
    for a, b in EXACT_NORM_REPLACEMENTS.items():
        s = s.replace(a, b)
    s = s.strip()
    s = re.sub(r"\s+", " ", s)
    return s.casefold()


ROLE_MAP = {
    "膜材料": "membrane_material",
    "水相单体": "aqueous_monomer",
    "油相单体": "organic_monomer",
    "有机溶剂": "organic_solvent",
    "基底": "substrate",
    "添加剂": "additive",
    "改性剂": "modifier",
}


def build_long_table(raw_df: pd.DataFrame, extract_columns: Iterable[str]) -> pd.DataFrame:
    rows = []
    entity_idx = 1
    raw_df = raw_df.copy()
    raw_df.columns = [str(c).strip() for c in raw_df.columns]

    for i, rec in raw_df.iterrows():
        source_record_index = i + 1
        source_excel_row = i + 2

        context = {c: rec[c] if c in raw_df.columns else None for c in CONTEXT_COLUMNS}

        for col in extract_columns:
            if col not in raw_df.columns:
                continue
            cell_text = normalize_cell_text(rec[col])
            if should_skip_value(cell_text):
                continue

            parts = split_components(cell_text)
            if not parts:
                continue

            for item_idx, part in enumerate(parts, start=1):
                rows.append(
                    {
                        "EntityIndex": entity_idx,
                        "Source_RecordIndex": source_record_index,
                        "Source_ExcelRow": source_excel_row,
                        "Source_Column": col,
                        "Source_Role": ROLE_MAP.get(col, col),
                        "Source_Cell_Text": cell_text,
                        "Item_Index_In_Cell": item_idx,
                        "Items_In_Cell": len(parts),
                        "Original_Name": part,
                        "Normalized_Match_Key": normalize_match_key(part),
                        **context,
                    }
                )
                entity_idx += 1

    return pd.DataFrame(rows)


def detect_name_column(df: pd.DataFrame) -> str:
    for c in NAME_ALIASES:
        if c in df.columns:
            return c
    if len(df.columns) == 1:
        return df.columns[0]
    raise ValueError(
        "Could not find the unresolved-name column. Please include one of: "
        + ", ".join(NAME_ALIASES)
    )


def build_unresolved_match_table(long_df: pd.DataFrame, unresolved_df: pd.DataFrame) -> pd.DataFrame:
    unresolved_df = unresolved_df.copy()
    unresolved_df.columns = [str(c).strip() for c in unresolved_df.columns]
    name_col = detect_name_column(unresolved_df)
    unresolved_df["Original_Name"] = unresolved_df[name_col].astype(str).map(normalize_cell_text)
    unresolved_df = unresolved_df[~unresolved_df["Original_Name"].map(should_skip_value)].copy()
    unresolved_df["Normalized_Match_Key"] = unresolved_df["Original_Name"].map(normalize_match_key)

    merged = unresolved_df.merge(
        long_df,
        how="left",
        on="Normalized_Match_Key",
        suffixes=("_unresolved", "_source"),
    )

    preferred = []
    unresolved_cols = [c for c in unresolved_df.columns if c != "Normalized_Match_Key"]
    for c in unresolved_cols:
        if c == "Original_Name":
            preferred.append("Original_Name_unresolved")
        else:
            preferred.append(c)

    source_cols = [
        "EntityIndex",
        "Source_RecordIndex",
        "Source_ExcelRow",
        "Source_Column",
        "Source_Role",
        "Source_Cell_Text",
        "Item_Index_In_Cell",
        "Items_In_Cell",
        "Original_Name_source",
        "文件名称",
        "发表年份",
        "DOI",
        "论文题目",
        "样品编号",
        "膜类型",
    ]
    existing = [c for c in source_cols if c in merged.columns]
    merged = merged[preferred + existing].copy()
    merged = merged.rename(
        columns={
            "Original_Name_unresolved": "Original_Name",
            "Original_Name_source": "Matched_Source_Name",
        }
    )
    return merged


def build_source_cell_table(raw_df: pd.DataFrame, extract_columns: Iterable[str]) -> pd.DataFrame:
    rows = []
    raw_df = raw_df.copy()
    raw_df.columns = [str(c).strip() for c in raw_df.columns]

    for i, rec in raw_df.iterrows():
        source_record_index = i + 1
        source_excel_row = i + 2
        context = {c: rec[c] if c in raw_df.columns else None for c in CONTEXT_COLUMNS}

        for col in extract_columns:
            if col not in raw_df.columns:
                continue
            cell_text = normalize_cell_text(rec[col])
            if should_skip_value(cell_text):
                continue
            items = split_components(cell_text)
            rows.append(
                {
                    "Source_RecordIndex": source_record_index,
                    "Source_ExcelRow": source_excel_row,
                    "Source_Column": col,
                    "Source_Role": ROLE_MAP.get(col, col),
                    "Source_Cell_Text": cell_text,
                    "Items_In_Cell": len(items),
                    "Split_Items": " | ".join(items),
                    **context,
                }
            )
    return pd.DataFrame(rows)


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Rebuild a source-traceable chemical long table from the raw RO database.")
    ap.add_argument("--raw-db", required=True, help="Path to the raw database (.csv/.xlsx)")
    ap.add_argument("--raw-sheet", default=None, help="Sheet name when the raw database is xlsx")
    ap.add_argument("--output", required=True, help="Output xlsx path")
    ap.add_argument(
        "--extract-columns",
        default=",".join(DEFAULT_EXTRACT_COLUMNS),
        help="Comma-separated source columns to extract."
    )
    ap.add_argument("--unresolved", default=None, help="Optional unresolved chemical workbook/table to exact-match against the rebuilt long table")
    ap.add_argument("--unresolved-sheet", default=None, help="Sheet name for the unresolved workbook if needed")
    return ap.parse_args()


def main() -> None:
    args = parse_args()
    extract_columns = [c.strip() for c in str(args.extract_columns).split(",") if c.strip()]

    raw_df = read_table(args.raw_db, args.raw_sheet)
    long_df = build_long_table(raw_df, extract_columns)
    cell_df = build_source_cell_table(raw_df, extract_columns)

    summary = pd.DataFrame(
        [
            {"metric": "raw_rows", "value": len(raw_df)},
            {"metric": "extracted_entities", "value": len(long_df)},
            {"metric": "source_cells", "value": len(cell_df)},
            {"metric": "extract_columns", "value": ", ".join(extract_columns)},
        ]
    )

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        summary.to_excel(writer, sheet_name="summary", index=False)
        long_df.to_excel(writer, sheet_name="entity_long", index=False)
        cell_df.to_excel(writer, sheet_name="source_cells", index=False)

        if args.unresolved:
            unresolved_df = read_table(args.unresolved, args.unresolved_sheet)
            match_df = build_unresolved_match_table(long_df, unresolved_df)
            match_df.to_excel(writer, sheet_name="unresolved_matches", index=False)

    report = {
        "raw_db": str(args.raw_db),
        "raw_sheet": args.raw_sheet,
        "output": str(out_path),
        "extract_columns": extract_columns,
        "raw_rows": int(len(raw_df)),
        "extracted_entities": int(len(long_df)),
        "source_cells": int(len(cell_df)),
        "unresolved": str(args.unresolved) if args.unresolved else None,
    }
    report_path = out_path.with_suffix(".json")
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"[OK] wrote: {out_path}")
    print(f"[OK] wrote: {report_path}")


if __name__ == "__main__":
    main()
