import argparse
import json
import re
from pathlib import Path
from typing import Iterable, List, Optional

import pandas as pd

# ============ 文件配置 File Configuration ============
# 在此修改默认路径与表名，命令行参数会覆盖此处配置
RAW_DB_PATH = "./test12/membrane_aligned_data_all.csv"  # 原始数据库路径 (.csv / .xlsx)
RAW_SHEET = None  # 原始数据库为 xlsx 时的 sheet 名，None 表示首表
UNRESOLVED_PATH = "./test12/处理缩写.xlsx"  # 待挂接溯源列的工作簿路径 (.xlsx)
UNRESOLVED_SHEET = "Sheet1"  # 要更新的 sheet 名
OUTPUT_PATH = "./test12/处理缩写后_index.xlsx"  # 输出 xlsx 路径
# 参与建索引的列（逗号分隔），留空则使用下方 DEFAULT_EXTRACT_COLUMNS
EXTRACT_COLUMNS_OVERRIDE = ""
# =====================================================

DEFAULT_EXTRACT_COLUMNS = [
    "膜材料",
    "水相单体",
    "油相单体",
    "有机溶剂",
    "基底",
    "添加剂",
    "改性剂",
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

NON_VALUE_TOKENS = {"", "nan", "none", "null", "n/a", "na", "-", "--"}
SPLIT_SEPARATORS = set([";", "\n", "\r", "|", "｜"])
EXACT_NORM_REPLACEMENTS = {
    "（": "(",
    "）": ")",
    "；": ";",
    "，": ",",
    "–": "-",
    "—": "-",
    "−": "-",
}


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


def read_all_sheets(path: str) -> dict:
    p = Path(path)
    suf = p.suffix.lower()
    if suf not in {".xlsx", ".xls"}:
        raise ValueError("The unresolved workbook must be an Excel file so all sheets can be preserved.")
    xls = pd.ExcelFile(path)
    return {sheet: pd.read_excel(path, sheet_name=sheet) for sheet in xls.sheet_names}


def normalize_cell_text(text: object) -> str:
    s = "" if pd.isna(text) else str(text)
    s = s.replace("\u3000", " ")
    s = s.replace("（", "(").replace("）", ")")
    s = s.replace("【", "[").replace("】", "]")
    s = s.replace("；", ";").replace("，", ",")
    s = re.sub(r"\s+", " ", s).strip()
    return s


def normalize_match_key(text: object) -> str:
    s = "" if pd.isna(text) else str(text)
    for a, b in EXACT_NORM_REPLACEMENTS.items():
        s = s.replace(a, b)
    s = s.strip()
    s = re.sub(r"\s+", " ", s)
    return s.casefold()


def should_skip_value(text: str) -> bool:
    return normalize_match_key(text) in NON_VALUE_TOKENS


def split_components(cell_text: str) -> List[str]:
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


def detect_name_column(df: pd.DataFrame) -> str:
    for c in NAME_ALIASES:
        if c in df.columns:
            return c
    if len(df.columns) == 1:
        return df.columns[0]
    raise ValueError(
        "Could not find the chemical-name column. Please include one of: " + ", ".join(NAME_ALIASES)
    )


def build_name_to_record_index_map(raw_df: pd.DataFrame, extract_columns: Iterable[str]) -> pd.DataFrame:
    raw_df = raw_df.copy()
    raw_df.columns = [str(c).strip() for c in raw_df.columns]

    rows = []
    for i, rec in raw_df.iterrows():
        source_record_index = i + 1
        for col in extract_columns:
            if col not in raw_df.columns:
                continue
            cell_text = normalize_cell_text(rec[col])
            if should_skip_value(cell_text):
                continue
            for part in split_components(cell_text):
                rows.append(
                    {
                        "Normalized_Match_Key": normalize_match_key(part),
                        "Source_RecordIndex": source_record_index,
                    }
                )

    if not rows:
        return pd.DataFrame(columns=["Normalized_Match_Key", "Source_RecordIndex_List", "Source_RecordIndex"])

    map_df = pd.DataFrame(rows).drop_duplicates()
    agg = (
        map_df.groupby("Normalized_Match_Key")["Source_RecordIndex"]
        .agg(lambda s: sorted({int(x) for x in s.dropna()}))
        .reset_index()
        .rename(columns={"Source_RecordIndex": "Source_RecordIndex_List"})
    )
    agg["Source_RecordIndex"] = agg["Source_RecordIndex_List"].map(
        lambda xs: "|".join(map(str, xs)) if xs else ""
    )
    return agg


def attach_source_record_index(unresolved_df: pd.DataFrame, mapping_df: pd.DataFrame) -> pd.DataFrame:
    df = unresolved_df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    name_col = detect_name_column(df)

    df["_Original_Name_Work"] = df[name_col].astype(str).map(normalize_cell_text)
    df["_Normalized_Match_Key"] = df["_Original_Name_Work"].map(normalize_match_key)

    merged = df.merge(
        mapping_df[["Normalized_Match_Key", "Source_RecordIndex"]],
        how="left",
        left_on="_Normalized_Match_Key",
        right_on="Normalized_Match_Key",
    )

    if "Source_RecordIndex" not in merged.columns:
        merged["Source_RecordIndex"] = ""
    merged["Source_RecordIndex"] = merged["Source_RecordIndex"].fillna("")

    # Keep only the original columns plus the single new trace-back column.
    original_cols = list(unresolved_df.columns)
    if "Source_RecordIndex" in original_cols:
        # If the column already exists, overwrite it in place while preserving order.
        out = merged[original_cols].copy()
        out["Source_RecordIndex"] = merged["Source_RecordIndex"].values
    else:
        out = merged[original_cols + ["Source_RecordIndex"]].copy()
    return out


def parse_args() -> argparse.Namespace:
    default_extract = (
        EXTRACT_COLUMNS_OVERRIDE.strip()
        if EXTRACT_COLUMNS_OVERRIDE and EXTRACT_COLUMNS_OVERRIDE.strip()
        else ",".join(DEFAULT_EXTRACT_COLUMNS)
    )
    ap = argparse.ArgumentParser(
        description="Add only one trace-back column (Source_RecordIndex) to an unresolved chemical workbook."
    )
    ap.add_argument("--raw-db", default=RAW_DB_PATH, help="Path to the raw database (.csv/.xlsx)")
    ap.add_argument("--raw-sheet", default=RAW_SHEET, help="Sheet name when the raw database is xlsx")
    ap.add_argument("--unresolved", default=UNRESOLVED_PATH, help="Path to the unresolved workbook (.xlsx)")
    ap.add_argument("--unresolved-sheet", default=UNRESOLVED_SHEET, help="Target sheet to update in the unresolved workbook")
    ap.add_argument("--output", default=OUTPUT_PATH, help="Output xlsx path")
    ap.add_argument(
        "--extract-columns",
        default=default_extract,
        help="Comma-separated source columns to extract from the raw database.",
    )
    args = ap.parse_args()
    if not args.raw_db or not args.unresolved or not args.output:
        ap.error("When file config is empty, --raw-db, --unresolved and --output are required.")
    return args


def main() -> None:
    args = parse_args()
    extract_columns = [c.strip() for c in str(args.extract_columns).split(",") if c.strip()]

    raw_df = read_table(args.raw_db, args.raw_sheet)
    mapping_df = build_name_to_record_index_map(raw_df, extract_columns)

    all_sheets = read_all_sheets(args.unresolved)
    if args.unresolved_sheet not in all_sheets:
        raise ValueError(f"Sheet not found: {args.unresolved_sheet}")

    all_sheets[args.unresolved_sheet] = attach_source_record_index(all_sheets[args.unresolved_sheet], mapping_df)

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        for sheet_name, df in all_sheets.items():
            df.to_excel(writer, sheet_name=sheet_name[:31], index=False)

    target_df = all_sheets[args.unresolved_sheet]
    nonempty = target_df["Source_RecordIndex"].astype(str).str.strip().ne("")
    multi = target_df["Source_RecordIndex"].astype(str).str.contains(r"\|", regex=True, na=False)

    report = {
        "raw_db": str(args.raw_db),
        "raw_sheet": args.raw_sheet,
        "unresolved": str(args.unresolved),
        "unresolved_sheet": args.unresolved_sheet,
        "output": str(out_path),
        "extract_columns": extract_columns,
        "raw_rows": int(len(raw_df)),
        "mapping_keys": int(len(mapping_df)),
        "updated_rows": int(len(target_df)),
        "matched_rows": int(nonempty.sum()),
        "multi_candidate_rows": int((nonempty & multi).sum()),
    }
    report_path = out_path.with_suffix(".json")
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"[OK] wrote: {out_path}")
    print(f"[OK] wrote: {report_path}")


if __name__ == "__main__":
    main()
