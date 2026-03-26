#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
验证2：检查原始数据库给出的文献信息，能否稳定定位到 MD，
以及名称/变体能否在对应 MD 中找到。

设计目标：
1. 配置区放在最前面；
2. 脚本独立，不依赖主回溯脚本；
3. 重点验证“数据库文献 -> MD -> 名称/变体”这条链；
4. 输出审计表，便于判断问题是在文献映射、token 变体，还是文章本身没有定义。
"""

from __future__ import annotations

# =========================
# 配置区（放前面）
# =========================
DEFAULT_INPUT = "./test12/test.xlsx"
DEFAULT_INPUT_SHEET = "Sheet1"
DEFAULT_RAW_DB = "./test12/membrane_aligned_data_all.csv"
DEFAULT_RAW_SHEET = "Sheet1"
DEFAULT_MD_ZIP = "./test12/RO_md_files.zip"  # 与 md_backtrace 脚本一致；若 zip 不在 test12 下请改成本机实际路径
DEFAULT_OUTPUT = "./test12/validate/validation2_audit.xlsx"
DEFAULT_ONLY_FAILED = True
DEFAULT_FAILED_STATUSES = [
    "TOKEN_NOT_IN_MD",
    "MD_NOT_FOUND",
    "CONTEXT_FOUND_BUT_NO_EXPANSION",
]
DEFAULT_MAX_ROWS = 0              # 0 表示不限制
DEFAULT_MAX_SOURCE_CANDIDATES = 3 # Source_RecordIndex 多值时，最多尝试前几个
DEFAULT_MAX_WINDOWS = 6
DEFAULT_WINDOW_RADIUS = 220
DEFAULT_VERBOSE = True

# 这些列用于从原始数据库定位文献
DOC_FILENAME_CANDIDATES = ["文件名称", "file_name", "File_Name", "filename", "文献文件名"]
DOC_DOI_CANDIDATES = ["DOI", "doi"]
DOC_TITLE_CANDIDATES = ["论文题目", "标题", "title", "Title", "文献标题"]

# 这些列仅用于辅助判断“数据库这一行里是否能看到该名称/相近写法”
CHEMICAL_SOURCE_COLUMNS = [
    "膜材料", "水相单体", "油相单体", "有机溶剂", "基底", "添加剂", "改性剂",
    "membrane_material", "aqueous_monomer", "organic_monomer", "solvent",
    "substrate", "additive", "modifier",
]

# 常见缩写变体映射，用于增强在 MD 中的搜索召回
TOKEN_ALIAS_MAP = {
    "NH2": ["amino"],
    "COOH": ["carboxyl", "carboxylated"],
    "GO": ["graphene oxide"],
    "HNT": ["halloysite nanotube", "halloysite nanotubes"],
    "MSN": ["mesoporous silica nanoparticle", "mesoporous silica nanoparticles"],
    "MOF": ["metal organic framework", "metal-organic framework"],
}

# =========================
# 标准库 / 第三方库
# =========================
import argparse
import json
import os
import re
import zipfile
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import pandas as pd

try:
    from tqdm import tqdm
except Exception:
    tqdm = None


# =========================
# 基础工具函数
# =========================
def stage(msg: str) -> None:
    print(msg, flush=True)


def read_table(path: str, sheet_name: Optional[str] = None) -> pd.DataFrame:
    path_obj = Path(path)
    if not path_obj.exists():
        raise FileNotFoundError(f"File not found: {path}")

    suffix = path_obj.suffix.lower()
    if suffix in {".xlsx", ".xlsm", ".xls"}:
        return pd.read_excel(path_obj, sheet_name=sheet_name)

    if suffix == ".csv":
        last_err = None
        for enc in ["utf-8", "utf-8-sig", "gb18030", "gbk", "latin1"]:
            try:
                return pd.read_csv(path_obj, encoding=enc, low_memory=False)
            except Exception as e:
                last_err = e
        raise last_err

    raise ValueError(f"Unsupported file type: {suffix}")


def first_present_column(df: pd.DataFrame, candidates: Sequence[str]) -> Optional[str]:
    cols = set(map(str, df.columns))
    for c in candidates:
        if c in cols:
            return c
    return None


def normalize_text(s: object) -> str:
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return ""
    text = str(s)
    text = text.replace("（", "(").replace("）", ")")
    text = text.replace("【", "[").replace("】", "]")
    text = text.replace("；", ";").replace("，", ",")
    text = text.replace("–", "-").replace("—", "-").replace("−", "-")
    text = text.replace("\u00a0", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return text


def normalize_key(s: object) -> str:
    return normalize_text(s).casefold()


def clean_doi(s: object) -> str:
    text = normalize_text(s)
    text = text.strip().lower()
    text = re.sub(r"^https?://(dx\.)?doi\.org/", "", text)
    return text


def stem_key(filename: str) -> str:
    name = Path(filename).stem
    return normalize_key(name)


def basename_key(filename: str) -> str:
    return normalize_key(Path(filename).name)


def token_pattern(token: str) -> Optional[re.Pattern]:
    token = normalize_text(token)
    if not token:
        return None
    escaped = re.escape(token)
    if re.fullmatch(r"[A-Za-z0-9_\-\.]+", token):
        pat = rf"(?<![A-Za-z0-9]){escaped}(?![A-Za-z0-9])"
    else:
        pat = escaped
    try:
        return re.compile(pat, re.IGNORECASE)
    except re.error:
        return None


def sentence_windows(text: str, token: str, max_hits: int = 4) -> List[str]:
    pat = token_pattern(token)
    if pat is None:
        return []
    chunks = re.split(r"(?<=[\.!?。；;\n])", text)
    hits: List[str] = []
    seen = set()
    for idx, chunk in enumerate(chunks):
        if pat.search(chunk):
            start = max(0, idx - 1)
            end = min(len(chunks), idx + 2)
            window = normalize_text(" ".join(chunks[start:end]))
            key = window.casefold()
            if key and key not in seen:
                hits.append(window)
                seen.add(key)
            if len(hits) >= max_hits:
                break
    return hits


def span_windows(text: str, token: str, max_hits: int = 4, radius: int = 220) -> List[str]:
    pat = token_pattern(token)
    if pat is None:
        return []
    hits: List[str] = []
    seen = set()
    for m in pat.finditer(text):
        a = max(0, m.start() - radius)
        b = min(len(text), m.end() + radius)
        window = normalize_text(text[a:b])
        key = window.casefold()
        if key and key not in seen:
            hits.append(window)
            seen.add(key)
        if len(hits) >= max_hits:
            break
    return hits


def parse_source_record_indices(value: object) -> List[int]:
    text = normalize_text(value)
    if not text:
        return []
    out: List[int] = []
    for part in re.split(r"[|,;\s]+", text):
        part = part.strip()
        if not part:
            continue
        if part.isdigit():
            out.append(int(part))
    return out


def generate_search_tokens(original_name: str, standardized_query: str = "") -> List[str]:
    toks: List[str] = []
    seen = set()

    def add(x: str) -> None:
        x = normalize_text(x)
        if not x:
            return
        k = x.casefold()
        if k not in seen:
            seen.add(k)
            toks.append(x)

    s = normalize_text(original_name)
    q = normalize_text(standardized_query)
    add(s)
    add(q)

    if s:
        add(s.replace("-", ""))
        add(s.replace("-", " "))
        add(re.sub(r"^\d+\s*-\s*", "", s))
        add(re.sub(r"\((.*?)\)", r"\1", s))

        parts = [p.strip() for p in re.split(r"[-_/]", s) if p.strip()]
        for p in parts:
            if len(p) >= 3:
                add(p)

        for key, aliases in TOKEN_ALIAS_MAP.items():
            if key in s:
                for a in aliases:
                    add(a)
                    add(s.replace(key, a))

    return toks[:12]


def summarize_verdict(md_found: bool, exact_hit: bool, norm_hit: bool, variant_hit: bool) -> str:
    if not md_found:
        return "BIBLIO_MISMATCH_OR_MD_MISSING"
    if exact_hit:
        return "EXACT_TOKEN_IN_MD"
    if norm_hit:
        return "NORMALIZED_TOKEN_IN_MD"
    if variant_hit:
        return "VARIANT_TOKEN_IN_MD"
    return "MD_FOUND_BUT_TOKEN_NOT_FOUND"


# =========================
# MD 目录与匹配
# =========================
def build_md_catalog(zip_path: str) -> List[Dict[str, str]]:
    entries: List[Dict[str, str]] = []
    with zipfile.ZipFile(zip_path, "r") as zf:
        members = [m for m in zf.namelist() if not m.endswith("/") and m.lower().endswith(".md")]
        iterator: Iterable[str] = members
        if tqdm is not None:
            iterator = tqdm(members, total=len(members), desc="MD zip scan", unit="file")
        for member in iterator:
            base = os.path.basename(member)
            entries.append(
                {
                    "member": member,
                    "basename": base,
                    "basename_key": basename_key(base),
                    "stem_key": stem_key(base),
                    "doi_key": clean_doi(base),
                }
            )
    return entries


def locate_md_member(raw_row: pd.Series, md_catalog: List[Dict[str, str]]) -> Tuple[Optional[str], str]:
    file_col = first_present_column(pd.DataFrame([raw_row]), DOC_FILENAME_CANDIDATES)
    doi_col = first_present_column(pd.DataFrame([raw_row]), DOC_DOI_CANDIDATES)
    title_col = first_present_column(pd.DataFrame([raw_row]), DOC_TITLE_CANDIDATES)

    raw_file = normalize_text(raw_row.get(file_col, "")) if file_col else ""
    raw_doi = normalize_text(raw_row.get(doi_col, "")) if doi_col else ""
    raw_title = normalize_text(raw_row.get(title_col, "")) if title_col else ""

    doi_key = clean_doi(raw_doi)
    file_key = basename_key(raw_file)
    file_stem = stem_key(raw_file)
    title_key = normalize_key(raw_title)

    # 1) DOI 精确/包含匹配
    if doi_key:
        for item in md_catalog:
            member_low = item["member"].lower()
            if doi_key == item["doi_key"] or doi_key in member_low:
                return item["member"], "doi"

    # 2) 文件名 basename / stem
    if file_key:
        for item in md_catalog:
            if file_key == item["basename_key"]:
                return item["member"], "filename"
    if file_stem:
        for item in md_catalog:
            if file_stem == item["stem_key"] or file_stem in item["stem_key"] or item["stem_key"] in file_stem:
                return item["member"], "filename_stem"

    # 3) 标题弱匹配（较保守）
    if title_key:
        short_title = re.sub(r"[^a-z0-9]+", " ", title_key)
        short_title = re.sub(r"\s+", " ", short_title).strip()
        if len(short_title) >= 12:
            for item in md_catalog:
                member_key = normalize_key(item["member"])
                if short_title[:40] and short_title[:40] in member_key:
                    return item["member"], "title_partial"

    return None, "none"


def read_md_member(zip_path: str, member: str) -> str:
    with zipfile.ZipFile(zip_path, "r") as zf:
        raw = zf.read(member)
    for enc in ["utf-8", "utf-8-sig", "gb18030", "gbk", "latin1"]:
        try:
            return raw.decode(enc, errors="ignore")
        except Exception:
            continue
    return raw.decode("utf-8", errors="ignore")


# =========================
# 审计核心逻辑
# =========================
def build_raw_doc_fields(raw_df: pd.DataFrame, idx: int) -> Dict[str, str]:
    row = raw_df.iloc[idx - 1]
    file_col = first_present_column(raw_df, DOC_FILENAME_CANDIDATES)
    doi_col = first_present_column(raw_df, DOC_DOI_CANDIDATES)
    title_col = first_present_column(raw_df, DOC_TITLE_CANDIDATES)
    return {
        "Raw_FileName": normalize_text(row.get(file_col, "")) if file_col else "",
        "Raw_DOI": normalize_text(row.get(doi_col, "")) if doi_col else "",
        "Raw_Title": normalize_text(row.get(title_col, "")) if title_col else "",
    }


def raw_row_has_name(raw_row: pd.Series, name: str) -> Tuple[bool, bool, str]:
    name_key = normalize_key(name)
    if not name_key:
        return False, False, ""

    cells = []
    for col in CHEMICAL_SOURCE_COLUMNS:
        if col in raw_row.index:
            val = normalize_text(raw_row.get(col, ""))
            if val:
                cells.append(val)

    joined = " || ".join(cells)
    joined_key = normalize_key(joined)
    exact = name_key in joined_key if joined_key else False

    similar = False
    evidence = ""
    if not exact:
        for tok in generate_search_tokens(name):
            tok_key = normalize_key(tok)
            if tok_key and tok_key in joined_key:
                similar = True
                evidence = tok
                break
    return exact, similar, evidence


def audit_one_row(
    row: pd.Series,
    raw_df: pd.DataFrame,
    md_catalog: List[Dict[str, str]],
    md_zip: str,
    max_source_candidates: int,
    max_windows: int,
    window_radius: int,
) -> Dict[str, object]:
    original_name = normalize_text(row.get("Original_Name", ""))
    standardized_query = normalize_text(row.get("Standardized_Query", ""))
    source_indices = parse_source_record_indices(row.get("Source_RecordIndex", ""))[:max_source_candidates]

    result: Dict[str, object] = {
        "EntityIndex": row.get("EntityIndex", ""),
        "RowIndex": row.get("RowIndex", ""),
        "Original_Name": original_name,
        "Source_RecordIndex": normalize_text(row.get("Source_RecordIndex", "")),
        "Checked_Source_RecordIndex": "",
        "Raw_FileName": "",
        "Raw_DOI": "",
        "Raw_Title": "",
        "DB_Row_Has_Name": False,
        "DB_Row_Has_Similar_Name": False,
        "DB_Row_Name_Evidence": "",
        "MD_Found": False,
        "MD_Match_Method": "none",
        "MD_File": "",
        "Exact_Hit": False,
        "Normalized_Hit": False,
        "Variant_Hit": False,
        "Matched_Variant": "",
        "Evidence_Snippet": "",
        "Search_Tokens": " | ".join(generate_search_tokens(original_name, standardized_query)),
        "Verdict": "NO_SOURCE_RECORD_INDEX",
        "Notes": "",
    }

    if not source_indices:
        return result

    search_tokens = generate_search_tokens(original_name, standardized_query)
    exact_token = normalize_text(original_name)
    norm_token = normalize_text(standardized_query) if standardized_query else ""

    for source_idx in source_indices:
        if source_idx < 1 or source_idx > len(raw_df):
            continue

        raw_row = raw_df.iloc[source_idx - 1]
        docs = build_raw_doc_fields(raw_df, source_idx)
        exact_in_db, similar_in_db, db_ev = raw_row_has_name(raw_row, original_name)
        member, how = locate_md_member(raw_row, md_catalog)

        result.update(
            {
                "Checked_Source_RecordIndex": source_idx,
                "Raw_FileName": docs["Raw_FileName"],
                "Raw_DOI": docs["Raw_DOI"],
                "Raw_Title": docs["Raw_Title"],
                "DB_Row_Has_Name": exact_in_db,
                "DB_Row_Has_Similar_Name": similar_in_db,
                "DB_Row_Name_Evidence": db_ev,
                "MD_Found": bool(member),
                "MD_Match_Method": how,
                "MD_File": member or "",
            }
        )

        if not member:
            result["Verdict"] = "BIBLIO_MISMATCH_OR_MD_MISSING"
            continue

        md_text = read_md_member(md_zip, member)

        # 先 exact，再 normalized，再 variants
        exact_hits = sentence_windows(md_text, exact_token, max_hits=max_windows) if exact_token else []
        if exact_hits:
            result.update(
                {
                    "Exact_Hit": True,
                    "Matched_Variant": exact_token,
                    "Evidence_Snippet": exact_hits[0],
                    "Verdict": "EXACT_TOKEN_IN_MD",
                }
            )
            return result

        norm_hits = []
        if norm_token and normalize_key(norm_token) != normalize_key(exact_token):
            norm_hits = sentence_windows(md_text, norm_token, max_hits=max_windows)
        if norm_hits:
            result.update(
                {
                    "Normalized_Hit": True,
                    "Matched_Variant": norm_token,
                    "Evidence_Snippet": norm_hits[0],
                    "Verdict": "NORMALIZED_TOKEN_IN_MD",
                }
            )
            return result

        variant_found = False
        for tok in search_tokens:
            if normalize_key(tok) in {normalize_key(exact_token), normalize_key(norm_token)}:
                continue
            hits = sentence_windows(md_text, tok, max_hits=max_windows)
            if not hits:
                hits = span_windows(md_text, tok, max_hits=max_windows, radius=window_radius)
            if hits:
                result.update(
                    {
                        "Variant_Hit": True,
                        "Matched_Variant": tok,
                        "Evidence_Snippet": hits[0],
                        "Verdict": "VARIANT_TOKEN_IN_MD",
                    }
                )
                variant_found = True
                break
        if variant_found:
            return result

        # 找到了 MD 但 name / 变体都没命中，继续试下一条 source_idx
        result["Verdict"] = "MD_FOUND_BUT_TOKEN_NOT_FOUND"

    return result


# =========================
# 输出
# =========================
def write_output(path: str, audit_df: pd.DataFrame) -> None:
    md_not_found = audit_df[audit_df["Verdict"].eq("BIBLIO_MISMATCH_OR_MD_MISSING")].copy()
    token_found = audit_df[audit_df["Verdict"].isin(["EXACT_TOKEN_IN_MD", "NORMALIZED_TOKEN_IN_MD", "VARIANT_TOKEN_IN_MD"])].copy()
    md_found_no_token = audit_df[audit_df["Verdict"].eq("MD_FOUND_BUT_TOKEN_NOT_FOUND")].copy()

    summary_rows = [
        {"Metric": "Total", "Value": len(audit_df)},
        {"Metric": "MD found", "Value": int(audit_df["MD_Found"].sum()) if "MD_Found" in audit_df.columns else 0},
        {"Metric": "Exact hit", "Value": int(audit_df["Exact_Hit"].sum()) if "Exact_Hit" in audit_df.columns else 0},
        {"Metric": "Normalized hit", "Value": int(audit_df["Normalized_Hit"].sum()) if "Normalized_Hit" in audit_df.columns else 0},
        {"Metric": "Variant hit", "Value": int(audit_df["Variant_Hit"].sum()) if "Variant_Hit" in audit_df.columns else 0},
    ]
    if not audit_df.empty:
        vc = audit_df["Verdict"].value_counts(dropna=False)
        for k, v in vc.items():
            summary_rows.append({"Metric": f"Verdict::{k}", "Value": int(v)})
    summary_df = pd.DataFrame(summary_rows)

    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        summary_df.to_excel(writer, sheet_name="summary", index=False)
        audit_df.to_excel(writer, sheet_name="audit_all", index=False)
        md_not_found.to_excel(writer, sheet_name="md_not_found", index=False)
        md_found_no_token.to_excel(writer, sheet_name="md_found_no_token", index=False)
        token_found.to_excel(writer, sheet_name="token_found", index=False)


# =========================
# 主函数
# =========================
def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="验证2：数据库文献信息 -> MD -> 名称/变体命中审计")
    p.add_argument("--input", default=DEFAULT_INPUT)
    p.add_argument("--input-sheet", default=DEFAULT_INPUT_SHEET)
    p.add_argument("--raw-db", default=DEFAULT_RAW_DB)
    p.add_argument("--raw-sheet", default=DEFAULT_RAW_SHEET)
    p.add_argument("--md-zip", default=DEFAULT_MD_ZIP)
    p.add_argument("--output", default=DEFAULT_OUTPUT)
    p.add_argument("--only-failed", action="store_true", default=DEFAULT_ONLY_FAILED)
    p.add_argument("--failed-statuses", nargs="*", default=DEFAULT_FAILED_STATUSES)
    p.add_argument("--max-rows", type=int, default=DEFAULT_MAX_ROWS)
    p.add_argument("--max-source-candidates", type=int, default=DEFAULT_MAX_SOURCE_CANDIDATES)
    p.add_argument("--max-windows", type=int, default=DEFAULT_MAX_WINDOWS)
    p.add_argument("--window-radius", type=int, default=DEFAULT_WINDOW_RADIUS)
    return p


def main() -> None:
    args = build_parser().parse_args()

    stage("阶段: 读取输入表")
    entity_df = read_table(args.input, args.input_sheet)
    if "EntityIndex" not in entity_df.columns:
        entity_df.insert(0, "EntityIndex", range(1, len(entity_df) + 1))

    stage("阶段: 读取原始数据库")
    raw_sheet = None if str(args.raw_db).lower().endswith(".csv") else args.raw_sheet
    raw_df = read_table(args.raw_db, raw_sheet)

    stage("阶段: 扫描 MD zip")
    md_catalog = build_md_catalog(args.md_zip)
    stage(f"MD 目录完成：条目数={len(md_catalog)}")

    work_df = entity_df.copy()
    if args.only_failed and "Backtrace_Status" in work_df.columns:
        failed_statuses = {normalize_text(x) for x in args.failed_statuses}
        work_df = work_df[work_df["Backtrace_Status"].astype(str).map(normalize_text).isin(failed_statuses)].copy()

    if args.max_rows and args.max_rows > 0:
        work_df = work_df.head(args.max_rows).copy()

    total = len(work_df)
    stage(f"阶段: 逐行验证（共 {total} 行）")

    rows = []
    iterator = work_df.iterrows()
    if tqdm is not None:
        iterator = tqdm(iterator, total=total, desc="Validate", unit="row")

    for _, row in iterator:
        rows.append(
            audit_one_row(
                row=row,
                raw_df=raw_df,
                md_catalog=md_catalog,
                md_zip=args.md_zip,
                max_source_candidates=args.max_source_candidates,
                max_windows=args.max_windows,
                window_radius=args.window_radius,
            )
        )

    audit_df = pd.DataFrame(rows)
    stage("阶段: 写出审计结果")
    write_output(args.output, audit_df)
    stage(f"完成：{args.output}")


if __name__ == "__main__":
    main()
