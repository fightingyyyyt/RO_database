import argparse
import importlib.util
import json
import math
import os
import re
import time
import zipfile
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

try:
    from tqdm import tqdm  # type: ignore
except Exception:  # pragma: no cover
    tqdm = None

# ============ 文件配置 File Configuration ============
# 在此集中管理默认输入/输出路径与关键参数；命令行参数会覆盖此处配置
INPUT_PATH = "./test12/test.xlsx"  # 输入 Excel/CSV（含 Source_RecordIndex）
INPUT_SHEET = "Sheet1"  # 输入为 xlsx 时的 sheet 名；空字符串表示自动选择

RAW_DB_PATH = "./test12/membrane_aligned_data_all.csv"  # 原始数据库 Excel/CSV
RAW_SHEET = None  # 原始库为 xlsx 时的 sheet 名；空字符串表示自动选择

MD_ZIP_PATH = "./test12/RO_md_files.zip"  # 包含 MD 文件的 zip
BASE_SCRIPT_PATH = r"D:\graduate_work\RO_database\Part1_chemExtract\step1\test10_no_part_polymer_deepseek\code_extract_name_fixed.py"  # base 抽取脚本路径


OUTPUT_XLSX_PATH = "./test12/test_index_backtrace_pubchem.xlsx"  # 输出 Excel
REPORT_JSON_PATH = "./test12/test_index_backtrace_pubchem_report.json"  # 输出 JSON 报告

RESUME_BT_PATH = ""  # 断点续跑：已保存的回溯结果（bt_df）Excel/CSV；填写后跳过回溯/查询阶段

DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY", "")
DEEPSEEK_MODEL = "deepseek-chat"

ONLY_UNRESOLVED = False  # 仅处理未解析行（Lookup_Status 属于 DEFAULT_UNRESOLVED）
# =====================================================

DEFAULT_UNRESOLVED = {
    "NO_HIT",
    "NO_SPECIFIC_CHEMICAL",
    "ABBR_EXPANDED_NO_HIT",
    "NOT_CHEMICAL_FROM_LLM",
    "CID_ONLY",
    "EMPTY_QUERY",
    "EMPTY_CANDIDATE",
}


def stage(msg: str) -> None:
    ts = time.strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def load_bt_df(path: str) -> pd.DataFrame:
    p = Path(path)
    if p.suffix.lower() in {".xlsx", ".xls"}:
        xls = pd.ExcelFile(path)
        sheets = set(xls.sheet_names)
        if "bt_df" in sheets:
            return pd.read_excel(path, sheet_name="bt_df")
        if "backtrace_hits" in sheets and "backtrace_unresolved" in sheets:
            a = pd.read_excel(path, sheet_name="backtrace_hits")
            b = pd.read_excel(path, sheet_name="backtrace_unresolved")
            return pd.concat([a, b], ignore_index=True)
        return pd.read_excel(path, sheet_name=xls.sheet_names[0])
    return read_table(path, None)

DOC_FILE_COLS = ["文件名称", "file_name", "filename", "md_file", "MD_File"]
DOC_DOI_COLS = ["DOI", "doi"]
DOC_TITLE_COLS = ["论文题目", "标题", "题目", "title", "Title", "paper_title"]

BACKTRACE_EXTRA_COLUMNS = [
    "Matched_Source_RecordIndex",
    "Source_RecordIndex",
    "Backtrace_Status",
    "Backtrace_Confidence",
    "Resolved_Full_Name",
    "Resolved_Composition",
    "Evidence_Snippet",
    "MD_File",
    "MD_Locate_Method",
    "MD_Candidate_Tried",
    "MD_Candidate_List",
    "MD_Accepted_Because",
    "Raw_FileName",
    "Raw_DOI",
    "Raw_Title",
    "Backtrace_Notes",
]

# Token expansion / filtering config (keep near top for tuning)
ABBR_REPLACEMENTS = {
    "NH2": "amino",
    "COOH": "carboxyl",
    "GO": "graphene oxide",
    "HNT": "halloysite nanotube",
    "MSN": "mesoporous silica nanoparticle",
    "MOF": "metal organic framework",
}

BAD_CONTEXT_WORDS = {
    "synthesized",
    "reaction",
    "normalized",
    "used",
    "prepared",
    "obtained",
    "calculated",
    "measured",
    "figure",
    "table",
    "based on",
    "according to",
}

MATERIAL_LIKE_HINTS = {
    "commercial membrane",
    "material series",
    "polymer",
    "nanoparticle",
    "framework",
    "functionalized",
    "modified material",
    "resin",
    "membrane",
    "support",
    "composite",
    "series",
}


def load_base_module(path: str):
    spec = importlib.util.spec_from_file_location("base_extract", path)
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    return mod


def normalize_text(text: Any) -> str:
    t = str(text or "").strip()
    t = t.replace("（", "(").replace("）", ")")
    t = t.replace("，", ",").replace("；", ";")
    t = t.replace("：", ":")
    t = re.sub(r"\s+", " ", t)
    return t


def clean_key(text: Any) -> str:
    t = normalize_text(text).lower()
    t = re.sub(r"https?://(dx\.)?doi\.org/", "", t)
    t = re.sub(r"\.md$", "", t)
    t = re.sub(r"\.[a-z0-9]{1,5}$", "", t)
    t = re.sub(r"[^a-z0-9\u4e00-\u9fff]+", "", t)
    return t


def is_blank(x: Any) -> bool:
    if x is None:
        return True
    if isinstance(x, float) and math.isnan(x):
        return True
    s = normalize_text(x)
    return s == "" or s.lower() == "nan"


def read_table(path: str, sheet_name: Optional[str] = None) -> pd.DataFrame:
    t0 = time.time()
    stage(f"读取表格: {path}" + (f" (sheet={sheet_name})" if sheet_name else ""))
    p = Path(path)
    if p.suffix.lower() in {".xlsx", ".xls"}:
        df = pd.read_excel(path, sheet_name=sheet_name) if sheet_name else pd.read_excel(path)
        stage(f"读取完成: {path}  行数={len(df)}  用时={time.time() - t0:.2f}s")
        return df
    for enc in ["utf-8", "utf-8-sig", "gb18030", "gbk", "gb2312", "latin1"]:
        try:
            df = pd.read_csv(path, encoding=enc)
            stage(f"读取完成: {path}  行数={len(df)}  encoding={enc}  用时={time.time() - t0:.2f}s")
            return df
        except Exception:
            continue
    raise ValueError(f"Unable to read table: {path}")


def pick_first_nonempty_sheet(xlsx_path: str) -> str:
    xls = pd.ExcelFile(xlsx_path)
    for s in xls.sheet_names:
        df = pd.read_excel(xlsx_path, sheet_name=s)
        if len(df.columns) > 0:
            return s
    raise ValueError("No readable sheet found")


def ensure_raw_rowindex(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = [str(c).strip() for c in out.columns]
    if "Source_RecordIndex" in out.columns:
        return out
    out.insert(0, "Source_RecordIndex", range(1, len(out) + 1))
    return out


def parse_source_record_indices(value: Any) -> List[int]:
    if is_blank(value):
        return []
    s = normalize_text(value)
    nums = re.findall(r"\d+", s)
    out = []
    seen = set()
    for n in nums:
        i = int(n)
        if i not in seen:
            out.append(i)
            seen.add(i)
    return out


def get_first_nonblank(row: pd.Series, candidates: List[str]) -> str:
    for c in candidates:
        if c in row.index and not is_blank(row[c]):
            return normalize_text(row[c])
    return ""


def detect_name_column(df: pd.DataFrame) -> str:
    for c in ["Original_Name", "original_name", "化学名称", "原始名称", "物质名称", "名称"]:
        if c in df.columns:
            return c
    raise ValueError("Input entities table must include Original_Name (or alias)")


def detect_source_index_column(df: pd.DataFrame) -> str:
    for c in ["Source_RecordIndex", "source_recordindex", "source_record_index"]:
        if c in df.columns:
            return c
    raise ValueError("Input entities table must include Source_RecordIndex")


def choose_entity_sheet(path: str, requested: str = "") -> Optional[str]:
    if not path.lower().endswith((".xlsx", ".xls")):
        return None
    if requested:
        return requested
    return pick_first_nonempty_sheet(path)


def peek_zip_text(zf: zipfile.ZipFile, member: str, max_bytes: int = 24000) -> str:
    with zf.open(member, "r") as f:
        data = f.read(max_bytes)
    for enc in ["utf-8", "utf-8-sig", "gb18030", "gbk", "latin1"]:
        try:
            return data.decode(enc)
        except Exception:
            continue
    return data.decode("utf-8", errors="ignore")


def read_md_member(zip_path: str, member: str) -> str:
    with zipfile.ZipFile(zip_path, "r") as zf:
        data = zf.read(member)
    for enc in ["utf-8", "utf-8-sig", "gb18030", "gbk", "latin1"]:
        try:
            return data.decode(enc)
        except Exception:
            continue
    return data.decode("utf-8", errors="ignore")


def extract_md_title(text: str) -> str:
    for line in str(text or "").splitlines()[:40]:
        s = normalize_text(line).lstrip("# ").strip()
        if len(s) >= 12:
            return s
    return ""


def extract_md_doi(text: str) -> str:
    m = re.search(r"\b10\.\d{4,9}/[-._;()/:A-Z0-9]+", str(text or ""), flags=re.I)
    return normalize_text(m.group(0)) if m else ""


def build_md_catalog(zip_path: str) -> List[Dict[str, str]]:
    stage(f"扫描 MD zip 建目录: {zip_path}")
    catalog: List[Dict[str, str]] = []
    with zipfile.ZipFile(zip_path, "r") as zf:
        members = zf.namelist()
        it = (
            tqdm(members, desc="MD zip scan", unit="file")
            if tqdm is not None and len(members) >= 200
            else members
        )
        for idx, member in enumerate(it, start=1):
            if member.endswith("/"):
                continue
            base = os.path.basename(member)
            if not base:
                continue
            head = peek_zip_text(zf, member)
            title = extract_md_title(head)
            doi = extract_md_doi(head)
            catalog.append(
                {
                    "member": member,
                    "base": base,
                    "stem": Path(base).stem,
                    "base_key": clean_key(base),
                    "stem_key": clean_key(Path(base).stem),
                    "title": title,
                    "title_key": clean_key(title),
                    "doi": doi,
                    "doi_key": clean_key(doi),
                }
            )
            if tqdm is None and idx % 500 == 0:
                stage(f"MD zip 扫描中: {idx}/{len(members)}")
    stage(f"MD 目录完成: 条目数={len(catalog)}")
    return catalog


def get_md_candidates(raw_row: pd.Series, catalog: List[Dict[str, str]]) -> List[Tuple[str, str]]:
    """
    保守的 MD 候选列表（不直接接受 fuzzy 作为最终结果）：
    - 优先 DOI
    - 再文件名 basename / stem
    - 再比较保守的 title partial
    - 最后才追加 fuzzy 作为低优先级候选

    返回 [(member, method), ...]，去重并保持顺序。
    """
    raw_file = get_first_nonblank(raw_row, DOC_FILE_COLS)
    raw_doi = get_first_nonblank(raw_row, DOC_DOI_COLS)
    raw_title = get_first_nonblank(raw_row, DOC_TITLE_COLS)

    file_keys = [clean_key(raw_file), clean_key(Path(raw_file).name), clean_key(Path(raw_file).stem)] if raw_file else []
    doi_key = clean_key(raw_doi) if raw_doi else ""
    title_key = clean_key(raw_title) if raw_title else ""

    seen = set()
    out: List[Tuple[str, str]] = []

    def add(member: str, method: str) -> None:
        if member and member not in seen:
            seen.add(member)
            out.append((member, method))

    # 1) DOI exact / containment
    if doi_key:
        for item in catalog:
            if item.get("doi_key") and (
                doi_key == item["doi_key"] or doi_key in item["doi_key"] or item["doi_key"] in doi_key
            ):
                add(item["member"], "doi")

    # 2) file basename / stem
    for fk in file_keys:
        if not fk:
            continue
        for item in catalog:
            if fk == item.get("base_key") or fk == item.get("stem_key"):
                add(item["member"], "filename")

    # 3) title partial (conservative)
    if title_key:
        short = re.sub(r"[^a-z0-9]+", " ", title_key.lower()).strip()
        short = re.sub(r"\s+", " ", short)
        if len(short) >= 12:
            key40 = short[:40]
            for item in catalog:
                tk = item.get("title_key", "")
                if not tk:
                    continue
                if key40 and (key40 in tk or tk in key40):
                    add(item["member"], "title_partial")

    # 4) fuzzy low priority candidate
    keys = [x for x in file_keys + [title_key] if x]
    if keys:
        best_member = ""
        best_score = -1
        for item in catalog:
            if item["member"] in seen:
                continue
            score = 0
            for k in keys:
                if not k:
                    continue
                base_k = item.get("base_key", "")
                stem_k = item.get("stem_key", "")
                title_k = item.get("title_key", "")
                for cand in [base_k, stem_k, title_k]:
                    if cand and (k in cand or cand in k):
                        score = max(score, min(len(k), len(cand)))
            if score > best_score:
                best_score = score
                best_member = item["member"]
        if best_member and best_score >= 8:
            add(best_member, "fuzzy")

    return out


def locate_md_member(raw_row: pd.Series, catalog: List[Dict[str, str]]) -> Tuple[str, str]:
    """
    兼容旧接口：返回“首个候选”。注意：最终是否接受应以 token 命中验证为准。
    """
    cands = get_md_candidates(raw_row, catalog)
    if not cands:
        return "", "not_found"
    return cands[0][0], cands[0][1]


def token_pattern(token: str) -> Optional[re.Pattern]:
    t = normalize_text(token)
    if not t or len(t) < 2:
        return None
    return re.compile(rf"(?<![A-Za-z0-9]){re.escape(t)}(?![A-Za-z0-9])", re.I)


def generate_search_tokens(original_name: str, standardized_query: str = "") -> List[str]:
    """
    Generate multiple search variants to improve MD hit recall.
    """
    base = []
    for x in [original_name, standardized_query]:
        s = normalize_text(x)
        if s:
            base.append(s)

    out: List[str] = []
    seen = set()

    def add(s: str) -> None:
        ss = normalize_text(s)
        if not ss:
            return
        k = ss.casefold()
        if k not in seen:
            out.append(ss)
            seen.add(k)

    for s in base:
        add(s)
        if "-" in s:
            add(s.replace("-", ""))
            add(s.replace("-", " "))
        if "_" in s:
            add(s.replace("_", " "))
        if "/" in s:
            add(s.replace("/", " "))
        # strip numeric prefix like "10-NH2-MCM-41" -> "NH2-MCM-41"
        add(re.sub(r"^\s*\d+\s*[-_/]\s*", "", s))

        # split into major pieces
        for part in re.split(r"[-_/]+", s):
            part = normalize_text(part)
            if len(part) >= 2:
                add(part)

        # abbreviation replacements (both raw and split parts)
        for abbr, full in ABBR_REPLACEMENTS.items():
            if re.search(rf"(?<![A-Za-z0-9]){re.escape(abbr)}(?![A-Za-z0-9])", s, flags=re.I):
                add(re.sub(rf"(?<![A-Za-z0-9]){re.escape(abbr)}(?![A-Za-z0-9])", full, s, flags=re.I))
                add(full)
            for part in re.split(r"[-_/]+", s):
                if part.strip().upper() == abbr:
                    add(full)

    # prioritize longer/more specific tokens first when searching
    out = sorted(out, key=lambda x: (-len(x), x))
    return out


def sentence_windows(text: str, token: str, max_hits: int = 8) -> List[str]:
    pat = token_pattern(token)
    if pat is None:
        return []
    pieces = re.split(r"(?<=[\.\!\?。；;\n])", str(text or ""))
    hits: List[str] = []
    seen = set()
    for i, sent in enumerate(pieces):
        if pat.search(sent):
            merged = " ".join(x.strip() for x in pieces[max(0, i - 1): min(len(pieces), i + 2)] if x.strip())
            merged = normalize_text(merged)
            k = merged.lower()
            if k and k not in seen:
                hits.append(merged[:1200])
                seen.add(k)
            if len(hits) >= max_hits:
                break
    return hits


def local_windows(text: str, token: str, span: int = 220, max_hits: int = 8) -> List[str]:
    pat = token_pattern(token)
    if pat is None:
        return []
    s = str(text or "")
    hits: List[str] = []
    seen = set()
    for m in pat.finditer(s):
        a = max(0, m.start() - span)
        b = min(len(s), m.end() + span)
        w = normalize_text(s[a:b])
        k = w.casefold()
        if k and k not in seen:
            hits.append(w[:1200])
            seen.add(k)
        if len(hits) >= max_hits:
            break
    return hits


def collect_context_windows(md_text: str, original_name: str, standardized_query: str) -> List[str]:
    tokens = generate_search_tokens(original_name, standardized_query)

    windows: List[str] = []
    seen = set()

    def add_window(w: str) -> None:
        ww = normalize_text(w)
        if not ww:
            return
        k = ww.casefold()
        if k not in seen:
            windows.append(ww)
            seen.add(k)

    # Search multiple tokens; collect both sentence-level and local windows.
    for tok in tokens[:25]:
        for w in sentence_windows(md_text, tok, max_hits=6):
            add_window(w)
        for w in local_windows(md_text, tok, span=240, max_hits=6):
            add_window(w)
        if len(windows) >= 18:
            break

    # Limit total windows to avoid blow-up.
    return windows[:20]


def cleanup_candidate_name(cand: str) -> str:
    c = normalize_text(cand)
    c = re.sub(r"^(the|a|an)\s+", "", c, flags=re.I)
    c = re.sub(r"\b(and|or|with|for|of|as)\s*$", "", c, flags=re.I)
    c = c.strip(" ,;:-")
    return c


def contains_bad_context_words(text: str) -> bool:
    t = normalize_text(text).casefold()
    for w in BAD_CONTEXT_WORDS:
        if w in t:
            return True
    return False


def extract_candidates_from_context(token: str, context: str) -> List[Dict[str, Any]]:
    token = normalize_text(token)
    ctx = context or ""
    if not token or not ctx:
        return []

    results: List[Dict[str, Any]] = []
    seen = set()
    # limit candidate length to avoid swallowing whole sentences
    full_pat = r"([A-Za-z][A-Za-z0-9,\-/'\s]{3,120})"
    tok = re.escape(token)
    patterns = [
        (rf"{full_pat}\s*[\(（]\s*{tok}\s*[\)）]", 0.99, "full(abbr)"),
        (rf"{tok}\s*[\(（]\s*{full_pat}\s*[\)）]", 0.98, "abbr(full)"),
        (rf"{tok}\s*=\s*{full_pat}", 0.96, "abbr=full"),
        (rf"{tok}\s+(?:stands\s+for|refers\s+to|denotes|means|is|was|were)\s+{full_pat}", 0.92, "definition"),
        (rf"{full_pat}\s*,\s*(?:abbreviated\s+as|denoted\s+as|hereafter|herein)\s+{tok}", 0.86, "apposition"),
    ]
    for pat, score, rule in patterns:
        for m in re.finditer(pat, ctx, flags=re.I):
            cand = cleanup_candidate_name(m.group(1))
            if len(cand) < 4 or len(cand) > 120:
                continue
            if re.fullmatch(r"[A-Z0-9\-_/]+", cand):
                continue
            if contains_bad_context_words(cand):
                continue
            evidence = normalize_text(m.group(0))[:700]
            if contains_bad_context_words(evidence):
                continue
            k = cand.lower()
            if k in seen:
                continue
            seen.add(k)
            results.append(
                {
                    "candidate": cand,
                    "score": score,
                    "rule": rule,
                    "evidence": evidence,
                }
            )

    class_hint_pat = rf"(?<![A-Za-z0-9]){tok}(?![A-Za-z0-9]).{{0,120}}?(commercial|additive|modifier|polymer|copolymer|membrane|framework|nanoparticle|series|resin|material|support|composite|functionalized|modified)"
    for m in re.finditer(class_hint_pat, ctx, flags=re.I):
        results.append(
            {
                "candidate": "",
                "score": 0.35,
                "rule": "class_hint",
                "evidence": normalize_text(m.group(0))[:700],
                "class_hint": True,
            }
        )
    return sorted(results, key=lambda x: x.get("score", 0), reverse=True)


def call_context_resolver_llm(
    token: str,
    windows: List[str],
    raw_meta: Dict[str, str],
    base_mod,
    api_key: str,
    model: str,
    timeout: int = 120,
) -> Dict[str, Any]:
    if not api_key:
        return {}

    user_content = {
        "token": token,
        "raw_meta": raw_meta,
        "context_windows": windows[:6],
    }
    system_msg = (
        "You resolve chemistry abbreviations using document context from membrane/materials papers. "
        "Return strict JSON only with schema: "
        "{\"status\":\"SPECIFIC_CHEMICAL|MATERIAL_CLASS|NO_CLUE\","
        "\"query\":\"best English chemical full name for PubChem\","
        "\"composition\":[\"component1\",\"component2\"],"
        "\"confidence\":0.0,"
        "\"evidence\":\"short quoted/paraphrased evidence\","
        "\"note\":\"short note\"}. "
        "Rules: only give query if the context strongly supports a specific chemical entity; "
        "for commercial/material abbreviations use MATERIAL_CLASS and optionally composition; "
        "prefer PubChem-searchable English names."
    )
    payload = {
        "model": model,
        "temperature": 0,
        "response_format": {"type": "json_object"},
        "messages": [
            {"role": "system", "content": system_msg},
            {"role": "user", "content": json.dumps(user_content, ensure_ascii=False)},
        ],
    }
    req = base_mod.request.Request(
        base_mod.DEEPSEEK_URL,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        method="POST",
    )
    try:
        with base_mod.request.urlopen(req, timeout=timeout, context=base_mod.SSL_CTX) as resp:
            raw = resp.read().decode("utf-8")
        obj = json.loads(raw)
        content = str(obj.get("choices", [{}])[0].get("message", {}).get("content", "") or "")
        parsed = base_mod.parse_json_loose(content)
        if not isinstance(parsed, dict):
            return {}
        status = str(parsed.get("status", "")).upper()
        if status not in {"SPECIFIC_CHEMICAL", "MATERIAL_CLASS", "NO_CLUE"}:
            status = "NO_CLUE"
        query = normalize_text(parsed.get("query", ""))
        comp = parsed.get("composition", [])
        if not isinstance(comp, list):
            comp = []
        composition = [normalize_text(x) for x in comp if normalize_text(x)]
        return {
            "status": status,
            "query": query,
            "composition": composition[:5],
            "confidence": float(parsed.get("confidence", 0) or 0),
            "evidence": normalize_text(parsed.get("evidence", ""))[:700],
            "note": normalize_text(parsed.get("note", ""))[:300],
        }
    except Exception:
        return {}


def merge_notes(*parts: str) -> str:
    return "; ".join([normalize_text(p) for p in parts if normalize_text(p)])


def try_lookup_query(
    query: str,
    original_name: str,
    base_mod,
    pubchem_cache: Dict[str, Dict[str, Any]],
    rewrite_cache: Dict[str, List[str]],
    material_cache: Dict[str, List[str]],
    api_key: str,
    model: str,
    base_notes: str,
) -> Tuple[Dict[str, Any], str]:
    q0 = normalize_text(query)
    q_low = q0.casefold()
    for hint in MATERIAL_LIKE_HINTS:
        if hint in q_low:
            return (
                {
                    "Lookup_Status": "NO_SPECIFIC_CHEMICAL",
                    "CID": "",
                    "Molecular_Formula": "",
                    "IUPAC_Name": "",
                    "Canonical_SMILES": "",
                    "Standardized_Query": q0,
                    "Match_Source": "MD Backtrace (classified)",
                },
                merge_notes(base_notes, "material_like_skip_pubchem"),
            )
    chem = base_mod.pubchem_lookup(
        query,
        pubchem_cache,
        api_key=api_key,
        model=model,
        rewrite_cache=rewrite_cache,
        material_cache=material_cache,
    )
    notes = base_notes
    if hasattr(base_mod, "apply_no_hit_reclassification"):
        chem, notes = base_mod.apply_no_hit_reclassification(
            chem=chem,
            query=query,
            original_name=original_name,
            record_type="backtrace_abbr",
            api_key=api_key,
            model=model,
            pubchem_cache=pubchem_cache,
            rewrite_cache=rewrite_cache,
            material_cache=material_cache,
            base_notes=base_notes,
        )
    return chem, notes


def init_bt_result(row: pd.Series, source_record_index: Any) -> Dict[str, Any]:
    return {
        "EntityIndex": row.get("EntityIndex", ""),
        "RowIndex": row.get("RowIndex", ""),
        "Original_Name": normalize_text(row.get("Original_Name", "")),
        "Source_RecordIndex": normalize_text(source_record_index),
        "Matched_Source_RecordIndex": "",
        "Backtrace_Status": "NOT_PROCESSED",
        "Backtrace_Confidence": 0.0,
        "Resolved_Full_Name": "",
        "Resolved_Composition": "",
        "Evidence_Snippet": "",
        "MD_File": "",
        "MD_Locate_Method": "",
        "MD_Candidate_Tried": 0,
        "MD_Candidate_List": "",
        "MD_Accepted_Because": "",
        "Raw_FileName": "",
        "Raw_DOI": "",
        "Raw_Title": "",
        "Backtrace_Notes": "",
    }


def process_one_row(
    row: pd.Series,
    raw_df: pd.DataFrame,
    zip_path: str,
    md_catalog: List[Dict[str, str]],
    base_mod,
    pubchem_cache: Dict[str, Dict[str, Any]],
    rewrite_cache: Dict[str, List[str]],
    material_cache: Dict[str, List[str]],
    api_key: str,
    model: str,
) -> Dict[str, Any]:
    original_name = normalize_text(row.get("Original_Name", ""))
    standardized_query = normalize_text(row.get("Standardized_Query", ""))
    idxs = parse_source_record_indices(row.get("Source_RecordIndex", ""))

    best = init_bt_result(row, row.get("Source_RecordIndex", ""))
    if not idxs:
        best["Backtrace_Status"] = "MISSING_SOURCE_RECORDINDEX"
        best["Backtrace_Notes"] = "Source_RecordIndex is empty"
        return best

    # Try only first 2~3 candidate sources for speed; stop early on reliable definition.
    for source_idx in idxs[:3]:
        if source_idx < 1 or source_idx > len(raw_df):
            candidate = init_bt_result(row, str(source_idx))
            candidate["Backtrace_Status"] = "RAW_ROW_NOT_FOUND"
            candidate["Backtrace_Notes"] = f"Source_RecordIndex {source_idx} out of range"
            if best["Backtrace_Status"] == "NOT_PROCESSED":
                best = candidate
            continue

        raw_row = raw_df.iloc[source_idx - 1]
        raw_file = get_first_nonblank(raw_row, DOC_FILE_COLS)
        raw_doi = get_first_nonblank(raw_row, DOC_DOI_COLS)
        raw_title = get_first_nonblank(raw_row, DOC_TITLE_COLS)

        candidate = init_bt_result(row, str(source_idx))
        candidate["Matched_Source_RecordIndex"] = str(source_idx)
        candidate["Raw_FileName"] = raw_file
        candidate["Raw_DOI"] = raw_doi
        candidate["Raw_Title"] = raw_title
        md_candidates = get_md_candidates(raw_row, md_catalog)
        candidate["MD_Candidate_List"] = " | ".join([f"{m}::{how}" for (m, how) in md_candidates[:12]])
        candidate["MD_Candidate_Tried"] = 0

        accepted_member = ""
        accepted_method = ""
        accepted_tokens: List[str] = []
        windows: List[str] = []

        # 逐个候选验证：只有在该 MD 中 token/变体真正命中时才接受
        tried = 0
        for member, how in md_candidates:
            tried += 1
            md_text = read_md_member(zip_path, member)
            windows = collect_context_windows(md_text, original_name, standardized_query)
            if windows:
                accepted_member = member
                accepted_method = how
                # 记录“是哪些 token 变体触发了命中”（用于审计）
                accepted_tokens = generate_search_tokens(original_name, standardized_query)[:6]
                break

        candidate["MD_Candidate_Tried"] = tried

        if not accepted_member:
            if not md_candidates:
                candidate["Backtrace_Status"] = "MD_NOT_FOUND"
                candidate["Backtrace_Notes"] = "Unable to locate any MD candidate from raw metadata"
            else:
                candidate["Backtrace_Status"] = "TOKEN_NOT_IN_MD"
                candidate["Backtrace_Notes"] = "No token hit in any MD candidate"
            # 不要把“最像的 MD”强行写到 MD_File（避免误导）
            candidate["MD_File"] = ""
            candidate["MD_Locate_Method"] = ""
            if best["Backtrace_Status"] in {"NOT_PROCESSED", "MD_NOT_FOUND"}:
                best = candidate
            continue

        candidate["MD_File"] = accepted_member
        candidate["MD_Locate_Method"] = accepted_method
        candidate["MD_Accepted_Because"] = f"token_hit; method={accepted_method}"

        # Extract candidates using multiple search tokens to improve recall.
        regex_candidates: List[Dict[str, Any]] = []
        for tok in generate_search_tokens(original_name, standardized_query)[:20]:
            for w in windows:
                regex_candidates.extend(extract_candidates_from_context(tok, w))

        # Deduplicate keeping highest score.
        dedup: Dict[str, Dict[str, Any]] = {}
        class_only: Optional[Dict[str, Any]] = None
        for rc in regex_candidates:
            if rc.get("class_hint"):
                if class_only is None or rc.get("score", 0) > class_only.get("score", 0):
                    class_only = rc
                continue
            key = normalize_text(rc.get("candidate", "")).lower()
            if not key:
                continue
            if key not in dedup or rc.get("score", 0) > dedup[key].get("score", 0):
                dedup[key] = rc
        ranked = sorted(dedup.values(), key=lambda x: x.get("score", 0), reverse=True)

        for rc in ranked[:5]:
            query = normalize_text(rc.get("candidate", ""))
            notes0 = merge_notes(f"backtrace_rule={rc.get('rule', '')}", f"md_locate={accepted_method}")
            current = init_bt_result(row, str(source_idx))
            current["Matched_Source_RecordIndex"] = str(source_idx)
            current.update(
                {
                    "Raw_FileName": raw_file,
                    "Raw_DOI": raw_doi,
                    "Raw_Title": raw_title,
                    "MD_File": accepted_member,
                    "MD_Locate_Method": accepted_method,
                    "MD_Candidate_Tried": candidate.get("MD_Candidate_Tried", 0),
                    "MD_Candidate_List": candidate.get("MD_Candidate_List", ""),
                    "MD_Accepted_Because": candidate.get("MD_Accepted_Because", ""),
                    "Resolved_Full_Name": query,
                    "Backtrace_Confidence": rc.get("score", 0),
                    "Evidence_Snippet": rc.get("evidence", ""),
                    "Backtrace_Notes": merge_notes(notes0, f"matched_tokens={'|'.join(accepted_tokens[:4])}" if accepted_tokens else ""),
                }
            )

            # If context indicates a material/commercial series, classify and stop without forcing CID lookup.
            if rc.get("class_hint") or any(h in normalize_text(current.get("Evidence_Snippet", "")).casefold() for h in MATERIAL_LIKE_HINTS):
                current["Backtrace_Status"] = "BACKTRACE_CLASSIFIED"
                current["Backtrace_Notes"] = merge_notes(current.get("Backtrace_Notes", ""), "material_like_context")
                best = current
                return current

            chem, notes1 = try_lookup_query(
                query=query,
                original_name=original_name,
                base_mod=base_mod,
                pubchem_cache=pubchem_cache,
                rewrite_cache=rewrite_cache,
                material_cache=material_cache,
                api_key=api_key,
                model=model,
                base_notes=current.get("Backtrace_Notes", ""),
            )
            current["Backtrace_Notes"] = notes1
            current.update({f"BT_{k}": v for k, v in chem.items()})
            if chem.get("Lookup_Status") in {"OK", "ABBR_EXPANDED_OK", "CID_ONLY"}:
                current["Backtrace_Status"] = "BACKTRACE_OK"
                return current
            if chem.get("Lookup_Status") in {"Polymer", "NO_SPECIFIC_CHEMICAL"}:
                current["Backtrace_Status"] = "BACKTRACE_CLASSIFIED"
                best = current
                return current
            if best["Backtrace_Status"] in {"NOT_PROCESSED", "MD_NOT_FOUND", "TOKEN_NOT_IN_MD"}:
                current["Backtrace_Status"] = "EXPANSION_FOUND_BUT_PUBCHEM_NO_HIT"
                best = current

        # If regex failed, try LLM on context.
        raw_meta = {
            "file_name": raw_file,
            "doi": raw_doi,
            "title": raw_title,
        }
        llm_res = call_context_resolver_llm(
            token=original_name,
            windows=windows,
            raw_meta=raw_meta,
            base_mod=base_mod,
            api_key=api_key,
            model=model,
        )
        if llm_res:
            current = init_bt_result(row, str(source_idx))
            current["Matched_Source_RecordIndex"] = str(source_idx)
            current.update(
                {
                    "Raw_FileName": raw_file,
                    "Raw_DOI": raw_doi,
                    "Raw_Title": raw_title,
                    "MD_File": accepted_member,
                    "MD_Locate_Method": accepted_method,
                    "MD_Candidate_Tried": candidate.get("MD_Candidate_Tried", 0),
                    "MD_Candidate_List": candidate.get("MD_Candidate_List", ""),
                    "MD_Accepted_Because": candidate.get("MD_Accepted_Because", ""),
                    "Resolved_Full_Name": llm_res.get("query", ""),
                    "Resolved_Composition": " | ".join(llm_res.get("composition", [])),
                    "Backtrace_Confidence": llm_res.get("confidence", 0),
                    "Evidence_Snippet": llm_res.get("evidence", "") or (windows[0][:700] if windows else ""),
                    "Backtrace_Notes": merge_notes("llm_context_resolver", llm_res.get("note", ""), f"md_locate={accepted_method}"),
                }
            )
            if llm_res.get("status") == "SPECIFIC_CHEMICAL" and llm_res.get("query"):
                chem, notes2 = try_lookup_query(
                    query=llm_res["query"],
                    original_name=original_name,
                    base_mod=base_mod,
                    pubchem_cache=pubchem_cache,
                    rewrite_cache=rewrite_cache,
                    material_cache=material_cache,
                    api_key=api_key,
                    model=model,
                    base_notes=current["Backtrace_Notes"],
                )
                current["Backtrace_Notes"] = notes2
                current.update({f"BT_{k}": v for k, v in chem.items()})
                if chem.get("Lookup_Status") in {"OK", "ABBR_EXPANDED_OK", "CID_ONLY"}:
                    current["Backtrace_Status"] = "BACKTRACE_OK"
                    return current
                if chem.get("Lookup_Status") in {"Polymer", "NO_SPECIFIC_CHEMICAL"}:
                    current["Backtrace_Status"] = "BACKTRACE_CLASSIFIED"
                    best = current
                    return current
                else:
                    current["Backtrace_Status"] = "LLM_EXPANSION_BUT_PUBCHEM_NO_HIT"
                best = current
            elif llm_res.get("status") == "MATERIAL_CLASS":
                current["Backtrace_Status"] = "BACKTRACE_CLASS_HINT"
                best = current
                return current
            elif best["Backtrace_Status"] in {"NOT_PROCESSED", "MD_NOT_FOUND", "TOKEN_NOT_IN_MD"}:
                current["Backtrace_Status"] = "CONTEXT_FOUND_BUT_NO_EXPANSION"
                best = current
            continue

        if class_only and best["Backtrace_Status"] in {"NOT_PROCESSED", "MD_NOT_FOUND", "TOKEN_NOT_IN_MD"}:
            candidate["Backtrace_Status"] = "BACKTRACE_CLASS_HINT"
            candidate["Backtrace_Confidence"] = class_only.get("score", 0)
            candidate["Evidence_Snippet"] = class_only.get("evidence", "")
            candidate["Backtrace_Notes"] = "Context suggests a material/commercial series rather than a specific small molecule"
            best = candidate
        elif best["Backtrace_Status"] in {"NOT_PROCESSED", "MD_NOT_FOUND", "TOKEN_NOT_IN_MD"}:
            candidate["Backtrace_Status"] = "CONTEXT_FOUND_BUT_NO_EXPANSION"
            candidate["Evidence_Snippet"] = windows[0][:700]
            candidate["Backtrace_Notes"] = "Found context in MD but no reliable full-name pattern"
            best = candidate

    if best["Backtrace_Status"] == "NOT_PROCESSED":
        best["Backtrace_Status"] = "UNRESOLVED"
    return best


def merge_backtrace_to_main(entity_df: pd.DataFrame, bt_df: pd.DataFrame, base_mod) -> pd.DataFrame:
    left = entity_df.copy()
    right = bt_df.copy()

    # Normalize merge keys to avoid dtype mismatches (e.g., Source_RecordIndex str vs int64).
    for df in (left, right):
        if "EntityIndex" in df.columns:
            df["EntityIndex"] = pd.to_numeric(df["EntityIndex"], errors="coerce").astype("Int64")
        if "RowIndex" in df.columns:
            df["RowIndex"] = pd.to_numeric(df["RowIndex"], errors="coerce").astype("Int64")
        if "Original_Name" in df.columns:
            df["Original_Name"] = df["Original_Name"].astype(str).map(normalize_text)
        if "Source_RecordIndex" in df.columns:
            df["Source_RecordIndex"] = df["Source_RecordIndex"].astype(str).map(normalize_text)
        if "Matched_Source_RecordIndex" in df.columns:
            df["Matched_Source_RecordIndex"] = df["Matched_Source_RecordIndex"].astype(str).map(normalize_text)

    if "EntityIndex" in left.columns and "EntityIndex" in right.columns:
        merged = left.merge(right, on=["EntityIndex"], how="left", suffixes=("", "_BT"))
    else:
        merged = left.merge(right, on=["RowIndex", "Original_Name", "Source_RecordIndex"], how="left")

    for c in BACKTRACE_EXTRA_COLUMNS:
        if c not in merged.columns:
            merged[c] = ""

    overwrite_map = {
        "Molecular_Formula": "BT_Molecular_Formula",
        "IUPAC_Name": "BT_IUPAC_Name",
        "Standardized_Query": "BT_Standardized_Query",
        "Canonical_SMILES": "BT_Canonical_SMILES",
        "CID": "BT_CID",
        "Match_Source": "BT_Match_Source",
    }
    # Ensure key output columns exist
    for dest in list(overwrite_map.keys()) + ["Lookup_Status", "Backtrace_Status", "Backtrace_Notes", "Resolved_Full_Name", "Resolved_Composition"]:
        if dest not in merged.columns:
            merged[dest] = ""

    if "Backtrace_Status" in merged.columns:
        hit_mask = merged["Backtrace_Status"].astype(str).eq("BACKTRACE_OK")
    else:
        hit_mask = pd.Series(False, index=merged.index)

    if hit_mask.any():
        for dest, src in overwrite_map.items():
            if src in merged.columns and dest in merged.columns:
                merged[dest] = merged[dest].astype("object")
                merged.loc[hit_mask, dest] = merged.loc[hit_mask, src].astype("object")

    # 问题 2：只要 Resolved_Full_Name 可信 或 BT_Standardized_Query 非空，就覆盖主表 Standardized_Query
    def looks_trustworthy_name(x: Any) -> bool:
        s = normalize_text(x)
        if not s:
            return False
        if len(s) < 4 or len(s) > 140:
            return False
        if contains_bad_context_words(s):
            return False
        # 避免把明显过程句/长句写入
        if len(s.split()) > 14:
            return False
        return True

    if "Standardized_Query" in merged.columns:
        bt_q = merged.get("BT_Standardized_Query", pd.Series("", index=merged.index)).fillna("").astype(str).map(normalize_text)
        res_full = merged.get("Resolved_Full_Name", pd.Series("", index=merged.index)).fillna("").astype(str).map(normalize_text)
        q_mask = bt_q.astype(str).str.len().gt(0)
        res_mask = res_full.map(looks_trustworthy_name)
        update_q_mask = q_mask | res_mask
        if update_q_mask.any():
            merged["Standardized_Query"] = merged["Standardized_Query"].astype("object")
            merged.loc[q_mask, "Standardized_Query"] = bt_q.loc[q_mask].astype("object")
            fallback = (~q_mask) & res_mask
            if fallback.any():
                merged.loc[fallback, "Standardized_Query"] = res_full.loc[fallback].astype("object")

    if "Lookup_Status" in merged.columns and hit_mask.any():
        merged["Lookup_Status"] = merged["Lookup_Status"].astype("object")
        merged.loc[hit_mask, "Lookup_Status"] = "BACKTRACE_OK"

        if "BT_Lookup_Status" in merged.columns:
            classified_mask = (
                merged["Backtrace_Status"].astype(str).eq("BACKTRACE_CLASSIFIED")
                & merged["BT_Lookup_Status"].notna()
            )
            if classified_mask.any():
                merged.loc[classified_mask, "Lookup_Status"] = (
                    merged.loc[classified_mask, "BT_Lookup_Status"].astype("object")
                )

    if "Match_Source" in merged.columns and hit_mask.any():
        merged["Match_Source"] = merged["Match_Source"].astype("object")
        merged.loc[hit_mask, "Match_Source"] = "MD Backtrace + PubChem"

    if "Decision_Notes" in merged.columns and hit_mask.any():
        merged["Decision_Notes"] = merged["Decision_Notes"].fillna("").astype("object")
        hit_notes = merged.loc[hit_mask, "Resolved_Full_Name"].fillna("").astype(str)
        merged.loc[hit_mask, "Decision_Notes"] = (
            merged.loc[hit_mask, "Decision_Notes"].astype(str).str.rstrip("; ")
            + "; backtrace_resolved="
            + hit_notes
        )

    return merged


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Backtrace abbreviations via Source_RecordIndex -> raw DB -> MD zip -> PubChem")
    p.add_argument("--input", default=INPUT_PATH, help="Input Excel/CSV with Source_RecordIndex")
    p.add_argument("--input-sheet", default=INPUT_SHEET, help="Sheet name for input workbook")
    p.add_argument("--raw-db", default=RAW_DB_PATH, help="Raw database Excel/CSV")
    p.add_argument("--raw-sheet", default=RAW_SHEET, help="Sheet name for raw DB workbook")
    p.add_argument("--md-zip", default=MD_ZIP_PATH, help="Zip containing MD files")
    p.add_argument("--base-script", default=BASE_SCRIPT_PATH, help="Path to code_extract_name_fixed.py")
    p.add_argument("--output", default=OUTPUT_XLSX_PATH, help="Output Excel path")
    p.add_argument("--report", default=REPORT_JSON_PATH, help="Output JSON report path")
    p.add_argument(
        "--resume-bt",
        default=RESUME_BT_PATH,
        help="Resume from a saved bt_df Excel/CSV (skips backtrace/query loop). If xlsx, prefers sheets: bt_df or backtrace_hits/backtrace_unresolved.",
    )
    p.add_argument("--api-key", default=DEEPSEEK_API_KEY)
    p.add_argument("--model", default=DEEPSEEK_MODEL)
    p.add_argument(
        "--only-unresolved",
        action="store_true",
        default=ONLY_UNRESOLVED,
        help="Only process unresolved Lookup_Status rows",
    )
    args = p.parse_args()
    if args.resume_bt:
        required = ["input", "output", "report", "base_script"]
    else:
        required = ["input", "raw_db", "md_zip", "output", "report", "base_script"]
    missing = [k for k in required if not getattr(args, k)]
    if missing:
        p.error(
            "When file config is empty, the following arguments are required: "
            + ", ".join(f"--{m.replace('_', '-')}" for m in missing)
        )
    return args

import re

def looks_like_abbreviation(name: str) -> bool:
    if not name:
        return False
    s = str(name).strip()
    if not s:
        return False

    # 很短的全大写/大写数字组合
    if re.fullmatch(r"[A-Z]{2,10}", s):
        return True

    # 带连字符的缩写/系列名，如 Isol-C, ABSA-TEA, X-1
    if re.fullmatch(r"[A-Za-z0-9]+(?:-[A-Za-z0-9]+){1,4}", s):
        return True

    # 括号中给出的缩写本体
    if re.fullmatch(r"[A-Z]{1,6}[0-9]{0,3}", s):
        return True

    # 含明显大写缩写片段，例如 GPOSS, MPD, TMC
    if re.fullmatch(r"[A-Z0-9]{3,12}", s):
        return True

    # 像 3-BPA / 2,4-DBSA-TEA 这种
    if re.fullmatch(r"[0-9,]+-[A-Za-z0-9]+(?:-[A-Za-z0-9]+)*", s):
        return True

    return False

def main() -> None:
    args = parse_args()
    stage("阶段: 选择工作表")
    input_sheet = choose_entity_sheet(args.input, args.input_sheet)

    stage("阶段: 读取输入实体表 & 原始数据库")
    entity_df = read_table(args.input, input_sheet)
    stage("阶段: 加载 base-script")
    base_mod = load_base_module(args.base_script)

    entity_df = entity_df.copy()
    entity_df.columns = [str(c).strip() for c in entity_df.columns]

    name_col = detect_name_column(entity_df)
    src_idx_col = detect_source_index_column(entity_df)
    if name_col != "Original_Name":
        entity_df = entity_df.rename(columns={name_col: "Original_Name"})
    if src_idx_col != "Source_RecordIndex":
        entity_df = entity_df.rename(columns={src_idx_col: "Source_RecordIndex"})
    if "EntityIndex" not in entity_df.columns:
        entity_df.insert(0, "EntityIndex", range(1, len(entity_df) + 1))
    if "RowIndex" not in entity_df.columns:
        entity_df.insert(0, "RowIndex", range(1, len(entity_df) + 1))

    if args.only_unresolved and "Lookup_Status" in entity_df.columns:
        work_df = entity_df[entity_df["Lookup_Status"].astype(str).isin(DEFAULT_UNRESOLVED)].copy()
    else:
        work_df = entity_df.copy()

    work_df = work_df[work_df["Original_Name"].astype(str).apply(looks_like_abbreviation)].copy()

    pubchem_cache: Dict[str, Dict[str, Any]] = {}
    rewrite_cache: Dict[str, List[str]] = {}
    material_cache: Dict[str, List[str]] = {}

    if args.resume_bt:
        stage(f"阶段: 断点续跑，读取已保存 bt_df: {args.resume_bt}")
        bt_df = load_bt_df(args.resume_bt)
        bt_df = bt_df.copy()
        bt_df.columns = [str(c).strip() for c in bt_df.columns]
    else:
        raw_sheet = choose_entity_sheet(args.raw_db, args.raw_sheet)
        raw_df = ensure_raw_rowindex(read_table(args.raw_db, raw_sheet))
        raw_df.columns = [str(c).strip() for c in raw_df.columns]

        stage("阶段: 构建 MD 目录")
        md_catalog = build_md_catalog(args.md_zip)

        bt_rows: List[Dict[str, Any]] = []
        total = len(work_df)
        stage(f"阶段: 逐行回溯/查询（共 {total} 行）")
        row_iter = work_df.iterrows()
        if tqdm is not None:
            row_iter = tqdm(row_iter, total=total, desc="Backtrace", unit="row")
        for i, (_, row) in enumerate(row_iter, start=1):
            bt = process_one_row(
                row=row,
                raw_df=raw_df,
                zip_path=args.md_zip,
                md_catalog=md_catalog,
                base_mod=base_mod,
                pubchem_cache=pubchem_cache,
                rewrite_cache=rewrite_cache,
                material_cache=material_cache,
                api_key=args.api_key,
                model=args.model,
            )
            bt_rows.append(bt)
            if tqdm is None and (i % 20 == 0 or i == total):
                stage(f"Processed {i}/{total}")

        bt_df = pd.DataFrame(bt_rows)
    stage("阶段: 合并回主表")
    # 先落一个中间结果，防止 merge 阶段报错全丢
    tmp_bt_path = Path(args.output).with_name(Path(args.output).stem + "_bt_only.xlsx")
    tmp_bt_path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(tmp_bt_path, engine="openpyxl") as writer:
        bt_df.to_excel(writer, sheet_name="bt_all", index=False)

    print(f"保存中间文件bt_df: {tmp_bt_path}")

    merged = merge_backtrace_to_main(entity_df, bt_df, base_mod)

    # Slim main table: keep only key columns. Detailed fields go to separate sheets.
    final_cols = [
        "EntityIndex",
        "Original_Name",
        "Source_RecordIndex",
        "Matched_Source_RecordIndex",
        "Resolved_Full_Name",
        "Resolved_Composition",
        "Standardized_Query",
        "Lookup_Status",
        "CID",
        "Molecular_Formula",
        "IUPAC_Name",
        "Match_Source",
        "Backtrace_Status",
        "Backtrace_Notes",
    ]
    for c in final_cols:
        if c not in merged.columns:
            merged[c] = ""
    merged_final = merged[final_cols].copy()

    md_catalog_size = 0 if args.resume_bt else int(len(md_catalog))
    summary = {
        "input_rows": int(len(entity_df)),
        "processed_rows": int(len(work_df)),
        "md_catalog_size": md_catalog_size,
        "resume_bt": str(args.resume_bt or ""),
        "backtrace_status_counts": bt_df["Backtrace_Status"].value_counts(dropna=False).to_dict() if len(bt_df) else {},
        "resolved_hits": int((bt_df["Backtrace_Status"] == "BACKTRACE_OK").sum()) if len(bt_df) else 0,
        "classified_hits": int((bt_df["Backtrace_Status"] == "BACKTRACE_CLASSIFIED").sum()) if len(bt_df) else 0,
    }

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    report_path = Path(args.report)
    report_path.parent.mkdir(parents=True, exist_ok=True)

    stage("阶段: 写出 Excel/JSON")
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        merged_final.to_excel(writer, sheet_name="final_merged", index=False)
        # Full backtrace details (including BT_* columns) in a separate sheet.
        bt_df.to_excel(writer, sheet_name="bt_detail", index=False)
        bt_df[bt_df["Backtrace_Status"] == "BACKTRACE_OK"].to_excel(writer, sheet_name="backtrace_hits", index=False)
        bt_df[bt_df["Backtrace_Status"] != "BACKTRACE_OK"].to_excel(writer, sheet_name="backtrace_unresolved", index=False)
        evidence_cols = [
            c
            for c in [
                "RowIndex",
                "Original_Name",
                "Source_RecordIndex",
                "Matched_Source_RecordIndex",
                "Resolved_Full_Name",
                "Resolved_Composition",
                "Backtrace_Status",
                "Backtrace_Confidence",
                "Evidence_Snippet",
                "MD_File",
                "MD_Locate_Method",
                "MD_Candidate_Tried",
                "MD_Candidate_List",
                "MD_Accepted_Because",
                "Raw_FileName",
                "Raw_DOI",
                "Raw_Title",
                "Backtrace_Notes",
            ]
            if c in bt_df.columns
        ]
        bt_df[evidence_cols].to_excel(writer, sheet_name="evidence", index=False)
        dict_df = bt_df[bt_df["Resolved_Full_Name"].astype(str).str.len() > 0][[
            "Original_Name",
            "Resolved_Full_Name",
            "Resolved_Composition",
            "Backtrace_Status",
            "Backtrace_Confidence",
            "Evidence_Snippet",
        ]].drop_duplicates()
        dict_df.to_excel(writer, sheet_name="abbr_dict_candidates", index=False)

    report_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    stage("阶段: 完成")
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    stage(f"Saved output: {output_path}")
    stage(f"Saved report: {report_path}")


if __name__ == "__main__":
    main()
