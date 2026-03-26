import argparse
import importlib.util
import json
import math
import os
import re
import zipfile
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

try:
    from tqdm import tqdm
except Exception:
    tqdm = None


# =========================
# 配置区（保留在前面，便于直接改）
# =========================

# 路径类默认配置（可直接在这里改；命令行参数会覆盖）
DEFAULT_INPUT_PATH = "./test12/test.xlsx"  # 输入 Excel/CSV（含 Source_RecordIndex）
DEFAULT_INPUT_SHEET = "Sheet1"  # 输入为 xlsx 时的 sheet 名；空字符串表示自动选择

DEFAULT_RAW_DB_PATH = "./test12/membrane_aligned_data_all.csv"  # 原始数据库 Excel/CSV
DEFAULT_RAW_SHEET = ""  # 原始库为 xlsx 时的 sheet 名；空字符串表示自动选择

DEFAULT_MD_ZIP_PATH = "./test12/RO_md_files.zip"  # 包含 MD 文件的 zip
DEFAULT_BASE_SCRIPT = r"D:\graduate_work\RO_database\Part1_chemExtract\step1\test10_no_part_polymer_deepseek\code_extract_name_fixed.py"  # base 抽取脚本路径

DEFAULT_OUTPUT_PATH = "./test12/test_index_backtrace_pubchem.xlsx"  # 输出 Excel 路径
DEFAULT_REPORT_PATH = "./test12/test_index_backtrace_pubchem_report.json"  # 输出 JSON 报告路径（可选，不填则不生成）

DEFAULT_BT_INPUT_PATH = ""  # 仅合并模式：已有 bt_only.xlsx 的路径
DEFAULT_BT_SHEET = "bt_all"  # bt_only.xlsx 中的 sheet 名
DEFAULT_SAVE_BT_ONLY_PATH = ""  # 新鲜跑时，bt_only 的保存路径（留空则自动生成 *_bt_only.xlsx）

# 其他默认行为配置
# 其他默认行为配置
DEFAULT_MODEL = "deepseek-chat"
DEFAULT_API_KEY = os.getenv("DEEPSEEK_API_KEY", "")

# 与老脚本保持一致：默认不过滤 unresolved，由命令行决定
DEFAULT_ONLY_UNRESOLVED = False  # 仅处理未解析行（Lookup_Status 属于 DEFAULT_UNRESOLVED）
DEFAULT_PROCESS_ALL_UNRESOLVED = False
DEFAULT_ENABLE_LLM = False


DEFAULT_UNRESOLVED = {
    "NO_HIT",
    "NO_SPECIFIC_CHEMICAL",
    "ABBR_EXPANDED_NO_HIT",
    "NOT_CHEMICAL_FROM_LLM",
    "CID_ONLY",
    "EMPTY_QUERY",
    "EMPTY_CANDIDATE",
}

DOC_FILE_COLS = ["文件名称", "file_name", "filename", "md_file", "MD_File"]
DOC_DOI_COLS = ["DOI", "doi"]
DOC_TITLE_COLS = ["论文题目", "标题", "题目", "title", "Title", "paper_title"]

FINAL_MAIN_EXTRA_COLS = [
    "Matched_Source_RecordIndex",
    "Backtrace_Status",
    "Backtrace_Confidence",
    "Resolved_Full_Name",
    "Resolved_Composition",
    "Lookup_Status",
    "Match_Source",
    "Molecular_Formula",
    "IUPAC_Name",
    "Standardized_Query",
    "Canonical_SMILES",
    "CID",
    "Backtrace_Notes",
]

EVIDENCE_COLS = [
    "EntityIndex",
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
    "Raw_FileName",
    "Raw_DOI",
    "Raw_Title",
    "Backtrace_Notes",
]

MATERIAL_KEYWORDS = [
    "nanoparticle", "nanosheet", "nanosphere", "nanotube", "nanofiller",
    "membrane", "framework", "graphene oxide", "metal-organic framework", "metal organic framework",
    "mcm-41", "mesoporous", "silica", "mof", "go", "msn", "poly(", "polymer", "copolymer",
    "modified", "functionalized", "grafted", "composite", "resin", "material", "support", "sheet",
]

ABBR_SEARCH_MAP = {
    "NH2": "amino",
    "COOH": "carboxyl",
    "SO3H": "sulfonic",
    "GO": "graphene oxide",
    "MOF": "metal organic framework",
    "MSN": "mesoporous silica nanoparticle",
}


def stage(msg: str) -> None:
    print(msg, flush=True)


def resolve_existing_path(path: str) -> str:
    p = Path(path)
    candidates = [
        p,
        Path.cwd() / path,
        Path(__file__).resolve().parent / path,
        Path(__file__).resolve().parent.parent / path,
    ]
    for c in candidates:
        if c.exists():
            return str(c.resolve())
    return str(p)


def load_base_module(path: str):
    real_path = resolve_existing_path(path)
    spec = importlib.util.spec_from_file_location("base_extract", real_path)
    if spec is None or spec.loader is None:
        raise FileNotFoundError(f"Unable to load base script: {real_path}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def normalize_text(text: Any) -> str:
    t = str(text or "").strip()
    t = t.replace("（", "(").replace("）", ")")
    t = t.replace("，", ",").replace("；", ";")
    t = t.replace("：", ":").replace("｜", "|")
    t = re.sub(r"\s+", " ", t)
    return t


def clean_key(text: Any) -> str:
    t = normalize_text(text).lower()
    t = re.sub(r"https?://(dx\.)?doi\.org/", "", t)
    t = re.sub(r"\.md$", "", t)
    t = re.sub(r"\.[a-z0-9]{1,6}$", "", t)
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
    p = Path(path)
    if p.suffix.lower() in {".xlsx", ".xls"}:
        return pd.read_excel(path, sheet_name=sheet_name) if sheet_name else pd.read_excel(path)
    for enc in ["utf-8", "utf-8-sig", "gb18030", "gbk", "gb2312", "latin1"]:
        try:
            return pd.read_csv(path, encoding=enc, low_memory=False)
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


def choose_sheet(path: str, requested: str = "") -> Optional[str]:
    if not path.lower().endswith((".xlsx", ".xls")):
        return None
    return requested or pick_first_nonempty_sheet(path)


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


def prepare_entity_df(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = [str(c).strip() for c in out.columns]
    name_col = detect_name_column(out)
    src_col = detect_source_index_column(out)
    if name_col != "Original_Name":
        out = out.rename(columns={name_col: "Original_Name"})
    if src_col != "Source_RecordIndex":
        out = out.rename(columns={src_col: "Source_RecordIndex"})
    if "EntityIndex" not in out.columns:
        out.insert(0, "EntityIndex", range(1, len(out) + 1))
    if "RowIndex" not in out.columns:
        out.insert(1, "RowIndex", range(1, len(out) + 1))
    return out


def ensure_raw_rowindex(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = [str(c).strip() for c in out.columns]
    if "Source_RecordIndex" not in out.columns:
        out.insert(0, "Source_RecordIndex", range(1, len(out) + 1))
    return out


def parse_source_record_indices(value: Any) -> List[int]:
    if is_blank(value):
        return []
    nums = re.findall(r"\d+", normalize_text(value))
    seen = set()
    out: List[int] = []
    for n in nums:
        i = int(n)
        if i not in seen:
            seen.add(i)
            out.append(i)
    return out


def choose_single_source_record_index(value: Any) -> Optional[int]:
    idxs = parse_source_record_indices(value)
    return idxs[0] if idxs else None


def get_first_nonblank(row: pd.Series, candidates: List[str]) -> str:
    for c in candidates:
        if c in row.index and not is_blank(row[c]):
            return normalize_text(row[c])
    return ""


def looks_like_abbreviation(name: Any) -> bool:
    s = normalize_text(name)
    if not s:
        return False
    if len(s) <= 2:
        return False
    if re.fullmatch(r"[A-Z]{2,10}", s):
        return True
    if re.fullmatch(r"[A-Z0-9]{3,16}", s):
        return True
    if re.fullmatch(r"[A-Za-z0-9]+(?:-[A-Za-z0-9]+){1,5}", s):
        return True
    if re.fullmatch(r"[0-9,]+-[A-Za-z0-9]+(?:-[A-Za-z0-9]+)*", s):
        return True
    if "(" in s and ")" in s and len(re.sub(r"[^A-Z]", "", s)) >= 2:
        return True
    return False


def build_md_catalog(zip_path: str) -> Dict[str, Any]:
    items: List[Dict[str, str]] = []
    doi_index: Dict[str, str] = {}
    file_index: Dict[str, str] = {}
    title_index: Dict[str, str] = {}
    with zipfile.ZipFile(zip_path, "r") as zf:
        members = [m for m in zf.namelist() if not m.endswith("/")]
        iterator = tqdm(members, desc="MD zip scan", unit="file") if tqdm is not None else members
        for member in iterator:
            base = os.path.basename(member)
            if not base:
                continue
            with zf.open(member, "r") as f:
                data = f.read(24000)
            head = ""
            for enc in ["utf-8", "utf-8-sig", "gb18030", "gbk", "latin1"]:
                try:
                    head = data.decode(enc)
                    break
                except Exception:
                    continue
            if not head:
                head = data.decode("utf-8", errors="ignore")
            title = ""
            for line in head.splitlines()[:40]:
                s = normalize_text(line).lstrip("# ").strip()
                if len(s) >= 12:
                    title = s
                    break
            m = re.search(r"\b10\.\d{4,9}/[-._;()/:A-Z0-9]+", head, flags=re.I)
            doi = normalize_text(m.group(0)) if m else ""
            item = {
                "member": member,
                "base_key": clean_key(base),
                "stem_key": clean_key(Path(base).stem),
                "title_key": clean_key(title),
                "doi_key": clean_key(doi),
            }
            items.append(item)
            if item["doi_key"]:
                doi_index.setdefault(item["doi_key"], member)
            for k in [item["base_key"], item["stem_key"]]:
                if k:
                    file_index.setdefault(k, member)
            if item["title_key"]:
                title_index.setdefault(item["title_key"], member)
    return {"items": items, "doi": doi_index, "file": file_index, "title": title_index}


def locate_md_member(raw_row: pd.Series, catalog: Dict[str, Any]) -> Tuple[str, str]:
    raw_file = get_first_nonblank(raw_row, DOC_FILE_COLS)
    raw_doi = get_first_nonblank(raw_row, DOC_DOI_COLS)
    raw_title = get_first_nonblank(raw_row, DOC_TITLE_COLS)

    file_keys = []
    if raw_file:
        file_keys.extend([clean_key(raw_file), clean_key(Path(raw_file).name), clean_key(Path(raw_file).stem)])
    doi_key = clean_key(raw_doi) if raw_doi else ""
    title_key = clean_key(raw_title) if raw_title else ""

    if doi_key:
        for k, member in catalog["doi"].items():
            if doi_key == k or doi_key in k or k in doi_key:
                return member, "doi"

    for fk in file_keys:
        if fk and fk in catalog["file"]:
            return catalog["file"][fk], "filename"

    if title_key:
        for k, member in catalog["title"].items():
            if title_key == k or title_key in k or k in title_key:
                return member, "title"

    best_member = ""
    best_score = -1
    keys = [x for x in file_keys + [title_key] if x]
    for item in catalog["items"]:
        score = 0
        for k in keys:
            for candidate in [item["base_key"], item["stem_key"], item["title_key"]]:
                if candidate and (k in candidate or candidate in k):
                    score = max(score, min(len(k), len(candidate)))
        if score > best_score:
            best_score = score
            best_member = item["member"]
    if best_member and best_score >= 8:
        return best_member, "fuzzy"
    return "", "not_found"


def read_md_member_cached(zip_path: str, member: str, cache: Dict[str, str]) -> str:
    if member in cache:
        return cache[member]
    with zipfile.ZipFile(zip_path, "r") as zf:
        data = zf.read(member)
    text = ""
    for enc in ["utf-8", "utf-8-sig", "gb18030", "gbk", "latin1"]:
        try:
            text = data.decode(enc)
            break
        except Exception:
            continue
    if not text:
        text = data.decode("utf-8", errors="ignore")
    cache[member] = text
    return text


def token_pattern(token: str) -> Optional[re.Pattern]:
    t = normalize_text(token)
    if not t or len(t) < 2:
        return None
    return re.compile(rf"(?<![A-Za-z0-9]){re.escape(t)}(?![A-Za-z0-9])", re.I)


def generate_search_tokens(original_name: str, standardized_query: str) -> List[str]:
    raw = [normalize_text(original_name), normalize_text(standardized_query)]
    out: List[str] = []
    seen = set()

    def add(x: str):
        x = normalize_text(x)
        if not x:
            return
        k = x.lower()
        if k not in seen:
            seen.add(k)
            out.append(x)

    for t in raw:
        add(t)
        m = re.match(r"^\d+(?:,\d+)*-(.+)$", t)
        if m:
            add(m.group(1))
        if "-" in t:
            parts = [p for p in t.split("-") if p]
            if len(parts) >= 2:
                add("-".join(parts[1:]))
            for p in parts:
                if len(p) >= 3 and not p.isdigit():
                    add(p)
        compact = re.sub(r"[^A-Za-z0-9]+", "", t).upper()
        for src, dst in ABBR_SEARCH_MAP.items():
            if src in compact:
                add(dst)
                if "MCM41" in compact:
                    add(f"{dst}-functionalized MCM-41")
        if compact == "AGO":
            add("aminated graphene oxide")
            add("amine-functionalized graphene oxide")
        if compact == "GO":
            add("graphene oxide")
    return out[:12]


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
                seen.add(k)
                hits.append(merged[:1400])
            if len(hits) >= max_hits:
                break
    return hits


def collect_context_windows(md_text: str, tokens: List[str]) -> List[str]:
    windows: List[str] = []
    seen = set()
    for tok in tokens:
        for w in sentence_windows(md_text, tok):
            k = w.lower()
            if k not in seen:
                seen.add(k)
                windows.append(w)
    return windows


def cleanup_candidate_name(cand: str) -> str:
    c = normalize_text(cand)
    c = re.sub(r"^(the|a|an|this|these|those)\s+", "", c, flags=re.I)
    c = re.sub(r"^(in this paper|in this study|here|namely|such as|including)\s*[,:]?\s*", "", c, flags=re.I)
    c = re.sub(r"\b(and|or|with|for|of|as)\s*$", "", c, flags=re.I)
    c = c.strip(" ,;:-")
    # keep the tail after cue phrases
    c = re.sub(r"^.*?\b(namely|is|was|were|denoted as|abbreviated as)\b\s*", "", c, flags=re.I)
    return c.strip(" ,;:-")


def extract_candidates_from_context(token: str, context: str) -> List[Dict[str, Any]]:
    token = normalize_text(token)
    ctx = context or ""
    if not token or not ctx:
        return []

    results: List[Dict[str, Any]] = []
    seen = set()
    full_pat = r"([A-Za-z][A-Za-z0-9,\-/'\s]{3,140})"
    patterns = [
        (rf"{full_pat}\s*[\(（]\s*{re.escape(token)}\s*[\)）]", 0.97, "full(abbr)"),
        (rf"{re.escape(token)}\s*[\(（]\s*{full_pat}\s*[\)）]", 0.95, "abbr(full)"),
        (rf"{re.escape(token)}\s+(?:is|was|were|refers to|denotes|stands for|means)\s+{full_pat}", 0.88, "definition"),
        (rf"{full_pat}\s*,\s*(?:abbreviated as|denoted as|hereafter|herein)\s+{re.escape(token)}", 0.84, "apposition"),
    ]
    for pat, score, rule in patterns:
        for m in re.finditer(pat, ctx, flags=re.I):
            cand = cleanup_candidate_name(m.group(1))
            if len(cand) < 4 or len(cand) > 140:
                continue
            if re.fullmatch(r"[A-Z0-9\-_/]+", cand):
                continue
            # keep shorter, chemistry-like tail if comma present
            if "," in cand:
                tail = cand.split(",")[-1].strip()
                if 4 <= len(tail) <= len(cand):
                    cand = tail
            k = cand.lower()
            if k in seen:
                continue
            seen.add(k)
            results.append({
                "candidate": cand,
                "score": score,
                "rule": rule,
                "evidence": normalize_text(m.group(0))[:700],
            })

    class_hint_pat = rf"(?<![A-Za-z0-9]){re.escape(token)}(?![A-Za-z0-9]).{{0,120}}?(commercial|additive|modifier|polymer|copolymer|membrane|framework|nanoparticle|series|resin|material|nanosheet|mesoporous)"
    for m in re.finditer(class_hint_pat, ctx, flags=re.I):
        results.append({
            "candidate": "",
            "score": 0.35,
            "rule": "class_hint",
            "evidence": normalize_text(m.group(0))[:700],
            "class_hint": True,
        })
    return sorted(results, key=lambda x: x.get("score", 0), reverse=True)


def merge_notes(*parts: str) -> str:
    return "; ".join([normalize_text(p) for p in parts if normalize_text(p)])


def query_looks_material(query: str) -> bool:
    q = normalize_text(query).lower()
    if not q:
        return False
    return any(k in q for k in MATERIAL_KEYWORDS)


def call_context_resolver_llm(
    token: str,
    windows: List[str],
    raw_meta: Dict[str, str],
    base_mod,
    api_key: str,
    model: str,
    timeout: int = 90,
) -> Dict[str, Any]:
    if not api_key:
        return {}

    user_content = {"token": token, "raw_meta": raw_meta, "context_windows": windows[:6]}
    system_msg = (
        "You resolve chemistry abbreviations from membrane/materials-paper context. "
        "Return strict JSON only with schema: "
        "{\"status\":\"SPECIFIC_CHEMICAL|MATERIAL_CLASS|NO_CLUE\","
        "\"query\":\"best English chemical full name for PubChem\","
        "\"composition\":[\"component1\",\"component2\"],"
        "\"confidence\":0.0,"
        "\"evidence\":\"short evidence\","
        "\"note\":\"short note\"}. "
        "Only give query for a specific small molecule or specific compound name. "
        "For functionalized materials / nanoparticles / frameworks / polymers use MATERIAL_CLASS."
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
        comp = parsed.get("composition", [])
        if not isinstance(comp, list):
            comp = []
        return {
            "status": status,
            "query": normalize_text(parsed.get("query", "")),
            "composition": [normalize_text(x) for x in comp if normalize_text(x)][:5],
            "confidence": float(parsed.get("confidence", 0) or 0),
            "evidence": normalize_text(parsed.get("evidence", ""))[:700],
            "note": normalize_text(parsed.get("note", ""))[:300],
        }
    except Exception:
        return {}


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


def init_bt_result(row: pd.Series, matched_source_record_index: Any = "") -> Dict[str, Any]:
    return {
        "EntityIndex": row.get("EntityIndex", ""),
        "RowIndex": row.get("RowIndex", ""),
        "Original_Name": normalize_text(row.get("Original_Name", "")),
        "Source_RecordIndex": normalize_text(row.get("Source_RecordIndex", "")),
        "Matched_Source_RecordIndex": normalize_text(matched_source_record_index),
        "Backtrace_Status": "NOT_PROCESSED",
        "Backtrace_Confidence": 0.0,
        "Resolved_Full_Name": "",
        "Resolved_Composition": "",
        "Evidence_Snippet": "",
        "MD_File": "",
        "MD_Locate_Method": "",
        "Raw_FileName": "",
        "Raw_DOI": "",
        "Raw_Title": "",
        "Backtrace_Notes": "",
        "BT_Molecular_Formula": "",
        "BT_IUPAC_Name": "",
        "BT_Standardized_Query": "",
        "BT_Canonical_SMILES": "",
        "BT_CID": "",
        "BT_Match_Source": "",
        "BT_Lookup_Status": "",
    }


def process_one_row(
    row: pd.Series,
    raw_df: pd.DataFrame,
    zip_path: str,
    md_catalog: Dict[str, Any],
    md_cache: Dict[str, str],
    base_mod,
    pubchem_cache: Dict[str, Dict[str, Any]],
    rewrite_cache: Dict[str, List[str]],
    material_cache: Dict[str, List[str]],
    api_key: str,
    model: str,
    enable_llm: bool,
) -> Dict[str, Any]:
    original_name = normalize_text(row.get("Original_Name", ""))
    standardized_query = normalize_text(row.get("Standardized_Query", ""))
    source_idx = choose_single_source_record_index(row.get("Source_RecordIndex", ""))

    best = init_bt_result(row, source_idx or "")
    if source_idx is None:
        best["Backtrace_Status"] = "MISSING_SOURCE_RECORDINDEX"
        best["Backtrace_Notes"] = "Source_RecordIndex is empty"
        return best

    if source_idx < 1 or source_idx > len(raw_df):
        best["Backtrace_Status"] = "RAW_ROW_NOT_FOUND"
        best["Backtrace_Notes"] = f"Source_RecordIndex {source_idx} out of range"
        return best

    raw_row = raw_df.iloc[source_idx - 1]
    raw_file = get_first_nonblank(raw_row, DOC_FILE_COLS)
    raw_doi = get_first_nonblank(raw_row, DOC_DOI_COLS)
    raw_title = get_first_nonblank(raw_row, DOC_TITLE_COLS)

    best["Matched_Source_RecordIndex"] = str(source_idx)
    best["Raw_FileName"] = raw_file
    best["Raw_DOI"] = raw_doi
    best["Raw_Title"] = raw_title

    member, locate_method = locate_md_member(raw_row, md_catalog)
    best["MD_File"] = member
    best["MD_Locate_Method"] = locate_method
    if not member:
        best["Backtrace_Status"] = "MD_NOT_FOUND"
        best["Backtrace_Notes"] = "Unable to locate MD from raw metadata"
        return best

    md_text = read_md_member_cached(zip_path, member, md_cache)
    tokens = generate_search_tokens(original_name, standardized_query)
    windows = collect_context_windows(md_text, tokens)
    if not windows:
        best["Backtrace_Status"] = "TOKEN_NOT_IN_MD"
        best["Backtrace_Notes"] = "No token hit in matched MD file"
        return best

    regex_candidates: List[Dict[str, Any]] = []
    class_only: Optional[Dict[str, Any]] = None
    for tok in tokens[:8]:
        for w in windows[:6]:
            for rc in extract_candidates_from_context(tok, w):
                if rc.get("class_hint"):
                    if class_only is None or rc.get("score", 0) > class_only.get("score", 0):
                        class_only = rc
                else:
                    regex_candidates.append(rc)

    dedup: Dict[str, Dict[str, Any]] = {}
    for rc in regex_candidates:
        key = normalize_text(rc.get("candidate", "")).lower()
        if not key:
            continue
        if key not in dedup or rc.get("score", 0) > dedup[key].get("score", 0):
            dedup[key] = rc
    ranked = sorted(dedup.values(), key=lambda x: x.get("score", 0), reverse=True)

    for rc in ranked[:2]:
        query = normalize_text(rc.get("candidate", ""))
        current = init_bt_result(row, source_idx)
        current.update({
            "Raw_FileName": raw_file,
            "Raw_DOI": raw_doi,
            "Raw_Title": raw_title,
            "MD_File": member,
            "MD_Locate_Method": locate_method,
            "Resolved_Full_Name": query,
            "Backtrace_Confidence": rc.get("score", 0),
            "Evidence_Snippet": rc.get("evidence", ""),
            "Backtrace_Notes": merge_notes(f"backtrace_rule={rc.get('rule', '')}", f"md_locate={locate_method}"),
        })

        if query_looks_material(query):
            current["Backtrace_Status"] = "BACKTRACE_CLASSIFIED"
            current["BT_Lookup_Status"] = "NO_SPECIFIC_CHEMICAL"
            current["BT_Match_Source"] = "MD Backtrace"
            current["Backtrace_Notes"] = merge_notes(current["Backtrace_Notes"], "material_like_query")
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
            base_notes=current["Backtrace_Notes"],
        )
        current["Backtrace_Notes"] = notes1
        current.update({f"BT_{k}": v for k, v in chem.items()})

        if chem.get("Lookup_Status") in {"OK", "ABBR_EXPANDED_OK", "CID_ONLY"}:
            current["Backtrace_Status"] = "BACKTRACE_OK"
            return current
        if chem.get("Lookup_Status") in {"Polymer", "NO_SPECIFIC_CHEMICAL"}:
            current["Backtrace_Status"] = "BACKTRACE_CLASSIFIED"
            return current
        best = current
        best["Backtrace_Status"] = "EXPANSION_FOUND_BUT_PUBCHEM_NO_HIT"

    raw_meta = {"file_name": raw_file, "doi": raw_doi, "title": raw_title}
    if enable_llm:
        llm_res = call_context_resolver_llm(
            token=original_name,
            windows=windows,
            raw_meta=raw_meta,
            base_mod=base_mod,
            api_key=api_key,
            model=model,
        )
    else:
        llm_res = {}

    if llm_res:
        current = init_bt_result(row, source_idx)
        current.update({
            "Raw_FileName": raw_file,
            "Raw_DOI": raw_doi,
            "Raw_Title": raw_title,
            "MD_File": member,
            "MD_Locate_Method": locate_method,
            "Resolved_Full_Name": llm_res.get("query", ""),
            "Resolved_Composition": " | ".join(llm_res.get("composition", [])),
            "Backtrace_Confidence": llm_res.get("confidence", 0),
            "Evidence_Snippet": llm_res.get("evidence", "") or (windows[0][:700] if windows else ""),
            "Backtrace_Notes": merge_notes("llm_context_resolver", llm_res.get("note", ""), f"md_locate={locate_method}"),
        })
        if llm_res.get("status") == "MATERIAL_CLASS":
            current["Backtrace_Status"] = "BACKTRACE_CLASSIFIED"
            current["BT_Lookup_Status"] = "NO_SPECIFIC_CHEMICAL"
            return current
        if llm_res.get("status") == "SPECIFIC_CHEMICAL" and llm_res.get("query"):
            query = llm_res["query"]
            if query_looks_material(query):
                current["Backtrace_Status"] = "BACKTRACE_CLASSIFIED"
                current["BT_Lookup_Status"] = "NO_SPECIFIC_CHEMICAL"
                return current
            chem, notes2 = try_lookup_query(
                query=query,
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
            elif chem.get("Lookup_Status") in {"Polymer", "NO_SPECIFIC_CHEMICAL"}:
                current["Backtrace_Status"] = "BACKTRACE_CLASSIFIED"
            else:
                current["Backtrace_Status"] = "LLM_EXPANSION_BUT_PUBCHEM_NO_HIT"
            return current

    if class_only:
        best["Backtrace_Status"] = "BACKTRACE_CLASSIFIED"
        best["Backtrace_Confidence"] = class_only.get("score", 0)
        best["Evidence_Snippet"] = class_only.get("evidence", "")
        best["Backtrace_Notes"] = "Context suggests material/commercial series"
        best["BT_Lookup_Status"] = "NO_SPECIFIC_CHEMICAL"
        return best

    if best["Backtrace_Status"] == "NOT_PROCESSED":
        best["Backtrace_Status"] = "CONTEXT_FOUND_BUT_NO_EXPANSION"
        best["Evidence_Snippet"] = windows[0][:700]
        best["Backtrace_Notes"] = "Found context but no reliable full-name pattern"
    return best


def merge_backtrace_to_main(entity_df: pd.DataFrame, bt_df: pd.DataFrame) -> pd.DataFrame:
    if "EntityIndex" in entity_df.columns and "EntityIndex" in bt_df.columns:
        merged = entity_df.merge(bt_df, on="EntityIndex", how="left", suffixes=("", "_bt"))
    else:
        keys = [c for c in ["RowIndex", "Original_Name"] if c in entity_df.columns and c in bt_df.columns]
        merged = entity_df.merge(bt_df, on=keys, how="left", suffixes=("", "_bt"))

    overwrite_map = {
        "Molecular_Formula": "BT_Molecular_Formula",
        "IUPAC_Name": "BT_IUPAC_Name",
        "Standardized_Query": "BT_Standardized_Query",
        "Canonical_SMILES": "BT_Canonical_SMILES",
        "CID": "BT_CID",
    }

    if "Backtrace_Status" in merged.columns:
        hit_mask = merged["Backtrace_Status"].astype(str).eq("BACKTRACE_OK")
        class_mask = merged["Backtrace_Status"].astype(str).eq("BACKTRACE_CLASSIFIED")
    else:
        hit_mask = pd.Series(False, index=merged.index)
        class_mask = pd.Series(False, index=merged.index)

    for dest, src in overwrite_map.items():
        if src in merged.columns and dest in merged.columns and hit_mask.any():
            merged[dest] = merged[dest].astype("object")
            merged.loc[hit_mask, dest] = merged.loc[hit_mask, src].astype("object")

    if "Lookup_Status" in merged.columns:
        merged["Lookup_Status"] = merged["Lookup_Status"].astype("object")
        if hit_mask.any():
            merged.loc[hit_mask, "Lookup_Status"] = "BACKTRACE_OK"
        if "BT_Lookup_Status" in merged.columns and class_mask.any():
            vals = merged.loc[class_mask, "BT_Lookup_Status"].fillna("").astype(str)
            keep = vals.ne("")
            if keep.any():
                idx = vals[keep].index
                merged.loc[idx, "Lookup_Status"] = vals.loc[idx]

    if "Match_Source" in merged.columns:
        merged["Match_Source"] = merged["Match_Source"].astype("object")
        if hit_mask.any():
            merged.loc[hit_mask, "Match_Source"] = "MD Backtrace + PubChem"
        if class_mask.any():
            merged.loc[class_mask, "Match_Source"] = "MD Backtrace"

    if "Decision_Notes" in merged.columns:
        merged["Decision_Notes"] = merged["Decision_Notes"].fillna("").astype("object")
        resolved = merged.get("Resolved_Full_Name", pd.Series("", index=merged.index)).fillna("").astype(str)
        notes_mask = hit_mask | class_mask
        if notes_mask.any():
            merged.loc[notes_mask, "Decision_Notes"] = (
                merged.loc[notes_mask, "Decision_Notes"].astype(str).str.rstrip("; ")
                + "; backtrace_resolved="
                + resolved.loc[notes_mask]
            ).str.strip("; ")

    # keep original columns + compact result columns only
    front_cols = [c for c in entity_df.columns if c in merged.columns]
    compact = [c for c in FINAL_MAIN_EXTRA_COLS if c in merged.columns and c not in front_cols]
    return merged[front_cols + compact]


def save_bt_only(bt_df: pd.DataFrame, path: str) -> None:
    out = Path(path)
    out.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(out, engine="openpyxl") as writer:
        bt_df.to_excel(writer, sheet_name="bt_all", index=False)


def write_outputs(output: str, merged: pd.DataFrame, bt_df: pd.DataFrame) -> None:
    out = Path(output)
    out.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(out, engine="openpyxl") as writer:
        merged.to_excel(writer, sheet_name="final_merged", index=False)
        bt_df.to_excel(writer, sheet_name="bt_all", index=False)
        bt_df[bt_df["Backtrace_Status"] == "BACKTRACE_OK"].to_excel(writer, sheet_name="backtrace_hits", index=False)
        bt_df[bt_df["Backtrace_Status"] != "BACKTRACE_OK"].to_excel(writer, sheet_name="backtrace_unresolved", index=False)
        evidence_cols = [c for c in EVIDENCE_COLS if c in bt_df.columns]
        bt_df[evidence_cols].to_excel(writer, sheet_name="evidence", index=False)
        dict_cols = [c for c in ["Original_Name", "Resolved_Full_Name", "Resolved_Composition", "Backtrace_Status", "Backtrace_Confidence", "Evidence_Snippet"] if c in bt_df.columns]
        dict_df = bt_df[bt_df["Resolved_Full_Name"].astype(str).str.len() > 0][dict_cols].drop_duplicates()
        dict_df.to_excel(writer, sheet_name="abbr_dict_candidates", index=False)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Backtrace abbreviations via Source_RecordIndex -> raw DB -> MD -> PubChem")
    p.add_argument("--input", default=DEFAULT_INPUT_PATH, help="Input Excel/CSV with Source_RecordIndex")
    p.add_argument("--input-sheet", default=DEFAULT_INPUT_SHEET, help="Sheet name for input workbook")
    p.add_argument("--raw-db", default=DEFAULT_RAW_DB_PATH, help="Raw database Excel/CSV")
    p.add_argument("--raw-sheet", default=DEFAULT_RAW_SHEET, help="Sheet name for raw DB workbook")
    p.add_argument("--md-zip", default=DEFAULT_MD_ZIP_PATH, help="Zip containing MD files")
    p.add_argument("--base-script", default=DEFAULT_BASE_SCRIPT, help="Path to code_extract_name_fixed.py")
    p.add_argument("--output", default=DEFAULT_OUTPUT_PATH, help="Output Excel path")
    p.add_argument("--report", default=DEFAULT_REPORT_PATH, help="Optional JSON report path")
    p.add_argument("--api-key", default=DEFAULT_API_KEY)
    p.add_argument("--model", default=DEFAULT_MODEL)
    p.add_argument("--only-unresolved", action="store_true", default=DEFAULT_ONLY_UNRESOLVED, help="Only process unresolved Lookup_Status rows")
    p.add_argument("--process-all-unresolved", action="store_true", default=DEFAULT_PROCESS_ALL_UNRESOLVED, help="Do not restrict backtrace to abbreviation-like rows")
    p.add_argument("--enable-llm", action="store_true", default=DEFAULT_ENABLE_LLM, help="Enable context LLM fallback (slower)")
    p.add_argument("--bt-input", default=DEFAULT_BT_INPUT_PATH, help="Existing bt_only.xlsx for merge-only mode")
    p.add_argument("--bt-sheet", default=DEFAULT_BT_SHEET, help="Sheet name inside bt_only.xlsx")
    p.add_argument("--save-bt-only", default=DEFAULT_SAVE_BT_ONLY_PATH, help="Optional path to save bt_all before merge")
    args = p.parse_args()

    # 基本必填校验：必须有 input 和 output
    if not args.input or not args.output:
        p.error("Both --input and --output (or their defaults in the config block) are required.")

    # 若非 merge-only 模式，则还需要 raw-db 和 md-zip
    if not args.bt_input and (not args.raw_db or not args.md_zip):
        p.error("Fresh run (without --bt-input) requires --raw-db and --md-zip (or their defaults in the config block).")

    return args


def main() -> None:
    args = parse_args()
    input_sheet = choose_sheet(args.input, args.input_sheet)
    stage("阶段: 读取输入表")
    entity_df = prepare_entity_df(read_table(args.input, input_sheet))

    if args.bt_input:
        stage("阶段: 使用已有 bt_only，直接合并")
        bt_df = read_table(args.bt_input, choose_sheet(args.bt_input, args.bt_sheet) or args.bt_sheet)
        merged = merge_backtrace_to_main(entity_df, bt_df)
        write_outputs(args.output, merged, bt_df)
        if args.report:
            summary = {
                "mode": "merge_only",
                "input_rows": int(len(entity_df)),
                "bt_rows": int(len(bt_df)),
                "backtrace_status_counts": bt_df["Backtrace_Status"].value_counts(dropna=False).to_dict() if len(bt_df) else {},
            }
            Path(args.report).write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
        stage(f"Saved output: {args.output}")
        return

    if not args.raw_db or not args.md_zip:
        raise ValueError("Fresh run requires --raw-db and --md-zip")

    raw_sheet = choose_sheet(args.raw_db, args.raw_sheet)
    stage("阶段: 读取原始数据库")
    raw_df = ensure_raw_rowindex(read_table(args.raw_db, raw_sheet))
    stage("阶段: 加载 base-script")
    base_mod = load_base_module(args.base_script)

    work_df = entity_df.copy()
    if args.only_unresolved and "Lookup_Status" in work_df.columns:
        work_df = work_df[work_df["Lookup_Status"].astype(str).isin(DEFAULT_UNRESOLVED)].copy()
    if not args.process_all_unresolved:
        work_df = work_df[work_df["Original_Name"].astype(str).apply(looks_like_abbreviation)].copy()

    stage(f"阶段: 扫描 MD zip")
    md_catalog = build_md_catalog(args.md_zip)
    stage(f"MD 目录完成：条目数={len(md_catalog['items'])}")

    pubchem_cache: Dict[str, Dict[str, Any]] = {}
    rewrite_cache: Dict[str, List[str]] = {}
    material_cache: Dict[str, List[str]] = {}
    md_cache: Dict[str, str] = {}

    bt_rows: List[Dict[str, Any]] = []
    total = len(work_df)
    stage(f"阶段: 逐行回溯/查询（共 {total} 行）")
    iterator = work_df.iterrows()
    if tqdm is not None:
        iterator = tqdm(iterator, total=total, desc="Backtrace", unit="row")
    for i, (_, row) in enumerate(iterator, start=1):
        if tqdm is not None:
            iterator.set_postfix_str(str(row.get("Original_Name", ""))[:30])
        else:
            stage(f"[{i}/{total}] {row.get('Original_Name', '')}")
        bt = process_one_row(
            row=row,
            raw_df=raw_df,
            zip_path=args.md_zip,
            md_catalog=md_catalog,
            md_cache=md_cache,
            base_mod=base_mod,
            pubchem_cache=pubchem_cache,
            rewrite_cache=rewrite_cache,
            material_cache=material_cache,
            api_key=args.api_key,
            model=args.model,
            enable_llm=args.enable_llm,
        )
        bt_rows.append(bt)

    bt_df = pd.DataFrame(bt_rows)
    bt_only_path = args.save_bt_only or str(Path(args.output).with_name(Path(args.output).stem + "_bt_only.xlsx"))
    stage("阶段: 保存中间 bt_only")
    save_bt_only(bt_df, bt_only_path)

    stage("阶段: 合并回主表")
    merged = merge_backtrace_to_main(entity_df, bt_df)
    write_outputs(args.output, merged, bt_df)

    summary = {
        "input_rows": int(len(entity_df)),
        "processed_rows": int(len(work_df)),
        "md_catalog_size": int(len(md_catalog["items"])),
        "backtrace_status_counts": bt_df["Backtrace_Status"].value_counts(dropna=False).to_dict() if len(bt_df) else {},
        "resolved_hits": int((bt_df["Backtrace_Status"] == "BACKTRACE_OK").sum()) if len(bt_df) else 0,
        "classified_hits": int((bt_df["Backtrace_Status"] == "BACKTRACE_CLASSIFIED").sum()) if len(bt_df) else 0,
        "bt_only_path": bt_only_path,
    }
    if args.report:
        Path(args.report).parent.mkdir(parents=True, exist_ok=True)
        Path(args.report).write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
        stage(f"Saved report: {args.report}")
    stage(json.dumps(summary, ensure_ascii=False, indent=2))
    stage(f"Saved bt_only: {bt_only_path}")
    stage(f"Saved output: {args.output}")


if __name__ == "__main__":
    main()
