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
# 配置区（保留在前面，便于直接改） 对比第五版 不再只盯缩写 回填更清晰的版本
# =========================

# 路径类默认配置（可直接在这里改；命令行参数会覆盖）
DEFAULT_INPUT_PATH = "./test12/处理缩写后_index.xlsx"  # 输入 Excel/CSV（含 Source_RecordIndex）
DEFAULT_INPUT_SHEET = "Sheet1"  # 输入为 xlsx 时的 sheet 名；空字符串表示自动选择

DEFAULT_RAW_DB_PATH = "./test12/membrane_aligned_data_all.csv"  # 原始数据库 Excel/CSV
DEFAULT_RAW_SHEET = ""  # 原始库为 xlsx 时的 sheet 名；空字符串表示自动选择

DEFAULT_MD_ZIP_PATH = "./test12/RO_md_files.zip"  # 包含 MD 文件的 zip
DEFAULT_BASE_SCRIPT = r"D:\graduate_work\RO_database\Part1_chemExtract\step1\test10_no_part_polymer_deepseek\code_extract_name_fixed.py"  # base 抽取脚本路径

DEFAULT_OUTPUT_PATH = "./test12/output/test8_1.xlsx"  # 输出 Excel 路径
DEFAULT_REPORT_PATH = "./test12/output/test8_1.json"  # 输出 JSON 报告路径（可选，不填则不生成）

DEFAULT_BT_INPUT_PATH =  r"D:\graduate_work\RO_database\Part1_chemExtract\step1\test12\output\test8_1_bt_only.xlsx"  # 仅合并模式：已有 bt_only.xlsx 的路径
DEFAULT_BT_SHEET = "bt_all"  # bt_only.xlsx 中的 sheet 名
DEFAULT_SAVE_BT_ONLY_PATH = ""  # 新鲜跑时，bt_only 的保存路径（留空则自动生成 *_bt_only.xlsx）

# 其他默认行为配置
DEFAULT_MODEL = "deepseek-chat"
DEFAULT_API_KEY = os.getenv("DEEPSEEK_API_KEY", "")
DEFAULT_ONLY_UNRESOLVED = False  # 仅处理未解析行（Lookup_Status 属于 DEFAULT_UNRESOLVED）
DEFAULT_PROCESS_ALL_UNRESOLVED = False
DEFAULT_RECHECK_STATUSES = {"NO_HIT", "NO_SPECIFIC_CHEMICAL"}
DEFAULT_ENABLE_PREWRITE_ALIGNMENT = False
DEFAULT_ENABLE_LLM = True
DEFAULT_MAX_SOURCE_TRIES = 3  # 多值 Source_RecordIndex 时，最多尝试前几个来源
DEFAULT_MAX_TOKEN_VARIANTS = 24
DEFAULT_MAX_CONTEXT_WINDOWS = 20
DEFAULT_MAX_REGEX_CANDIDATES = 5

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

# 主表仅保留与输入表一致的结构，至多外加 Matched_Source_RecordIndex；过程字段放到 review_needed / evidence
MAIN_TABLE_EXTRA_COLUMN = "Matched_Source_RecordIndex"

# 需要人工核对的 Backtrace_Status（非成功/已分类即视为需核对）
REVIEW_NEEDED_STATUSES = {
    "TOKEN_NOT_IN_MD",
    "MD_NOT_FOUND",
    "CONTEXT_FOUND_BUT_NO_EXPANSION",
    "EXPANSION_FOUND_BUT_PUBCHEM_NO_HIT",
    "LLM_EXPANSION_BUT_PUBCHEM_NO_HIT",
    "BACKTRACE_CLASS_HINT",
    "RAW_ROW_NOT_FOUND",
    "MISSING_SOURCE_RECORDINDEX",
    "NOT_PROCESSED",
}

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
    "MD_Candidate_Tried",
    "MD_Candidate_List",
    "MD_Accepted_Because",
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
    "commercial", "series", "model", "substrate",
]

ABBR_SEARCH_MAP = {
    "NH2": ["amino", "amine"],
    "COOH": ["carboxyl", "carboxylic"],
    "SO3H": ["sulfonic", "sulfonated"],
    "GO": ["graphene oxide"],
    "MOF": ["metal organic framework", "metal-organic framework"],
    "MSN": ["mesoporous silica nanoparticle", "mesoporous silica nanoparticles"],
    "HNT": ["halloysite nanotube", "halloysite nanotubes"],
    "MCM": ["mcm-41"],
}

TOKEN_ALIAS_MAP = {
    "AGO": ["aminated graphene oxide", "amine-functionalized graphene oxide"],
    "HNT-COOH": ["carboxylated HNT", "carboxylated halloysite nanotube", "COOH-HNT"],
    "NH2-MCM-41": ["amino-functionalized MCM-41", "amino MCM-41"],
    "MCM-41-NH2": ["amino-functionalized MCM-41", "amino MCM-41"],
}

BAD_CANDIDATE_PHRASES = [
    "synthesized", "reaction", "normalized", "used", "prepared", "obtained",
    "calculated", "measured", "according to", "based on", "figure", "table",
    "compared with", "showed that", "demonstrated", "in this paper", "in this study",
    "radius of", "surface area", "contact angle", "flux", "rejection",
    "followed by", "detected in", "modified by", "coated with", "immersed in",
    "weight loss", "step between", "reported that", "attributed to",
    "immersion", "during the", "after the", "before the", "between the",
    "resulting in", "leading to", "consisting of", "composed of",
    "dissolved in", "mixed with", "added to", "placed in", "soaked in",
    "dipped in", "treated with", "blended with", "washing", "rinsing",
    "filtration", "centrifugation", "sonication", "annealing", "drying",
    "coating the", "casting the", "spinning the", "heating the",
]

BAD_STD_QUERY_PHRASES = BAD_CANDIDATE_PHRASES + [
    "immersed", "solution", "dissolved", "stirred", "heated", "dried",
    "subsequently", "was added", "mixed with", "poured", "coated",
    "rinsed", "washed", "filtered", "centrifuged", "sonicated",
    "purchased from", "supplied by", "obtained from", "provided by",
    "bought from", "donated by", "received from",
    "inc.", "inc,", "ltd.", "ltd,", "co.", "co,", "corp.", "corp,",
    "company", "supplier", "manufacturer", "vendor", "distributor",
    "usa", "u.s.a", "u.s.a.", "china", "germany", "japan", "korea", "india",
    "france", "uk", "canada", "taiwan", "singapore",
    "osmonics", "hydranautics", "dow chemical", "toray", "ge water",
    "nitto denko", "koch membrane", "filmtec", "sepro", "sigma-aldrich",
    "sigma aldrich", "merck", "alfa aesar", "fisher scientific", "aladdin",
    "sinopharm", "acros", "aldrich", "fluka", "tci",
    "wt%", "vol%", "mol/l", "mg/l", "g/l", "mol%",
    "respectively", "therefore", "however", "moreover", "furthermore",
    "the result", "was found", "it was", "can be", "which was",
    "membrane was", "membrane is", "onto the", "into the",
    "in order to", "as shown", "as reported", "as described",
]


ABBR_BAD_PREFIXES = [
    "and ", "or ", "but ", "also ", "then ", "yet ",
    "followed by ", "used as ", "reported that ", "detected in ",
    "modified by ", "coated with ", "immersed in ", "role of ",
    "illustrates the structure of ", "as well as ", "which comprised of ",
    "somewhat ", "according to ", "based on ", "for test and ",
]

GENERIC_SINGLE_WORDS = {
    "glass", "gold", "membrane", "solvent", "solvents", "structure",
    "hydrophilicity", "accordi", "lower", "higher", "different",
    "organic", "role", "signal", "fluid", "mixture", "test",
}

CHEMICAL_ENDING_HINTS = [
    "acid", "amine", "amide", "aniline", "imide", "chloride", "bromide",
    "iodide", "fluoride", "oxide", "hydroxide", "sulfonate", "sulfonic acid",
    "phosphonic acid", "phosphate", "imidazolium chloride", "alcohol", "ketone",
    "aldehyde", "ester", "ether", "piperazine", "succinimide", "nanotube",
    "nanotubes", "graphene oxide", "quantum dots", "mcm-41",
    "halloysite nanotubes", "polyamide", "polyethersulfone",
]


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
    t = t.replace("–", "-").replace("—", "-").replace("−", "-")
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


def looks_like_nonchemical_generic(name: Any) -> bool:
    s = normalize_text(name).lower()
    if not s:
        return True
    if s in {"membrane", "support", "substrate", "glass", "hydrophilicity", "roughness", "permeability", "anti-fouling", "antifouling"}:
        return True
    if len(s.split()) == 1 and s in GENERIC_SINGLE_WORDS:
        return True
    return False


def is_recheck_candidate(row: pd.Series) -> bool:
    name = normalize_text(row.get("Original_Name", ""))
    if not name:
        return False
    status = normalize_text(row.get("Lookup_Status", "")).upper()
    if status not in DEFAULT_UNRESOLVED:
        return False
    if looks_like_abbreviation(name):
        return True
    if looks_like_nonchemical_generic(name):
        return False
    if status in DEFAULT_RECHECK_STATUSES:
        return True
    if len(name) >= 8 and any(ch.isalpha() for ch in name):
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


def get_md_candidates(raw_row: pd.Series, catalog: Dict[str, Any]) -> List[Tuple[str, str]]:
    """
    保守的 MD 候选列表：优先 DOI，再文件名 basename/stem，再标题部分匹配。
    不直接返回 fuzzy 作为唯一结果；fuzzy 仅作为末尾候选，是否接受由「token 是否在该 MD 中命中」决定。
    返回 [(member, method), ...]，去重且保持顺序。
    """
    raw_file = get_first_nonblank(raw_row, DOC_FILE_COLS)
    raw_doi = get_first_nonblank(raw_row, DOC_DOI_COLS)
    raw_title = get_first_nonblank(raw_row, DOC_TITLE_COLS)

    file_keys: List[str] = []
    if raw_file:
        file_keys.extend([clean_key(raw_file), clean_key(Path(raw_file).name), clean_key(Path(raw_file).stem)])
    file_keys = [x for x in file_keys if x]
    doi_key = clean_key(raw_doi) if raw_doi else ""
    title_key = clean_key(raw_title) if raw_title else ""

    seen: set = set()
    out: List[Tuple[str, str]] = []

    def add(member: str, method: str) -> None:
        if member and member not in seen:
            seen.add(member)
            out.append((member, method))

    # 1) DOI 精确匹配或后缀匹配（DOI 的 suffix 部分高度唯一）
    if doi_key and len(doi_key) >= 6:
        for k, member in catalog["doi"].items():
            if not k:
                continue
            if doi_key == k:
                add(member, "doi")
            elif len(doi_key) >= 8 and len(k) >= 8:
                shorter, longer = (doi_key, k) if len(doi_key) <= len(k) else (k, doi_key)
                if shorter in longer and len(shorter) >= 0.55 * len(longer):
                    add(member, "doi")

    # 2) 文件名 basename / stem（精确 + 包含）
    for fk in file_keys:
        if not fk or len(fk) < 4:
            continue
        if fk in catalog["file"]:
            add(catalog["file"][fk], "filename")
        else:
            for ck, cm in catalog["file"].items():
                if not ck:
                    continue
                if fk in ck and len(fk) >= 0.6 * len(ck):
                    add(cm, "filename_stem")
                elif ck in fk and len(ck) >= 0.6 * len(fk):
                    add(cm, "filename_stem")

    # 3) 标题部分匹配（标题足够长、有足够重叠才接受）
    if title_key and len(title_key) >= 12:
        short = re.sub(r"[^a-z0-9]+", " ", title_key.lower()).strip()
        short = re.sub(r"\s+", " ", short)
        if len(short) >= 12:
            key60 = short[:60]
            for k, member in catalog["title"].items():
                if not k or len(k) < 12:
                    continue
                if key60 in k and len(key60) >= 0.3 * len(k):
                    add(member, "title_partial")
                elif k in key60 and len(k) >= 0.3 * len(key60):
                    add(member, "title_partial")

    # 4) fuzzy 仅作为额外候选追加，不替代上述；只有在该 MD 中 token 命中时才接受
    keys = [x for x in file_keys + [title_key] if x]
    if keys:
        best_score = -1
        best_member = ""
        for item in catalog["items"]:
            if item["member"] in seen:
                continue
            score = 0
            for k in keys:
                for candidate in [item["base_key"], item["stem_key"], item["title_key"]]:
                    if candidate and (k in candidate or candidate in k):
                        score = max(score, min(len(k), len(candidate)))
            if score > best_score and score >= 8:
                best_score = score
                best_member = item["member"]
        if best_member:
            add(best_member, "fuzzy")

    return out


def locate_md_member(raw_row: pd.Series, catalog: Dict[str, Any]) -> Tuple[str, str]:
    """
    兼容旧接口：返回“首个候选”。注意最终是否接受以 token 命中验证为准。
    """
    cands = get_md_candidates(raw_row, catalog)
    if not cands:
        return "", "not_found"
    return cands[0][0], cands[0][1]


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
        if not x or len(x) < 2:
            return
        k = x.lower()
        if k not in seen:
            seen.add(k)
            out.append(x)

    for t in raw:
        add(t)
        if not t:
            continue
        add(t.replace("-", ""))
        add(t.replace("-", " "))
        add(t.replace("_", " "))
        add(t.replace("/", " "))
        t2 = re.sub(r"^[\d,]+\s*-\s*", "", t)
        if t2 != t:
            add(t2)
        t3 = re.sub(r"\((.*?)\)", r"\1", t)
        if t3 != t:
            add(t3)
        parts = [p for p in re.split(r"[-_/]", t) if p]
        for p in parts:
            if len(p) >= 2 and not p.isdigit():
                add(p)
        if len(parts) >= 2:
            add("-".join(parts[1:]))
            add(" ".join(parts[1:]))
            add("-".join(reversed(parts)))

        upper_t = t.upper()
        if upper_t in TOKEN_ALIAS_MAP:
            for a in TOKEN_ALIAS_MAP[upper_t]:
                add(a)

        compact = re.sub(r"[^A-Za-z0-9]+", "", upper_t)
        for src, dsts in ABBR_SEARCH_MAP.items():
            if src in compact or src in upper_t:
                for dst in dsts:
                    add(dst)
                    add(t.replace(src, dst))
                    add(t2.replace(src, dst))

        for alias_key, alias_vals in TOKEN_ALIAS_MAP.items():
            if alias_key in upper_t or alias_key in compact:
                for av in alias_vals:
                    add(av)

    origin = normalize_text(original_name)
    if origin:
        add(re.sub(r"[-_\s]+", "", origin))
        stripped = re.sub(r"[-_]?\d+$", "", origin)
        if stripped and stripped != origin:
            add(stripped)
        if "-" in origin:
            add(origin.split("-")[0])
            add(origin.rsplit("-", 1)[0])

    return out[:DEFAULT_MAX_TOKEN_VARIANTS]


def sentence_windows(text: str, token: str, max_hits: int = 6) -> List[str]:
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


def span_windows(text: str, token: str, max_hits: int = 6, radius: int = 300) -> List[str]:
    pat = token_pattern(token)
    if pat is None:
        return []
    hits: List[str] = []
    seen = set()
    for m in pat.finditer(str(text or "")):
        a = max(0, m.start() - radius)
        b = min(len(text), m.end() + radius)
        w = normalize_text(text[a:b])
        k = w.lower()
        if k and k not in seen:
            hits.append(w[:1400])
            seen.add(k)
        if len(hits) >= max_hits:
            break
    return hits


def find_first_occurrences(md_text: str, abbreviation: str, max_occ: int = 3, radius: int = 400) -> List[str]:
    """定位缩写在 MD 中前 N 次出现，返回按出现顺序排列的上下文窗口。"""
    pat = token_pattern(abbreviation)
    if pat is None:
        return []
    windows: List[str] = []
    prev_end = -999
    for m in pat.finditer(md_text):
        if m.start() - prev_end < 100:
            continue
        a = max(0, m.start() - radius)
        b = min(len(md_text), m.end() + radius)
        window = normalize_text(md_text[a:b])
        if window and len(window) > 20:
            windows.append(window[:1400])
            prev_end = m.end()
        if len(windows) >= max_occ:
            break
    return windows


def collect_context_windows(md_text: str, tokens: List[str]) -> Tuple[List[str], List[str]]:
    windows: List[str] = []
    matched_tokens: List[str] = []
    seen = set()
    seen_tok = set()
    for tok in tokens:
        local_hit = False
        for w in sentence_windows(md_text, tok, max_hits=6):
            k = w.lower()
            if k not in seen:
                seen.add(k)
                windows.append(w)
            local_hit = True
        for w in span_windows(md_text, tok, max_hits=6):
            k = w.lower()
            if k not in seen:
                seen.add(k)
                windows.append(w)
            local_hit = True
        if local_hit and tok.lower() not in seen_tok:
            seen_tok.add(tok.lower())
            matched_tokens.append(tok)
        if len(windows) >= DEFAULT_MAX_CONTEXT_WINDOWS and len(matched_tokens) >= 2:
            break
    return windows[:DEFAULT_MAX_CONTEXT_WINDOWS], matched_tokens


def cleanup_candidate_name(cand: str) -> str:
    c = normalize_text(cand)
    c = re.sub(r"^(the|a|an|this|these|those|its|their|our|some|such)\s+", "", c, flags=re.I)
    c = re.sub(
        r"^(followed by|modified by|detected in|used as|reported that|"
        r"immersed in|coated with|based on|according to|prepared from|"
        r"obtained from|dissolved in|mixed with|added to|placed in|"
        r"soaked in|dipped in|treated with|blended with|combined with|"
        r"consisting of|composed of|derived from|resulting from|"
        r"attributed to|referred to as|known as|described as)\s+",
        "", c, flags=re.I,
    )
    c = re.sub(r"^(and|or|but|also|then|yet|nor)\s+", "", c, flags=re.I)
    c = re.sub(
        r"^(in this paper|in this study|in this work|here|namely|"
        r"such as|including|e\.g\.,?|i\.e\.,?|for example|for instance)\s*[,:]?\s*",
        "", c, flags=re.I,
    )
    c = re.sub(r"\b(and|or|with|for|of|as|by|in|on|at|to)\s*$", "", c, flags=re.I)
    c = c.strip(" ,;:-")
    c = re.sub(
        r"^.*?\b(namely|is|was|were|denoted as|abbreviated as|stands for|means|"
        r"refers to|defined as|called|named)\b\s*",
        "", c, flags=re.I,
    )
    c = c.strip(" ,;:-")
    c = re.sub(r"^(followed by|modified by|detected in|coated with|immersed in|role of|which comprised of|as well as|illustrates the structure of)\s+", "", c, flags=re.I)
    c = re.sub(r"^[a-z]{1,2}\s+([a-z].*)$", r"\1", c)
    return c.strip(" ,;:-")


def is_bad_candidate(cand: str) -> bool:
    s = normalize_text(cand)
    c = s.lower()
    if not c:
        return True
    if len(c) < 4 or len(c) > 120:
        return True
    if len(c.split()) > 8:
        return True
    if re.fullmatch(r"[A-Z0-9\-_/]+", s):
        return True
    if any(p in c for p in BAD_CANDIDATE_PHRASES):
        return True

    if re.match(r"^(and|or|but|also|then|yet|nor|so|if)\b", c):
        return True
    if re.match(r"^(followed|detected|reported|modified|coated|immersed|"
                 r"dissolved|mixed|added|placed|soaked|dipped|treated|"
                 r"blended|combined|consisting|composed|derived|resulting|"
                 r"attributed|prepared|obtained|used|described|observed)\b", c):
        return True
    if re.match(r"^(the|a|an|this|these|those|its|their)\s+\w+\s+(is|was|were|are|has|had|have)\b", c):
        return True

    open_p = s.count("(") + s.count("（")
    close_p = s.count(")") + s.count("）")
    if open_p != close_p:
        return True

    if re.search(r"\b(into|onto|from|between|during|after|before|upon|through)\s+the\b", c):
        return True
    if re.search(r"\b(was|were|is|are|has|had|have|been|being)\s+\w+ed\b", c):
        return True
    if re.search(r"\b\d+(\.\d+)?\s*(%|wt|vol|mol|mg|g/l|ml|min|hour|h|ppm|bar|mpa)\b", c):
        return True

    return False


def is_trusted_abbreviation_expansion(cand: Any, abbr: str = "") -> bool:
    s = normalize_text(cand)
    if not s:
        return False
    sl = s.lower()
    if len(s) < 4 or len(s) > 120:
        return False
    if any(sl.startswith(p) for p in ABBR_BAD_PREFIXES):
        return False
    if any(p in sl for p in BAD_CANDIDATE_PHRASES):
        return False
    if re.search(r"\b(role of|illustrates the structure of|different organic solvents|somewhat lower|weight loss|step between)\b", sl):
        return False
    if re.match(r"^(and|or|but|also|then|which|where|when|while|that)\b", sl):
        return False
    if re.match(r"^(reported|detected|modified|coated|immersed|used|followed|illustrates|selected|supplied|comprised)\b", sl):
        return False
    if re.search(r"\b(usa|inc|ltd|corp|company|supplier|vendor|commercial)\b", sl):
        return False
    if re.fullmatch(r"[A-Z0-9\-_/]+", s):
        return False
    if s.count("(") != s.count(")") or s.count("（") != s.count("）"):
        return False
    m = re.match(r"^[a-z]{1,2}\s+([a-z].+)$", sl)
    if m and any(h in m.group(1) for h in CHEMICAL_ENDING_HINTS):
        s = normalize_text(m.group(1))
        sl = s.lower()
    words = sl.split()
    if len(words) > 8:
        return False
    if len(words) == 1 and sl in GENERIC_SINGLE_WORDS:
        return False
    if re.search(r"\b(followed|detected|reported|selected|supplied|illustrates|comprised|according|based)\b", sl):
        return False
    if re.search(r"\b\d+(\.\d+)?\s*(%|wt|vol|mol|mg|g/l|ml|min|hour|h|ppm|bar|mpa)\b", sl):
        return False
    if "," in s and re.search(r",\s*[A-Z][a-z]+$", s):
        return False
    if len(words) == 1:
        if len(sl) < 5:
            return False
        if not any(sl.endswith(h) for h in CHEMICAL_ENDING_HINTS):
            return False
    has_hint = any(sl.endswith(h) or h in sl for h in CHEMICAL_ENDING_HINTS)
    has_caps_mix = bool(re.search(r"[A-Z]", s) and re.search(r"[a-z]", s))
    has_paren = "(" in s or "（" in s
    if not (has_hint or has_caps_mix or has_paren or len(words) >= 2):
        return False
    return True


def detect_material_from_text(*texts: str) -> bool:
    merged = " ".join(normalize_text(t).lower() for t in texts if normalize_text(t))
    return any(k in merged for k in MATERIAL_KEYWORDS)


def extract_candidates_from_context(token: str, context: str) -> List[Dict[str, Any]]:
    token = normalize_text(token)
    ctx = context or ""
    if not token or not ctx:
        return []

    results: List[Dict[str, Any]] = []
    seen = set()
    chem_pat = r"([A-Za-z][A-Za-z0-9,\-/()\'\s]{3,120})"
    tok_esc = re.escape(token)
    patterns = [
        (rf"{chem_pat}\s*[\(（]\s*{tok_esc}\s*[\)）]", 0.97, "full(abbr)"),
        (rf"{tok_esc}\s*[\(（]\s*{chem_pat}\s*[\)）]", 0.95, "abbr(full)"),
        (rf"{tok_esc}\s*(?:=|:|stands for|means|refers to|denotes|is|was|were)\s*{chem_pat}", 0.90, "definition"),
        (rf"{chem_pat}\s*,\s*(?:abbreviated as|denoted as|hereafter|herein|referred to as)\s+{tok_esc}", 0.84, "apposition"),
        (rf"{tok_esc}\s*,?\s*(?:known as|also called|also known as|named|i\.e\.,?)\s+{chem_pat}", 0.82, "known_as"),
        (rf"{chem_pat}\s*[\(（][^)）]{{0,20}}{tok_esc}[^)）]{{0,20}}[\)）]", 0.78, "full(..abbr..)"),
    ]
    for pat, score, rule in patterns:
        for m in re.finditer(pat, ctx, flags=re.I):
            cand = cleanup_candidate_name(m.group(1))
            if "," in cand:
                tail = cand.split(",")[-1].strip()
                if 4 <= len(tail) <= len(cand):
                    cand = tail
            if is_bad_candidate(cand):
                continue
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

    class_hint_pat = rf"(?<![A-Za-z0-9]){re.escape(token)}(?![A-Za-z0-9]).{{0,150}}?(commercial|additive|modifier|polymer|copolymer|membrane|framework|nanoparticle|series|resin|material|nanosheet|mesoporous|functionalized|modified)"
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



def normalize_pubchem_query(query: Any) -> str:
    q = cleanup_candidate_name(query)
    if not q:
        return ""
    q = re.sub(r'^"+|"+$', "", q)
    # 去掉前缀连接词
    q = re.sub(r"^(and|or|but|also|then|yet|nor)\s+", "", q, flags=re.I)
    # 去掉介绍性短语
    q = re.sub(r"^(solution of|aqueous solution of|mixture of|blend of|blend containing|containing|including|comprised of|consisting of|composed of|derived from|reported as|known as|described as)\s+", "", q, flags=re.I)
    q = re.sub(r"^(the|a|an|this|these|those)\s+", "", q, flags=re.I)
    # 去掉工艺/说明残片（截断到此处）
    q = re.sub(r"\b(modified|functionalized|used|prepared|based|supported|coated|grafted|loaded)\b.*$", "", q, flags=re.I)
    # 去掉括号内的供应商/国家信息
    q = re.sub(r"\((?:[^()]*(?:sigma|aldrich|hydranautics|osmonics|usa|china|japan|germany|supplier|company|corp|inc|ltd)[^()]*)\)", "", q, flags=re.I)
    # 去掉末尾泛描述词
    q = re.sub(r"\b(solvent|derivative|membrane|composite|material|monomer|polymer|product|grade)\b$", "", q, flags=re.I)
    q = re.sub(r"\b(solution|mixture|blend)\b$", "", q, flags=re.I)
    q = re.sub(r"\b(additive|modifier|filler|dopant|crosslinker|cross-linker|agent|reagent)\b$", "", q, flags=re.I)
    # 去掉数值/单位
    q = re.sub(r"\b\d+(?:\.\d+)?\s*(?:wt%|vol%|mol%|mg/l|g/l|mmol|mol/l|m|mm|cm|nm|μm|um|kda|ppm|bar|mpa|°c|c)\b", "", q, flags=re.I)
    q = re.sub(r"\s*[,;:]\s*$", "", q)
    q = q.strip(" ,;:-")
    q = re.sub(r"^of\s+", "", q, flags=re.I)
    q = re.sub(r"\s+", " ", q).strip()
    # 逗号前取 head（如果 head 足够长）
    if "," in q:
        head = q.split(",", 1)[0].strip()
        if len(head) >= 4:
            q = head
    # 从关系从句截断
    for sep_pat in [r"\bwhich\b", r"\bthat\b", r"\bwhere\b", r"\bwhile\b", r"\bafter\b", r"\bbefore\b", r"\bduring\b"]:
        q = re.split(sep_pat, q, flags=re.I)[0].strip(" ,;:-")
    q = normalize_text(q)
    return q

def is_trustworthy_std_query(x: Any) -> bool:
    s = normalize_text(x)
    if not s:
        return False
    if len(s) < 4 or len(s) > 120:
        return False
    sl = s.lower()
    words = sl.split()
    if any(p in sl for p in BAD_STD_QUERY_PHRASES):
        return False
    if len(words) > 8:
        return False
    if re.fullmatch(r"[A-Z0-9\-_/]+", s):
        return False
    if re.fullmatch(r"[A-Z][a-z]+", s):
        return False
    if sl in GENERIC_SINGLE_WORDS or looks_like_nonchemical_generic(sl):
        return False
    if re.match(r"^(and|or|but|also|then|so|yet|nor|for|as|if|which|that)\b", sl):
        return False
    if re.match(r"^(followed|detected|reported|modified|coated|immersed|dissolved|mixed|added|placed|soaked|dipped|treated|blended|combined|used|prepared|obtained|described|observed|showed|found|noted|maintained|enhanced|optimized|deposited)\b", sl):
        return False
    if re.search(r"\b(role of|illustrates the structure of|different organic solvents|weight loss|step between|maintained at|deposited on|solution of|supported by|supplied by|purchased from)\b", sl):
        return False
    if re.search(r",\s*[A-Z][a-z]+$", s):
        return False
    if s.count("(") != s.count(")") or s.count("（") != s.count("）"):
        return False
    if len(words) == 1:
        if len(s) < 5:
            return False
        if re.fullmatch(r"[a-z]+", s) and not any(sl.endswith(h) for h in CHEMICAL_ENDING_HINTS):
            return False
    return True


def looks_like_single_cid_candidate(query: Any, original_name: Any = "") -> bool:
    q = normalize_pubchem_query(query)
    if not is_trustworthy_std_query(q):
        return False
    ql = q.lower()
    if query_looks_material(q) or detect_material_from_text(q):
        return False
    materialish = ["nanoparticle", "nanotube", "framework", "mcm-41", "mof", "graphene oxide", "quantum dots", "membrane", "composite", "polymer", "copolymer"]
    if any(k in ql for k in materialish):
        return False
    if re.search(r"\b(series|model|grade|commercial|product|membrane)\b", ql):
        return False
    return True

def call_alignment_guard_llm(token: str, candidate: str, context_windows: List[str], base_mod, api_key: str, model: str, timeout: int = 60) -> Dict[str, Any]:
    if not api_key:
        return {}
    user_content = {
        "token": token,
        "candidate": candidate,
        "context_windows": context_windows[:3],
    }
    system_msg = (
        "You verify whether an extracted candidate is a clean standalone chemical/material entity name suitable for a structured database. "
        "Reject sentence fragments, process phrases, supplier names, generic words, and partial names. "
        "If possible, normalize the candidate into the cleanest standalone entity name. "
        "Return strict JSON only: "
        "{\"is_valid_entity\":true/false,\"clean_name\":\"normalized clean entity name or empty\",\"entity_type\":\"SPECIFIC_CHEMICAL|MATERIAL_CLASS|COMMERCIAL_PRODUCT|NO_SPECIFIC_CHEMICAL|NO_CLUE\",\"confidence\":0.0,\"reason\":\"short explanation\",\"retry_backtrace\":true/false}"
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
        return {
            "is_valid_entity": bool(parsed.get("is_valid_entity", False)),
            "clean_name": normalize_text(parsed.get("clean_name", "")),
            "entity_type": str(parsed.get("entity_type", "NO_CLUE")).upper(),
            "confidence": float(parsed.get("confidence", 0) or 0),
            "reason": normalize_text(parsed.get("reason", ""))[:300],
            "retry_backtrace": bool(parsed.get("retry_backtrace", False)),
        }
    except Exception:
        return {}


def align_candidate_for_pubchem(token: str, candidate: str, context_windows: List[str], base_mod, api_key: str, model: str, strict_abbr: bool = False) -> Dict[str, Any]:
    raw = normalize_pubchem_query(candidate)
    trusted = is_trustworthy_std_query(raw)
    if strict_abbr:
        trusted = trusted and is_trusted_abbreviation_expansion(raw, token)
    entity_type = "SPECIFIC_CHEMICAL" if (trusted and looks_like_single_cid_candidate(raw, token)) else ("NO_CLUE" if not trusted else "NO_SPECIFIC_CHEMICAL")
    return {
        "accepted": trusted,
        "clean_name": raw if trusted else "",
        "entity_type": entity_type,
        "reason": "rule_gate_pass" if trusted else "rule_gate_fail",
        "retry_backtrace": not trusted,
        "confidence": 0.62 if trusted else 0.0,
    }

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


def call_candidate_reviewer_llm(
    token: str,
    candidates: List[Dict[str, Any]],
    context_windows: List[str],
    base_mod,
    api_key: str,
    model: str,
    timeout: int = 60,
) -> Dict[str, Any]:
    """规则抽出候选后，让 LLM 当评委判断哪个最可信、是否是干净的化学名。"""
    if not api_key:
        return {}

    cand_list = [
        {"name": c.get("candidate", ""), "rule": c.get("rule", ""),
         "evidence": c.get("evidence", "")[:300]}
        for c in candidates[:6] if c.get("candidate")
    ]
    user_content = {
        "abbreviation": token,
        "regex_candidates": cand_list,
        "context": [w[:500] for w in context_windows[:3]],
    }
    system_msg = (
        "You review candidate chemical/material names extracted by regex from a scientific paper. "
        "Given an abbreviation and candidate expansions, judge which (if any) is a credible, "
        "COMPLETE chemical name or material name.\n\n"
        "REJECT these — they are NOT valid names:\n"
        "- Sentence fragments: 'followed by immersion in X acid'\n"
        "- Process descriptions: 'modified by coating the copolymer of'\n"
        "- Connector-prefixed: 'or (3-bromopropyl)phosphonic acid', 'and polyamide'\n"
        "- Brand/supplier info: 'Sigma-Aldrich', 'Hydranautics, USA'\n"
        "- Generic words: 'glass', 'membrane', 'gold', 'hydrophilicity'\n"
        "- Incomplete/unbalanced parenthetical content\n"
        "- Weight/measurement phrases: 'weight loss step between'\n\n"
        "ACCEPT these as valid names:\n"
        "- Complete compound names: 'trimesoyl chloride', '3-aminopropyltriethoxysilane'\n"
        "- Complete material names: 'amino-functionalized graphene oxide'\n"
        "- Specific polymer names: 'poly(vinylidene fluoride)'\n\n"
        "Return strict JSON:\n"
        "{\"best_candidate\":\"the clean complete name, or empty string if NONE are good\","
        "\"entity_type\":\"SPECIFIC_CHEMICAL|MATERIAL_CLASS|COMMERCIAL_PRODUCT|NO_CLUE\","
        "\"confidence\":0.0-1.0,"
        "\"is_clean_name\":true or false,"
        "\"reason\":\"one-sentence explanation\"}"
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
        return {
            "best_candidate": normalize_text(parsed.get("best_candidate", "")),
            "entity_type": str(parsed.get("entity_type", "NO_CLUE")).upper(),
            "confidence": float(parsed.get("confidence", 0) or 0),
            "is_clean_name": bool(parsed.get("is_clean_name", False)),
            "reason": normalize_text(parsed.get("reason", ""))[:300],
        }
    except Exception:
        return {}


def call_abbr_expansion_llm(
    abbreviation: str,
    first_occurrence_contexts: List[str],
    general_contexts: List[str],
    base_mod,
    api_key: str,
    model: str,
    timeout: int = 60,
) -> Dict[str, Any]:
    """让 DeepSeek 根据缩写前几次出现的上下文直接判断其全称和类型。"""
    if not api_key:
        return {}

    user_content = {
        "abbreviation": abbreviation,
        "first_occurrences": first_occurrence_contexts[:3],
        "additional_context": general_contexts[:2],
    }
    system_msg = (
        "You are a chemistry/materials science expert. "
        "Given an abbreviation used in a scientific paper and context excerpts around its "
        "first few occurrences, determine what it stands for.\n\n"
        "Instructions:\n"
        "1. Look for explicit definitions: 'full name (ABBR)', 'ABBR = full name', "
        "'ABBR, also known as full name', etc.\n"
        "2. If explicitly defined, extract ONLY the clean, complete full name — "
        "no process verbs, connector words, or sentence fragments.\n"
        "   Good: 'piperazine'  Bad: 'followed by piperazine' or 'aqueous piperazine solution'\n"
        "3. Determine entity_type:\n"
        "   SPECIFIC_CHEMICAL — a specific compound (e.g. trimesoyl chloride, piperazine)\n"
        "   MATERIAL_CLASS — functionalized materials, nanoparticles, composites, polymers "
        "(e.g. graphene oxide, amino-functionalized silica)\n"
        "   COMMERCIAL_PRODUCT — commercial product, brand, or membrane model "
        "(e.g. Nafion 117, BW30, FilmTec)\n"
        "   NO_SPECIFIC_CHEMICAL — the abbreviation refers to a technique, parameter, "
        "measurement, or non-chemical concept\n"
        "   NO_CLUE — cannot determine from context\n"
        "4. If not explicitly defined and you can only infer, set is_explicitly_defined=false "
        "and lower confidence.\n"
        "5. If context is insufficient, return entity_type NO_CLUE.\n\n"
        "Return strict JSON:\n"
        "{\"abbreviation\":\"input abbreviation\","
        "\"is_explicitly_defined\":true/false,"
        "\"full_name\":\"complete chemical/material name, or empty string\","
        "\"entity_type\":\"SPECIFIC_CHEMICAL|MATERIAL_CLASS|COMMERCIAL_PRODUCT|"
        "NO_SPECIFIC_CHEMICAL|NO_CLUE\","
        "\"confidence\":0.0-1.0,"
        "\"reason\":\"one-sentence explanation\"}"
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
        return {
            "abbreviation": normalize_text(parsed.get("abbreviation", abbreviation)),
            "is_explicitly_defined": bool(parsed.get("is_explicitly_defined", False)),
            "full_name": normalize_text(parsed.get("full_name", "")),
            "entity_type": str(parsed.get("entity_type", "NO_CLUE")).upper(),
            "confidence": float(parsed.get("confidence", 0) or 0),
            "reason": normalize_text(parsed.get("reason", ""))[:300],
        }
    except Exception:
        return {}


def call_nonabbr_resolver_llm(
    token: str,
    windows: List[str],
    raw_meta: Dict[str, str],
    base_mod,
    api_key: str,
    model: str,
    timeout: int = 90,
) -> Dict[str, Any]:
    """对非缩写条目，让 DeepSeek 根据上下文判断其更规范的原始化学全称。
    与 call_context_resolver_llm 的区别：
    - 不限于 NO_HIT/NO_SPECIFIC_CHEMICAL 状态
    - 明确要求返回 entity_type 细分（polymer/composite/commercial_product 等）
    - 对已有名称也要求 DeepSeek 确认是否更规范
    """
    if not api_key:
        return {}

    user_content = {"token": token, "raw_meta": raw_meta, "context_windows": windows[:6]}
    system_msg = (
        "You are a chemistry/materials science expert reviewing chemical names from membrane science papers. "
        "Given a chemical name (token) and context excerpts from the paper, determine:\n"
        "1. What is the most accurate, standardized chemical name for this entity?\n"
        "2. What type of entity is it?\n\n"
        "entity_type options:\n"
        "  specific_chemical — a specific small molecule, salt, monomer, reagent (e.g. piperazine, trimesoyl chloride)\n"
        "  polymer — a polymer or copolymer (e.g. polyethersulfone, polysulfone)\n"
        "  composite — a composite material or blend\n"
        "  material — functionalized material, nanoparticle, framework, modified surface\n"
        "  commercial_product — commercial membrane, brand name, product code\n"
        "  no_specific_chemical — technique, parameter, measurement, process description\n"
        "  no_clue — cannot determine from context\n\n"
        "Instructions:\n"
        "- If the token IS already a clean, standard chemical name, confirm it and return it as query.\n"
        "- If the token is an informal/abbreviated/trade name, return the proper IUPAC or common chemical name.\n"
        "- Only set entity_type=specific_chemical if it is a discrete compound searchable in PubChem.\n"
        "- Do NOT return sentence fragments, process descriptions, or supplier names as query.\n\n"
        "Return strict JSON:\n"
        "{\"entity_type\":\"specific_chemical|polymer|composite|material|commercial_product|no_specific_chemical|no_clue\","
        "\"query\":\"best standardized name for PubChem lookup, or empty string\","
        "\"composition\":[\"component1\",\"component2\"],"
        "\"confidence\":0.0-1.0,"
        "\"evidence\":\"short evidence snippet\","
        "\"note\":\"short explanation\"}"
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
        etype = str(parsed.get("entity_type", "no_clue")).lower()
        comp = parsed.get("composition", [])
        if not isinstance(comp, list):
            comp = []
        return {
            "entity_type": etype,
            "query": normalize_text(parsed.get("query", "")),
            "composition": [normalize_text(x) for x in comp if normalize_text(x)][:5],
            "confidence": float(parsed.get("confidence", 0) or 0),
            "evidence": normalize_text(parsed.get("evidence", ""))[:700],
            "note": normalize_text(parsed.get("note", ""))[:300],
        }
    except Exception:
        return {}



def run_full_enrichment_pipeline(
    query: str,
    original_name: str,
    base_mod,
    pubchem_cache,
    rewrite_cache,
    material_cache,
    api_key: str,
    model: str,
    base_notes: str,
):
    """
    Mirror the full code_extract_name_fixed.py pipeline for a single candidate query:
      1. call_deepseek_batch() to classify (SINGLE / MIXTURE / POLYMER / ABBR / ...)
      2. For MIXTURE: split components, lookup each, pick best
      3. For ABBR: use abbr_candidates from DeepSeek, resolve each, pick best
      4. For SINGLE / SINGLE_NOISY / fallback: use single_query then q_norm
      5. apply_no_hit_reclassification() on every PubChem result
    Returns (chem_dict, notes_str).
    """
    q_norm = normalize_pubchem_query(query)

    # Score helper
    def status_score(s):
        return {"OK": 10, "ABBR_EXPANDED_OK": 10, "CID_ONLY": 6,
                "NO_SPECIFIC_CHEMICAL": 2, "Polymer": 2,
                "NO_HIT": 1, "ABBR_EXPANDED_NO_HIT": 1, "EMPTY_QUERY": 0}.get(str(s), 1)

    def do_pubchem(q, record_type="backtrace"):
        """Lookup q in PubChem then reclassify NO_HIT."""
        q = normalize_pubchem_query(q)
        if not q:
            return {"Lookup_Status": "EMPTY_QUERY", "Standardized_Query": ""}, base_notes
        chem = base_mod.pubchem_lookup(
            q, pubchem_cache,
            api_key=api_key, model=model,
            rewrite_cache=rewrite_cache, material_cache=material_cache,
        )
        chem = dict(chem)
        chem.setdefault("Standardized_Query", q)
        chem["_PubChem_Query"] = q
        if hasattr(base_mod, "apply_no_hit_reclassification"):
            chem, notes = base_mod.apply_no_hit_reclassification(
                chem=chem, query=q, original_name=original_name,
                record_type=record_type,
                api_key=api_key, model=model,
                pubchem_cache=pubchem_cache,
                rewrite_cache=rewrite_cache,
                material_cache=material_cache,
                base_notes=base_notes,
            )
        else:
            notes = base_notes
        return chem, notes

    # Step 1: DeepSeek classification
    ds_result = None
    if api_key and hasattr(base_mod, "call_deepseek_batch"):
        try:
            batch = base_mod.call_deepseek_batch([(1, q_norm)], api_key=api_key, model=model)
            ds_result = batch.get(1)
        except Exception as e:
            pass  # fall through to direct lookup

    if not ds_result:
        # No DeepSeek result — fall back to direct lookup
        return do_pubchem(q_norm, "backtrace")

    category = str(ds_result.get("category", "SINGLE")).upper()
    single_query = normalize_pubchem_query(str(ds_result.get("single_query", "") or ""))
    components = ds_result.get("components", [])
    abbr_candidates = ds_result.get("abbr_candidates", [])
    ds_notes = str(ds_result.get("notes", "") or "")

    best_chem = {"Lookup_Status": "EMPTY_QUERY", "Standardized_Query": q_norm}
    best_notes = "; ".join(x for x in [base_notes, f"ds_cat={category}", ds_notes] if x)
    best_score = -1

    def update_best(chem, notes):
        nonlocal best_chem, best_notes, best_score
        sc = status_score(chem.get("Lookup_Status"))
        if sc > best_score:
            best_score = sc
            best_chem = chem
            best_notes = notes
        return sc >= status_score("OK")  # True if we got a hit → can short-circuit

    # ------------------------------------------------------------------
    # Step 2: Category-specific handling
    # ------------------------------------------------------------------

    if category in ("POLYMER", "PROCESS", "NON_ENTITY"):
        # These are not directly lookup-able as single compounds
        lstat = "Polymer" if category == "POLYMER" else "NO_SPECIFIC_CHEMICAL"
        best_chem = {"Lookup_Status": lstat, "Standardized_Query": q_norm}
        best_notes = "; ".join(x for x in [base_notes, f"ds_cat={category}", ds_notes] if x)
        return best_chem, best_notes

    elif category == "MIXTURE":
        # Use LLM-provided components first; fall back to simple split
        comp_list = components if components else []
        if not comp_list and hasattr(base_mod, "split_mixture_components"):
            try:
                comp_list = base_mod.split_mixture_components(q_norm, {}, set())
            except Exception:
                comp_list = []

        if not comp_list:
            # single fallback
            comp_list = [{"name": single_query or q_norm, "role": "primary", "rank": 1}]

        for comp in sorted(comp_list, key=lambda c: c.get("rank", 99)):
            cname = normalize_pubchem_query(str(comp.get("name", "") or ""))
            if not cname:
                continue
            # Check if this component looks like an abbreviation
            is_abbr_comp = (
                hasattr(base_mod, "is_likely_abbr_token") and base_mod.is_likely_abbr_token(cname)
            )
            if is_abbr_comp and api_key and hasattr(base_mod, "resolve_abbr_candidate_once"):
                try:
                    resolved = base_mod.resolve_abbr_candidate_once(
                        cname, api_key=api_key, model=model, context_name=original_name
                    )
                    if resolved.get("status") == "CHEMICAL" and resolved.get("query"):
                        cname = normalize_pubchem_query(resolved["query"])
                except Exception:
                    pass
            chem, notes = do_pubchem(cname, "backtrace_mixture")
            if update_best(chem, notes):
                return best_chem, best_notes

    elif category == "ABBR":
        # Try each abbr_candidate from DeepSeek
        candidates = sorted(abbr_candidates, key=lambda c: -float(c.get("confidence", 0) or 0))
        if not candidates:
            # Ask DeepSeek to resolve the abbreviation directly
            if api_key and hasattr(base_mod, "resolve_abbr_candidate_once"):
                try:
                    resolved = base_mod.resolve_abbr_candidate_once(
                        q_norm, api_key=api_key, model=model, context_name=original_name
                    )
                    if resolved.get("status") == "CHEMICAL" and resolved.get("query"):
                        candidates = [{"query": resolved["query"], "confidence": 0.8}]
                except Exception:
                    pass

        for cand in candidates[:3]:
            cq = normalize_pubchem_query(str(cand.get("query", "") or ""))
            if not cq:
                continue
            chem, notes = do_pubchem(cq, "backtrace_abbr")
            if update_best(chem, notes):
                return best_chem, best_notes

        # Also try the raw q_norm as fallback
        chem, notes = do_pubchem(q_norm, "backtrace_abbr")
        update_best(chem, notes)

    else:
        # SINGLE, SINGLE_NOISY, or unknown — try single_query first, then q_norm
        queries_to_try = []
        if single_query and single_query.lower() != q_norm.lower():
            queries_to_try.append(single_query)
        queries_to_try.append(q_norm)
        # Also build simple variants (head before comma, strip suffix, etc.)
        for raw_q in list(queries_to_try):
            if "," in raw_q:
                head = normalize_pubchem_query(raw_q.split(",", 1)[0].strip())
                if head and head.lower() not in [x.lower() for x in queries_to_try]:
                    queries_to_try.append(head)

        for q_try in queries_to_try:
            chem, notes = do_pubchem(q_try, "backtrace")
            if update_best(chem, notes):
                return best_chem, best_notes

    return best_chem, best_notes


def try_lookup_query(
    query: str,
    original_name: str,
    base_mod,
    pubchem_cache,
    rewrite_cache,
    material_cache,
    api_key: str,
    model: str,
    base_notes: str,
):
    """Thin wrapper — delegates to run_full_enrichment_pipeline()."""
    return run_full_enrichment_pipeline(
        query=query,
        original_name=original_name,
        base_mod=base_mod,
        pubchem_cache=pubchem_cache,
        rewrite_cache=rewrite_cache,
        material_cache=material_cache,
        api_key=api_key,
        model=model,
        base_notes=base_notes,
    )


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
        "MD_Candidate_Tried": 0,
        "MD_Candidate_List": "",
        "MD_Accepted_Because": "",
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


def rank_status(status: str) -> int:
    order = {
        "BACKTRACE_OK": 7,
        "BACKTRACE_NAME_OK": 6,
        "BACKTRACE_CLASSIFIED": 5,
        "EXPANSION_FOUND_BUT_PUBCHEM_NO_HIT": 4,
        "LLM_EXPANSION_BUT_PUBCHEM_NO_HIT": 4,
        "CONTEXT_FOUND_BUT_NO_EXPANSION": 3,
        "TOKEN_NOT_IN_MD": 2,
        "MD_NOT_FOUND": 1,
        "RAW_ROW_NOT_FOUND": 0,
        "MISSING_SOURCE_RECORDINDEX": 0,
    }
    return order.get(status, -1)

def process_one_source(
    row: pd.Series,
    source_idx: int,
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

    best = init_bt_result(row, source_idx)
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

    candidates = get_md_candidates(raw_row, md_catalog)
    if not candidates:
        best["Backtrace_Status"] = "MD_NOT_FOUND"
        best["Backtrace_Notes"] = "Unable to locate any MD candidate from raw metadata"
        return best

    best["MD_Candidate_List"] = " | ".join([f"{m}::{how}" for (m, how) in candidates[:12]])

    def _has_strong_token_hit(matched_toks: List[str], orig: str) -> bool:
        """原始名或紧密变体命中 → 强命中。用于 title/fuzzy 候选的把关。"""
        orig_n = normalize_text(orig).lower()
        if not orig_n or len(orig_n) < 2:
            return bool(matched_toks)
        if len(orig_n) <= 5:
            return bool(matched_toks)
        strong_forms: set = {orig_n}
        for ch in ["-", "_", " ", "/"]:
            v = orig_n.replace(ch, "")
            if v:
                strong_forms.add(v)
            v2 = orig_n.replace(ch, " ").strip()
            if v2:
                strong_forms.add(v2)
        trimmed = re.sub(r"^[\d,]+\s*-\s*", "", orig_n)
        if trimmed and trimmed != orig_n:
            strong_forms.add(trimmed)
        stripped = re.sub(r"[-_]?\d+$", "", orig_n)
        if stripped and stripped != orig_n and len(stripped) >= 3:
            strong_forms.add(stripped)
        parts = [p for p in re.split(r"[-_/]", orig_n) if p]
        if parts:
            longest_part = max(parts, key=len)
            if len(longest_part) >= 3:
                strong_forms.add(longest_part)
        upper_orig = orig_n.upper()
        for alias_key, alias_vals in TOKEN_ALIAS_MAP.items():
            if alias_key in upper_orig:
                for av in alias_vals:
                    strong_forms.add(av.lower())
        for tok in matched_toks:
            tl = tok.lower()
            if tl in strong_forms:
                return True
            if len(tl) >= 3 and any(tl in sf for sf in strong_forms):
                return True
            if len(tl) >= 3 and any(sf in tl for sf in strong_forms if len(sf) >= 3):
                return True
        return False

    md_candidate_tried = 0
    for member, locate_method in candidates:
        md_candidate_tried += 1
        md_text = read_md_member_cached(zip_path, member, md_cache)
        tokens = generate_search_tokens(original_name, standardized_query)
        windows, matched_tokens = collect_context_windows(md_text, tokens)
        if not windows:
            continue
        if locate_method not in ("doi", "filename", "filename_stem"):
            if not _has_strong_token_hit(matched_tokens, original_name):
                continue
        best["MD_File"] = member
        best["MD_Locate_Method"] = locate_method
        best["MD_Candidate_Tried"] = md_candidate_tried
        accept_reason = "token_hit+biblio" if locate_method in ("doi", "filename", "filename_stem") else "strong_token_hit"
        best["MD_Accepted_Because"] = f"{accept_reason}; method={locate_method}; matched={'|'.join(matched_tokens[:4])}"

        # ── 缩写主流程：规则定位第一次出现 + DeepSeek 判断全称 ──
        is_abbr = looks_like_abbreviation(original_name)
        if is_abbr and api_key and enable_llm:
            first_occ = find_first_occurrences(md_text, original_name, max_occ=3, radius=400)
            if not first_occ:
                compact = re.sub(r"[-_\s]+", "", original_name)
                if compact.lower() != original_name.lower() and len(compact) >= 2:
                    first_occ = find_first_occurrences(md_text, compact, max_occ=3, radius=400)

            if first_occ:
                llm_exp = call_abbr_expansion_llm(
                    abbreviation=original_name,
                    first_occurrence_contexts=first_occ,
                    general_contexts=windows[:2],
                    base_mod=base_mod,
                    api_key=api_key,
                    model=model,
                )
                if llm_exp and llm_exp.get("entity_type", "NO_CLUE") != "NO_CLUE":
                    exp_full = normalize_text(llm_exp.get("full_name", ""))
                    exp_type = llm_exp.get("entity_type", "NO_CLUE")
                    exp_conf = llm_exp.get("confidence", 0)
                    exp_defined = llm_exp.get("is_explicitly_defined", False)
                    exp_reason = llm_exp.get("reason", "")
                    exp_note = (
                        f"llm_abbr_exp: type={exp_type}, conf={exp_conf:.2f}, "
                        f"defined={exp_defined}, name={exp_full[:60]}"
                    )

                    if exp_type in ("MATERIAL_CLASS", "COMMERCIAL_PRODUCT", "NO_SPECIFIC_CHEMICAL") and exp_conf >= 0.55:
                        best.update({
                            "Backtrace_Status": "BACKTRACE_CLASSIFIED",
                            "BT_Lookup_Status": "NO_SPECIFIC_CHEMICAL",
                            "BT_Match_Source": "MD Backtrace + DeepSeek",
                            "Resolved_Full_Name": exp_full,
                            "Evidence_Snippet": first_occ[0][:700],
                            "Backtrace_Confidence": exp_conf,
                            "Backtrace_Notes": merge_notes(f"md_locate={locate_method}", exp_note),
                        })
                        return best

                    if exp_full and exp_type == "SPECIFIC_CHEMICAL":
                        aligned = align_candidate_for_pubchem(
                            token=original_name,
                            candidate=exp_full,
                            context_windows=first_occ + windows[:2],
                            base_mod=base_mod,
                            api_key="",
                            model=model,
                            strict_abbr=True,
                        )
                        if aligned.get("accepted"):
                            cleaned = aligned.get("clean_name", "")
                            current = init_bt_result(row, source_idx)
                            current.update({
                                "Raw_FileName": raw_file,
                                "Raw_DOI": raw_doi,
                                "Raw_Title": raw_title,
                                "MD_File": member,
                                "MD_Locate_Method": locate_method,
                                "MD_Candidate_Tried": md_candidate_tried,
                                "Resolved_Full_Name": cleaned,
                                "BT_Standardized_Query": cleaned,
                                "Backtrace_Confidence": max(exp_conf, aligned.get("confidence", 0)),
                                "Evidence_Snippet": first_occ[0][:700],
                                "Backtrace_Notes": merge_notes(f"md_locate={locate_method}", exp_note, aligned.get("reason", "")),
                            })
                            if query_looks_material(cleaned):
                                current["Backtrace_Status"] = "BACKTRACE_CLASSIFIED"
                                current["BT_Lookup_Status"] = "NO_SPECIFIC_CHEMICAL"
                                current["BT_Match_Source"] = "MD Backtrace + DeepSeek"
                                return current
                            chem, notes1 = try_lookup_query(
                                query=cleaned,
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
                            current["Backtrace_Status"] = "BACKTRACE_NAME_OK"
                            current["BT_Lookup_Status"] = "EXPANSION_FOUND_BUT_PUBCHEM_NO_HIT"
                            best = current

                    best["Backtrace_Notes"] = merge_notes(
                        best.get("Backtrace_Notes", ""),
                        exp_note,
                        "LLM expansion did not pass abbreviation quality gate; trying regex fallback",
                    )

        # ── 非缩写条目：无论任何状态，都让 DeepSeek 确认最终名称 ──
        regex_candidates = []
        class_only = None
        if (not is_abbr) and enable_llm and api_key:
            raw_meta = {"file_name": raw_file, "doi": raw_doi, "title": raw_title}
            # 使用新的非缩写专用 LLM：判断更规范的化学全称和类型
            pre_llm = call_nonabbr_resolver_llm(
                token=original_name,
                windows=windows,
                raw_meta=raw_meta,
                base_mod=base_mod,
                api_key=api_key,
                model=model,
            )
            if pre_llm:
                etype = pre_llm.get("entity_type", "no_clue")
                best["Backtrace_Notes"] = merge_notes(
                    best.get("Backtrace_Notes", ""),
                    f"nonabbr_llm: type={etype}, conf={pre_llm.get('confidence', 0):.2f}"
                )
                # 材料/聚合物/复合/商品/非化学实体 → 直接分类
                if etype in ("polymer", "composite", "material", "commercial_product", "no_specific_chemical"):
                    best.update({
                        "Backtrace_Status": "BACKTRACE_CLASSIFIED",
                        "BT_Lookup_Status": "NO_SPECIFIC_CHEMICAL",
                        "BT_Match_Source": "MD Backtrace + DeepSeek",
                        "Resolved_Full_Name": pre_llm.get("query", "") or original_name,
                        "Resolved_Composition": " | ".join(pre_llm.get("composition", [])),
                        "Evidence_Snippet": pre_llm.get("evidence", "") or (windows[0][:700] if windows else ""),
                        "Backtrace_Confidence": pre_llm.get("confidence", 0),
                        "Backtrace_Notes": merge_notes(
                            best.get("Backtrace_Notes", ""),
                            pre_llm.get("note", ""),
                            f"md_locate={locate_method}"
                        ),
                    })
                    return best

                # specific_chemical → 规范化 query，强制走 PubChem
                if etype == "specific_chemical" and pre_llm.get("query"):
                    q_raw = pre_llm.get("query", "")
                    q_norm = normalize_pubchem_query(q_raw)
                    if q_norm and is_trustworthy_std_query(q_norm):
                        current = init_bt_result(row, source_idx)
                        current.update({
                            "Raw_FileName": raw_file,
                            "Raw_DOI": raw_doi,
                            "Raw_Title": raw_title,
                            "MD_File": member,
                            "MD_Locate_Method": locate_method,
                            "MD_Candidate_Tried": md_candidate_tried,
                            "Resolved_Full_Name": q_norm,
                            "BT_Standardized_Query": q_norm,
                            "Resolved_Composition": " | ".join(pre_llm.get("composition", [])),
                            "Backtrace_Confidence": pre_llm.get("confidence", 0),
                            "Evidence_Snippet": pre_llm.get("evidence", "") or (windows[0][:700] if windows else ""),
                            "Backtrace_Notes": merge_notes(
                                "nonabbr_deepseek_specific",
                                pre_llm.get("note", ""),
                                f"md_locate={locate_method}"
                            ),
                        })
                        chem, notes_pre = try_lookup_query(
                            query=q_norm,
                            original_name=original_name,
                            base_mod=base_mod,
                            pubchem_cache=pubchem_cache,
                            rewrite_cache=rewrite_cache,
                            material_cache=material_cache,
                            api_key=api_key,
                            model=model,
                            base_notes=current["Backtrace_Notes"],
                        )
                        current["Backtrace_Notes"] = notes_pre
                        current.update({f"BT_{k}": v for k, v in chem.items()})
                        if chem.get("Lookup_Status") in {"OK", "ABBR_EXPANDED_OK", "CID_ONLY"}:
                            current["Backtrace_Status"] = "BACKTRACE_OK"
                            return current
                        if chem.get("Lookup_Status") in {"Polymer", "NO_SPECIFIC_CHEMICAL"}:
                            current["Backtrace_Status"] = "BACKTRACE_CLASSIFIED"
                            best = current if rank_status(current.get("Backtrace_Status", "")) > rank_status(best.get("Backtrace_Status", "")) else best
                        else:
                            current["Backtrace_Status"] = "EXPANSION_FOUND_BUT_PUBCHEM_NO_HIT"
                            current["BT_Lookup_Status"] = "EXPANSION_FOUND_BUT_PUBCHEM_NO_HIT"
                            best = current if rank_status(current.get("Backtrace_Status", "")) > rank_status(best.get("Backtrace_Status", "")) else best
                elif etype == "no_clue":
                    # DeepSeek 无法判断 → 继续规则回溯，最终保留 NO_CLUE
                    best["Backtrace_Notes"] = merge_notes(best.get("Backtrace_Notes", ""), "nonabbr_llm=NO_CLUE, falling through to regex")

        for tok in matched_tokens[:8]:
            for w in windows[:8]:
                for rc in extract_candidates_from_context(tok, w):
                    if rc.get("class_hint"):
                        if class_only is None or rc.get("score", 0) > class_only.get("score", 0):
                            class_only = rc
                    else:
                        regex_candidates.append(rc)

        dedup = {}
        for rc in regex_candidates:
            key = normalize_text(rc.get("candidate", "")).lower()
            if not key:
                continue
            if key not in dedup or rc.get("score", 0) > dedup[key].get("score", 0):
                dedup[key] = rc
        ranked = sorted(dedup.values(), key=lambda x: x.get("score", 0), reverse=True)
        if is_abbr:
            ranked = [r for r in ranked if is_trusted_abbreviation_expansion(r.get("candidate", ""), original_name)]

        if enable_llm and api_key and (ranked or windows):
            review = call_candidate_reviewer_llm(
                token=original_name,
                candidates=ranked[:6],
                context_windows=windows[:4],
                base_mod=base_mod,
                api_key=api_key,
                model=model,
            )
            if review:
                review_note = f"llm_review: type={review.get('entity_type','')}, conf={review.get('confidence',0):.2f}, clean={review.get('is_clean_name',False)}, reason={review.get('reason','')}"
                best["Backtrace_Notes"] = merge_notes(best.get("Backtrace_Notes", ""), review_note)

                if review.get("entity_type") in ("MATERIAL_CLASS", "COMMERCIAL_PRODUCT") and not review.get("is_clean_name"):
                    best["Backtrace_Status"] = "BACKTRACE_CLASSIFIED"
                    best["BT_Lookup_Status"] = "NO_SPECIFIC_CHEMICAL"
                    best["BT_Match_Source"] = "MD Backtrace + LLM Review"
                    best["Evidence_Snippet"] = windows[0][:700] if windows else ""
                    return best

                if review.get("is_clean_name") and review.get("best_candidate"):
                    llm_cand = review["best_candidate"]
                    if (not is_abbr) or is_trusted_abbreviation_expansion(llm_cand, original_name):
                        ranked = [
                            {"candidate": llm_cand, "score": max(0.92, review.get("confidence", 0.9)),
                             "rule": "llm_reviewed", "evidence": review.get("reason", "")}
                        ] + [r for r in ranked if normalize_text(r.get("candidate", "")).lower() != llm_cand.lower()]
                    else:
                        best["Backtrace_Notes"] = merge_notes(best.get("Backtrace_Notes", ""), f"llm_review_rejected={llm_cand}")
                elif review.get("confidence", 0) >= 0.6 and not review.get("is_clean_name") and ranked:
                    best["Backtrace_Status"] = "CONTEXT_FOUND_BUT_NO_EXPANSION"
                    best["Evidence_Snippet"] = windows[0][:700] if windows else ""
                    best["Backtrace_Notes"] = merge_notes(
                        best.get("Backtrace_Notes", ""),
                        "LLM reviewer rejected all regex candidates as not clean names",
                    )
                    return best

        if detect_material_from_text(original_name, standardized_query, " ".join(windows[:3])) and not ranked:
            best["Backtrace_Status"] = "BACKTRACE_CLASSIFIED"
            best["BT_Lookup_Status"] = "NO_SPECIFIC_CHEMICAL"
            best["BT_Match_Source"] = "MD Backtrace"
            best["Evidence_Snippet"] = windows[0][:700]
            best["Backtrace_Notes"] = merge_notes(f"md_locate={locate_method}", "material_like_context")
            return best

        for rc in ranked[:DEFAULT_MAX_REGEX_CANDIDATES]:
            query = normalize_text(rc.get("candidate", ""))
            if is_abbr and not is_trusted_abbreviation_expansion(query, original_name):
                continue
            aligned = align_candidate_for_pubchem(
                token=original_name,
                candidate=query,
                context_windows=windows,
                base_mod=base_mod,
                api_key="",
                model=model,
                strict_abbr=is_abbr,
            )
            if not aligned.get("accepted"):
                if aligned.get("retry_backtrace"):
                    best["Backtrace_Notes"] = merge_notes(best.get("Backtrace_Notes", ""), f"candidate_rejected={query}", aligned.get("reason", ""))
                continue
            query = aligned.get("clean_name", query)
            current = init_bt_result(row, source_idx)
            current.update({
                "Raw_FileName": raw_file,
                "Raw_DOI": raw_doi,
                "Raw_Title": raw_title,
                "MD_File": member,
                "MD_Locate_Method": locate_method,
                "MD_Candidate_Tried": md_candidate_tried,
                "Resolved_Full_Name": query,
                "BT_Standardized_Query": query,
                "Backtrace_Confidence": max(rc.get("score", 0), aligned.get("confidence", 0)),
                "Evidence_Snippet": rc.get("evidence", ""),
                "Backtrace_Notes": merge_notes(f"backtrace_rule={rc.get('rule', '')}", f"md_locate={locate_method}", f"matched_tokens={'|'.join(matched_tokens[:4])}", aligned.get("reason", "")),
            })

            if query_looks_material(query) or detect_material_from_text(query, current["Evidence_Snippet"]):
                current["Backtrace_Status"] = "BACKTRACE_CLASSIFIED"
                current["BT_Lookup_Status"] = "NO_SPECIFIC_CHEMICAL"
                current["BT_Match_Source"] = "MD Backtrace"
                current["Backtrace_Notes"] = merge_notes(current["Backtrace_Notes"], "material_like_query")
                best = current if best is None or rank_status(current.get("Backtrace_Status", "")) > rank_status(best.get("Backtrace_Status", "")) else best
                continue

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
                best = current if best is None or rank_status(current.get("Backtrace_Status", "")) > rank_status(best.get("Backtrace_Status", "")) else best
                continue
            if is_abbr:
                current["Backtrace_Status"] = "BACKTRACE_NAME_OK"
            else:
                current["Backtrace_Status"] = "EXPANSION_FOUND_BUT_PUBCHEM_NO_HIT"
            current["BT_Lookup_Status"] = "EXPANSION_FOUND_BUT_PUBCHEM_NO_HIT"
            best = current

        raw_meta = {"file_name": raw_file, "doi": raw_doi, "title": raw_title}
        llm_res = {}
        if enable_llm:
            llm_res = call_context_resolver_llm(
                token=original_name,
                windows=windows,
                raw_meta=raw_meta,
                base_mod=base_mod,
                api_key=api_key,
                model=model,
            )

        if llm_res:
            current = init_bt_result(row, source_idx)
            current.update({
                "Raw_FileName": raw_file,
                "Raw_DOI": raw_doi,
                "Raw_Title": raw_title,
                "MD_File": member,
                "MD_Locate_Method": locate_method,
                "MD_Candidate_Tried": md_candidate_tried,
                "Resolved_Full_Name": llm_res.get("query", ""),
                "BT_Standardized_Query": normalize_pubchem_query(llm_res.get("query", "")),
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
                aligned = align_candidate_for_pubchem(
                    token=original_name,
                    candidate=llm_res["query"],
                    context_windows=windows,
                    base_mod=base_mod,
                    api_key="",
                    model=model,
                    strict_abbr=is_abbr,
                )
                q = aligned.get("clean_name", "")
                current["Backtrace_Notes"] = merge_notes(current["Backtrace_Notes"], aligned.get("reason", ""))
                if (not aligned.get("accepted")) or query_looks_material(q):
                    current["Backtrace_Status"] = "BACKTRACE_CLASSIFIED"
                    current["BT_Lookup_Status"] = "NO_SPECIFIC_CHEMICAL"
                    return current
                chem, notes2 = try_lookup_query(
                    query=q,
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
                    if is_abbr:
                        current["Backtrace_Status"] = "BACKTRACE_NAME_OK"
                    else:
                        current["Backtrace_Status"] = "EXPANSION_FOUND_BUT_PUBCHEM_NO_HIT"
                    current["BT_Lookup_Status"] = "EXPANSION_FOUND_BUT_PUBCHEM_NO_HIT"
                return current

        if class_only:
            best["Backtrace_Status"] = "BACKTRACE_CLASSIFIED"
            best["Backtrace_Confidence"] = class_only.get("score", 0)
            best["Evidence_Snippet"] = class_only.get("evidence", "")
            best["Backtrace_Notes"] = merge_notes(best.get("Backtrace_Notes", ""), "Context suggests material/commercial series")
            best["BT_Lookup_Status"] = "NO_SPECIFIC_CHEMICAL"
            return best

        if best["Backtrace_Status"] == "NOT_PROCESSED":
            best["Backtrace_Status"] = "CONTEXT_FOUND_BUT_NO_EXPANSION"
            best["Evidence_Snippet"] = windows[0][:700]
            best["Backtrace_Notes"] = merge_notes(
                f"md_locate={locate_method}",
                f"matched_tokens={'|'.join(matched_tokens[:4])}",
                "Found context but no reliable full-name pattern",
            )
        return best

    best["Backtrace_Status"] = "TOKEN_NOT_IN_MD"
    best["MD_Candidate_Tried"] = md_candidate_tried
    best["Backtrace_Notes"] = "No token hit in any MD candidate"
    # 不要把“最像的候选”强行写到 MD_File，避免误导；候选列表已在 MD_Candidate_List
    best["MD_File"] = ""
    best["MD_Locate_Method"] = ""
    return best


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
    max_source_tries: int,
) -> Dict[str, Any]:
    source_idxs = parse_source_record_indices(row.get("Source_RecordIndex", ""))
    if not source_idxs:
        best = init_bt_result(row, "")
        best["Backtrace_Status"] = "MISSING_SOURCE_RECORDINDEX"
        best["Backtrace_Notes"] = "Source_RecordIndex is empty"
        return best

    tried = source_idxs[: max(1, max_source_tries)]
    best: Optional[Dict[str, Any]] = None
    tried_notes: List[str] = []

    for source_idx in tried:
        current = process_one_source(
            row=row,
            source_idx=source_idx,
            raw_df=raw_df,
            zip_path=zip_path,
            md_catalog=md_catalog,
            md_cache=md_cache,
            base_mod=base_mod,
            pubchem_cache=pubchem_cache,
            rewrite_cache=rewrite_cache,
            material_cache=material_cache,
            api_key=api_key,
            model=model,
            enable_llm=enable_llm,
        )
        tried_notes.append(f"{source_idx}:{current.get('Backtrace_Status', '')}")
        if best is None or rank_status(current.get("Backtrace_Status", "")) > rank_status(best.get("Backtrace_Status", "")):
            best = current
        if current.get("Backtrace_Status") == "BACKTRACE_OK":
            break

    assert best is not None
    best["Backtrace_Notes"] = merge_notes(best.get("Backtrace_Notes", ""), f"source_tries={'|'.join(tried_notes)}")
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
        st = merged["Backtrace_Status"].fillna("").astype(str)
        hit_mask = st.eq("BACKTRACE_OK")
        name_mask = st.eq("BACKTRACE_NAME_OK")
        class_mask = st.eq("BACKTRACE_CLASSIFIED")
    else:
        hit_mask = pd.Series(False, index=merged.index)
        name_mask = pd.Series(False, index=merged.index)
        class_mask = pd.Series(False, index=merged.index)

    for dest, src in overwrite_map.items():
        if dest == "Standardized_Query":
            continue
        if src in merged.columns and dest in merged.columns and hit_mask.any():
            bt_vals = merged[src].fillna("").astype(str)
            has_val = bt_vals.str.len() > 0
            mask = hit_mask & has_val
            if mask.any():
                merged[dest] = merged[dest].astype("object")
                merged.loc[mask, dest] = merged.loc[mask, src].fillna("").astype("object")

    if "Standardized_Query" in merged.columns:
        bt_q = merged.get("BT_Standardized_Query", pd.Series("", index=merged.index)).fillna("").astype(str).map(normalize_text)
        res_full = merged.get("Resolved_Full_Name", pd.Series("", index=merged.index)).fillna("").astype(str).map(normalize_text)
        # BACKTRACE_OK：强制写回 Standardized_Query（只要 BT_Standardized_Query 非空且可信）
        # BACKTRACE_NAME_OK：也允许写回（名称可信但 PubChem 未命中）
        writable = hit_mask | name_mask
        bt_q_ok = bt_q.map(is_trustworthy_std_query) & writable
        res_ok = res_full.map(is_trustworthy_std_query) & writable
        if "Original_Name" in merged.columns:
            orig_series = merged["Original_Name"].fillna("").astype(str)
            abbr_mask = orig_series.map(looks_like_abbreviation)
            bt_q_ok = bt_q_ok & (~abbr_mask | bt_q.map(lambda x: is_trusted_abbreviation_expansion(x)))
            res_ok = res_ok & (~abbr_mask | res_full.map(lambda x: is_trusted_abbreviation_expansion(x)))
        merged["Standardized_Query"] = merged["Standardized_Query"].astype("object")
        if bt_q_ok.any():
            merged.loc[bt_q_ok, "Standardized_Query"] = bt_q.loc[bt_q_ok].astype("object")
        fallback = (~bt_q_ok) & res_ok
        if fallback.any():
            merged.loc[fallback, "Standardized_Query"] = res_full.loc[fallback].astype("object")

    if "Lookup_Status" in merged.columns:
        merged["Lookup_Status"] = merged["Lookup_Status"].astype("object")
        if hit_mask.any():
            merged.loc[hit_mask, "Lookup_Status"] = "HIT"
        if name_mask.any():
            vals = merged.get("BT_Lookup_Status", pd.Series("BACKTRACE_NAME_OK", index=merged.index)).fillna("").astype(str)
            idx = vals[name_mask].index
            if len(idx):
                merged.loc[idx, "Lookup_Status"] = vals.loc[idx].replace({"": "BACKTRACE_NAME_OK"})
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
        if name_mask.any():
            merged.loc[name_mask, "Match_Source"] = "MD Backtrace (query only)"
        if class_mask.any():
            merged.loc[class_mask, "Match_Source"] = "MD Backtrace"

    if "Decision_Notes" in merged.columns:
        merged["Decision_Notes"] = merged["Decision_Notes"].fillna("").astype("object")
        resolved = merged.get("Resolved_Full_Name", pd.Series("", index=merged.index)).fillna("").astype(str)
        notes_mask = hit_mask | class_mask | name_mask
        if notes_mask.any():
            merged.loc[notes_mask, "Decision_Notes"] = (
                merged.loc[notes_mask, "Decision_Notes"].astype(str).str.rstrip("; ")
                + "; backtrace_resolved="
                + resolved.loc[notes_mask]
            ).str.strip("; ")

    return merged

try:
    from openpyxl.cell.cell import ILLEGAL_CHARACTERS_RE as _ILLEGAL_CHARS_RE
except ImportError:
    _ILLEGAL_CHARS_RE = re.compile(r"[\x00-\x08\x0B\x0C\x0E-\x1F]")
_EXCEL_MAX_CELL_LENGTH = 32767


def sanitize_df_for_excel(df: pd.DataFrame) -> pd.DataFrame:
    """去掉所有字符串列中 openpyxl 不允许写入的控制字符，防止 IllegalCharacterError。
    同时截断超过 Excel 单元格最大长度（32767）的文本。
    """
    out = df.copy()
    for col in out.columns:
        if out[col].dtype == object:
            out[col] = out[col].apply(
                lambda v: _ILLEGAL_CHARS_RE.sub("", str(v))[:_EXCEL_MAX_CELL_LENGTH]
                if pd.notna(v) and isinstance(v, str) else v
            )
    return out


def save_bt_only(bt_df: pd.DataFrame, path: str) -> None:
    out = Path(path)
    out.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(out, engine="openpyxl") as writer:
        sanitize_df_for_excel(bt_df).to_excel(writer, sheet_name="bt_all", index=False)


def build_final_main(merged: pd.DataFrame, entity_df: pd.DataFrame) -> pd.DataFrame:
    """主表尽量保持与原始输入表一致的结构与列顺序，至多新增 Matched_Source_RecordIndex。"""
    final_cols = [c for c in entity_df.columns if c in merged.columns]
    if MAIN_TABLE_EXTRA_COLUMN in merged.columns and MAIN_TABLE_EXTRA_COLUMN not in final_cols:
        final_cols.append(MAIN_TABLE_EXTRA_COLUMN)
    out = merged.copy()
    for c in final_cols:
        if c not in out.columns:
            out[c] = ""
    return out[final_cols]


def build_review_needed(merged: pd.DataFrame, entity_df: pd.DataFrame) -> pd.DataFrame:
    """需要人工核对的条目：Backtrace_Status 为未解决/需核对状态，带辅助列。"""
    if "Backtrace_Status" not in merged.columns:
        return pd.DataFrame()
    status = merged["Backtrace_Status"].fillna("").astype(str)
    need_review = (status.str.len() > 0) & status.isin(REVIEW_NEEDED_STATUSES)
    if not need_review.any():
        review_cols = list(entity_df.columns) + [
            MAIN_TABLE_EXTRA_COLUMN,
            "Backtrace_Status",
            "Resolved_Full_Name",
            "Resolved_Composition",
            "Backtrace_Notes",
            "MD_File",
        ]
        return pd.DataFrame(columns=[c for c in review_cols if c in merged.columns])
    review_cols = [c for c in entity_df.columns if c in merged.columns]
    for c in [MAIN_TABLE_EXTRA_COLUMN, "Backtrace_Status", "Resolved_Full_Name", "Resolved_Composition", "Backtrace_Notes", "MD_File"]:
        if c in merged.columns and c not in review_cols:
            review_cols.append(c)
    return merged.loc[need_review, review_cols].copy()


def build_evidence_sheet(merged: pd.DataFrame) -> pd.DataFrame:
    """详细证据与过程信息，供审计/调试。"""
    key_cols = [c for c in ["EntityIndex", "RowIndex", "Original_Name", "Source_RecordIndex"] if c in merged.columns]
    evidence_cols = [c for c in EVIDENCE_COLS if c in merged.columns and c not in key_cols]
    bt_cols = [c for c in merged.columns if c.startswith("BT_")]
    cols = key_cols + evidence_cols + bt_cols
    cols = [c for c in cols if c in merged.columns]
    return merged[cols].copy() if cols else pd.DataFrame()


def write_outputs(output: str, merged: pd.DataFrame, bt_df: pd.DataFrame, entity_df: pd.DataFrame) -> None:
    out = Path(output)
    out.parent.mkdir(parents=True, exist_ok=True)
    final_main_df = sanitize_df_for_excel(build_final_main(merged, entity_df))
    review_df = sanitize_df_for_excel(build_review_needed(merged, entity_df))
    evidence_df = sanitize_df_for_excel(build_evidence_sheet(merged))
    bt_clean = sanitize_df_for_excel(bt_df)
    with pd.ExcelWriter(out, engine="openpyxl") as writer:
        final_main_df.to_excel(writer, sheet_name="final_main", index=False)
        review_df.to_excel(writer, sheet_name="review_needed", index=False)
        evidence_df.to_excel(writer, sheet_name="evidence", index=False)
        bt_clean.to_excel(writer, sheet_name="bt_all", index=False)
        dict_cols = [c for c in ["Original_Name", "Resolved_Full_Name", "Resolved_Composition", "Backtrace_Status", "Backtrace_Confidence", "Evidence_Snippet"] if c in bt_clean.columns]
        dict_df = bt_clean[bt_clean["Resolved_Full_Name"].astype(str).str.len() > 0][dict_cols].drop_duplicates()
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
    p.add_argument("--max-source-tries", type=int, default=DEFAULT_MAX_SOURCE_TRIES, help="Max Source_RecordIndex candidates to try per row")
    p.add_argument("--bt-input", default=DEFAULT_BT_INPUT_PATH, help="Existing bt_only.xlsx for merge-only mode")
    p.add_argument("--bt-sheet", default=DEFAULT_BT_SHEET, help="Sheet name inside bt_only.xlsx")
    p.add_argument("--save-bt-only", default=DEFAULT_SAVE_BT_ONLY_PATH, help="Optional path to save bt_all before merge")
    args = p.parse_args()

    if not args.input or not args.output:
        p.error("Both --input and --output (or their defaults in the config block) are required.")
    if not args.bt_input and (not args.raw_db or not args.md_zip):
        p.error("Fresh run (without --bt-input) requires --raw-db and --md-zip (or their defaults in the config block).")
    return args


def main() -> None:
    args = parse_args()
    if args.enable_llm and not normalize_text(args.api_key):
        print("\n" + "="*80)
        print("❌ 错误：LLM 功能已启用，但未提供 DEEPSEEK_API_KEY")
        print("="*80)
        print("\n请通过以下方式之一提供 API key：")
        print("  1. 设置环境变量：set DEEPSEEK_API_KEY=your_key_here")
        print("  2. 使用命令行参数：--api-key your_key_here")
        print("\n如果不想使用 LLM 功能，请添加参数：--no-llm")
        print("="*80 + "\n")
        raise ValueError("缺少 DEEPSEEK_API_KEY，无法运行 LLM 模式。请提供 API key 或使用 --no-llm 参数。")
    input_sheet = choose_sheet(args.input, args.input_sheet)
    stage("阶段: 读取输入表")
    entity_df = prepare_entity_df(read_table(args.input, input_sheet))

    if args.bt_input:
        stage("阶段: 使用已有 bt_only，直接合并")
        bt_df = read_table(args.bt_input, choose_sheet(args.bt_input, args.bt_sheet) or args.bt_sheet)
        merged = merge_backtrace_to_main(entity_df, bt_df)
        write_outputs(args.output, merged, bt_df, entity_df)
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

    # ── 整张表都进入处理流程；仅当用户明确指定 --only-unresolved 时才做行筛选 ──
    work_df = entity_df.copy()
    if args.only_unresolved and "Lookup_Status" in work_df.columns:
        work_df = work_df[work_df["Lookup_Status"].astype(str).isin(DEFAULT_UNRESOLVED)].copy()
    # args.process_all_unresolved 和缩写限制逻辑已废弃；整表都跑回溯
    stage(f"处理模式: {'仅未解析行 (only-unresolved)' if args.only_unresolved else '整张表全量处理'}")

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
            max_source_tries=args.max_source_tries,
        )
        bt_rows.append(bt)

    bt_df = pd.DataFrame(bt_rows)
    bt_only_path = args.save_bt_only or str(Path(args.output).with_name(Path(args.output).stem + "_bt_only.xlsx"))
    stage("阶段: 保存中间 bt_only")
    save_bt_only(bt_df, bt_only_path)

    stage("阶段: 合并回主表")
    merged = merge_backtrace_to_main(entity_df, bt_df)
    write_outputs(args.output, merged, bt_df, entity_df)

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
