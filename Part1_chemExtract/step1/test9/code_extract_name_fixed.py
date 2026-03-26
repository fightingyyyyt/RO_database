import argparse
import csv
import json
import os
import re
import ssl
import time
from pathlib import Path
from collections import defaultdict
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib import error, parse, request

import pandas as pd

# ========================
# 改这里，文件名啥的，API_KEY 也可以在下面 DEFAULT_API_KEY = os.getenv() 后面的那个空的双引号里加进去
# ========================
DEFAULT_INPUT_FILE = r"D:\graduate_work\RO_database\Part1_chemExtract\step1\test9\step1.xlsx"
DEFAULT_OUTPUT_FILE = r"D:\graduate_work\RO_database\Part1_chemExtract\step1\test9\step2_enriched_long_2.xlsx"
DEFAULT_REPORT_FILE = r"D:\graduate_work\RO_database\Part1_chemExtract\step1\test9\step2_enriched_report_2.json"
DEFAULT_CHECKPOINT_FILE = r"D:\graduate_work\RO_database\Part1_chemExtract\step1\test9\step2_enriched_checkpoint_2.csv"
DEFAULT_MODEL = "deepseek-chat"
DEFAULT_API_KEY = os.getenv("DEEPSEEK_API_KEY", "")
DEFAULT_CHUNK_SIZE = 20

DEEPSEEK_URL = "https://api.deepseek.com/chat/completions"
PUBCHEM_BASE = "https://pubchem.ncbi.nlm.nih.gov/rest/pug"
SSL_CTX = ssl._create_unverified_context()

CATEGORY_MAP = {
    "SINGLE": "单一化学实体",
    "SINGLE_NOISY": "单一化学实体 + 噪声",
    "POLYMER": "高分子 / 大分子材料",
    "MIXTURE": "多组分 / 复合体系",
    "PROCESS": "工艺 / 条件 / 非化学对象",
    "NON_ENTITY": "非实体 / 占位项",
    "ABBR": "缩写 / 代码 / 歧义项",
}

SOLUTION_WORDS = ["水相", "水溶液", "aqueous", "in water", "solution"]
POLYMER_WORDS = [
    "poly(",
    "poly-",
    "poly ",
    "polymer",
    "cellulose",
    "纤维素",
    "树脂",
    "membrane",
    "nanocellulose",
]
MIXTURE_WORDS = ["complex", "complexes", "composite", "nanocomposite", "mixture", "blend"]
MIXTURE_CONNECTOR_PATTERN = re.compile(r"\s*(?:/|\+|,|;| and )\s*", re.IGNORECASE)
ABBR_BINDING_RE = re.compile(r"^\s*(.+?)\s*[\(（]\s*([A-Za-z][A-Za-z0-9\-]{1,20})\s*[\)）]\s*$")
ABBR_BINDING_RE_REV = re.compile(r"^\s*([A-Za-z][A-Za-z0-9\-]{1,20})\s*[\(（]\s*(.+?)\s*[\)）]\s*$")
PAIR_TOKEN_RE = re.compile(r"^([A-Za-z0-9]+)-([A-Za-z0-9]+)$")

KNOWN_ABBR_SINGLE = {
    "DA",  # dopamine
    "CYS",
    "CA",
    "EDA",
    "ETOH",
    "DMSO",
    "DMF",
    "DMA",
    "DMAC",
    "DMC",
    "DEG",
    "MEK",
    "DADMAC",
}
KNOWN_ABBR_POLYMER = {
    "DANC",  # dialdehyde nanocellulose
    "CDA",
    "PEI",
}
KNOWN_ABBR_EXPANSIONS = {
    "DA": "dopamine",
    "CYS": "cysteamine",
    "CA": "cystamine dihydrochloride",
    "EDA": "ethylenediamine",
    "ETOH": "ethanol",
    "DMSO": "dimethyl sulfoxide",
    "DMF": "dimethylformamide",
    "DMA": "dimethylacetamide",
    "DMAC": "dimethylacetamide",
    "DMC": "dimethyl carbonate",
    "DEG": "diethylene glycol",
    "MEK": "methyl ethyl ketone",
    "DADMAC": "diallyldimethylammonium chloride",
    "DANC": "dialdehyde nanocellulose",
    "CDA": "cellulose diacetate",
    "PEI": "polyethylenimine",
}
MIXTURE_MODIFIER_WORDS = {
    "acetate",
    "acrylate",
    "methacrylate",
    "chloride",
    "sulfate",
    "oxide",
    "hydroxide",
    "amine",
    "dopamine",
    "cyanoethyl",
}
NON_ENTITY_COMPONENT_HINTS = {"sample", "code", "condition", "unknown", "blank", "na", "n/a", "none", "null"}


CHEM_HINT_WORDS = {
    "acid",
    "amine",
    "amide",
    "alcohol",
    "dopamine",
    "cellulose",
    "nanocellulose",
    "polymer",
    "resin",
    "oxide",
    "chloride",
    "acetate",
    "acrylate",
    "sulfate",
    "hydroxide",
    "nanoparticle",
    "nanofiber",
    "复合",
    "纤维素",
    "聚",
}

ABBR_CONTEXT_BLACKLIST = {
    "water",
    "in water",
    "aqueous",
    "solution",
    "phase",
    "water phase",
    "aqueous phase",
    "organic phase",
    "oil phase",
    "air",
    "rt",
    "room temperature",
    "ph",
    "neutral",
    "acidic",
    "basic",
    "水相",
    "油相",
    "有机相",
    "水溶液",
    "溶液",
    "室温",
    "中性",
    "酸性",
    "碱性",
}

OUTPUT_COLUMNS = [
    "RowIndex",
    "Original_Name",
    "Category_Code",
    "Category",
    "Subcategory",
    "Record_Type",
    "Component_Rank",
    "Role",
    "Equivalent_Group",
    "Abbr_Candidate_Rank",
    "Confidence",
    "Needs_Review",
    "Decision_Notes",
    "Molecular_Formula",
    "IUPAC_Name",
    "Standardized_Query",
    "Canonical_SMILES",
    "CID",
    "Match_Source",
    "Lookup_Status",
]


def render_progress(done: int, total: int, start_ts: float) -> None:
    total = max(total, 1)
    ratio = max(0.0, min(1.0, done / total))
    width = 30
    filled = int(width * ratio)
    bar = "#" * filled + "-" * (width - filled)
    elapsed = time.time() - start_ts
    print(f"\rProgress [{bar}] {done}/{total} ({ratio*100:5.1f}%) | {elapsed:6.1f}s", end="", flush=True)


def normalize_text(text: str) -> str:
    t = str(text or "").strip()
    t = t.replace("（", "(").replace("）", ")")
    t = t.replace("，", ",")
    t = re.sub(r"\s+", " ", t)
    return t


def _is_valid_abbr(abbr: str) -> bool:
    up = abbr.upper()
    if not re.match(r"^[A-Z][A-Z0-9\-]{1,12}$", up):
        return False
    alpha_count = sum(1 for c in up if c.isalpha())
    if alpha_count < 2:
        return False
    low = normalize_text(abbr).lower()
    if low in ABBR_CONTEXT_BLACKLIST:
        return False
    if any(x in low for x in ["%", "wt", "vol", "mol/l", "molar", "buffer"]):
        return False
    # Most valid abbreviations are compact tokens, not long words like "aqueous".
    if len(low) > 6 and low not in {"cell", "pei", "danc", "dadmac"}:
        return False
    return True


def _is_valid_full_name(full: str) -> bool:
    s = normalize_text(full)
    if len(s) < 4 or len(s) > 120:
        return False
    if re.match(r"^[A-Z]{1,4}\d{1,4}[A-Za-z]?$", s):
        return False
    if re.match(r"^[A-Z0-9\-_/]+$", s):
        return False
    low = s.lower()
    if low in ABBR_CONTEXT_BLACKLIST:
        return False
    if any(k in low for k in ["solution", "phase", "water phase", "aqueous phase", "水相", "油相"]):
        return False
    return bool(re.search(r"[A-Za-z\u4e00-\u9fff]", s))


def _has_chem_hint(text: str) -> bool:
    low = normalize_text(text).lower()
    return any(k in low for k in CHEM_HINT_WORDS)


def build_abbr_lexicon(names: List[str]) -> Tuple[Dict[str, Set[str]], Set[str]]:
    lex: Dict[str, Set[str]] = defaultdict(set)
    evidence_count: Dict[str, int] = defaultdict(int)
    for raw in names:
        t = normalize_text(raw)
        m = ABBR_BINDING_RE.match(t)
        if m:
            full = normalize_text(m.group(1))
            abbr = normalize_text(m.group(2)).upper()
            if _is_valid_abbr(abbr) and _is_valid_full_name(full):
                lex[abbr].add(full)
                evidence_count[abbr] += 1
        mr = ABBR_BINDING_RE_REV.match(t)
        if mr:
            abbr = normalize_text(mr.group(1)).upper()
            full = normalize_text(mr.group(2))
            if _is_valid_abbr(abbr) and _is_valid_full_name(full):
                # Avoid cases like "DA(DA)".
                if normalize_text(full).upper() != abbr:
                    lex[abbr].add(full)
                    evidence_count[abbr] += 1

    strong_abbrs: Set[str] = set()
    for abbr, fulls in lex.items():
        if abbr in KNOWN_ABBR_SINGLE or abbr in KNOWN_ABBR_POLYMER:
            strong_abbrs.add(abbr)
            continue
        if evidence_count.get(abbr, 0) >= 2:
            strong_abbrs.add(abbr)
            continue
        if any(_has_chem_hint(x) for x in fulls):
            strong_abbrs.add(abbr)

    clean_lex = {}
    for abbr, fulls in lex.items():
        if abbr in strong_abbrs:
            clean_lex[abbr] = fulls

    return clean_lex, strong_abbrs


def is_chem_abbr_token(token: str, abbr_lexicon: Dict[str, Set[str]], strong_abbrs: Set[str]) -> bool:
    up = token.upper()
    if up in KNOWN_ABBR_SINGLE or up in KNOWN_ABBR_POLYMER:
        return True
    if up in strong_abbrs:
        return True
    return False


def is_likely_abbr_token(token: str) -> bool:
    t = normalize_text(token)
    if not t or " " in t:
        return False
    return _is_valid_abbr(t)


def get_local_abbr_expansion(token: str, abbr_lexicon: Dict[str, Set[str]]) -> str:
    up = normalize_text(token).upper()
    if not up:
        return ""
    if up in KNOWN_ABBR_EXPANSIONS:
        return KNOWN_ABBR_EXPANSIONS[up]
    fulls = list(abbr_lexicon.get(up, []))
    if not fulls:
        return ""
    english = [x for x in fulls if not contains_cjk(x)]
    candidates = english if english else fulls
    candidates = sorted(candidates, key=lambda x: (-len(normalize_text(x)), normalize_text(x)))
    return normalize_text(candidates[0]) if candidates else ""


def detect_forced_mixture(
    original_name: str,
    current_code: str,
    abbr_lexicon: Dict[str, Set[str]],
    strong_abbrs: Set[str],
) -> Tuple[bool, str]:
    name = normalize_text(original_name)
    low = name.lower()

    if any(w in low for w in SOLUTION_WORDS):
        return True, "solution context"
    if any(w in low for w in MIXTURE_WORDS):
        return True, "mixture keyword"
    if MIXTURE_CONNECTOR_PATTERN.search(name):
        return True, "mixture connector"

    return False, ""


def split_mixture_components(name: str, abbr_lexicon: Dict[str, Set[str]], strong_abbrs: Set[str]) -> List[Dict[str, Any]]:
    text = normalize_text(name)
    raw_parts: List[str] = []

    if MIXTURE_CONNECTOR_PATTERN.search(text):
        raw_parts = [normalize_text(x) for x in MIXTURE_CONNECTOR_PATTERN.split(text) if normalize_text(x)]
    else:
        # Only a fallback split for explicit "ABBR-ABBR" style strings when LLM components are missing.
        m = PAIR_TOKEN_RE.match(text.upper())
        if m:
            left, right = m.group(1), m.group(2)
            if is_chem_abbr_token(left, abbr_lexicon, strong_abbrs) and is_chem_abbr_token(right, abbr_lexicon, strong_abbrs):
                raw_parts = [left, right]

    if not raw_parts:
        low = text.lower()
        toks = [x for x in re.split(r"\s+", text) if x]
        if len(toks) >= 3 and any(w in low for w in POLYMER_WORDS):
            raw_parts = [toks[0], " ".join(toks[1:])]

    if not raw_parts:
        raw_parts = [text]

    seen = set()
    parts = []
    for p in raw_parts:
        k = p.lower()
        if not p or k in seen:
            continue
        seen.add(k)
        parts.append(p)

    return [
        {"name": p, "role": "primary" if i == 1 else "co_component", "rank": i, "equivalent_group": f"G{i}"}
        for i, p in enumerate(parts, start=1)
    ]


def is_non_entity_component(name: str) -> bool:
    low = normalize_text(name).lower()
    if not low:
        return True
    if low in NON_ENTITY_COMPONENT_HINTS:
        return True
    return False


def ensure_path_writable(target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    probe = target.parent / f".__write_test_{int(time.time() * 1000)}.tmp"
    probe.write_text("ok", encoding="utf-8")
    probe.unlink(missing_ok=True)


def build_query_variants(query: str) -> List[str]:
    q = normalize_text(query)
    if not q:
        return []
    variants = [q]

    n = q
    n = re.sub(r"(?i)\b(nanoparticles?|nps?)\b", "", n)
    n = n.replace("纳米颗粒", "")
    n = re.sub(r"[-\s]+$", "", n).strip()
    if n and n not in variants:
        variants.append(n)

    # fullname(abbr) -> fullname
    m = re.match(r"^(.+?)\s*[\(][^()]+[\)]\s*$", q)
    if m:
        left = normalize_text(m.group(1))
        if left and left not in variants:
            variants.append(left)

    if "·" in q:
        n2 = q.replace("·", ".")
        if n2 not in variants:
            variants.append(n2)

    return variants


def contains_cjk(text: str) -> bool:
    return bool(re.search(r"[\u4e00-\u9fff]", str(text or "")))


def token_set_for_match(text: str) -> Set[str]:
    low = normalize_text(text).lower()
    return {x for x in re.findall(r"[a-z]{3,}", low) if x not in {"with", "from", "into", "acid"}}


def has_semantic_overlap(source: str, candidate: str) -> bool:
    if contains_cjk(source):
        return True
    s = token_set_for_match(source)
    if not s:
        return True
    c = token_set_for_match(candidate)
    if not c:
        return False
    return bool(s.intersection(c))


def rewrite_query_for_pubchem(
    raw_query: str,
    api_key: str,
    model: str,
    rewrite_cache: Dict[str, List[str]],
    timeout: int = 60,
) -> List[str]:
    q = normalize_text(raw_query)
    if not q:
        return []
    if q in rewrite_cache:
        return rewrite_cache[q]
    if not api_key:
        rewrite_cache[q] = []
        return []

    system_msg = (
        "You rewrite noisy Chinese/English chemistry names into PubChem-friendly English queries. "
        "Return strict JSON only with this schema: "
        "{\"queries\":[\"query1\",\"query2\",\"query3\"]}. "
        "Rules: keep only likely real chemical entities; remove sample IDs/process words; "
        "for mixtures, output separate component names; prioritize IUPAC/common English names."
    )
    payload = {
        "model": model,
        "temperature": 0,
        "response_format": {"type": "json_object"},
        "messages": [
            {"role": "system", "content": system_msg},
            {"role": "user", "content": q},
        ],
    }
    req = request.Request(
        DEEPSEEK_URL,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        method="POST",
    )
    try:
        with request.urlopen(req, timeout=timeout, context=SSL_CTX) as resp:
            raw = resp.read().decode("utf-8")
        obj = json.loads(raw)
        content = str(obj.get("choices", [{}])[0].get("message", {}).get("content", "") or "")
        parsed = parse_json_loose(content)
        qs = parsed.get("queries", [])
        out: List[str] = []
        seen = set()
        for item in qs if isinstance(qs, list) else []:
            t = normalize_text(str(item or ""))
            if not t:
                continue
            if contains_cjk(t):
                continue
            if not has_semantic_overlap(q, t):
                continue
            k = t.lower()
            if k in seen:
                continue
            seen.add(k)
            out.append(t)
            if len(out) >= 3:
                break
        rewrite_cache[q] = out
        return out
    except Exception:
        rewrite_cache[q] = []
        return []


def is_material_like_name(query: str) -> bool:
    q = normalize_text(query).lower()
    if not q:
        return False
    if re.search(r"#\s*\d+[a-z]?\s*$", q):
        return True
    hints = [
        "nanofiber",
        "nanofibers",
        "fiber",
        "fibers",
        "nanoparticle",
        "nanoparticles",
        "composite",
        "nanocomposite",
        "liposome",
        "liposomes",
        "material",
        "sample",
        "specimen",
    ]
    return any(h in q for h in hints)


def resolve_material_to_pubchem_queries(
    raw_query: str,
    api_key: str,
    model: str,
    material_cache: Dict[str, List[str]],
    timeout: int = 80,
) -> List[str]:
    q = normalize_text(raw_query)
    if not q:
        return []
    if q in material_cache:
        return material_cache[q]
    if not api_key:
        material_cache[q] = []
        return []

    system_msg = (
        "You are a chemistry material resolver. Given a noisy material/sample name, infer its likely chemical "
        "composition and produce PubChem-searchable English chemical names for components. "
        "Return strict JSON only: "
        "{\"queries\":[\"component_query_1\",\"component_query_2\",\"component_query_3\"],"
        "\"notes\":\"short rationale\"}. "
        "Rules: remove sample IDs and process words; keep only real chemical entities; prioritize PubChem-accepted names."
    )
    payload = {
        "model": model,
        "temperature": 0,
        "response_format": {"type": "json_object"},
        "messages": [
            {"role": "system", "content": system_msg},
            {"role": "user", "content": q},
        ],
    }
    req = request.Request(
        DEEPSEEK_URL,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        method="POST",
    )
    try:
        with request.urlopen(req, timeout=timeout, context=SSL_CTX) as resp:
            raw = resp.read().decode("utf-8")
        obj = json.loads(raw)
        content = str(obj.get("choices", [{}])[0].get("message", {}).get("content", "") or "")
        parsed = parse_json_loose(content)
        qs = parsed.get("queries", [])
        out: List[str] = []
        seen = set()
        for item in qs if isinstance(qs, list) else []:
            t = normalize_text(str(item or ""))
            if not t:
                continue
            if contains_cjk(t):
                continue
            if not has_semantic_overlap(q, t):
                continue
            k = t.lower()
            if k in seen:
                continue
            seen.add(k)
            out.append(t)
            if len(out) >= 3:
                break
        material_cache[q] = out
        return out
    except Exception:
        material_cache[q] = []
        return []


def parse_json_loose(text: str) -> Dict[str, Any]:
    s = str(text or "").strip()
    if not s:
        return {}
    try:
        obj = json.loads(s)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        pass

    s = re.sub(r"^```(?:json)?\s*", "", s, flags=re.IGNORECASE)
    s = re.sub(r"\s*```$", "", s)
    try:
        obj = json.loads(s)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        pass

    l = s.find("{")
    r = s.rfind("}")
    if l != -1 and r != -1 and r > l:
        mid = s[l : r + 1]
        try:
            obj = json.loads(mid)
            return obj if isinstance(obj, dict) else {}
        except Exception:
            return {}
    return {}


def to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def normalize_components(raw_components: Any) -> List[Dict[str, Any]]:
    if not isinstance(raw_components, list):
        return []
    out: List[Dict[str, Any]] = []
    for i, c in enumerate(raw_components, start=1):
        if isinstance(c, dict):
            name = normalize_text(c.get("name", ""))
            if not name:
                continue
            out.append(
                {
                    "name": name,
                    "role": str(c.get("role", "co_component") or "co_component"),
                    "rank": c.get("rank", i),
                    "equivalent_group": str(c.get("equivalent_group", "") or ""),
                }
            )
        else:
            name = normalize_text(str(c or ""))
            if not name:
                continue
            out.append(
                {
                    "name": name,
                    "role": "co_component",
                    "rank": i,
                    "equivalent_group": "",
                }
            )
    return out


def normalize_abbr_candidates(raw_candidates: Any) -> List[Dict[str, Any]]:
    if not isinstance(raw_candidates, list):
        return []
    out: List[Dict[str, Any]] = []
    for c in raw_candidates:
        if isinstance(c, dict):
            q = normalize_text(c.get("query", ""))
            conf = to_float(c.get("confidence", 0.0), 0.0)
        else:
            q = normalize_text(str(c or ""))
            conf = 0.4 if q else 0.0
        if not q:
            continue
        out.append({"query": q, "confidence": conf})
    return out


def call_deepseek_batch(rows: List[Tuple[int, str]], api_key: str, model: str, timeout: int = 180) -> Dict[int, Dict[str, Any]]:
    system_msg = (
        "You are a chemistry-name parser for chemical engineering datasets. Return strict JSON only. "
        "Allowed category: SINGLE,SINGLE_NOISY,POLYMER,MIXTURE,PROCESS,NON_ENTITY,ABBR.\n"
        "Rules: first judge whether the input contains multiple chemical substances in chemistry/chemical-engineering context. "
        "If yes, category must be MIXTURE. If no, do not force split.\n"
        "Alias parentheses usually single entity; solution context=>MIXTURE; abbreviations are chemistry-first.\n"
        "For single_query/components/abbr_candidates.query, always prefer English PubChem-searchable names (not Chinese).\n"
        "For MIXTURE output components(name,role,rank,equivalent_group); each component name should be standardized English query text for lookup.\n"
        "For ABBR output top3 candidates(query,confidence).\n"
        "Output JSON: {\"results\":[{\"RowIndex\":int,\"category\":str,\"subcategory\":str,\"single_query\":str,"
        "\"components\":[{\"name\":str,\"role\":str,\"rank\":int,\"equivalent_group\":str}],"
        "\"abbr_candidates\":[{\"query\":str,\"confidence\":float}],\"notes\":str}]}"
    )
    block = "\n".join(f"{rid}\t{name}" for rid, name in rows)
    payload = {
        "model": model,
        "temperature": 0,
        "response_format": {"type": "json_object"},
        "messages": [
            {"role": "system", "content": system_msg},
            {"role": "user", "content": "Parse each record (RowIndex<TAB>Original_Name):\n" + block},
        ],
    }

    req = request.Request(
        DEEPSEEK_URL,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        method="POST",
    )
    with request.urlopen(req, timeout=timeout, context=SSL_CTX) as resp:
        raw = resp.read().decode("utf-8")
    obj = json.loads(raw)
    content = str(obj.get("choices", [{}])[0].get("message", {}).get("content", "") or "")
    parsed = parse_json_loose(content)

    raw_results = parsed.get("results", [])
    if isinstance(raw_results, dict):
        raw_results = list(raw_results.values())
    if not isinstance(raw_results, list):
        raw_results = []

    out: Dict[int, Dict[str, Any]] = {}
    for it in raw_results:
        if not isinstance(it, dict):
            continue
        try:
            rid = int(it.get("RowIndex"))
        except Exception:
            continue
        out[rid] = {
            "category": str(it.get("category", "NON_ENTITY")).upper(),
            "subcategory": str(it.get("subcategory", "") or ""),
            "single_query": str(it.get("single_query", "") or ""),
            "components": normalize_components(it.get("components", [])),
            "abbr_candidates": normalize_abbr_candidates(it.get("abbr_candidates", [])),
            "notes": str(it.get("notes", "") or ""),
        }
    return out


def resolve_abbr_candidate_once(
    token: str,
    api_key: str,
    model: str,
    context_name: str = "",
    timeout: int = 90,
) -> Dict[str, str]:
    tok = normalize_text(token)
    if not tok:
        return {"status": "NOT_CHEMICAL", "query": "", "note": "empty token"}

    system_msg = (
        "You are a chemistry abbreviation resolver for chemical engineering datasets. "
        "Return strict JSON only with shape: {\"status\":\"CHEMICAL|NOT_CHEMICAL\",\"query\":\"...\",\"note\":\"...\"}. "
        "Interpret the token in chemistry/chemical-engineering context. "
        "If there is a likely chemical meaning, return exactly one best English chemical name in query. "
        "Assume the term is mentioned in chemical membrane materials / polyamide membrane context. "
        "If no reliable chemical candidate exists, return status NOT_CHEMICAL and empty query."
    )
    payload = {
        "model": model,
        "temperature": 0,
        "response_format": {"type": "json_object"},
        "messages": [
            {"role": "system", "content": system_msg},
            {
                "role": "user",
                "content": (
                    f"Token: {tok}\n"
                    + (f"Original_Name context: {normalize_text(context_name)}" if normalize_text(context_name) else "")
                ),
            },
        ],
    }
    req = request.Request(
        DEEPSEEK_URL,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        method="POST",
    )
    with request.urlopen(req, timeout=timeout, context=SSL_CTX) as resp:
        raw = resp.read().decode("utf-8")
    obj = json.loads(raw)
    content = str(obj.get("choices", [{}])[0].get("message", {}).get("content", "") or "")
    parsed = parse_json_loose(content)
    status = str(parsed.get("status", "NOT_CHEMICAL")).upper()
    if status not in {"CHEMICAL", "NOT_CHEMICAL"}:
        status = "NOT_CHEMICAL"
    query = normalize_text(parsed.get("query", ""))
    if status == "CHEMICAL" and not query:
        status = "NOT_CHEMICAL"
    note = str(parsed.get("note", "") or "")
    return {"status": status, "query": query, "note": note}


def make_not_chemical_lookup(query: str) -> Dict[str, Any]:
    return {
        "Molecular_Formula": "",
        "IUPAC_Name": "",
        "Standardized_Query": normalize_text(query),
        "Canonical_SMILES": "",
        "CID": "",
        "Match_Source": "DeepSeek",
        "Lookup_Status": "NOT_CHEMICAL_FROM_LLM",
    }


def http_get_json(url: str, timeout: int = 25) -> Optional[Dict[str, Any]]:
    try:
        req = request.Request(url, method="GET")
        with request.urlopen(req, timeout=timeout, context=SSL_CTX) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except error.HTTPError:
        return None
    except Exception:
        return None


def pubchem_lookup(
    query: str,
    cache: Dict[str, Dict[str, Any]],
    api_key: str,
    model: str,
    rewrite_cache: Dict[str, List[str]],
    material_cache: Dict[str, List[str]],
) -> Dict[str, Any]:
    variants = build_query_variants(query)
    if contains_cjk(query):
        for rq in rewrite_query_for_pubchem(query, api_key=api_key, model=model, rewrite_cache=rewrite_cache):
            for v in build_query_variants(rq):
                if v and v not in variants:
                    variants.append(v)

    if not variants:
        return {
            "Molecular_Formula": "",
            "IUPAC_Name": "",
            "Standardized_Query": "",
            "Canonical_SMILES": "",
            "CID": "",
            "Match_Source": "",
            "Lookup_Status": "EMPTY_QUERY",
        }

    for q in variants:
        if q in cache and cache[q].get("Lookup_Status") == "OK":
            return cache[q]

        cid_obj = http_get_json(f"{PUBCHEM_BASE}/compound/name/{parse.quote(q)}/cids/JSON")
        if not cid_obj or "IdentifierList" not in cid_obj or not cid_obj["IdentifierList"].get("CID"):
            cache[q] = {
                "Molecular_Formula": "",
                "IUPAC_Name": "",
                "Standardized_Query": q,
                "Canonical_SMILES": "",
                "CID": "",
                "Match_Source": "PubChem",
                "Lookup_Status": "NO_HIT",
            }
            continue

        cid = cid_obj["IdentifierList"]["CID"][0]
        prop_obj = http_get_json(
            f"{PUBCHEM_BASE}/compound/cid/{cid}/property/MolecularFormula,IUPACName,CanonicalSMILES,ConnectivitySMILES/JSON"
        )
        props = {}
        if prop_obj and "PropertyTable" in prop_obj and prop_obj["PropertyTable"].get("Properties"):
            props = prop_obj["PropertyTable"]["Properties"][0]
        out = {
            "Molecular_Formula": props.get("MolecularFormula", ""),
            "IUPAC_Name": props.get("IUPACName", ""),
            "Standardized_Query": q,
            "Canonical_SMILES": props.get("CanonicalSMILES", "") or props.get("ConnectivitySMILES", ""),
            "CID": str(cid),
            "Match_Source": "PubChem",
            "Lookup_Status": "OK" if props else "CID_ONLY",
        }
        cache[q] = out
        if out["Lookup_Status"] == "OK":
            return out

    if is_material_like_name(query):
        for mq in resolve_material_to_pubchem_queries(
            query,
            api_key=api_key,
            model=model,
            material_cache=material_cache,
        ):
            for v in build_query_variants(mq):
                if not v or v in variants:
                    continue
                variants.append(v)
                if v in cache and cache[v].get("Lookup_Status") == "OK":
                    return cache[v]
                cid_obj = http_get_json(f"{PUBCHEM_BASE}/compound/name/{parse.quote(v)}/cids/JSON")
                if not cid_obj or "IdentifierList" not in cid_obj or not cid_obj["IdentifierList"].get("CID"):
                    cache[v] = {
                        "Molecular_Formula": "",
                        "IUPAC_Name": "",
                        "Standardized_Query": v,
                        "Canonical_SMILES": "",
                        "CID": "",
                        "Match_Source": "PubChem",
                        "Lookup_Status": "NO_HIT",
                    }
                    continue
                cid = cid_obj["IdentifierList"]["CID"][0]
                prop_obj = http_get_json(
                    f"{PUBCHEM_BASE}/compound/cid/{cid}/property/MolecularFormula,IUPACName,CanonicalSMILES,ConnectivitySMILES/JSON"
                )
                props = {}
                if prop_obj and "PropertyTable" in prop_obj and prop_obj["PropertyTable"].get("Properties"):
                    props = prop_obj["PropertyTable"]["Properties"][0]
                out = {
                    "Molecular_Formula": props.get("MolecularFormula", ""),
                    "IUPAC_Name": props.get("IUPACName", ""),
                    "Standardized_Query": v,
                    "Canonical_SMILES": props.get("CanonicalSMILES", "") or props.get("ConnectivitySMILES", ""),
                    "CID": str(cid),
                    "Match_Source": "PubChem",
                    "Lookup_Status": "OK" if props else "CID_ONLY",
                }
                cache[v] = out
                if out["Lookup_Status"] == "OK":
                    return out

    return {
        "Molecular_Formula": "",
        "IUPAC_Name": "",
        "Standardized_Query": variants[0],
        "Canonical_SMILES": "",
        "CID": "",
        "Match_Source": "PubChem",
        "Lookup_Status": "NO_HIT",
    }


def classify_no_hit_reason(
    query: str,
    original_name: str,
    record_type: str,
    api_key: str,
    model: str,
    timeout: int = 90,
) -> Dict[str, Any]:
    q = normalize_text(query)
    o = normalize_text(original_name)
    if not api_key:
        return {"status": "NO_HIT", "retry_queries": [], "note": "no api key"}

    system_msg = (
        "You are a chemistry entity validator for chemical engineering datasets. "
        "Return strict JSON only with schema: "
        "{\"status\":\"POLYMER|NO_SPECIFIC_CHEMICAL|SPECIFIC_CHEMICAL\","
        "\"retry_queries\":[\"...\"],\"note\":\"...\"}. "
        "Task: For a NO_HIT query, decide if it is (1) a polymer/material class, "
        "(2) a non-specific chemical class/family, or (3) a specific chemical entity. "
        "If SPECIFIC_CHEMICAL, provide 1-3 better English PubChem-searchable retry_queries."
    )
    user_msg = (
        f"Original_Name: {o}\n"
        f"Current_Query: {q}\n"
        f"Record_Type: {record_type}"
    )
    payload = {
        "model": model,
        "temperature": 0,
        "response_format": {"type": "json_object"},
        "messages": [
            {"role": "system", "content": system_msg},
            {"role": "user", "content": user_msg},
        ],
    }
    req = request.Request(
        DEEPSEEK_URL,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        method="POST",
    )
    try:
        with request.urlopen(req, timeout=timeout, context=SSL_CTX) as resp:
            raw = resp.read().decode("utf-8")
        obj = json.loads(raw)
        content = str(obj.get("choices", [{}])[0].get("message", {}).get("content", "") or "")
        parsed = parse_json_loose(content)
    except Exception:
        return {"status": "NO_HIT", "retry_queries": [], "note": "no_hit classifier error"}

    status = str(parsed.get("status", "NO_HIT")).upper()
    if status not in {"POLYMER", "NO_SPECIFIC_CHEMICAL", "SPECIFIC_CHEMICAL"}:
        status = "NO_HIT"

    retries = []
    for item in parsed.get("retry_queries", []) if isinstance(parsed.get("retry_queries", []), list) else []:
        cand = normalize_text(str(item or ""))
        if not cand:
            continue
        if contains_cjk(cand):
            continue
        if not has_semantic_overlap(o or q, cand):
            continue
        if cand.lower() in {x.lower() for x in retries}:
            continue
        retries.append(cand)
        if len(retries) >= 3:
            break

    note = str(parsed.get("note", "") or "")
    return {"status": status, "retry_queries": retries, "note": note}


def apply_no_hit_reclassification(
    chem: Dict[str, Any],
    query: str,
    original_name: str,
    record_type: str,
    api_key: str,
    model: str,
    pubchem_cache: Dict[str, Dict[str, Any]],
    rewrite_cache: Dict[str, List[str]],
    material_cache: Dict[str, List[str]],
    base_notes: str,
) -> Tuple[Dict[str, Any], str]:
    source_status = str(chem.get("Lookup_Status", "") or "")
    if source_status not in {"NO_HIT", "ABBR_EXPANDED_NO_HIT"}:
        return chem, base_notes

    cls = classify_no_hit_reason(
        query=query,
        original_name=original_name,
        record_type=record_type,
        api_key=api_key,
        model=model,
    )
    status = cls.get("status", "NO_HIT")
    note = str(cls.get("note", "") or "")
    notes = "; ".join(x for x in [base_notes, f"no_hit_class={status}", note] if x)

    if status == "POLYMER":
        new_chem = {**chem, "Lookup_Status": "Polymer"}
        return new_chem, notes
    if status == "NO_SPECIFIC_CHEMICAL":
        new_chem = {**chem, "Lookup_Status": "NO_SPECIFIC_CHEMICAL"}
        return new_chem, notes

    if status == "SPECIFIC_CHEMICAL":
        for rq in cls.get("retry_queries", []):
            retried = pubchem_lookup(
                rq,
                pubchem_cache,
                api_key=api_key,
                model=model,
                rewrite_cache=rewrite_cache,
                material_cache=material_cache,
            )
            if retried.get("Lookup_Status") in {"OK", "CID_ONLY"}:
                return retried, "; ".join(x for x in [notes, f"retry_hit={rq}"] if x)
        return {**chem, "Lookup_Status": source_status}, notes

    return {**chem, "Lookup_Status": source_status}, notes


def write_checkpoint_rows(checkpoint_file: Path, rows: List[Dict[str, Any]], write_header: bool) -> None:
    if not rows:
        return
    mode = "w" if write_header else "a"
    with checkpoint_file.open(mode, encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_COLUMNS)
        if write_header:
            writer.writeheader()
        for r in rows:
            writer.writerow({k: r.get(k, "") for k in OUTPUT_COLUMNS})


def prepare_input_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize the input table to the required columns.

    Supported cases:
    1) Already contains RowIndex + Original_Name
    2) Only contains a single chemical-name column such as `chemcial_name`
    3) Uses a common alias for the name column
    """
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]

    if "Original_Name" in df.columns and "RowIndex" in df.columns:
        out = df.copy()
    else:
        candidate_name_cols = [
            "chemcial_name",  # keep user's current column spelling
            "chemical_name",
            "Chemical_Name",
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

        name_col = None
        for col in candidate_name_cols:
            if col in df.columns:
                name_col = col
                break

        if name_col is None:
            non_empty_cols = [c for c in df.columns if df[c].notna().any()]
            if len(df.columns) == 1:
                name_col = df.columns[0]
            elif non_empty_cols:
                name_col = non_empty_cols[0]
            else:
                raise ValueError(
                    "Input must include a chemical-name column. Supported names include "
                    "`chemcial_name`, `chemical_name`, or `Original_Name`."
                )

        out = df.rename(columns={name_col: "Original_Name"}).copy()
        if "RowIndex" not in out.columns:
            out.insert(0, "RowIndex", range(1, len(out) + 1))

    out["Original_Name"] = out["Original_Name"].astype(str).str.strip()
    out = out[out["Original_Name"].ne("") & out["Original_Name"].str.lower().ne("nan")].copy()
    out["RowIndex"] = range(1, len(out) + 1)
    return out


def enrich_dataframe_stream(
    df: pd.DataFrame,
    api_key: str,
    model: str,
    chunk_size: int,
    checkpoint_file: Path,
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    df = prepare_input_dataframe(df)

    pubchem_cache: Dict[str, Dict[str, Any]] = {}
    rewrite_cache: Dict[str, List[str]] = {}
    material_cache: Dict[str, List[str]] = {}
    abbr_expand_cache: Dict[str, Dict[str, str]] = {}
    all_rows: List[Dict[str, Any]] = []
    counts = {"single_entity": 0, "mixture_component": 0, "abbr_candidate": 0, "non_entity": 0}
    parse_failures = 0

    items = [(int(r.RowIndex), normalize_text(r.Original_Name)) for r in df.itertuples(index=False)]
    abbr_lexicon, strong_abbrs = build_abbr_lexicon([x[1] for x in items])

    def resolve_abbr_with_cache(token: str, context_name: str = "") -> Dict[str, str]:
        key = normalize_text(token).upper()
        local = get_local_abbr_expansion(token, abbr_lexicon)
        if local:
            return {"status": "CHEMICAL", "query": local, "note": "local abbr lexicon/known-map"}
        if not key:
            return {"status": "NOT_CHEMICAL", "query": "", "note": "empty token"}
        if key in abbr_expand_cache:
            return abbr_expand_cache[key]
        try:
            out = resolve_abbr_candidate_once(token, api_key=api_key, model=model, context_name=context_name)
        except Exception:
            out = {"status": "NOT_CHEMICAL", "query": "", "note": "abbr resolve error"}

        if out.get("status") == "CHEMICAL" and not has_semantic_overlap(context_name or token, out.get("query", "")):
            out = {"status": "NOT_CHEMICAL", "query": "", "note": "abbr resolve rejected: low overlap"}

        abbr_expand_cache[key] = out
        return out

    def lookup_component_with_abbr_fallback(
        component_name: str,
        row_notes: str,
        source_name: str,
    ) -> Tuple[Dict[str, Any], str]:
        chem = pubchem_lookup(
            component_name,
            pubchem_cache,
            api_key=api_key,
            model=model,
            rewrite_cache=rewrite_cache,
            material_cache=material_cache,
        )
        out_notes = row_notes
        if chem["Lookup_Status"] in {"NO_HIT", "EMPTY_QUERY", "CID_ONLY"} and is_likely_abbr_token(component_name):
            abbr_res = resolve_abbr_with_cache(component_name, context_name=source_name)
            abbr_note = f"abbr {component_name} => {abbr_res.get('query', '') or 'NOT_CHEMICAL'}"
            out_notes = "; ".join(x for x in [out_notes, abbr_note, abbr_res.get("note", "")] if x)
            if abbr_res["status"] == "CHEMICAL":
                chem2 = pubchem_lookup(
                    abbr_res["query"],
                    pubchem_cache,
                    api_key=api_key,
                    model=model,
                    rewrite_cache=rewrite_cache,
                    material_cache=material_cache,
                )
                if chem2["Lookup_Status"] == "OK":
                    chem = {**chem2, "Lookup_Status": "ABBR_EXPANDED_OK", "Match_Source": "PubChem+DeepSeek"}
                else:
                    chem = {
                        **chem2,
                        "Standardized_Query": normalize_text(abbr_res["query"]),
                        "Lookup_Status": "ABBR_EXPANDED_NO_HIT",
                        "Match_Source": "PubChem+DeepSeek",
                    }
            else:
                chem = make_not_chemical_lookup(component_name)

        chem, out_notes = apply_no_hit_reclassification(
            chem=chem,
            query=component_name,
            original_name=source_name,
            record_type="mixture_component",
            api_key=api_key,
            model=model,
            pubchem_cache=pubchem_cache,
            rewrite_cache=rewrite_cache,
            material_cache=material_cache,
            base_notes=out_notes,
        )
        return chem, out_notes

    if checkpoint_file.exists():
        checkpoint_file.unlink()
    checkpoint_header_written = False

    start_ts = time.time()
    total_items = len(items)

    for start in range(0, len(items), chunk_size):
        chunk = items[start : start + chunk_size]
        try:
            parsed_map = call_deepseek_batch(chunk, api_key=api_key, model=model)
        except Exception:
            parsed_map = {}

        chunk_rows: List[Dict[str, Any]] = []

        for row_index, original_name in chunk:
            parsed = parsed_map.get(
                row_index,
                {
                    "category": "NON_ENTITY",
                    "subcategory": "",
                    "single_query": "",
                    "components": [],
                    "abbr_candidates": [],
                    "notes": "DeepSeek parse missing",
                },
            )
            if row_index not in parsed_map:
                parse_failures += 1

            code = str(parsed.get("category", "NON_ENTITY")).upper()
            if code not in CATEGORY_MAP:
                code = "NON_ENTITY"

            forced_mixture, force_reason = False, ""
            # Respect LLM MIXTURE as primary decision; rules only prevent obvious misses for non-MIXTURE outputs.
            if code != "MIXTURE":
                forced_mixture, force_reason = detect_forced_mixture(original_name, code, abbr_lexicon, strong_abbrs)
                if forced_mixture:
                    code = "MIXTURE"
                    parsed["components"] = split_mixture_components(original_name, abbr_lexicon, strong_abbrs)
                    parsed["notes"] = "; ".join(
                        x for x in [str(parsed.get("notes", "") or ""), f"rule override: {force_reason}"] if x
                    )

            base = {
                "RowIndex": row_index,
                "Original_Name": original_name,
                "Category_Code": code,
                "Category": CATEGORY_MAP[code],
                "Subcategory": str(parsed.get("subcategory", "") or ""),
            }
            notes = str(parsed.get("notes", "") or "")

            if code in {"SINGLE", "SINGLE_NOISY", "POLYMER"}:
                q = normalize_text(parsed.get("single_query", "") or original_name)
                chem = pubchem_lookup(
                    q,
                    pubchem_cache,
                    api_key=api_key,
                    model=model,
                    rewrite_cache=rewrite_cache,
                    material_cache=material_cache,
                )

                rescue_components: List[str] = []
                rescue_reason = ""
                if chem["Lookup_Status"] in {"NO_HIT", "EMPTY_QUERY", "CID_ONLY"}:
                    m_pair = PAIR_TOKEN_RE.match(original_name.upper())
                    if m_pair:
                        left, right = m_pair.group(1), m_pair.group(2)
                        if is_likely_abbr_token(left) and is_likely_abbr_token(right):
                            rescue_components = [left, right]
                            rescue_reason = "single->mixture rescue: hyphenated abbreviation pair"

                    if not rescue_components:
                        cands = resolve_material_to_pubchem_queries(
                            original_name,
                            api_key=api_key,
                            model=model,
                            material_cache=material_cache,
                        )
                        filtered = []
                        seen = set()
                        for cand in cands:
                            cn = normalize_text(cand)
                            if not cn or is_non_entity_component(cn):
                                continue
                            if not has_semantic_overlap(original_name, cn):
                                continue
                            lk = cn.lower()
                            if lk in seen:
                                continue
                            seen.add(lk)
                            filtered.append(cn)
                        if len(filtered) >= 2:
                            rescue_components = filtered
                            rescue_reason = "single->mixture rescue: material component decomposition"

                if rescue_components:
                    mix_base = {
                        **base,
                        "Category_Code": "MIXTURE",
                        "Category": CATEGORY_MAP["MIXTURE"],
                    }
                    for rk, cname in enumerate(rescue_components, start=1):
                        comp_chem, comp_notes = lookup_component_with_abbr_fallback(
                            cname,
                            "; ".join(x for x in [notes, rescue_reason] if x),
                            original_name,
                        )
                        rec = {
                            **mix_base,
                            "Record_Type": "mixture_component",
                            "Component_Rank": rk,
                            "Role": "primary" if rk == 1 else "co_component",
                            "Equivalent_Group": f"G{rk}",
                            "Abbr_Candidate_Rank": "",
                            "Confidence": "",
                            "Needs_Review": comp_chem["Lookup_Status"] not in {"OK", "ABBR_EXPANDED_OK"},
                            "Decision_Notes": comp_notes,
                            **comp_chem,
                        }
                        chunk_rows.append(rec)
                        counts["mixture_component"] += 1
                    continue

                chem, notes_single = apply_no_hit_reclassification(
                    chem=chem,
                    query=q,
                    original_name=original_name,
                    record_type="single_entity",
                    api_key=api_key,
                    model=model,
                    pubchem_cache=pubchem_cache,
                    rewrite_cache=rewrite_cache,
                    material_cache=material_cache,
                    base_notes=notes,
                )

                rec = {
                    **base,
                    "Record_Type": "single_entity",
                    "Component_Rank": 1,
                    "Role": "primary",
                    "Equivalent_Group": "G1",
                    "Abbr_Candidate_Rank": "",
                    "Confidence": "",
                    "Needs_Review": chem["Lookup_Status"] != "OK",
                    "Decision_Notes": notes_single,
                    **chem,
                }
                chunk_rows.append(rec)
                counts["single_entity"] += 1
                continue


            if code == "MIXTURE":
                comps = parsed.get("components", [])
                if not isinstance(comps, list):
                    comps = []

                normalized_components = []
                for c in comps:
                    if not isinstance(c, dict):
                        continue
                    cname = normalize_text(c.get("name", ""))
                    if is_non_entity_component(cname):
                        continue
                    normalized_components.append(
                        {
                            "name": cname,
                            "role": str(c.get("role", "co_component") or "co_component"),
                            "rank": c.get("rank", 999),
                            "equivalent_group": str(c.get("equivalent_group", "") or ""),
                        }
                    )

                if not normalized_components:
                    normalized_components = split_mixture_components(original_name, abbr_lexicon, strong_abbrs)

                def _rank(x: Dict[str, Any]) -> int:
                    try:
                        return int(x.get("rank", 999))
                    except Exception:
                        return 999

                for c in sorted(normalized_components, key=_rank):
                    cname = normalize_text(c.get("name", ""))
                    if not cname:
                        continue
                    role = str(c.get("role", "co_component") or "co_component")
                    try:
                        rk = int(c.get("rank", 1))
                    except Exception:
                        rk = 1
                    eq = str(c.get("equivalent_group", "") or "") or f"G{rk}"
                    chem, row_notes = lookup_component_with_abbr_fallback(cname, notes, original_name)

                    rec = {
                        **base,
                        "Record_Type": "mixture_component",
                        "Component_Rank": rk,
                        "Role": role,
                        "Equivalent_Group": eq,
                        "Abbr_Candidate_Rank": "",
                        "Confidence": "",
                        "Needs_Review": chem["Lookup_Status"] not in {"OK", "ABBR_EXPANDED_OK"},
                        "Decision_Notes": row_notes,
                        **chem,
                    }
                    chunk_rows.append(rec)
                    counts["mixture_component"] += 1
                continue

            if code == "ABBR":
                cands = parsed.get("abbr_candidates", [])
                if not isinstance(cands, list):
                    cands = []
                cands = cands[:3]
                if not cands:
                    cands = [{"query": original_name, "confidence": 0.2}]

                if len(cands) >= 2:
                    c1 = to_float(cands[0].get("confidence", 0.0), 0.0)
                    c2 = to_float(cands[1].get("confidence", 0.0), 0.0)
                    strong_top1 = (c2 <= 0 and c1 > 0) or (c2 > 0 and c1 >= 1.99 * c2)
                    if strong_top1:
                        cands = [cands[0]]

                if len(cands) > 1:
                    while len(cands) < 3:
                        cands.append({"query": "", "confidence": 0.0})

                for i, c in enumerate(cands, start=1):
                    q = normalize_text(c.get("query", ""))
                    try:
                        conf = float(c.get("confidence", 0.0))
                    except Exception:
                        conf = 0.0
                    row_notes = notes
                    chem = pubchem_lookup(
                    q,
                    pubchem_cache,
                    api_key=api_key,
                    model=model,
                    rewrite_cache=rewrite_cache,
                    material_cache=material_cache,
                ) if q else {
                        "Molecular_Formula": "",
                        "IUPAC_Name": "",
                        "Standardized_Query": "",
                        "Canonical_SMILES": "",
                        "CID": "",
                        "Match_Source": "",
                        "Lookup_Status": "EMPTY_CANDIDATE",
                    }

                    should_expand_abbr = bool(q) and is_likely_abbr_token(q) and (
                        conf < 0.35 or chem["Lookup_Status"] in {"NO_HIT", "EMPTY_QUERY", "EMPTY_CANDIDATE"}
                    )
                    if should_expand_abbr:
                        abbr_res = resolve_abbr_with_cache(q, context_name=original_name)
                        abbr_note = f"abbr {q} => {abbr_res.get('query', '') or 'NOT_CHEMICAL'}"
                        row_notes = "; ".join(x for x in [row_notes, abbr_note, abbr_res.get("note", "")] if x)
                        if abbr_res["status"] == "CHEMICAL":
                            chem2 = pubchem_lookup(
                                abbr_res["query"],
                                pubchem_cache,
                                api_key=api_key,
                                model=model,
                                rewrite_cache=rewrite_cache,
                                material_cache=material_cache,
                            )
                            if chem2["Lookup_Status"] == "OK":
                                chem = {**chem2, "Lookup_Status": "ABBR_EXPANDED_OK", "Match_Source": "PubChem+DeepSeek"}
                            else:
                                chem = {
                                    **chem2,
                                    "Standardized_Query": normalize_text(abbr_res["query"]),
                                    "Lookup_Status": "ABBR_EXPANDED_NO_HIT",
                                    "Match_Source": "PubChem+DeepSeek",
                                }
                        else:
                            chem = make_not_chemical_lookup(q)

                    chem, row_notes = apply_no_hit_reclassification(
                        chem=chem,
                        query=q,
                        original_name=original_name,
                        record_type="abbr_candidate",
                        api_key=api_key,
                        model=model,
                        pubchem_cache=pubchem_cache,
                        rewrite_cache=rewrite_cache,
                        material_cache=material_cache,
                        base_notes=row_notes,
                    )

                    rec = {
                        **base,
                        "Record_Type": "abbr_candidate",
                        "Component_Rank": "",
                        "Role": "",
                        "Equivalent_Group": "",
                        "Abbr_Candidate_Rank": i,
                        "Confidence": conf,
                        "Needs_Review": (conf < 0.35) or (chem["Lookup_Status"] not in {"OK", "ABBR_EXPANDED_OK"}),
                        "Decision_Notes": row_notes,
                        **chem,
                    }
                    chunk_rows.append(rec)
                    counts["abbr_candidate"] += 1
                continue

            rec = {
                **base,
                "Record_Type": "non_entity",
                "Component_Rank": "",
                "Role": "",
                "Equivalent_Group": "",
                "Abbr_Candidate_Rank": "",
                "Confidence": "",
                "Needs_Review": False,
                "Decision_Notes": notes,
                "Molecular_Formula": "",
                "IUPAC_Name": "",
                "Standardized_Query": "",
                "Canonical_SMILES": "",
                "CID": "",
                "Match_Source": "",
                "Lookup_Status": "SKIP_NON_ENTITY",
            }
            chunk_rows.append(rec)
            counts["non_entity"] += 1

        # Persist chunk immediately so work is not lost.
        write_checkpoint_rows(checkpoint_file, chunk_rows, write_header=not checkpoint_header_written)
        checkpoint_header_written = True

        all_rows.extend(chunk_rows)
        done = min(start + chunk_size, total_items)
        render_progress(done, total_items, start_ts)

    print()
    out_df = pd.DataFrame(all_rows, columns=OUTPUT_COLUMNS)
    report = {
        "input_rows": int(len(df)),
        "output_rows": int(len(out_df)),
        "record_type_counts": counts,
        "deepseek_parse_failures": int(parse_failures),
        "lookup_status_counts": out_df["Lookup_Status"].value_counts().to_dict() if len(out_df) else {},
        "needs_review_ratio": float(out_df["Needs_Review"].mean()) if len(out_df) else 0.0,
        "pubchem_cache_size": int(len(pubchem_cache)),
        "checkpoint_file": str(checkpoint_file),
    }
    return out_df, report


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Full chemistry enrichment pipeline with checkpointing")
    p.add_argument("--input", default=DEFAULT_INPUT_FILE)
    p.add_argument("--output", default=DEFAULT_OUTPUT_FILE)
    p.add_argument("--report", default=DEFAULT_REPORT_FILE)
    p.add_argument("--checkpoint", default=DEFAULT_CHECKPOINT_FILE)
    p.add_argument("--api-key", default=DEFAULT_API_KEY)
    p.add_argument("--model", default=DEFAULT_MODEL)
    p.add_argument("--chunk-size", type=int, default=DEFAULT_CHUNK_SIZE)
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    if not args.api_key:
        raise ValueError("Missing API key. Use --api-key or DEEPSEEK_API_KEY")

    inp = Path(args.input)
    outp = Path(args.output)
    rep = Path(args.report)
    ck = Path(args.checkpoint)

    if not inp.exists():
        raise FileNotFoundError(f"Input file not found: {inp}")

    # Preflight writable checks before expensive network calls.
    ensure_path_writable(outp)
    ensure_path_writable(rep)
    ensure_path_writable(ck)

    df_in = pd.read_excel(inp)
    df_in = prepare_input_dataframe(df_in)
    print("Detected input columns:", df_in.columns.tolist())
    print(df_in.head(5).to_string(index=False))

    df_out, summary = enrich_dataframe_stream(
        df_in,
        api_key=args.api_key,
        model=args.model,
        chunk_size=args.chunk_size,
        checkpoint_file=ck,
    )

    # Final writes (checkpoint already has full/partial results).
    df_out.to_excel(outp, index=False)
    rep.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"Saved output: {outp}")
    print(f"Saved report: {rep}")
    print(f"Saved checkpoint: {ck}")
    print(json.dumps(summary, ensure_ascii=False, indent=2))
