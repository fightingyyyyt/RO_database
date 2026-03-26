"""
Chemical Normalizer v3 (fast + robust)

Input : step1.xlsx (default: first column contains raw names)
Output:
  - step2.xlsx              (3 columns only)
  - step2_debug.xlsx        (full debug with status/category/candidates/source)
  - step2_failures.xlsx     (all non-FOUND rows)
Extras:
  - chem_cache.sqlite       (persistent cache: name->cids, smiles->cids, cid->props)
  - step2_checkpoint.csv    (checkpoint for resume)

Recommended:
  pip install pandas openpyxl requests tqdm pubchempy rdkit deep-translator py2opsin
"""

import argparse
import csv
import json
import math
import os
import re
import sqlite3
import time
import unicodedata
from dataclasses import dataclass, asdict
from difflib import SequenceMatcher
from typing import Dict, List, Optional, Tuple
from urllib.parse import quote

import pandas as pd
import requests

# Optional deps
HAS_TQDM = False
try:
    from tqdm.auto import tqdm
    HAS_TQDM = True
except Exception:
    tqdm = None

HAS_RDKIT = False
try:
    from rdkit import Chem
    from rdkit.Chem import rdMolDescriptors
    from rdkit import RDLogger
    HAS_RDKIT = True
    # silence noisy RDKit warnings like "not removing hydrogen atom without neighbors"
    RDLogger.DisableLog("rdApp.warning")
except Exception:
    Chem = None
    rdMolDescriptors = None

HAS_DEEP_TRANSLATOR = False
try:
    from deep_translator import GoogleTranslator
    HAS_DEEP_TRANSLATOR = True
except Exception:
    GoogleTranslator = None

HAS_PY2OPSIN = False
try:
    from py2opsin import py2opsin
    HAS_PY2OPSIN = True
except Exception:
    py2opsin = None


# ------------------------------ config ------------------------------------- #
PUBCHEM_BASE = "https://pubchem.ncbi.nlm.nih.gov/rest/pug"
USER_AGENT = "ChemicalNormalizerV3/3.0"
DEFAULT_TIMEOUT = 8
MAX_RETRIES = 3
MIN_REQUEST_INTERVAL = 0.12  # rate-limit (avoid 503/429)
SAVE_EVERY = 100             # checkpoint

# Conservative skip keywords (only for strong skip decisions)
MATERIAL_HINTS = {
    "membrane", "substrate", "support", "composite", "nanosheet", "nanoparticle", "hydrogel",
    "thin-film", "tfc", "tfm", "layer", "active layer", "mesh", "filter", "fabric",
    "asymmetric membrane", "scaffold", "matrix"
}
PROCESS_HINTS = {
    "cleaning", "activation", "etching", "annealing", "ultrasonication",
    "crosslinking", "cross-linking", "oxidation", "hydrolysis"
}
CHEM_HINTS = {
    "acid", "chloride", "bromide", "iodide", "fluoride", "nitrate", "sulfate", "phosphate",
    "amine", "diamine", "triamine", "aniline", "pyridine", "benzene", "phenyl",
    "aldehyde", "ketone", "ester", "ether", "alcohol", "thiol",
    "sulfonyl", "sulfone", "isocyanate", "epoxide", "urea"
}
NOTE_KEYWORDS = {
    "solution", "aqueous", "dispersion", "suspension", "buffer", "emulsion"
}

# Tiny safe CN glossary (keep small; translator handles rest)
CN_GLOSSARY = {
    "哌嗪": "piperazine",
    "间苯二胺": "m-phenylenediamine",
    "均苯三甲酰氯": "trimesoyl chloride",
    "对苯二甲酰氯": "terephthaloyl chloride",
    "聚醚砜": "polyethersulfone",
    "聚砜": "polysulfone",
    "聚丙烯腈": "polyacrylonitrile",
    "丙酮": "acetone",
    "二甲基甲酰胺": "N,N-dimethylformamide",
    "二甲基亚砜": "dimethyl sulfoxide",
    "NMP": "1-methyl-2-pyrrolidinone",
}


@dataclass
class ResolveResult:
    RowIndex: int
    Original_Name: str
    Molecular_Formula: Optional[str] = None
    IUPAC_Name: Optional[str] = None
    Standardized_Query: Optional[str] = None
    CID: Optional[int] = None
    Canonical_SMILES: Optional[str] = None
    Match_Source: Optional[str] = None  # pubchem_name / pubchem_substance / opsin_smiles / cir_smiles
    Status: str = "FAILED"              # FOUND / SKIPPED / FAILED
    Failure_Category: Optional[str] = None
    Failure_Detail: Optional[str] = None
    Candidates_Tried: Optional[str] = None


# ------------------------------ utilities ---------------------------------- #
def unify_text(text: str) -> str:
    if text is None or (isinstance(text, float) and math.isnan(text)):
        return ""
    s = str(text).strip()
    s = unicodedata.normalize("NFKC", s)
    s = s.replace("（", "(").replace("）", ")")
    s = s.replace("【", "[").replace("】", "]")
    s = s.replace("，", ",").replace("；", ";").replace("：", ":")
    s = s.replace("—", "-").replace("–", "-")
    s = re.sub(r"[™®©]", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def normalize_spelling(s: str) -> str:
    repl = {
        "sulph": "sulf",
        "aluminium": "aluminum",
        "caesium": "cesium",
        "fibre": "fiber",
    }
    out = s
    for a, b in repl.items():
        out = re.sub(a, b, out, flags=re.IGNORECASE)
    return out


def strip_concentration_and_context(s: str) -> str:
    out = s

    # remove leading concentrations/percentages
    out = re.sub(r"^\s*\d+(\.\d+)?\s*(wt%|vol%|mol%|%|ppm|ppb)\s*", "", out, flags=re.I)
    out = re.sub(r"^\s*\d+(\.\d+)?\s*(mg/l|g/l|mg|g|kg|ml|l|mM|uM|μM|nM|M)\s*", "", out, flags=re.I)

    # remove trailing solution descriptors (only at tail)
    out = re.sub(
        r"\b(aqueous solution|aqueous|solution|suspension|dispersion|buffer)\b\s*$",
        "",
        out,
        flags=re.I,
    )

    # remove obvious tail notes in parentheses if they are short alias/note
    m = re.match(r"^(.*\S)\s*\(([^()]*)\)\s*$", out)
    if m:
        prefix = m.group(1).strip()
        inner = m.group(2).strip()
        inner_l = inner.lower()
        looks_like_short_alias = (1 <= len(inner) <= 18) and re.fullmatch(r"[A-Za-z0-9+\-]{1,18}", inner)
        looks_like_note = (inner_l in NOTE_KEYWORDS) or any(k in inner_l for k in NOTE_KEYWORDS)
        if prefix and (looks_like_short_alias or looks_like_note):
            out = prefix

    out = re.sub(r"\s+", " ", out).strip(" ;,")
    return out


def has_chinese(s: str) -> bool:
    return bool(re.search(r"[\u4e00-\u9fff]", s))


def looks_like_model_code(s: str) -> bool:
    # e.g. TW30-1812-75, BW30, UF-150, etc.
    if re.search(r"\b(TW30|BW30|SW30|XLE|NF90|NF270|RO)\b", s, flags=re.I):
        return True
    return bool(re.fullmatch(r"[A-Za-z]{1,5}\d[\w\-]{2,}", s))


def is_measurement_only(s: str) -> bool:
    low = s.lower()
    if not re.search(r"\d", low):
        return False
    return any(k in low for k in ["wt%", "vol%", "mol%", "%", "ppm", "ppb", "mg/l", "g/l", "khz", "hz", "µm", "um"])


def is_mixture_or_list(s: str) -> bool:
    # Avoid false positive for locants like 1,4-
    if "," in s and re.match(r"^\d+,\d+[-(]", s):
        return False

    # separators suggesting mixture/list
    if "/" in s and not re.search(r"\d+/\d+", s):
        parts = [p.strip() for p in s.split("/") if p.strip()]
        return len(parts) >= 2

    if ";" in s:
        parts = [p.strip() for p in s.split(";") if p.strip()]
        return len(parts) >= 2

    # "A and B", "A + B"
    if re.search(r"\b(and|\+)\b", s, flags=re.I):
        return True

    # comma-separated list with multiple short tokens
    if "," in s:
        parts = [p.strip() for p in s.split(",") if p.strip()]
        if len(parts) >= 2:
            short_parts = sum(len(p) <= 12 for p in parts)
            if short_parts >= 2:
                return True

    return False


def chemical_likeness(s: str) -> float:
    """
    Heuristic score: higher means more likely to be a chemical name.
    Used only to avoid over-skipping.
    """
    low = s.lower()
    score = 0.0
    if re.search(r"\d+,\d+|\d+-", s):  # locants
        score += 0.7
    if any(h in low for h in CHEM_HINTS):
        score += 0.6
    if re.search(r"[a-z]", s) and re.search(r"[A-Z]", s):
        score += 0.2
    if len(s) >= 8:
        score += 0.2
    return score


def similarity(a: str, b: str) -> float:
    return SequenceMatcher(None, a.lower(), b.lower()).ratio()


# ------------------------------ translator --------------------------------- #
class OptionalTranslator:
    def __init__(self, enable_google: bool = True):
        self.google = None
        if enable_google and HAS_DEEP_TRANSLATOR:
            try:
                self.google = GoogleTranslator(source="auto", target="en")
            except Exception:
                self.google = None

    def translate(self, text: str) -> str:
        if not text or not has_chinese(text):
            return text

        tmp = text
        for zh, en in CN_GLOSSARY.items():
            tmp = tmp.replace(zh, en)

        if not has_chinese(tmp):
            return tmp

        # Do not translate formula-like strings
        if re.fullmatch(r"[A-Za-z0-9\-\+\(\)\[\],./\s]+", tmp):
            return tmp

        if self.google is None:
            return tmp

        try:
            return self.google.translate(tmp)
        except Exception:
            return tmp


# ------------------------------ SQLite cache -------------------------------- #
class SQLiteCache:
    def __init__(self, path: str):
        self.path = path
        self.conn = sqlite3.connect(path)
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self._init_tables()

    def _init_tables(self):
        cur = self.conn.cursor()
        cur.execute("""
        CREATE TABLE IF NOT EXISTS name_cids (
            query TEXT NOT NULL,
            domain TEXT NOT NULL,
            cids_json TEXT,
            PRIMARY KEY (query, domain)
        );
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS smiles_cids (
            smiles TEXT PRIMARY KEY,
            cids_json TEXT
        );
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS cid_props (
            cid INTEGER PRIMARY KEY,
            molecular_formula TEXT,
            iupac_name TEXT,
            isomeric_smiles TEXT,
            connectivity_smiles TEXT
        );
        """)
        self.conn.commit()

    def get_name_cids(self, query: str, domain: str) -> Optional[List[int]]:
        cur = self.conn.cursor()
        row = cur.execute("SELECT cids_json FROM name_cids WHERE query=? AND domain=?", (query, domain)).fetchone()
        if not row or row[0] is None:
            return None
        try:
            return json.loads(row[0])
        except Exception:
            return None

    def set_name_cids(self, query: str, domain: str, cids: List[int]):
        cur = self.conn.cursor()
        cur.execute(
            "INSERT OR REPLACE INTO name_cids(query, domain, cids_json) VALUES(?,?,?)",
            (query, domain, json.dumps(cids))
        )
        self.conn.commit()

    def get_smiles_cids(self, smiles: str) -> Optional[List[int]]:
        cur = self.conn.cursor()
        row = cur.execute("SELECT cids_json FROM smiles_cids WHERE smiles=?", (smiles,)).fetchone()
        if not row or row[0] is None:
            return None
        try:
            return json.loads(row[0])
        except Exception:
            return None

    def set_smiles_cids(self, smiles: str, cids: List[int]):
        cur = self.conn.cursor()
        cur.execute(
            "INSERT OR REPLACE INTO smiles_cids(smiles, cids_json) VALUES(?,?)",
            (smiles, json.dumps(cids))
        )
        self.conn.commit()

    def get_cid_props(self, cid: int) -> Optional[dict]:
        cur = self.conn.cursor()
        row = cur.execute(
            "SELECT molecular_formula, iupac_name, isomeric_smiles, connectivity_smiles FROM cid_props WHERE cid=?",
            (cid,)
        ).fetchone()
        if not row:
            return None
        return {
            "MolecularFormula": row[0],
            "IUPACName": row[1],
            "IsomericSMILES": row[2],
            "ConnectivitySMILES": row[3],
        }

    def set_cid_props(self, cid: int, props: dict):
        cur = self.conn.cursor()
        cur.execute(
            "INSERT OR REPLACE INTO cid_props(cid, molecular_formula, iupac_name, isomeric_smiles, connectivity_smiles) "
            "VALUES(?,?,?,?,?)",
            (
                cid,
                props.get("MolecularFormula"),
                props.get("IUPACName"),
                props.get("IsomericSMILES"),
                props.get("ConnectivitySMILES"),
            )
        )
        self.conn.commit()

    def close(self):
        self.conn.close()


# ------------------------------ PubChem client ------------------------------ #
class PubChemClient:
    def __init__(self, timeout: int, min_interval: float, max_retries: int, cache: SQLiteCache):
        self.timeout = timeout
        self.min_interval = min_interval
        self.max_retries = max_retries
        self.cache = cache
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT})
        self._last_req_t = 0.0

    def _rate_limit(self):
        dt = time.time() - self._last_req_t
        if dt < self.min_interval:
            time.sleep(self.min_interval - dt)

    def _get_json(self, url: str) -> Optional[dict]:
        for attempt in range(self.max_retries):
            try:
                self._rate_limit()
                r = self.session.get(url, timeout=self.timeout)
                self._last_req_t = time.time()

                if r.status_code == 200:
                    return r.json()

                # common throttling
                if r.status_code in (429, 500, 503):
                    time.sleep(min(2 ** attempt, 8) * 0.6)
                    continue

                # not found / bad request
                if r.status_code in (400, 404):
                    return None

                time.sleep(min(2 ** attempt, 8) * 0.6)
            except Exception:
                time.sleep(min(2 ** attempt, 8) * 0.6)
        return None

    def name_to_cids(self, name: str, domain: str = "compound") -> List[int]:
        """
        domain: compound or substance
        """
        cached = self.cache.get_name_cids(name, domain)
        if cached is not None:
            return cached

        # NOTE: PubChem supports /compound/name/<name>/cids and /substance/name/<name>/cids
        url = f"{PUBCHEM_BASE}/{domain}/name/{quote(name)}/cids/JSON"
        data = self._get_json(url)
        if not data:
            self.cache.set_name_cids(name, domain, [])
            return []

        cids = data.get("IdentifierList", {}).get("CID", []) or []
        # ensure int list
        out = []
        for x in cids:
            try:
                out.append(int(x))
            except Exception:
                pass
        self.cache.set_name_cids(name, domain, out)
        return out

    def smiles_to_cids(self, smiles: str) -> List[int]:
        cached = self.cache.get_smiles_cids(smiles)
        if cached is not None:
            return cached

        url = f"{PUBCHEM_BASE}/compound/smiles/{quote(smiles)}/cids/JSON"
        data = self._get_json(url)
        if not data:
            self.cache.set_smiles_cids(smiles, [])
            return []

        cids = data.get("IdentifierList", {}).get("CID", []) or []
        out = []
        for x in cids:
            try:
                out.append(int(x))
            except Exception:
                pass
        self.cache.set_smiles_cids(smiles, out)
        return out

    def fetch_props_batch(self, cids: List[int]) -> Dict[int, dict]:
        """
        Batch fetch: MolecularFormula, IUPACName, IsomericSMILES, ConnectivitySMILES
        Returns dict[cid] = props
        """
        # split into cached vs missing
        missing = []
        out: Dict[int, dict] = {}
        for cid in cids:
            props = self.cache.get_cid_props(cid)
            if props is not None:
                out[cid] = props
            else:
                missing.append(cid)

        if not missing:
            return out

        # PubChem batch limit: keep it modest
        CHUNK = 200
        for i in range(0, len(missing), CHUNK):
            chunk = missing[i:i+CHUNK]
            cid_str = ",".join(str(x) for x in chunk)
            url = (
                f"{PUBCHEM_BASE}/compound/cid/{cid_str}/property/"
                "MolecularFormula,IUPACName,IsomericSMILES,ConnectivitySMILES/JSON"
            )
            data = self._get_json(url)
            if not data:
                continue

            rows = data.get("PropertyTable", {}).get("Properties", []) or []
            for row in rows:
                cid = row.get("CID")
                if cid is None:
                    continue
                try:
                    cid = int(cid)
                except Exception:
                    continue
                props = {
                    "MolecularFormula": row.get("MolecularFormula"),
                    "IUPACName": row.get("IUPACName"),
                    "IsomericSMILES": row.get("IsomericSMILES"),
                    "ConnectivitySMILES": row.get("ConnectivitySMILES"),
                }
                out[cid] = props
                self.cache.set_cid_props(cid, props)

        return out


# ------------------------------ OPSIN / CIR -------------------------------- #
def opsin_name_to_smiles(name: str) -> Optional[str]:
    if not HAS_PY2OPSIN:
        return None
    try:
        res = py2opsin(
            chemical_name=name,
            output_format="SMILES",
            allow_acid=True,
            allow_radicals=False,
            allow_bad_stereo=True,
        )
        if res and isinstance(res, str):
            return res.strip()
    except Exception:
        return None
    return None


def cir_name_to_smiles(session: requests.Session, name: str, timeout: int = 6) -> Optional[str]:
    try:
        url = f"https://cactus.nci.nih.gov/chemical/structure/{quote(name)}/smiles"
        r = session.get(url, timeout=timeout)
        if r.status_code == 200:
            txt = r.text.strip()
            return txt if txt else None
        return None
    except Exception:
        return None


# ------------------------------ resolver ----------------------------------- #
class ChemicalResolverV3:
    def __init__(
        self,
        pubchem: PubChemClient,
        translator: OptionalTranslator,
        max_candidates: int = 8
    ):
        self.pubchem = pubchem
        self.translator = translator
        self.max_candidates = max_candidates

        self.abbrev_map: Dict[str, str] = {}
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT})

        # in-memory caches to reduce sqlite churn
        self._opsin_cache: Dict[str, Optional[str]] = {}
        self._cir_cache: Dict[str, Optional[str]] = {}

    def build_abbreviation_map(self, names: List[str]) -> None:
        """
        Learn: Full Name (ABBR)  => ABBR -> Full Name
        Only when ABBR looks short and full looks chemical-like (or at least not pure process).
        """
        for raw in names:
            s = unify_text(raw)
            m = re.match(r"^(.*\S)\s*\(([A-Za-z][A-Za-z0-9+\-]{1,20})\)\s*$", s)
            if not m:
                continue
            full = strip_concentration_and_context(m.group(1).strip())
            abbr = m.group(2).strip()

            if len(full) < 4:
                continue
            # avoid learning "membrane (TFC)" as abbreviation mapping
            low_full = full.lower()
            if any(k in low_full for k in PROCESS_HINTS) and chemical_likeness(full) < 0.5:
                continue
            self.abbrev_map.setdefault(abbr, full)

    def generate_candidates(self, raw_name: str) -> List[str]:
        raw = unify_text(raw_name)
        if not raw:
            return []

        candidates: List[str] = []
        seen = set()

        def add(x: str):
            if not x:
                return
            x = unify_text(x)
            x = normalize_spelling(x)
            x = re.sub(r"\s+", " ", x).strip(" ;,")
            if len(x) < 2:
                return
            key = x.lower()
            if key not in seen:
                seen.add(key)
                candidates.append(x)

        add(raw)

        c1 = strip_concentration_and_context(raw)
        add(c1)

        # tail alias case: "full (ABBR)" => try full + abbrev expansion
        m = re.match(r"^(.*\S)\s*\(([A-Za-z][A-Za-z0-9+\-]{1,20})\)\s*$", raw)
        if m:
            full = strip_concentration_and_context(m.group(1).strip())
            abbr = m.group(2).strip()
            add(full)
            if abbr in self.abbrev_map:
                add(self.abbrev_map[abbr])

        # abbreviation-only expansion
        if raw in self.abbrev_map:
            add(self.abbrev_map[raw])
        if c1 in self.abbrev_map:
            add(self.abbrev_map[c1])

        # Chinese translation (keep original too)
        if has_chinese(raw):
            tr = self.translator.translate(raw)
            add(tr)
            add(strip_concentration_and_context(tr))

        # If there's a trailing (...) that is NOT a short alias, try prefix too
        m2 = re.match(r"^(.*\S)\s*\(([^()]*)\)\s*$", c1)
        if m2:
            prefix = m2.group(1).strip()
            inner = m2.group(2).strip()
            add(prefix)
            # Only try "inner" when it looks like a real chemical phrase (not short alias)
            if len(inner) >= 8 and re.search(r"[a-z]", inner) and (" " in inner or "-" in inner):
                add(inner)

        return candidates[: self.max_candidates]

    def classify_skip(self, raw_name: str, candidates: List[str]) -> Tuple[Optional[str], Optional[str]]:
        raw = unify_text(raw_name)
        if not raw:
            return "EMPTY", "empty cell"

        if looks_like_model_code(raw):
            return "MODEL_CODE", "product/membrane model code"

        if is_measurement_only(raw):
            return "MEASUREMENT", "measurement/concentration/condition"

        if re.fullmatch(r"[-+]?\d+(\.\d+)?", raw):
            return "NUMERIC", "pure numeric value"

        if is_mixture_or_list(raw):
            return "MULTI_COMPONENT", "mixture or multiple chemicals in one cell (needs split)"

        # MATERIAL/PROCESS: only skip when it does NOT look chemical-like at all
        low = raw.lower()
        if any(k in low for k in MATERIAL_HINTS) or any(k in low for k in PROCESS_HINTS):
            if chemical_likeness(raw) < 0.4:
                return "MATERIAL_OR_PROCESS", "material/process/system rather than a single compound"

        # Abbreviation-only without expansion (skip to avoid wasting PubChem calls)
        if re.fullmatch(r"[A-Za-z0-9+\-]{2,12}", raw) and raw.isupper():
            if raw not in self.abbrev_map:
                return "ABBREVIATION_ONLY", "abbreviation/code without expansion context"

        return None, None

    def choose_best_cid(self, query: str, cids: List[int]) -> Tuple[Optional[int], Optional[dict], float]:
        """
        Choose best CID based on similarity between query and IUPACName (cheap, no synonyms).
        """
        if not cids:
            return None, None, -1.0
        props_map = self.pubchem.fetch_props_batch(cids[:8])
        best_cid = None
        best_props = None
        best_score = -1.0
        for cid in cids[:8]:
            props = props_map.get(cid)
            if not props:
                continue
            iupac = props.get("IUPACName") or ""
            sc = similarity(query, iupac) if iupac else 0.0

            # light bonus: if query is substring
            if iupac and query.lower() in iupac.lower():
                sc += 0.08

            if sc > best_score:
                best_score = sc
                best_cid = cid
                best_props = props
        return best_cid, best_props, best_score

    def rdkit_canonicalize(self, smiles: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
        if not HAS_RDKIT or not smiles:
            return smiles, None
        try:
            mol = Chem.MolFromSmiles(smiles)
            if mol is None:
                return smiles, None
            can = Chem.MolToSmiles(mol, canonical=True, isomericSmiles=True)
            formula = rdMolDescriptors.CalcMolFormula(mol)
            return can, formula
        except Exception:
            return smiles, None

    # -------------------- pass 1: name-only (fast) -------------------- #
    def resolve_fast(self, row_idx: int, raw_name: str) -> ResolveResult:
        res = ResolveResult(RowIndex=row_idx, Original_Name=raw_name)
        candidates = self.generate_candidates(raw_name)
        res.Candidates_Tried = " || ".join(candidates)

        skip_cat, skip_detail = self.classify_skip(raw_name, candidates)
        if skip_cat:
            res.Status = "SKIPPED"
            res.Failure_Category = skip_cat
            res.Failure_Detail = skip_detail
            return res

        best = None  # (cid, props, score, source, query)
        for q in candidates:
            # compound name
            cids = self.pubchem.name_to_cids(q, domain="compound")
            cid, props, sc = self.choose_best_cid(q, cids)
            if cid is not None and props is not None:
                best = (cid, props, sc, "pubchem_name", q)
                if sc >= 0.92:  # early stop if confident
                    break

            # substance name (recovers more aliases sometimes)
            cids_s = self.pubchem.name_to_cids(q, domain="substance")
            cid2, props2, sc2 = self.choose_best_cid(q, cids_s)
            if cid2 is not None and props2 is not None:
                # tiny bias to prefer compound-name, but allow substance if better
                sc2_adj = sc2 + 0.03
                if best is None or sc2_adj > best[2]:
                    best = (cid2, props2, sc2_adj, "pubchem_substance", q)

        if best is None:
            res.Status = "FAILED"
            res.Failure_Category = "NOT_FOUND_FAST"
            res.Failure_Detail = "no match from PubChem name search in fast pass"
            return res

        cid, props, sc, source, q = best
        res.Status = "FOUND"
        res.Match_Source = source
        res.Standardized_Query = q
        res.CID = cid

        res.Molecular_Formula = props.get("MolecularFormula")
        res.IUPAC_Name = props.get("IUPACName")

        smiles = props.get("IsomericSMILES") or props.get("ConnectivitySMILES")
        can_smi, rd_formula = self.rdkit_canonicalize(smiles)
        res.Canonical_SMILES = can_smi
        if not res.Molecular_Formula and rd_formula:
            res.Molecular_Formula = rd_formula

        return res

    # -------------------- pass 2: hard cases (slow) -------------------- #
    def resolve_deep(self, row_idx: int, raw_name: str, existing: ResolveResult) -> ResolveResult:
        """
        Only called when fast pass failed (NOT_FOUND_FAST).
        Try OPSIN / CIR -> SMILES -> PubChem.
        """
        res = existing
        candidates = self.generate_candidates(raw_name)

        # OPSIN
        for q in candidates:
            if q in self._opsin_cache:
                smi = self._opsin_cache[q]
            else:
                smi = opsin_name_to_smiles(q)
                self._opsin_cache[q] = smi

            if not smi:
                continue

            cids = self.pubchem.smiles_to_cids(smi)
            cid, props, sc = self.choose_best_cid(q, cids)
            if cid is not None and props is not None:
                res.Status = "FOUND"
                res.Match_Source = "opsin_smiles"
                res.Standardized_Query = q
                res.CID = cid
                res.Molecular_Formula = props.get("MolecularFormula")
                res.IUPAC_Name = props.get("IUPACName")

                smiles = props.get("IsomericSMILES") or props.get("ConnectivitySMILES") or smi
                can_smi, rd_formula = self.rdkit_canonicalize(smiles)
                res.Canonical_SMILES = can_smi
                if not res.Molecular_Formula and rd_formula:
                    res.Molecular_Formula = rd_formula
                return res

        # CIR
        for q in candidates:
            if q in self._cir_cache:
                smi = self._cir_cache[q]
            else:
                smi = cir_name_to_smiles(self.session, q, timeout=6)
                self._cir_cache[q] = smi

            if not smi:
                continue

            cids = self.pubchem.smiles_to_cids(smi)
            cid, props, sc = self.choose_best_cid(q, cids)
            if cid is not None and props is not None:
                res.Status = "FOUND"
                res.Match_Source = "cir_smiles"
                res.Standardized_Query = q
                res.CID = cid
                res.Molecular_Formula = props.get("MolecularFormula")
                res.IUPAC_Name = props.get("IUPACName")

                smiles = props.get("IsomericSMILES") or props.get("ConnectivitySMILES") or smi
                can_smi, rd_formula = self.rdkit_canonicalize(smiles)
                res.Canonical_SMILES = can_smi
                if not res.Molecular_Formula and rd_formula:
                    res.Molecular_Formula = rd_formula
                return res

        # still failed
        res.Status = "FAILED"
        res.Failure_Category = "NOT_RESOLVED"
        res.Failure_Detail = "fast pass failed, and OPSIN/CIR did not resolve"
        return res


# ------------------------------ IO helpers --------------------------------- #
def load_checkpoint(checkpoint_csv: str) -> Dict[int, ResolveResult]:
    """
    Load previously processed rows from checkpoint CSV.
    Returns dict[row_idx] -> ResolveResult
    """
    if not os.path.exists(checkpoint_csv):
        return {}
    out = {}
    df = pd.read_csv(checkpoint_csv)
    for _, row in df.iterrows():
        idx = int(row["RowIndex"])
        rr = ResolveResult(
            RowIndex=idx,
            Original_Name=str(row.get("Original_Name", "")),
            Molecular_Formula=row.get("Molecular_Formula") if pd.notna(row.get("Molecular_Formula")) else None,
            IUPAC_Name=row.get("IUPAC_Name") if pd.notna(row.get("IUPAC_Name")) else None,
            Standardized_Query=row.get("Standardized_Query") if pd.notna(row.get("Standardized_Query")) else None,
            CID=int(row["CID"]) if pd.notna(row.get("CID")) else None,
            Canonical_SMILES=row.get("Canonical_SMILES") if pd.notna(row.get("Canonical_SMILES")) else None,
            Match_Source=row.get("Match_Source") if pd.notna(row.get("Match_Source")) else None,
            Status=str(row.get("Status", "FAILED")),
            Failure_Category=row.get("Failure_Category") if pd.notna(row.get("Failure_Category")) else None,
            Failure_Detail=row.get("Failure_Detail") if pd.notna(row.get("Failure_Detail")) else None,
            Candidates_Tried=row.get("Candidates_Tried") if pd.notna(row.get("Candidates_Tried")) else None,
        )
        out[idx] = rr
    return out


def append_checkpoint(checkpoint_csv: str, rows: List[ResolveResult], write_header_if_new: bool):
    fieldnames = list(asdict(rows[0]).keys()) if rows else []
    mode = "a" if os.path.exists(checkpoint_csv) else "w"
    with open(checkpoint_csv, mode, newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if write_header_if_new and mode == "w":
            writer.writeheader()
        for r in rows:
            writer.writerow(asdict(r))


def save_outputs(results: List[ResolveResult], out_main: str, out_debug: str, out_failed: str):
    debug_df = pd.DataFrame([asdict(r) for r in results]).sort_values("RowIndex")

    main_df = debug_df[["Original_Name", "Molecular_Formula", "IUPAC_Name"]].copy()
    main_df.to_excel(out_main, index=False)

    with pd.ExcelWriter(out_debug, engine="openpyxl") as writer:
        debug_df.to_excel(writer, sheet_name="all_results", index=False)
        debug_df[debug_df["Status"] == "FOUND"].to_excel(writer, sheet_name="found", index=False)
        debug_df[debug_df["Status"] == "SKIPPED"].to_excel(writer, sheet_name="skipped", index=False)
        debug_df[debug_df["Status"] == "FAILED"].to_excel(writer, sheet_name="failed", index=False)

    debug_df[debug_df["Status"] != "FOUND"].to_excel(out_failed, index=False)


# ------------------------------ main --------------------------------------- #
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default="./step1/test5/step1.xlsx")
    ap.add_argument("--output", default="./step1/test5/step2.xlsx")
    ap.add_argument("--debug", default="./step1/test5/step2_debug.xlsx")
    ap.add_argument("--failed", default="./step1/test5/step2_failures.xlsx")
    ap.add_argument("--cache", default="./step1/test5/chem_cache.sqlite")
    ap.add_argument("--checkpoint", default="./step1/test5/step2_checkpoint.csv")
    ap.add_argument("--resume", action="store_true", help="resume from checkpoint if exists")
    ap.add_argument("--no-translate", action="store_true")
    ap.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT)
    ap.add_argument("--deep", action="store_true", help="run deep pass (OPSIN/CIR) on failed after fast pass")
    ap.add_argument("--save-every", type=int, default=SAVE_EVERY)
    args = ap.parse_args()

    if not os.path.exists(args.input):
        raise FileNotFoundError(f"Cannot find input file: {args.input}")

    df = pd.read_excel(args.input)
    if df.empty:
        raise ValueError("Input workbook is empty")

    col = df.columns[0]
    names = df[col].fillna("").astype(str).tolist()
    n = len(names)

    cache = SQLiteCache(args.cache)
    pubchem = PubChemClient(timeout=args.timeout, min_interval=MIN_REQUEST_INTERVAL, max_retries=MAX_RETRIES, cache=cache)
    translator = OptionalTranslator(enable_google=(not args.no_translate))
    resolver = ChemicalResolverV3(pubchem=pubchem, translator=translator, max_candidates=8)

    print(f"Loaded {n} rows from {args.input}")
    print("Building abbreviation map...")
    resolver.build_abbreviation_map(names)
    print(f"Abbreviation map size: {len(resolver.abbrev_map)}")

    done: Dict[int, ResolveResult] = {}
    if args.resume:
        done = load_checkpoint(args.checkpoint)
        if done:
            print(f"Resume enabled: loaded {len(done)} rows from checkpoint: {args.checkpoint}")

    results: List[ResolveResult] = []
    found = failed = skipped = 0

    # If resuming, preload counters
    for rr in done.values():
        if rr.Status == "FOUND":
            found += 1
        elif rr.Status == "SKIPPED":
            skipped += 1
        else:
            failed += 1

    batch_to_save: List[ResolveResult] = []
    start_t = time.time()

    iterable = list(enumerate(names, start=1))
    if HAS_TQDM:
        pbar = tqdm(iterable, total=n, desc="Fast pass (PubChem name)", unit="item")
    else:
        pbar = iterable

    for i, raw in pbar:
        if i in done:
            continue

        rr = resolver.resolve_fast(i, raw)
        results.append(rr)
        batch_to_save.append(rr)

        if rr.Status == "FOUND":
            found += 1
        elif rr.Status == "SKIPPED":
            skipped += 1
        else:
            failed += 1

        # update progress
        if HAS_TQDM:
            elapsed = max(time.time() - start_t, 1e-6)
            rate = elapsed / max((found + failed + skipped), 1)
            pbar.set_postfix(found=found, failed=failed, skipped=skipped, sec_per_item=f"{rate:.2f}", refresh=False)
        else:
            if i % 50 == 0:
                print(f"[{i}/{n}] found={found} failed={failed} skipped={skipped}")

        # checkpoint
        if len(batch_to_save) >= args.save_every:
            append_checkpoint(args.checkpoint, batch_to_save, write_header_if_new=True)
            batch_to_save = []

    # flush remaining checkpoint
    if batch_to_save:
        append_checkpoint(args.checkpoint, batch_to_save, write_header_if_new=True)

    # merge resume + new
    merged = {**done}
    for rr in results:
        merged[rr.RowIndex] = rr
    all_results = [merged[i] for i in sorted(merged.keys())]

    # Deep pass only on fast-failed rows (optional)
    if args.deep:
        deep_targets = [r for r in all_results if r.Status == "FAILED" and r.Failure_Category == "NOT_FOUND_FAST"]
        print(f"\nDeep pass enabled: {len(deep_targets)} hard cases (OPSIN/CIR)...")

        if HAS_TQDM:
            p2 = tqdm(deep_targets, total=len(deep_targets), desc="Deep pass (OPSIN/CIR)", unit="item")
        else:
            p2 = deep_targets

        updated = 0
        for rr in p2:
            new_rr = resolver.resolve_deep(rr.RowIndex, rr.Original_Name, rr)
            if new_rr.Status == "FOUND" and rr.Status != "FOUND":
                updated += 1
            merged[new_rr.RowIndex] = new_rr

            if HAS_TQDM:
                p2.set_postfix(newly_found=updated, refresh=False)

        all_results = [merged[i] for i in sorted(merged.keys())]

    print("\nSaving output files...")
    save_outputs(all_results, args.output, args.debug, args.failed)

    # summary
    c = pd.Series([r.Status for r in all_results]).value_counts()
    print("=" * 72)
    print("DONE")
    print(f"Main:   {args.output}")
    print(f"Debug:  {args.debug}")
    print(f"Failed: {args.failed}")
    print(f"Cache:  {args.cache}")
    print(f"Checkpoint: {args.checkpoint}")
    print(c.to_string())
    print("=" * 72)

    cache.close()


if __name__ == "__main__":
    main()