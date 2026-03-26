#!/usr/bin/env python3
"""
Chemical Normalizer v3.1 (data-driven fix from your 120-row audit)

What changed vs v3.0
--------------------
1) Process/condition rows (Drying at 20°C..., storage, treatment, etc.) -> SKIPPED (PROCESS_CONDITION)
2) Brand/product rows (Dow/FilmTec, SW30HR...) -> SKIPPED (BRAND_OR_PRODUCT)
3) Abbreviation-only rows (DCC, DBP, DBSA, DAPS...) are no longer SKIPPED.
   They become FAILED (ABBREVIATION_ONLY) so they show up in failures for dictionary-building.
4) Better candidate cleanup for nano-forms:
   - "CuO nanoparticles", "CuO-NPs", "CuO纳米颗粒", "二氧化硅纳米颗粒" -> try core ("CuO", "silica"/"SiO2")
5) Add PubChem formula search (fastformula) for formula-like queries:
   - fixes cases where name search is weak (e.g., CuO, hydrates like CuSO4·5H2O).
6) Better stripping of phase tags:
   - "(water phase)", "(aqueous phase)", "水相", "油相" etc.

Outputs
-------
- step2.xlsx              (3 columns)
- step2_debug.xlsx        (all + found/failed/skipped)
- step2_failures.xlsx     (non-FOUND)
- chem_cache.sqlite       (persistent cache)
- step2_checkpoint.csv    (resume)

Install
-------
pip install pandas openpyxl requests tqdm rdkit deep-translator py2opsin
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


PUBCHEM_BASE = "https://pubchem.ncbi.nlm.nih.gov/rest/pug"
USER_AGENT = "ChemicalNormalizerV3.1/3.1"
DEFAULT_TIMEOUT = 8
MAX_RETRIES = 3
MIN_REQUEST_INTERVAL = 0.12
SAVE_EVERY = 100


# --------------------------- domain-specific knobs -------------------------- #
BRAND_WORDS = {
    "dow", "filmtec", "dupont", "hydranautics", "toray", "lg chem", "ge osmonics",
    "vontron", "koch", "lanxess", "toyobo", "microdyn", "nadir", "sepro"
}

# process/condition markers (these should be skipped)
PROCESS_WORDS = {
    "dry", "drying", "storage", "stored", "aging", "soaking", "rinsing", "washing",
    "blowing", "annealing", "curing", "heating", "cooling", "stirring",
    "ultrasonication", "sonication", "exposure", "treatment", "plasma",
    "vacuum", "air drying", "oven", "freeze-drying", "lyophil", "calcination"
}

# nano/material modifiers: we try to strip these to recover core chemical
NANO_MODIFIERS = {
    "nanoparticle", "nanoparticles", "nano-particle", "nano-particles",
    "nanosheet", "nanosheets", "nanofiller", "nanofillers", "nanorod", "nanorods",
    "np", "nps", "nps.", "nps,", "nano", "liposome", "liposomes", "complex", "complexes"
}

# common phase tags to strip
PHASE_TAGS = {
    "water phase", "aqueous phase", "oil phase", "organic phase",
    "water", "aqueous", "oil",
    "水相", "油相", "有机相", "水溶液", "溶液"
}

# chemistry hints (helps decide whether to skip "material-like" strings)
CHEM_HINTS = {
    "acid", "chloride", "bromide", "iodide", "fluoride", "nitrate", "sulfate", "phosphate",
    "amine", "diamine", "triamine", "aniline", "pyridine", "benzene", "phenyl",
    "aldehyde", "ketone", "ester", "ether", "alcohol", "thiol",
    "sulfonyl", "sulfone", "isocyanate", "epoxide", "urea"
}

MATERIAL_HINTS = {
    "membrane", "substrate", "support", "composite", "nanosheet", "nanoparticle", "hydrogel",
    "thin-film", "tfc", "tfm", "layer", "mesh", "filter", "fabric",
    "asymmetric membrane", "reverse osmosis", "ro membrane", "ro膜", "膜"
}

POLYMER_HINTS = {
    "poly(", "poly ", "poly-", "polyamide", "cellulose", "chitosan", "alginate", "sericin",
    "protein", "gelatin", "starch"
}

# Tiny safe CN glossary (extendable)
CN_GLOSSARY = {
    "乙酸": "acetic acid",
    "丙醇": "propanol",
    "乙醇": "ethanol",
    "丙酮": "acetone",
    "二氧化硅": "silica",
    "纳米颗粒": "nanoparticles",
    "水相": "water phase",
    "油相": "oil phase",
    "丝胶": "sericin",
    "二乙酸纤维素": "cellulose diacetate",
    "二醋酸纤维素": "cellulose diacetate",
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
    Match_Source: Optional[str] = None  # pubchem_name / pubchem_substance / pubchem_formula / opsin_smiles / cir_smiles
    Status: str = "FAILED"              # FOUND / SKIPPED / FAILED
    Failure_Category: Optional[str] = None
    Failure_Detail: Optional[str] = None
    Candidates_Tried: Optional[str] = None


def unify_text(text: str) -> str:
    if text is None or (isinstance(text, float) and math.isnan(text)):
        return ""
    s = str(text).strip()
    s = unicodedata.normalize("NFKC", s)
    s = s.replace("（", "(").replace("）", ")")
    s = s.replace("【", "[").replace("】", "]")
    s = s.replace("，", ",").replace("；", ";").replace("：", ":")
    s = s.replace("—", "-").replace("–", "-")
    s = s.replace("℃", "°C")
    s = re.sub(r"[™®©]", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def normalize_spelling(s: str) -> str:
    repl = {"sulph": "sulf", "aluminium": "aluminum", "caesium": "cesium", "fibre": "fiber"}
    out = s
    for a, b in repl.items():
        out = re.sub(a, b, out, flags=re.IGNORECASE)
    return out


def has_chinese(s: str) -> bool:
    return bool(re.search(r"[\u4e00-\u9fff]", s))


def strip_phase_tags(s: str) -> str:
    """
    Remove obvious phase labels like '(water phase)', '(aqueous)', '水相' etc.
    """
    out = s

    # remove bracketed phase tags at end
    out = re.sub(r"\((water phase|aqueous phase|oil phase|organic phase)\)\s*$", "", out, flags=re.I)
    out = re.sub(r"\((water|aqueous|oil|organic)\)\s*$", "", out, flags=re.I)

    # remove Chinese phase tags in parentheses
    out = re.sub(r"[（(]\s*(水相|油相|有机相)\s*[)）]\s*$", "", out)

    # remove trailing "... 水相" / "... 油相"
    out = re.sub(r"\s*(水相|油相|有机相)\s*$", "", out)

    return out.strip(" ;,") if out else out


def strip_concentration_and_context(s: str) -> str:
    out = s

    # remove leading concentrations/percentages
    out = re.sub(r"^\s*\d+(\.\d+)?\s*(wt%|vol%|mol%|%|ppm|ppb)\s*", "", out, flags=re.I)
    out = re.sub(r"^\s*\d+(\.\d+)?\s*(mg/l|g/l|mg|g|kg|ml|l|mM|uM|μM|nM|M)\s*", "", out, flags=re.I)

    # strip phase tags
    out = strip_phase_tags(out)

    # remove trailing solution descriptors
    out = re.sub(r"\b(aqueous solution|solution|suspension|dispersion|buffer)\b\s*$", "", out, flags=re.I)

    # remove obvious tail notes in parentheses if they are short alias/note
    m = re.match(r"^(.*\S)\s*\(([^()]*)\)\s*$", out)
    if m:
        prefix = m.group(1).strip()
        inner = m.group(2).strip()
        inner_l = inner.lower()
        looks_like_short_alias = (1 <= len(inner) <= 18) and re.fullmatch(r"[A-Za-z0-9+\-]{1,18}", inner)
        looks_like_phase = inner_l in PHASE_TAGS or any(t in inner_l for t in PHASE_TAGS)
        if prefix and (looks_like_short_alias or looks_like_phase):
            out = prefix

    out = re.sub(r"\s+", " ", out).strip(" ;,")
    return out


def strip_nano_modifiers(s: str) -> str:
    """
    Attempt to recover core chemical from 'X nanoparticles', 'X-NPs', 'X纳米颗粒', etc.
    """
    out = s

    # normalize common nano tags
    out = out.replace("纳米颗粒", "nanoparticles")
    out = out.replace("纳米粒子", "nanoparticles")

    # suffix patterns: "-NPs", "-NP", "_NPs"
    out = re.sub(r"[-_]\s*(NPs?|nps?)\b", "", out)

    # remove trailing words
    out = re.sub(r"\b(nanoparticles?|nanosheets?|nanofillers?|nanorods?)\b\s*$", "", out, flags=re.I)
    out = re.sub(r"\b(nanoparticles?|nanosheets?|nanofillers?|nanorods?)\b", "", out, flags=re.I)

    return re.sub(r"\s+", " ", out).strip(" ;,") if out else out


def looks_like_model_code(s: str) -> bool:
    if re.search(r"\b(TW30|BW30|SW30|XLE|NF90|NF270|RO)\b", s, flags=re.I):
        return True
    return bool(re.fullmatch(r"[A-Za-z]{1,5}\d[\w\-]{2,}", s))


def is_measurement_only(s: str) -> bool:
    low = s.lower()
    if not re.search(r"\d", low):
        return False
    return any(k in low for k in ["wt%", "vol%", "mol%", "%", "ppm", "ppb", "mg/l", "g/l", "khz", "hz", "µm", "um", "°c"])


def is_mixture_or_list(s: str) -> bool:
    # Avoid false positive for locants like 1,4-
    if "," in s and re.match(r"^\d+,\d+[-(]", s):
        return False

    if "/" in s and not re.search(r"\d+/\d+", s):
        parts = [p.strip() for p in s.split("/") if p.strip()]
        return len(parts) >= 2

    if ";" in s:
        parts = [p.strip() for p in s.split(";") if p.strip()]
        return len(parts) >= 2

    if re.search(r"\b(and|\+)\b", s, flags=re.I):
        return True

    return False


def is_process_condition_text(s: str) -> bool:
    low = s.lower()

    if any(w in low for w in PROCESS_WORDS):
        return True

    # temperature/time patterns
    if re.search(r"\b\d+(\.\d+)?\s*°\s*c\b", low):
        return True
    if re.search(r"\b(at|for|during)\b.*\b\d+(\.\d+)?\s*(min|h|hr|hrs|hour|hours|day|days)\b", low):
        return True

    return False


def is_brand_or_product(s: str) -> bool:
    low = s.lower()
    if any(b in low for b in BRAND_WORDS):
        return True
    # typical membrane product strings: "SW30HR", "TW30-1812-75"
    if re.search(r"\b(sw30|bw30|tw30)\w*\b", low):
        return True
    return False


def chemical_likeness(s: str) -> float:
    low = s.lower()
    score = 0.0
    if re.search(r"\d+,\d+|\d+-", s):
        score += 0.7
    if any(h in low for h in CHEM_HINTS):
        score += 0.6
    if len(s) >= 8:
        score += 0.2
    return score


def looks_formula_like(s: str) -> bool:
    """
    Rough formula detection: CuO, SiO2, CuSO4·5H2O, etc.
    """
    t = s.replace("·", ".")
    # allow dot-separated hydrates
    parts = t.split(".")
    if not parts:
        return False
    base = parts[0].strip()
    if not re.fullmatch(r"(?:[A-Z][a-z]?\d*){2,}", base):
        return False
    # remaining parts could be "5H2O"
    for p in parts[1:]:
        p = p.strip()
        if not p:
            continue
        if not re.fullmatch(r"\d*(?:[A-Z][a-z]?\d*){1,}", p):
            return False
    return True


def similarity(a: str, b: str) -> float:
    return SequenceMatcher(None, (a or "").lower(), (b or "").lower()).ratio()


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
        if not text:
            return text
        tmp = text
        for zh, en in CN_GLOSSARY.items():
            tmp = tmp.replace(zh, en)

        if not has_chinese(tmp):
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
        );""")
        cur.execute("""
        CREATE TABLE IF NOT EXISTS smiles_cids (
            smiles TEXT PRIMARY KEY,
            cids_json TEXT
        );""")
        cur.execute("""
        CREATE TABLE IF NOT EXISTS formula_cids (
            formula TEXT PRIMARY KEY,
            cids_json TEXT
        );""")
        cur.execute("""
        CREATE TABLE IF NOT EXISTS cid_props (
            cid INTEGER PRIMARY KEY,
            molecular_formula TEXT,
            iupac_name TEXT,
            isomeric_smiles TEXT,
            connectivity_smiles TEXT
        );""")
        self.conn.commit()

    def get_one(self, sql: str, params: Tuple):
        cur = self.conn.cursor()
        return cur.execute(sql, params).fetchone()

    def put(self, sql: str, params: Tuple):
        cur = self.conn.cursor()
        cur.execute(sql, params)
        self.conn.commit()

    def get_name_cids(self, query: str, domain: str) -> Optional[List[int]]:
        row = self.get_one("SELECT cids_json FROM name_cids WHERE query=? AND domain=?", (query, domain))
        if not row:
            return None
        return json.loads(row[0]) if row[0] else []

    def set_name_cids(self, query: str, domain: str, cids: List[int]):
        self.put("INSERT OR REPLACE INTO name_cids(query,domain,cids_json) VALUES(?,?,?)", (query, domain, json.dumps(cids)))

    def get_smiles_cids(self, smiles: str) -> Optional[List[int]]:
        row = self.get_one("SELECT cids_json FROM smiles_cids WHERE smiles=?", (smiles,))
        if not row:
            return None
        return json.loads(row[0]) if row[0] else []

    def set_smiles_cids(self, smiles: str, cids: List[int]):
        self.put("INSERT OR REPLACE INTO smiles_cids(smiles,cids_json) VALUES(?,?)", (smiles, json.dumps(cids)))

    def get_formula_cids(self, formula: str) -> Optional[List[int]]:
        row = self.get_one("SELECT cids_json FROM formula_cids WHERE formula=?", (formula,))
        if not row:
            return None
        return json.loads(row[0]) if row[0] else []

    def set_formula_cids(self, formula: str, cids: List[int]):
        self.put("INSERT OR REPLACE INTO formula_cids(formula,cids_json) VALUES(?,?)", (formula, json.dumps(cids)))

    def get_cid_props(self, cid: int) -> Optional[dict]:
        row = self.get_one(
            "SELECT molecular_formula, iupac_name, isomeric_smiles, connectivity_smiles FROM cid_props WHERE cid=?",
            (cid,)
        )
        if not row:
            return None
        return {"MolecularFormula": row[0], "IUPACName": row[1], "IsomericSMILES": row[2], "ConnectivitySMILES": row[3]}

    def set_cid_props(self, cid: int, props: dict):
        self.put(
            "INSERT OR REPLACE INTO cid_props(cid,molecular_formula,iupac_name,isomeric_smiles,connectivity_smiles) VALUES(?,?,?,?,?)",
            (cid, props.get("MolecularFormula"), props.get("IUPACName"), props.get("IsomericSMILES"), props.get("ConnectivitySMILES"))
        )

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
                if r.status_code in (429, 500, 503):
                    time.sleep(min(2 ** attempt, 8) * 0.6)
                    continue
                if r.status_code in (400, 404):
                    return None
                time.sleep(min(2 ** attempt, 8) * 0.6)
            except Exception:
                time.sleep(min(2 ** attempt, 8) * 0.6)
        return None

    def name_to_cids(self, name: str, domain: str = "compound") -> List[int]:
        cached = self.cache.get_name_cids(name, domain)
        if cached is not None:
            return cached
        url = f"{PUBCHEM_BASE}/{domain}/name/{quote(name)}/cids/JSON"
        data = self._get_json(url)
        if not data:
            self.cache.set_name_cids(name, domain, [])
            return []
        cids = data.get("IdentifierList", {}).get("CID", []) or []
        out = [int(x) for x in cids if str(x).isdigit()]
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
        out = [int(x) for x in cids if str(x).isdigit()]
        self.cache.set_smiles_cids(smiles, out)
        return out

    def formula_to_cids(self, formula: str) -> List[int]:
        """
        PubChem supports molecular formula search. The fast synchronous variant is 'fastformula'.
        Example (TXT): /compound/fastformula/C9H8O4/cids/TXT
        Here we use JSON.
        """
        f = formula.replace("·", ".").strip()
        cached = self.cache.get_formula_cids(f)
        if cached is not None:
            return cached

        url = f"{PUBCHEM_BASE}/compound/fastformula/{quote(f)}/cids/JSON"
        data = self._get_json(url)
        if not data:
            self.cache.set_formula_cids(f, [])
            return []
        cids = data.get("IdentifierList", {}).get("CID", []) or []
        out = [int(x) for x in cids if str(x).isdigit()]
        self.cache.set_formula_cids(f, out)
        return out

    def fetch_props_batch(self, cids: List[int]) -> Dict[int, dict]:
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

        CHUNK = 200
        for i in range(0, len(missing), CHUNK):
            chunk = missing[i:i+CHUNK]
            cid_str = ",".join(str(x) for x in chunk)
            url = f"{PUBCHEM_BASE}/compound/cid/{cid_str}/property/MolecularFormula,IUPACName,IsomericSMILES,ConnectivitySMILES/JSON"
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
        res = py2opsin(chemical_name=name, output_format="SMILES", allow_acid=True, allow_radicals=False, allow_bad_stereo=True)
        return res.strip() if isinstance(res, str) and res.strip() else None
    except Exception:
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
class ChemicalResolverV3_1:
    def __init__(self, pubchem: PubChemClient, translator: OptionalTranslator, max_candidates: int = 10):
        self.pubchem = pubchem
        self.translator = translator
        self.max_candidates = max_candidates
        self.abbrev_map: Dict[str, str] = {}
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT})
        self._opsin_cache: Dict[str, Optional[str]] = {}
        self._cir_cache: Dict[str, Optional[str]] = {}

    def build_abbreviation_map(self, names: List[str]) -> None:
        for raw in names:
            s = unify_text(raw)
            m = re.match(r"^(.*\S)\s*\(([A-Za-z][A-Za-z0-9+\-]{1,20})\)\s*$", s)
            if not m:
                continue
            full = strip_concentration_and_context(m.group(1).strip())
            abbr = m.group(2).strip()

            if len(full) < 4:
                continue
            # avoid learning pure process phrases
            if is_process_condition_text(full) and chemical_likeness(full) < 0.4:
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
            x = strip_phase_tags(x)
            x = re.sub(r"\s+", " ", x).strip(" ;,")
            if len(x) < 2:
                return
            key = x.lower()
            if key not in seen:
                seen.add(key)
                candidates.append(x)

        # raw
        add(raw)

        # cleaned context
        c1 = strip_concentration_and_context(raw)
        add(c1)

        # nano-stripped versions
        add(strip_nano_modifiers(c1))
        add(strip_nano_modifiers(raw))

        # tail alias: full (ABBR)
        m = re.match(r"^(.*\S)\s*\(([A-Za-z][A-Za-z0-9+\-]{1,20})\)\s*$", raw)
        if m:
            full = strip_concentration_and_context(m.group(1).strip())
            abbr = m.group(2).strip()
            add(full)
            if abbr in self.abbrev_map:
                add(self.abbrev_map[abbr])

        # abbreviation expansion
        if raw in self.abbrev_map:
            add(self.abbrev_map[raw])
        if c1 in self.abbrev_map:
            add(self.abbrev_map[c1])

        # Chinese translation
        if has_chinese(raw):
            tr = self.translator.translate(raw)
            add(tr)
            add(strip_concentration_and_context(tr))
            add(strip_nano_modifiers(tr))

        # punctuation simplification
        for c in list(candidates):
            simp = re.sub(r"\s*-\s*", "-", c)
            simp = re.sub(r"\s*,\s*", ",", simp)
            add(simp)

        return candidates[: self.max_candidates]

    def classify(self, raw_name: str, candidates: List[str]) -> Tuple[Optional[str], Optional[str]]:
        raw = unify_text(raw_name)
        if not raw:
            return "EMPTY", "empty cell"

        # 1) process/condition first (your audit says these should be SKIPPED)
        if is_process_condition_text(raw):
            return "PROCESS_CONDITION", "process/condition description (drying/storage/treatment/etc.)"

        # 2) brand/product
        if is_brand_or_product(raw):
            return "BRAND_OR_PRODUCT", "brand/product/model string, not a chemical"

        # 3) clear model code
        if looks_like_model_code(raw):
            return "MODEL_CODE", "product/membrane model code"

        # 4) measurement-only
        if is_measurement_only(raw):
            return "MEASUREMENT", "measurement/concentration/condition"

        # 5) multi-component
        if is_mixture_or_list(raw):
            return "MULTI_COMPONENT", "mixture/multiple chemicals in one cell (needs split)"

        # 6) polymers / biomaterials (PubChem often won't give single formula)
        low = raw.lower()
        if any(p in low for p in POLYMER_HINTS):
            # If it still looks like a small molecule (rare), do not skip
            if chemical_likeness(raw) < 0.6:
                return "POLYMER_OR_BIOMATERIAL", "polymer/biomaterial; usually not a single small-molecule record"

        # 7) material-like phrases: only skip if not chemical-like
        if any(k in low for k in MATERIAL_HINTS):
            if chemical_likeness(raw) < 0.4:
                return "MATERIAL_OR_SYSTEM", "material/system phrase rather than a single compound"

        # NOTE: abbreviation-only should NOT be skipped (your request).
        # We'll mark it FAILED later to push into dictionary workflow.
        return None, None

    def choose_best_cid(self, query: str, cids: List[int]) -> Tuple[Optional[int], Optional[dict], float]:
        if not cids:
            return None, None, -1.0
        props_map = self.pubchem.fetch_props_batch(cids[:10])
        best_cid = None
        best_props = None
        best_score = -1.0
        for cid in cids[:10]:
            props = props_map.get(cid)
            if not props:
                continue
            iupac = props.get("IUPACName") or ""
            # if no iupac (common for inorganics), don't punish too much
            sc = similarity(query, iupac) if iupac else 0.15
            if iupac and query.lower() in iupac.lower():
                sc += 0.08
            if sc > best_score:
                best_score, best_cid, best_props = sc, cid, props
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

    def resolve_fast(self, row_idx: int, raw_name: str) -> ResolveResult:
        res = ResolveResult(RowIndex=row_idx, Original_Name=raw_name)
        candidates = self.generate_candidates(raw_name)
        res.Candidates_Tried = " || ".join(candidates)

        skip_cat, skip_detail = self.classify(raw_name, candidates)
        if skip_cat:
            res.Status = "SKIPPED"
            res.Failure_Category = skip_cat
            res.Failure_Detail = skip_detail
            return res

        # abbreviation-only without expansion -> FAILED (not skipped)
        raw = unify_text(raw_name).strip()
        if re.fullmatch(r"[A-Za-z0-9+\-]{2,12}", raw) and raw.isupper() and raw not in self.abbrev_map:
            res.Status = "FAILED"
            res.Failure_Category = "ABBREVIATION_ONLY"
            res.Failure_Detail = "abbreviation without expansion; add to alias dictionary"
            return res

        best = None  # (cid, props, score, source, query)

        for q in candidates:
            # (A) formula search if formula-like
            if looks_formula_like(q):
                cids_f = self.pubchem.formula_to_cids(q)
                cidf, propsf, scf = self.choose_best_cid(q, cids_f)
                if cidf is not None and propsf is not None:
                    best = (cidf, propsf, scf + 0.04, "pubchem_formula", q)
                    if scf >= 0.95:
                        break

            # (B) compound-name
            cids = self.pubchem.name_to_cids(q, domain="compound")
            cid, props, sc = self.choose_best_cid(q, cids)
            if cid is not None and props is not None:
                best = (cid, props, sc, "pubchem_name", q)
                if sc >= 0.92:
                    break

            # (C) substance-name
            cids_s = self.pubchem.name_to_cids(q, domain="substance")
            cid2, props2, sc2 = self.choose_best_cid(q, cids_s)
            if cid2 is not None and props2 is not None:
                sc2_adj = sc2 + 0.03
                if best is None or sc2_adj > best[2]:
                    best = (cid2, props2, sc2_adj, "pubchem_substance", q)

        if best is None:
            res.Status = "FAILED"
            res.Failure_Category = "NOT_FOUND_FAST"
            res.Failure_Detail = "no match from PubChem (name/substance/formula) in fast pass"
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

    def resolve_deep(self, row_idx: int, raw_name: str, existing: ResolveResult) -> ResolveResult:
        res = existing
        candidates = self.generate_candidates(raw_name)

        for q in candidates:
            smi = self._opsin_cache.get(q)
            if smi is None:
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
                res.Canonical_SMILES, rd_formula = self.rdkit_canonicalize(smiles)
                if not res.Molecular_Formula and rd_formula:
                    res.Molecular_Formula = rd_formula
                return res

        for q in candidates:
            smi = self._cir_cache.get(q)
            if smi is None:
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
                res.Canonical_SMILES, rd_formula = self.rdkit_canonicalize(smiles)
                if not res.Molecular_Formula and rd_formula:
                    res.Molecular_Formula = rd_formula
                return res

        res.Status = "FAILED"
        res.Failure_Category = "NOT_RESOLVED"
        res.Failure_Detail = "fast pass failed; OPSIN/CIR did not resolve"
        return res


# ------------------------------ IO helpers --------------------------------- #
def load_checkpoint(checkpoint_csv: str) -> Dict[int, ResolveResult]:
    if not os.path.exists(checkpoint_csv):
        return {}
    df = pd.read_csv(checkpoint_csv)
    out = {}
    for _, row in df.iterrows():
        idx = int(row["RowIndex"])
        out[idx] = ResolveResult(
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
    return out


def append_checkpoint(checkpoint_csv: str, rows: List[ResolveResult]):
    if not rows:
        return
    fieldnames = list(asdict(rows[0]).keys())
    mode = "a" if os.path.exists(checkpoint_csv) else "w"
    with open(checkpoint_csv, mode, newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if mode == "w":
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


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default="./step1//test7_part_polymer/step1.xlsx")
    ap.add_argument("--output", default="./step1/test7_part_polymer/step2.xlsx")
    ap.add_argument("--debug", default="./step1/test7_part_polymer/step2_debug.xlsx")
    ap.add_argument("--failed", default="./step1/test7_part_polymer/step2_failures.xlsx")
    ap.add_argument("--cache", default="./step1/test7_part_polymer/chem_cache.sqlite")
    ap.add_argument("--checkpoint", default="./step1/test7_part_polymer/step2_checkpoint.csv")
    ap.add_argument("--resume", action="store_true")
    ap.add_argument("--no-translate", action="store_true")
    ap.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT)
    ap.add_argument("--deep", action="store_true", help="run OPSIN/CIR on NOT_FOUND_FAST rows after fast pass")
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
    resolver = ChemicalResolverV3_1(pubchem=pubchem, translator=translator, max_candidates=10)

    print(f"Loaded {n} rows from {args.input}")
    print("Building abbreviation map...")
    resolver.build_abbreviation_map(names)
    print(f"Abbreviation map size: {len(resolver.abbrev_map)}")

    done = load_checkpoint(args.checkpoint) if args.resume else {}
    if done:
        print(f"Resume enabled: loaded {len(done)} rows from {args.checkpoint}")

    merged: Dict[int, ResolveResult] = dict(done)
    new_rows: List[ResolveResult] = []

    found = sum(1 for r in merged.values() if r.Status == "FOUND")
    skipped = sum(1 for r in merged.values() if r.Status == "SKIPPED")
    failed = sum(1 for r in merged.values() if r.Status == "FAILED")

    iterable = list(enumerate(names, start=1))
    pbar = tqdm(iterable, total=n, desc="Fast pass", unit="item") if HAS_TQDM else iterable

    t0 = time.time()
    for i, raw in pbar:
        if i in merged:
            continue
        rr = resolver.resolve_fast(i, raw)
        merged[i] = rr
        new_rows.append(rr)

        if rr.Status == "FOUND":
            found += 1
        elif rr.Status == "SKIPPED":
            skipped += 1
        else:
            failed += 1

        if HAS_TQDM:
            elapsed = max(time.time() - t0, 1e-6)
            sec_per = elapsed / max((found + skipped + failed), 1)
            pbar.set_postfix(found=found, skipped=skipped, failed=failed, sec_per_item=f"{sec_per:.2f}", refresh=False)
        elif i % 50 == 0:
            print(f"[{i}/{n}] found={found} skipped={skipped} failed={failed}")

        if len(new_rows) >= args.save_every:
            append_checkpoint(args.checkpoint, new_rows)
            new_rows = []

    if new_rows:
        append_checkpoint(args.checkpoint, new_rows)

    all_results = [merged[i] for i in sorted(merged.keys())]

    if args.deep:
        targets = [r for r in all_results if r.Status == "FAILED" and r.Failure_Category == "NOT_FOUND_FAST"]
        print(f"\nDeep pass targets: {len(targets)}")
        p2 = tqdm(targets, total=len(targets), desc="Deep pass", unit="item") if HAS_TQDM else targets
        updated = 0
        for r in p2:
            rr2 = resolver.resolve_deep(r.RowIndex, r.Original_Name, r)
            if rr2.Status == "FOUND":
                updated += 1
            merged[rr2.RowIndex] = rr2
            if HAS_TQDM:
                p2.set_postfix(newly_found=updated, refresh=False)
        all_results = [merged[i] for i in sorted(merged.keys())]

    print("\nSaving output files...")
    save_outputs(all_results, args.output, args.debug, args.failed)

    s = pd.Series([r.Status for r in all_results]).value_counts()
    print("=" * 72)
    print("DONE")
    print(f"Main: {args.output}")
    print(f"Debug: {args.debug}")
    print(f"Failed: {args.failed}")
    print(f"Cache: {args.cache}")
    print(f"Checkpoint: {args.checkpoint}")
    print(s.to_string())
    print("=" * 72)

    cache.close()


if __name__ == "__main__":
    main()
