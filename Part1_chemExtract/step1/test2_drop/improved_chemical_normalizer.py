
"""
Robust chemical name normalization pipeline for step1.xlsx -> step2.xlsx

Key ideas
---------
1.不要激进地移除所有括号。内部括号往往是有效 IUPAC 名称的组成部分。
2.首先对行进行分类：单一化合物 vs. 混合物/材料/工艺/模型代码/碎片信息。
3.从语料库中构建缩写词典，例如：
1,4-环己二胺 (CHDA) -> CHDA => 1,4-环己二胺
4.使用分层解析器：
PubChem 化合物名称搜索
PubChem 物质名称搜索
OPSIN（名称转 SMILES）-> 通过 SMILES 查询 PubChem   
CIR / CACTUS（名称转 SMILES）-> 通过 SMILES 查询 PubChem
在找到结构后使用 RDKit，用于验证/标准化。
6.生成一个干净的 step2.xlsx（3 列），并附带详细的调试/失败记录工作簿。
Recommended install
-------------------
conda install -c conda-forge rdkit openpyxl pandas
pip install pubchempy requests transformers sentencepiece torch py2opsin deep-translator
"""

import json
import math
import os
import re
import time
import unicodedata
from dataclasses import dataclass, asdict
from difflib import SequenceMatcher
from typing import Dict, List, Optional, Tuple
from urllib.parse import quote

import pandas as pd
import pubchempy as pcp
import requests

# ------------------------------ optional deps ------------------------------ #
HAS_RDKIT = False
try:
    from rdkit import Chem
    from rdkit.Chem import rdMolDescriptors
    HAS_RDKIT = True
except Exception:
    Chem = None
    rdMolDescriptors = None

HAS_TRANSFORMERS = False
try:
    from transformers import pipeline
    HAS_TRANSFORMERS = True
except Exception:
    pipeline = None

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
USER_AGENT = "ChemicalNormalizer/2.0"
REQUEST_TIMEOUT = 15
MAX_RETRIES = 4
SLEEP_BETWEEN = 0.15

NOTE_KEYWORDS = {
    "solution", "aqueous", "water solution", "dispersion", "suspension",
    "gelation", "active layer etching", "etching", "as cleaning agent",
    "cleaning agent", "phase", "coverage", "support membrane", "membrane",
    "nanosheet", "nanofillers", "composite substrate", "substrate",
    "bilayer", "bilayers", "shell", "bag filter", "cartridge filter",
    "hydrogel", "proteoliposomes", "containing", "modified", "doped",
    "nanoparticles", "nanoparticle"
}

MATERIAL_KEYWORDS = {
    "membrane", "filter", "support", "nanosheet", "polymer", "material",
    "composite", "substrate", "bilayer", "hydrogel", "proteoliposome",
    "proteoliposomes", "zeolite", "nanotube", "mof nanosheet", "mesh",
    "shell", "hydroxide", "protein", "asymmetric membrane", "ro膜", "膜",
    "纳米片", "沸石", "蛋白", "树枝状大分子", "复合物"
}

PROCESS_KEYWORDS = {
    "activation", "cleaning", "hydrolysis", "annealing", "immersion",
    "exposure", "removal", "oxidation", "ultrasonication", "cross-linking",
    "crosslinking", "acetylation", "anti-fouling", "anti-scalant",
    "antiscalant", "fouling", "gelation", "etching"
}

# A tiny chemistry-aware glossary. Keep it small and safe; the translator handles the rest.
CN_GLOSSARY = {
    "樟脑磺酸": "camphorsulfonic acid",
    "吡啶二羧酰氯": "pyridinedicarbonyl dichloride",
    "溴异丁酰溴": "2-bromoisobutyryl bromide",
    "二氧六环": "1,4-dioxane",
    "丙酮": "acetone",
    "三氯硅烷": "trichlorosilane",
    "缓冲液": "buffer",
    "水溶液": "aqueous solution",
    "溶液": "solution",
    "聚酰胺": "polyamide",
    "沸石": "zeolite",
    "纳米片": "nanosheet",
    "蛋白": "protein",
    "二氨基二苯砜": "diaminodiphenyl sulfone",
}


@dataclass
class ResolveResult:
    Original_Name: str
    Molecular_Formula: Optional[str] = None
    IUPAC_Name: Optional[str] = None
    Standardized_Query: Optional[str] = None
    CID: Optional[int] = None
    Canonical_SMILES: Optional[str] = None
    Match_Source: Optional[str] = None
    Status: str = "FAILED"           # FOUND / SKIPPED / FAILED
    Failure_Category: Optional[str] = None
    Failure_Detail: Optional[str] = None
    Candidates_Tried: Optional[str] = None


class OptionalTranslator:
    """
    Preferred order:
    1) transformers local model (deep-learning)
    2) deep_translator GoogleTranslator
    """
    def __init__(self):
        self.pipe = None
        self.google = None

        if HAS_TRANSFORMERS:
            try:
                # Smaller than NLLB and enough for short phrases. First run downloads weights.
                self.pipe = pipeline(
                    "translation",
                    model="Helsinki-NLP/opus-mt-zh-en",
                    max_length=256
                )
            except Exception:
                self.pipe = None

        if self.pipe is None and HAS_DEEP_TRANSLATOR:
            try:
                self.google = GoogleTranslator(source="auto", target="en")
            except Exception:
                self.google = None

    @staticmethod
    def contains_chinese(text: str) -> bool:
        return bool(re.search(r"[\u4e00-\u9fff]", text))

    def translate(self, text: str) -> str:
        if not text or not self.contains_chinese(text):
            return text

        # glossary first
        tmp = text
        for zh, en in CN_GLOSSARY.items():
            tmp = tmp.replace(zh, en)

        # if glossary already removed all Chinese, stop here
        if not self.contains_chinese(tmp):
            return tmp

        # avoid translating strings that are almost entirely formula-like
        if re.fullmatch(r"[A-Za-z0-9\-\+\(\)\[\],./\s]+", tmp):
            return tmp

        try:
            if self.pipe is not None:
                out = self.pipe(tmp)
                if out and isinstance(out, list):
                    return out[0]["translation_text"]
        except Exception:
            pass

        try:
            if self.google is not None:
                return self.google.translate(tmp)
        except Exception:
            pass

        return tmp


class ChemicalResolver:
    def __init__(self):
        self.translator = OptionalTranslator()
        self.abbrev_map: Dict[str, str] = {}
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT})
        self.pubchem_cache: Dict[Tuple[str, str], Optional[int]] = {}
        self.compound_cache: Dict[int, dict] = {}
        self.cir_cache: Dict[str, Optional[str]] = {}
        self.opsin_cache: Dict[str, Optional[str]] = {}

    # --------------------------- text normalization ------------------------ #
    @staticmethod
    def unify_text(text: str) -> str:
        if text is None or (isinstance(text, float) and math.isnan(text)):
            return ""
        s = str(text).strip()
        s = unicodedata.normalize("NFKC", s)
        s = s.replace("（", "(").replace("）", ")")
        s = s.replace("【", "[").replace("】", "]")
        s = s.replace("，", ",").replace("；", ";").replace("：", ":")
        s = s.replace("·", "·").replace("—", "-").replace("–", "-")
        s = re.sub(r"[™®©]", "", s)
        s = re.sub(r"\s+", " ", s).strip()
        return s

    @staticmethod
    def normalize_spelling(s: str) -> str:
        # British/American and a few common variants
        repl = {
            "sulph": "sulf",
            "aluminium": "aluminum",
            "caesium": "cesium",
            "fibre": "fiber",
            "amidopropyl": "aminopropyl",  # very common typo in papers
            "azoiadamantane": "azoniadamantane",  # likely OCR/typing issue
            "l-azoiadamantane": "1-azoniadamantane",
        }
        out = s
        for a, b in repl.items():
            out = re.sub(a, b, out, flags=re.IGNORECASE)
        return out

    @staticmethod
    def strip_concentration_and_context(s: str) -> str:
        out = s

        # remove leading concentrations/percentages
        out = re.sub(r"^\s*\d+(\.\d+)?\s*(wt%|vol%|mol%|%|ppm|ppb)\s*", "", out, flags=re.I)
        out = re.sub(r"^\s*\d+(\.\d+)?\s*(mg/l|g/l|mg|g|kg|ml|l|mM|uM|μM|nM|M)\s*", "", out, flags=re.I)

        # remove trailing solution / suspension descriptors
        out = re.sub(
            r"\b(aqueous solution|aqueous|solution|suspension|dispersion|buffer)\b.*$",
            "",
            out,
            flags=re.I,
        )

        # remove parenthetical notes only at END if they look like notes/aliases
        m = re.search(r"^(.*)\(([^()]*)\)\s*$", out)
        if m:
            prefix = m.group(1).strip()
            inner = m.group(2).strip()
            inner_l = inner.lower()

            looks_like_alias = (
                1 <= len(inner) <= 18
                and re.fullmatch(r"[A-Za-z0-9+\-]{1,18}", inner) is not None
            )
            looks_like_note = (
                inner_l in NOTE_KEYWORDS
                or any(k in inner_l for k in NOTE_KEYWORDS)
                or re.search(r"\b\d+(\.\d+)?\s*(wt%|ppm|ppb|mg/l|g/l|khz|hz)\b", inner_l)
                or re.fullmatch(r"[A-Z]|\d+(\.\d+)?", inner) is not None
            )

            # only strip final (...) when it is clearly alias/note
            if prefix and (looks_like_alias or looks_like_note):
                out = prefix

        out = re.sub(r"\s+", " ", out).strip(" ;,")
        return out

    @staticmethod
    def strip_stereo_prefix(s: str) -> str:
        out = s.strip()

        # only remove leading stereo signs; do NOT touch internal stereo notation
        patterns = [
            r"^\((\+|-|±)\)-\s*",
            r"^(rac|dl|d|l)-\s*",
        ]
        for pat in patterns:
            out2 = re.sub(pat, "", out, flags=re.I)
            if out2 != out and len(out2) >= 4:
                out = out2
        return out

    @staticmethod
    def looks_formula_like(s: str) -> bool:
        return bool(re.fullmatch(r"(?:[A-Z][a-z]?\d*){2,}", s))

    @staticmethod
    def is_numeric_or_measurement(s: str) -> bool:
        return bool(re.fullmatch(r"[\d.\-+/%µμa-zA-Z\s()]+", s)) and bool(
            re.search(r"\d", s)
        ) and any(
            k in s.lower()
            for k in ["wt%", "%", "ppm", "ppb", "mg/l", "g/l", "khz", "hz", "µm", "um"]
        )

    @staticmethod
    def is_fragment(s: str) -> bool:
        return s in {"-Cl", "-F", "-H", "-OH", "-CH2COO-", "-CH2NH3+"}

    @staticmethod
    def is_mixture_or_list(s: str) -> bool:
        # careful: 1,4-diaminobenzene is NOT a list
        if "/" in s and not re.search(r"\d+/\d+", s):
            parts = [p.strip() for p in s.split("/") if p.strip()]
            if len(parts) >= 2:
                return True

        if "," in s:
            if re.match(r"^\d+,\d+[-(]", s):  # locant pattern, likely one compound
                return False
            parts = [p.strip() for p in s.split(",") if p.strip()]
            if len(parts) >= 2:
                # if most parts are short tokens => probably list
                short_parts = sum(len(p) <= 12 for p in parts)
                if short_parts >= 2:
                    return True
        return False

    @staticmethod
    def is_material_or_process(s: str) -> bool:
        low = s.lower()
        return any(k in low for k in MATERIAL_KEYWORDS) or any(k in low for k in PROCESS_KEYWORDS)

    @staticmethod
    def is_model_code(s: str) -> bool:
        return bool(re.fullmatch(r"[A-Za-z]{1,4}\d[\w\-]{2,}", s)) or bool(
            re.search(r"\b(TW30|BW30|UP150|TU-\d+)\b", s, flags=re.I)
        )

    @staticmethod
    def similarity(a: str, b: str) -> float:
        return SequenceMatcher(None, a.lower(), b.lower()).ratio()

    # -------------------------- abbreviation map --------------------------- #
    def build_abbreviation_map(self, names: List[str]) -> None:
        """
        Learn corpus-specific aliases:
            1,4-cyclohexanediamine (CHDA) -> CHDA => 1,4-cyclohexanediamine
        We only learn if the bracket content is short and abbreviation-like.
        """
        for raw in names:
            s = self.unify_text(raw)
            m = re.match(r"^(.*\S)\s*\(([A-Za-z][A-Za-z0-9+\-]{1,20})\)\s*$", s)
            if not m:
                continue
            full = m.group(1).strip()
            abbr = m.group(2).strip()

            # only learn when full part is not itself material/process noise
            if len(full) >= 6 and not self.is_material_or_process(full):
                self.abbrev_map.setdefault(abbr, full)

    # ---------------------------- candidate gen ---------------------------- #
    def generate_candidates(self, raw_name: str) -> List[str]:
        raw = self.unify_text(raw_name)
        if not raw:
            return []

        candidates = []
        seen = set()

        def add(x: str):
            if not x:
                return
            x = self.unify_text(x)
            x = self.normalize_spelling(x)
            x = re.sub(r"\s+", " ", x).strip(" ;,")
            if len(x) < 2:
                return
            key = x.lower()
            if key not in seen:
                seen.add(key)
                candidates.append(x)

        # 0) raw
        add(raw)

        # 1) strip context only (safe)
        c1 = self.strip_concentration_and_context(raw)
        add(c1)

        # 2) stereo-prefix removed
        add(self.strip_stereo_prefix(c1))
        add(self.strip_stereo_prefix(raw))

        # 3) if final (...) is abbreviation, expand it instead of keeping only abbreviation
        m = re.match(r"^(.*\S)\s*\(([A-Za-z][A-Za-z0-9+\-]{1,20})\)\s*$", raw)
        if m:
            full = self.strip_concentration_and_context(m.group(1).strip())
            abbr = m.group(2).strip()
            add(full)
            if abbr in self.abbrev_map:
                add(self.abbrev_map[abbr])

        # 4) direct abbreviation expansion
        plain = raw.strip()
        if plain in self.abbrev_map:
            add(self.abbrev_map[plain])
        if c1 in self.abbrev_map:
            add(self.abbrev_map[c1])

        # 5) Chinese translation, but keep original too
        if self.translator.contains_chinese(raw):
            tr_raw = self.translator.translate(raw)
            add(tr_raw)
            add(self.strip_concentration_and_context(tr_raw))
            add(self.strip_stereo_prefix(tr_raw))

        # 6) British/American variants already handled in normalize_spelling
        # 7) if there is a trailing alias, also try prefix without that alias
        m2 = re.search(r"^(.*)\(([^()]*)\)\s*$", c1)
        if m2:
            prefix = m2.group(1).strip()
            inner = m2.group(2).strip()
            # try prefix always; but do NOT keep only inner unless it looks like a real chemical name
            add(prefix)
            if (
                len(inner) >= 8
                and re.search(r"[a-z]", inner)
                and (" " in inner or "-" in inner)
                and not re.fullmatch(r"[A-Za-z0-9+\-]{1,20}", inner)
            ):
                add(inner)

        # 8) common punctuation simplifications (careful, last)
        for c in list(candidates):
            # don't destroy locants too early; only normalize odd spaces around punctuation
            simp = re.sub(r"\s*-\s*", "-", c)
            simp = re.sub(r"\s*,\s*", ",", simp)
            simp = re.sub(r"\s*\(\s*", "(", simp)
            simp = re.sub(r"\s*\)\s*", ")", simp)
            add(simp)

        return candidates

    # --------------------------- network helpers --------------------------- #
    def _sleep_backoff(self, attempt: int):
        time.sleep(min(2 ** attempt, 8) * 0.5)

    def pubchem_get_cids(self, query: str, domain: str = "compound") -> List[int]:
        key = (query, domain)
        if key in self.pubchem_cache:
            cid = self.pubchem_cache[key]
            return [] if cid is None else [cid]

        for attempt in range(MAX_RETRIES):
            try:
                cids = pcp.get_cids(query, namespace="name", domain=domain, list_return="flat")
                if cids:
                    # cache only first here; caller may separately use list when needed
                    self.pubchem_cache[key] = cids[0]
                    return cids
                self.pubchem_cache[key] = None
                return []
            except Exception:
                self._sleep_backoff(attempt)
        self.pubchem_cache[key] = None
        return []

    def pubchem_compound_from_cid(self, cid: int):
        if cid in self.compound_cache:
            return self.compound_cache[cid]
        for attempt in range(MAX_RETRIES):
            try:
                cmpd = pcp.Compound.from_cid(cid)
                self.compound_cache[cid] = cmpd
                return cmpd
            except Exception:
                self._sleep_backoff(attempt)
        return None

    def cir_name_to_smiles(self, name: str) -> Optional[str]:
        if name in self.cir_cache:
            return self.cir_cache[name]
        url = f"https://cactus.nci.nih.gov/chemical/structure/{quote(name)}/smiles"
        for attempt in range(MAX_RETRIES):
            try:
                r = self.session.get(url, timeout=REQUEST_TIMEOUT)
                if r.status_code == 200:
                    txt = r.text.strip()
                    self.cir_cache[name] = txt or None
                    return txt or None
                if r.status_code in {404, 400}:
                    self.cir_cache[name] = None
                    return None
            except Exception:
                pass
            self._sleep_backoff(attempt)
        self.cir_cache[name] = None
        return None

    def opsin_name_to_smiles(self, name: str) -> Optional[str]:
        if name in self.opsin_cache:
            return self.opsin_cache[name]
        if not HAS_PY2OPSIN:
            self.opsin_cache[name] = None
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
                self.opsin_cache[name] = res
                return res
        except Exception:
            pass
        self.opsin_cache[name] = None
        return None

    # ----------------------------- scoring -------------------------------- #
    def compound_score(self, query: str, compound) -> float:
        score = 0.0
        query_l = query.lower()

        try:
            iupac = (compound.iupac_name or "").lower()
        except Exception:
            iupac = ""

        syns = []
        try:
            syns = compound.synonyms[:20] if compound.synonyms else []
        except Exception:
            syns = []

        if iupac:
            score = max(score, self.similarity(query_l, iupac))
            if query_l == iupac:
                score += 0.5

        for syn in syns:
            syn_l = str(syn).lower()
            score = max(score, self.similarity(query_l, syn_l))
            if query_l == syn_l:
                score += 0.5

        # avoid picking tiny fragments for long names
        if len(query) >= 8 and iupac and len(iupac) <= 6:
            score -= 0.2

        return score

    def smiles_to_pubchem(self, smiles: str):
        for attempt in range(MAX_RETRIES):
            try:
                compounds = pcp.get_compounds(smiles, namespace="smiles")
                if compounds:
                    return compounds[0]
                return None
            except Exception:
                self._sleep_backoff(attempt)
        return None

    def rdkit_canonicalize(self, smiles: str) -> Tuple[Optional[str], Optional[str]]:
        if not HAS_RDKIT or not smiles:
            return smiles, None
        try:
            mol = Chem.MolFromSmiles(smiles)
            if mol is None:
                return smiles, None
            can = Chem.MolToSmiles(mol, canonical=True)
            formula = rdMolDescriptors.CalcMolFormula(mol)
            return can, formula
        except Exception:
            return smiles, None

    # ------------------------------ main logic ----------------------------- #
    def classify(self, raw_name: str, generated_candidates: List[str]) -> Tuple[Optional[str], Optional[str]]:
        raw = self.unify_text(raw_name)
        low = raw.lower()

        if not raw:
            return "EMPTY", "empty cell"

        if self.is_fragment(raw):
            return "FRAGMENT", "functional group / fragment, not a standalone compound"

        if re.fullmatch(r"[-+]?\d+(\.\d+)?", raw):
            return "NUMERIC", "pure numeric value"

        if self.is_numeric_or_measurement(raw):
            return "MEASUREMENT", "measurement / concentration / condition"

        if self.is_model_code(raw):
            return "MODEL_CODE", "product or membrane model code"

        if self.is_mixture_or_list(raw):
            return "MULTI_COMPONENT", "mixture or multiple chemicals in one cell"

        if self.is_material_or_process(raw):
            return "MATERIAL_OR_PROCESS", "material / process / system rather than a single compound"

        # if every candidate is still very short abbreviation-like, treat as abbreviation-only
        if generated_candidates and all(len(c) <= 12 for c in generated_candidates):
            if re.fullmatch(r"[A-Za-z0-9+\-]{2,12}", raw):
                return "ABBREVIATION_ONLY", "abbreviation/code without enough context"

        return None, None

    def resolve_one(self, raw_name: str) -> ResolveResult:
        result = ResolveResult(Original_Name=raw_name)
        candidates = self.generate_candidates(raw_name)
        result.Candidates_Tried = " || ".join(candidates[:20]) if candidates else None

        skip_cat, skip_detail = self.classify(raw_name, candidates)
        # still allow resolution attempt for abbreviation if expansion exists
        if skip_cat and skip_cat not in {"ABBREVIATION_ONLY"}:
            result.Status = "SKIPPED"
            result.Failure_Category = skip_cat
            result.Failure_Detail = skip_detail
            return result

        best = None
        best_query = None
        best_source = None
        best_score = -1.0

        # ---- 1) PubChem compound-name and substance-name ---- #
        for q in candidates:
            # compound search
            cids = self.pubchem_get_cids(q, domain="compound")
            for cid in cids[:3]:
                cmpd = self.pubchem_compound_from_cid(cid)
                if cmpd is None:
                    continue
                sc = self.compound_score(q, cmpd)
                if sc > best_score:
                    best, best_query, best_source, best_score = cmpd, q, "pubchem_compound_name", sc
                if sc >= 1.2:
                    break

            # substance search often recovers more aliases/non-standard names
            cids = self.pubchem_get_cids(q, domain="substance")
            for cid in cids[:5]:
                cmpd = self.pubchem_compound_from_cid(cid)
                if cmpd is None:
                    continue
                sc = self.compound_score(q, cmpd) + 0.05
                if sc > best_score:
                    best, best_query, best_source, best_score = cmpd, q, "pubchem_substance_name", sc

            time.sleep(SLEEP_BETWEEN)

        # ---- 2) OPSIN -> SMILES -> PubChem ---- #
        if best is None or best_score < 0.80:
            for q in candidates:
                smi = self.opsin_name_to_smiles(q)
                if not smi:
                    continue
                cmpd = self.smiles_to_pubchem(smi)
                if cmpd:
                    sc = self.compound_score(q, cmpd) + 0.10
                    if sc > best_score:
                        best, best_query, best_source, best_score = cmpd, q, "opsin_smiles", sc
                time.sleep(SLEEP_BETWEEN)

        # ---- 3) CIR -> SMILES -> PubChem ---- #
        if best is None or best_score < 0.80:
            for q in candidates:
                smi = self.cir_name_to_smiles(q)
                if not smi:
                    continue
                cmpd = self.smiles_to_pubchem(smi)
                if cmpd:
                    sc = self.compound_score(q, cmpd) + 0.05
                    if sc > best_score:
                        best, best_query, best_source, best_score = cmpd, q, "cir_smiles", sc
                time.sleep(SLEEP_BETWEEN)

        # ---- finalize ---- #
        if best is not None:
            result.Status = "FOUND"
            result.Standardized_Query = best_query
            result.Match_Source = best_source
            try:
                result.CID = best.cid
            except Exception:
                pass

            try:
                result.IUPAC_Name = best.iupac_name
            except Exception:
                result.IUPAC_Name = None

            try:
                result.Molecular_Formula = best.molecular_formula
            except Exception:
                result.Molecular_Formula = None

            # if PubChem formula missing but RDKit can derive it from SMILES, use that as complement
            smiles = None
            try:
                smiles = best.canonical_smiles
            except Exception:
                smiles = None

            can_smi, rd_formula = self.rdkit_canonicalize(smiles)
            result.Canonical_SMILES = can_smi

            if not result.Molecular_Formula and rd_formula:
                result.Molecular_Formula = rd_formula

            # fallback for name
            if not result.IUPAC_Name:
                try:
                    syns = best.synonyms or []
                    result.IUPAC_Name = syns[0] if syns else None
                except Exception:
                    pass

            return result

        # failed after all layers
        result.Status = "FAILED"
        result.Failure_Category = skip_cat or "NOT_RESOLVED"
        result.Failure_Detail = skip_detail or "no confident match from PubChem/OPSIN/CIR"
        return result


def save_outputs(results: List[ResolveResult], out_main: str, out_debug: str, out_failed: str) -> None:
    debug_df = pd.DataFrame([asdict(r) for r in results])

    # required step2.xlsx: 3 columns only
    main_df = debug_df[["Original_Name", "Molecular_Formula", "IUPAC_Name"]].copy()
    main_df.columns = ["Original_Name", "Molecular_Formula", "IUPAC_Name"]
    main_df.to_excel(out_main, index=False)

    # detailed workbook
    with pd.ExcelWriter(out_debug, engine="openpyxl") as writer:
        debug_df.to_excel(writer, sheet_name="all_results", index=False)
        debug_df[debug_df["Status"] == "FOUND"].to_excel(writer, sheet_name="found", index=False)
        debug_df[debug_df["Status"] != "FOUND"].to_excel(writer, sheet_name="not_found_or_skipped", index=False)

    debug_df[debug_df["Status"] != "FOUND"].to_excel(out_failed, index=False)


def main():
    input_file = "./test2/step1.xlsx"
    output_main = "./test2/step2.xlsx"
    output_debug = "./test2/step2_debug.xlsx"
    output_failed = "./test2/step2_failures.xlsx"

    if not os.path.exists(input_file):
        raise FileNotFoundError(f"Cannot find input file: {input_file}")

    df = pd.read_excel(input_file)
    if df.empty:
        raise ValueError("Input workbook is empty")

    # default: use first column
    col = df.columns[0]
    names = df[col].fillna("").astype(str).tolist()

    resolver = ChemicalResolver()
    resolver.build_abbreviation_map(names)

    results: List[ResolveResult] = []
    for i, name in enumerate(names, start=1):
        res = resolver.resolve_one(name)
        results.append(res)

        if i % 50 == 0 or i == len(names):
            found_n = sum(r.Status == "FOUND" for r in results)
            print(f"[{i}/{len(names)}] found={found_n} failed_or_skipped={i - found_n}")

    save_outputs(results, output_main, output_debug, output_failed)

    debug_df = pd.DataFrame([asdict(r) for r in results])
    print("=" * 72)
    print("DONE")
    print(f"Input:  {input_file}")
    print(f"Main:   {output_main}")
    print(f"Debug:  {output_debug}")
    print(f"Failed: {output_failed}")
    print(debug_df["Status"].value_counts(dropna=False))
    print("=" * 72)


if __name__ == "__main__":
    main()
