# -*- coding: utf-8 -*-
"""
concentration_cleaner_balanced_regenerated.py

重构目标：
- 在保留当前脚本整体框架的前提下，优先修复：
  1) 短缩写误匹配：短缩写只允许查本地映射表，不做公网“猜测”
  2) additive / modifier 相别误判：更保守、更透明
  3) PubChem MW fallback 不完整：逐级回退，逐步缓存
  4) 单位归一化对 % (w/v) 等变体支持不足
  5) v/v% 所需溶质密度未充分利用
  6) 长度不齐的多值字段：尽量部分保留，不整组报废
- 输出 concentration_review 时增加 identity/phase/density/conversion 的透明度字段

依赖：
    pip install pandas openpyxl requests tqdm
"""

from __future__ import annotations

import datetime as dt
import json
import math
import numbers
import os
import re
import time
import traceback
import warnings
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import quote

import pandas as pd
import requests

try:
    from openpyxl.cell.cell import ILLEGAL_CHARACTERS_RE
except ImportError:
    ILLEGAL_CHARACTERS_RE = re.compile(r"[\x00-\x08\x0B\x0C\x0E-\x1F]")

try:
    from tqdm.auto import tqdm
except ImportError:
    def tqdm(iterable: Iterable[Any], *args: Any, **kwargs: Any) -> Iterable[Any]:
        """未安装 tqdm 时退化为无进度条。"""
        return iterable


# =========================================================
# 0) 固定配置区：改这里即可
# =========================================================

@dataclass
class AppConfig:
    # ---------- 输入（主表可为 .xlsx / .csv；CSV 无 sheet）----------
    input_main_excel: str = "./test3/membrane_aligned_data_all.csv"
    input_main_sheet: Optional[str] = None

    # 可选外部单位表；没有也没关系，脚本会从主表 + review 表自建 unit_catalog
    unit_catalog_excel: Optional[str] = "./test3/统计单位.xlsx"
    unit_catalog_sheet: Optional[str] = None

    identity_table_excel: str = "./test3/task1.xlsx"
    identity_table_sheet: Optional[str] = None

    md_zip_path: Optional[str] = "./test3/反渗透膜_output.zip"
    md_extract_dir: str = "./md_extracted"
    use_traceback: bool = True
    max_traceback_snippets: int = 3
    traceback_window_chars: int = 260

    # ---------- 输出 ----------
    output_excel: str = "./test3/output/all_3_5.xlsx"

    # ---------- 审查中间结果（整表，与 pubchem/llm 的 json 缓存不同）----------
    # build_review_sheet 结束后写入；写 Excel 失败时，下次可设 resume_from_review_checkpoint=True 跳过审查。
    review_checkpoint_path: str = "./test3/output/review_checkpoint.pkl"
    save_review_checkpoint: bool = True
    resume_from_review_checkpoint: bool = False

    # 单单元格字符串上限（Excel 约 32767，略小留余量）；超长则截断并标注
    excel_max_cell_chars: int = 32700
    # write_workbook 失败时写入的诊断 JSON（路径 + 抽样坏单元格）
    excel_failure_diag_json: str = "./test3/output/write_excel_failure.json"

    # ---------- 缓存 ----------
    pubchem_cache_json: str = "./test3/cache_pubchem_mw.json"
    llm_cache_json: str = "./test3/cache_llm_judgements.json"
    # 单位修复 / 公式推断（与 slot 判断缓存分开，便于审计与清理）
    llm_unit_aux_cache_json: str = "./test3/cache_llm_unit_aux.json"

    # ---------- 功能开关 ----------
    pubchem_enabled: bool = True
    llm_enabled: bool = True

    # ---------- API ----------
    llm_base_url: str = "https://api.deepseek.com/v1"
    llm_api_key: str = os.getenv("DEEPSEEK_API_KEY", "")
    llm_model: str = "deepseek-chat"
    timeout_seconds: int = 30

    # ---------- 运行策略 ----------
    sleep_between_pubchem_calls: float = 0.15
    round_digits: int = 6
    short_abbrev_max_len: int = 6
    # 是否显示主表行进度（build_review_sheet 最耗时的循环）
    show_progress: bool = True

    # LLM 单位辅助：仅 high 置信度自动采用；medium 采用但强制人工复核标记
    llm_unit_aux_min_auto_confidence: str = "medium"  # "high" | "medium" — medium 时 manual_review


CFG = AppConfig()


# =========================================================
# 1) 槽位定义
# =========================================================

@dataclass
class SlotSpec:
    slot_name: str
    solute_col: str
    value_col: str
    unit_col: str
    default_phase: str
    output_value_col: str
    output_status_col: str  # 历史字段名保留；主表不再写入逐项长状态串


SLOT_SPECS: List[SlotSpec] = [
    SlotSpec(
        slot_name="aqueous_monomer",
        solute_col="水相单体",
        value_col="水相单体浓度",
        unit_col="水相单体浓度_单位",
        default_phase="aqueous",
        output_value_col="水相单体浓度_wt%",
        output_status_col="水相单体浓度_wt%_status",
    ),
    SlotSpec(
        slot_name="organic_monomer",
        solute_col="油相单体",
        value_col="油相单体浓度",
        unit_col="油相单体浓度_单位",
        default_phase="organic",
        output_value_col="油相单体浓度_wt%",
        output_status_col="油相单体浓度_wt%_status",
    ),
    SlotSpec(
        slot_name="additive",
        solute_col="添加剂",
        value_col="添加剂浓度",
        unit_col="添加剂浓度_单位",
        default_phase="unknown",
        output_value_col="添加剂浓度_wt%",
        output_status_col="添加剂浓度_wt%_status",
    ),
    SlotSpec(
        slot_name="modifier",
        solute_col="改性剂",
        value_col="改性剂浓度",
        unit_col="改性剂浓度_单位",
        default_phase="unknown",
        output_value_col="改性剂浓度_wt%",
        output_status_col="改性剂浓度_wt%_status",
    ),
    SlotSpec(
        slot_name="test_nacl",
        solute_col="测试NaCl浓度",  # 这个 slot 的溶质固定为 NaCl，下面 split 时会特殊处理
        value_col="测试NaCl浓度",
        unit_col="测试NaCl浓度_单位",
        default_phase="aqueous",
        output_value_col="测试NaCl浓度_wt%",
        output_status_col="测试NaCl浓度_wt%_status",
    ),
]


# =========================================================
# 2) 本地词典：可不断补充
# =========================================================

SOLVENT_DENSITY_DICT: Dict[str, float] = {
    "water": 1.000,
    "hexane": 0.659,
    "n-hexane": 0.659,
    "cyclohexane": 0.779,
    "heptane": 0.684,
    "octane": 0.703,
    "isooctane": 0.692,
    "toluene": 0.867,
    "xylene": 0.860,
    "isopar": 0.760,
    "kerosene": 0.800,
    "mineral oil": 0.830,
    "ethanol": 0.789,
    "methanol": 0.792,
    "isopropanol": 0.786,
    "acetone": 0.791,
    "dmf": 0.944,
    "dmac": 0.937,
    "nmp": 1.028,
    "chloroform": 1.489,
    "dichloromethane": 1.326,
}

# 纯物质密度：只用于溶质本身（尤其 v/v%）
SOLUTE_DENSITY_DICT: Dict[str, float] = {
    "triethylamine": 0.726,
    "piperidine": 0.862,
    "aniline": 1.022,
    "ethanol": 0.789,
    "methanol": 0.792,
    "isopropanol": 0.786,
    "glycerol": 1.261,
    "nmp": 1.028,
    "dmf": 0.944,
    "dmac": 0.937,
    "toluene": 0.867,
    "xylene": 0.860,
    "cyclohexane": 0.779,
    "hexane": 0.659,
    "n-hexane": 0.659,
    "heptane": 0.684,
    "octane": 0.703,
    "isooctane": 0.692,
    "acetone": 0.791,
    "chloroform": 1.489,
    "dichloromethane": 1.326,
}

COMMON_LIQUIDS = {
    "triethylamine", "piperidine", "aniline", "ethanol", "methanol", "isopropanol",
    "glycerol", "nmp", "dmf", "dmac", "toluene", "xylene", "hexane", "n-hexane",
    "cyclohexane", "heptane", "octane", "isooctane", "acetone", "chloroform",
    "dichloromethane"
}
COMMON_SOLIDS = {
    "sodium chloride", "nacl", "m-phenylenediamine", "mpd", "piperazine", "pip",
    "trimesoyl chloride", "tmc", "camphorsulfonic acid", "csa", "sodium hydroxide",
    "naoh", "sodium carbonate", "sodium dodecyl sulfate", "sds", "potassium persulfate",
    "edc", "nhs"
}

# 用于 additive/modifier 的保守相别判断
COMMON_AQUEOUS_PHASE_SOLUTES = {
    "nacl", "sodium chloride", "mpd", "m-phenylenediamine", "pip", "piperazine",
    "sds", "sodium dodecyl sulfate", "edc", "nhs", "tea", "triethylamine",
    "naoh", "sodium hydroxide", "sodium carbonate", "csa", "camphorsulfonic acid",
    "cucl2", "ascorbic acid", "polyvinyl alcohol", "pva", "pvs"
}
COMMON_ORGANIC_PHASE_SOLUTES = {
    "tmc", "trimesoyl chloride", "btec", "mm-btec", "cyanuric chloride",
    "acyl chloride", "benzene-1,3,5-tricarbonyl chloride"
}

SOLVENT_ALIASES: Dict[str, List[str]] = {
    "water": ["water", "h2o", "deionized water", "distilled water", "di water", "去离子水", "水"],
    "n-hexane": ["n-hexane", "hexane", "正己烷", "己烷"],
    "cyclohexane": ["cyclohexane", "环己烷"],
    "heptane": ["heptane", "庚烷"],
    "octane": ["octane", "辛烷"],
    "isooctane": ["isooctane", "异辛烷"],
    "toluene": ["toluene", "甲苯"],
    "xylene": ["xylene", "二甲苯"],
    "isopar": ["isopar"],
    "kerosene": ["kerosene", "煤油"],
    "mineral oil": ["mineral oil", "矿物油"],
    "ethanol": ["ethanol", "alcohol", "乙醇"],
    "methanol": ["methanol", "甲醇"],
    "isopropanol": ["isopropanol", "ipa", "异丙醇"],
    "acetone": ["acetone", "丙酮"],
    "dmf": ["dmf", "n,n-dimethylformamide", "二甲基甲酰胺"],
    "dmac": ["dmac", "dimethylacetamide", "二甲基乙酰胺"],
    "nmp": ["nmp", "n-methyl-2-pyrrolidone", "甲基吡咯烷酮"],
    "chloroform": ["chloroform", "三氯甲烷"],
    "dichloromethane": ["dichloromethane", "methylene chloride", "二氯甲烷"],
}

PERCENT_TYPE_BY_FAMILY = {
    "mass_fraction": "WT_PERCENT",
    "mass_volume_percent": "WV_PERCENT",
    "volume_fraction": "VOL_PERCENT",
}

SUCCESS_STATUS = {
    "DIRECT_WT",
    "SAFE_CONVERTED",
    "ASSUMED_CONVERTED",
    "RULE_NORMALIZED_CONVERTED",
    "LLM_UNIT_REPAIRED_CONVERTED",
    "LLM_FORMULA_CONVERTED",
}


SOLID_HINT_PATTERNS = [
    "acrylamide", "bisacrylamide", "persulfate", "hydroxide", "carbonate",
    "sulfonate", "sulfate", "chloride", "salt"
]
LIQUID_HINT_PATTERNS = [
    "ethanol", "methanol", "isopropanol", "glycerol", "triethylamine",
    "aniline", "toluene", "xylene", "hexane", "cyclohexane", "heptane",
    "octane", "isooctane", "acetone", "chloroform", "dichloromethane"
]

PERCENT_CONTEXT_PRIORITY = {
    "explicit_text": 100,
    "sample_name": 70,
    "aqueous_solid_rule": 45,
    "aqueous_default_rule": 35,
    "organic_liquid_rule": 40,
    "organic_solid_rule": 28,
}


# =========================================================
# 2b) identity 合理性 / 材料关键词（修复：Graphene oxide -> methane 等误匹配）
# =========================================================

# 聚合物 / 纳米材料 / 膜配方语境：命中 identity 表时若与标准名几乎无重叠，应拒绝该映射
MATERIAL_KEYWORD_RE = re.compile(
    r"\b("
    r"graphene|oxide|poly|polymer|nanocomposite|composite|nanofiller|nanoparticle|"
    r"membrane|membranes|cnt\b|carbon\s*nanotube|nanotube|go\b|rgo|"
    r"cellulose|chitosan|peg\b|peo\b|pva\b|pan\b|psf\b|pes\b|"
    r"crosslink|graft|nanosheet|"
    r"填料|复合|膜|氧化石墨烯|石墨烯|聚合物"
    r")\b",
    flags=re.I,
)

# 明显“小分子/简单试剂”名称（用于与材料名冲突检测）
SMALL_MOLECULE_HINT_RE = re.compile(
    r"\b(methane|ethane|propane|butane|benzene|toluene|xylene|water|acetone|methanol|ethanol)\b",
    flags=re.I,
)

# 互斥关键词对：(orig 必须命中左列之一，std 命中右列之一) -> 视为可疑
CONFLICT_KEYWORD_PAIRS: List[Tuple[re.Pattern, re.Pattern]] = [
    (re.compile(r"graphene|graphene\s*oxide|氧化石墨烯|石墨烯", re.I), re.compile(r"\bmethane\b|\bethane\b", re.I)),
    (re.compile(r"oxide|氧化", re.I), re.compile(r"\bmethane\b", re.I)),
]


def _token_set_for_similarity(s: str) -> set:
    """用于名称相似度的 token（字母数字，去短词）。"""
    s = normalize_key(s)
    parts = re.findall(r"[a-z0-9]{2,}", s)
    return {p for p in parts if p not in {"the", "of", "and", "for", "acid", "salt"}}


def token_jaccard(a: str, b: str) -> float:
    sa = _token_set_for_similarity(a)
    sb = _token_set_for_similarity(b)
    if not sa or not sb:
        return 0.0
    inter = len(sa & sb)
    union = len(sa | sb)
    return float(inter) / float(union) if union else 0.0


def is_identity_plausible(
    original_solute: str,
    standardized_solute: str,
    iupac_name: str,
    formula: str,
) -> Tuple[bool, str]:
    """
    对本地 identity 表命中做保守校验，避免把材料名映射到无关小分子。
    返回 (是否接受该映射, 简短原因码)。
    """
    orig = norm_text(original_solute)
    std = norm_text(standardized_solute)
    if not orig or not std:
        return True, "ok_empty"
    if normalize_key(orig) == normalize_key(std):
        return True, "exact_same"

    # 互斥关键词冲突
    for pa, pb in CONFLICT_KEYWORD_PAIRS:
        if pa.search(orig) and pb.search(std):
            return False, "conflict_keyword_pair"

    j = token_jaccard(orig, std)
    if j >= 0.35:
        return True, f"token_jaccard_ok_{j:.2f}"

    # 材料语境 + 与标准名几乎无重叠 -> 拒绝
    if MATERIAL_KEYWORD_RE.search(orig) and j < 0.2:
        if SMALL_MOLECULE_HINT_RE.search(std) or len(normalize_key(std)) <= 12:
            return False, "material_context_vs_unrelated_small_molecule"

    # 极长原名 vs 极短标准名，且 token 重叠极低
    if len(normalize_key(orig)) >= 18 and len(normalize_key(std)) <= 12 and j < 0.15:
        return False, "length_mismatch_low_overlap"

    # IUPAC / 分子式与原名完全无交集时，略放宽：仍要求一定重叠
    extra = " ".join([norm_text(iupac_name), norm_text(formula)])
    if extra.strip():
        j2 = max(j, token_jaccard(orig, extra))
        if j2 >= 0.25:
            return True, f"token_jaccard_ok_with_extra_{j2:.2f}"

    if j < 0.12:
        return False, "token_overlap_too_low"

    return True, f"token_jaccard_marginal_{j:.2f}"


def apply_identity_plausibility_filter(rec: Dict[str, Any]) -> None:
    """
    在 resolve_solute_identity 之后调用：
    - 若本地表命中明显不合理，则回退到原名并清空 CID/MW，避免错误 PubChem/MW 换算。
    - 设置 identity_trust_for_mw：被拒绝的命中不再走 MW。
    """
    src = norm_text(rec.get("identity_source", ""))
    orig = norm_text(rec.get("original_solute", ""))

    rec["identity_match_rejected"] = False
    rec["identity_trust_for_mw"] = True

    if not orig:
        return

    if src.startswith("UNRESOLVED_ABBREVIATION"):
        rec["identity_trust_for_mw"] = False
        return

    if src == "UNRESOLVED" or src.startswith("EMPTY"):
        return

    if not src.startswith("IDENTITY_TABLE"):
        return

    std = norm_text(rec.get("standardized_solute", ""))
    ok, reason = is_identity_plausible(orig, std, rec.get("IUPAC_Name", ""), rec.get("formula", ""))
    if ok:
        return

    rec["identity_match_rejected"] = True
    rec["identity_trust_for_mw"] = False
    rec["standardized_solute"] = orig
    rec["IUPAC_Name"] = ""
    rec["formula"] = ""
    rec["CID"] = ""
    rec["MW_from_table"] = None
    rec["identity_source"] = "IDENTITY_TABLE_REJECTED_SIMILARITY"
    rec["identity_confidence"] = "low"
    rec["identity_reason"] = (
        f"Local identity-table match rejected ({reason}). "
        f"Original name kept; MW/PubChem disabled to prevent wrong conversions."
    )


# =========================================================
# 3) 通用工具
# =========================================================

def norm_text(x: Any) -> str:
    if x is None:
        return ""
    try:
        if pd.isna(x):
            return ""
    except Exception:
        pass
    s = str(x).strip()
    s = s.replace("\u3000", " ").replace("\xa0", " ")
    s = s.replace("％", "%").replace("μ", "u").replace("µ", "u")
    s = re.sub(r"\s+", " ", s)
    return s.strip()



def normalize_key(s: Any) -> str:
    s = norm_text(s).lower()
    s = s.replace("（", "(").replace("）", ")")
    s = s.replace("，", ",").replace("；", ";")
    s = re.sub(r"\s+", " ", s)
    return s.strip()



def simplify_key(s: Any) -> str:
    s = normalize_key(s)
    s = re.sub(r"[\s\-\_\.\,\;\:\(\)\[\]\{\}/\\]+", "", s)
    return s



def normalize_abbr_key(s: Any) -> str:
    raw = norm_text(s)
    raw = re.sub(r"[\s\-_/\\\.\(\)]+", "", raw)
    return raw.upper()



def is_short_abbreviation(name: Any, max_len: int = 6) -> bool:
    raw = norm_text(name)
    if not raw:
        return False
    compact = re.sub(r"[\s\-_/\\\.\(\)]+", "", raw)
    if not compact:
        return False
    if len(compact) > max_len:
        return False
    if not re.fullmatch(r"[A-Za-z0-9]+", compact):
        return False
    alpha = [ch for ch in compact if ch.isalpha()]
    if not alpha:
        return False
    upper_ratio = sum(ch.isupper() for ch in alpha) / len(alpha)
    return upper_ratio >= 0.6



def allow_simplified_alias(name: Any) -> bool:
    raw = norm_text(name)
    simp = simplify_key(raw)
    if not raw or not simp:
        return False
    if is_short_abbreviation(raw, max_len=CFG.short_abbrev_max_len):
        return False
    if len(simp) >= 7:
        return True
    return any(ch in raw for ch in [" ", "-", ",", "(", ")", "/"])



def strip_excel_leading_quote(s: str) -> str:
    """Excel 导出时可能带前导单引号，如 '1-5。"""
    s = norm_text(s)
    if len(s) >= 2 and s[0] in "'’‘":
        return s[1:].strip()
    return s


def normalize_scientific_text_for_float(s: str) -> str:
    """
    将 10^-2、1×10^-2 等规范为可 float() 的字符串（不改动 ppm 等非纯幂表达式）。
    """
    t = strip_excel_leading_quote(s)
    if not t:
        return t
    t = t.replace("％", "%")
    t = re.sub(r"[\u2212\u2013\u2014⁻]", "-", t)
    t = t.replace("×", "x").replace("X", "x")

    m = re.match(r"^\s*10\s*\^\s*([+-]?\d+)\s*$", t, re.I)
    if m:
        return str(10.0 ** int(m.group(1)))

    m = re.match(r"^\s*([-+]?\d*\.?\d+)\s*x\s*10\s*\^\s*([+-]?\d+)\s*$", t, re.I)
    if m:
        return str(float(m.group(1)) * (10 ** int(m.group(2))))

    return t


def looks_like_date_or_chinese_calendar_junk(s: str) -> bool:
    s = norm_text(s)
    if re.search(r"\d+\s*月\s*\d+\s*日", s):
        return True
    if re.search(r"\b(january|february|march|april|may|june|july|august|september|october|november|december)\b", s, re.I):
        if re.search(r"\d{1,4}", s):
            return True
    return False


def looks_like_ratio_only_token(s: str) -> bool:
    s = norm_text(s)
    if re.fullmatch(r"\d+\s*:\s*\d+", s):
        return True
    if re.fullmatch(r"\d+\s+and\s+\d+", s, re.I):
        return True
    return False


def looks_like_suspicious_ocr_ratio_digits(s: str) -> bool:
    """如 1072 疑似 1:10 OCR 错位（保守：仅无单位时触发）。"""
    s = norm_text(s)
    return bool(re.fullmatch(r"\d{4}", s))


def try_parse_numeric_range_midpoint(raw: str) -> Optional[Tuple[float, str]]:
    """
    识别 2000-4000、1-5 等范围，默认取中值；返回 (mid, reason_note)。
    """
    s = strip_excel_leading_quote(norm_text(raw))
    if not s:
        return None
    s = s.replace(",", "")
    if looks_like_date_or_chinese_calendar_junk(s):
        return None
    if looks_like_ratio_only_token(s):
        return None

    m = re.match(
        r"^\s*([-+]?\d*\.?\d+(?:[eE][-+]?\d+)?)\s*[-~～至到\u2013\u2014]\s*([-+]?\d*\.?\d+(?:[eE][-+]?\d+)?)\s*$",
        s,
    )
    if not m:
        return None
    try:
        a = float(m.group(1))
        b = float(m.group(2))
    except Exception:
        return None
    mid = (a + b) / 2.0
    note = f"Parsed range {m.group(1)}-{m.group(2)}; using midpoint {mid:g}"
    return mid, note


def _preprocess_concentration_value_cell(rec: Dict[str, Any]) -> None:
    """
    单元格级预处理：Excel 引号、科学计数法、范围中值、日期/比例/可疑 OCR 标记。
    不新增宽表列；备注写入 _parse_notes，最终并入 final_reason。
    """
    rec["traceback_recovered_pair"] = ""
    rec["_parse_notes"] = []
    rec["_block_numeric_conversion"] = False
    rec["_ratio_semantics_unclear"] = False
    rec["_force_md_traceback"] = False
    rec["_md_pair_key"] = ""

    raw_v = norm_text(rec.get("original_value", ""))
    raw_u = norm_text(rec.get("original_unit", ""))
    if not raw_v:
        return

    v = strip_excel_leading_quote(raw_v)
    if v != raw_v:
        rec["_parse_notes"].append("Stripped Excel leading quote from value cell.")

    if looks_like_date_or_chinese_calendar_junk(v):
        rec["_block_numeric_conversion"] = True
        rec["_force_md_traceback"] = True
        rec["_parse_notes"].append(
            "Suspicious date-like/OCR-like value; blocked direct numeric concentration parse; prefer traceback."
        )
        return

    if looks_like_ratio_only_token(v):
        rec["_ratio_semantics_unclear"] = True
        rec["_force_md_traceback"] = True
        rec["_parse_notes"].append(
            "Ratio-like value detected; semantics unclear without explicit concentration context — no automatic wt% conversion."
        )
        return

    if looks_like_suspicious_ocr_ratio_digits(v) and not raw_u:
        rec["_force_md_traceback"] = True
        rec["_parse_notes"].append(
            "Suspicious 4-digit token (possible OCR-corrupted ratio); prefer traceback."
        )

    rng = try_parse_numeric_range_midpoint(v)
    if rng is not None:
        mid, note = rng
        rec["original_value"] = f"{mid:g}"
        rec["_parse_notes"].append(note)
        return

    nv = normalize_scientific_text_for_float(v)
    if nv != v:
        rec["original_value"] = nv
        rec["_parse_notes"].append("Normalized scientific notation (e.g. 10^-2) for numeric parsing.")
    else:
        rec["original_value"] = v


def parse_float_maybe(x: Any) -> Optional[float]:
    s = norm_text(x)
    if not s:
        return None

    s = strip_excel_leading_quote(s)
    s = s.replace(",", "")
    s = normalize_scientific_text_for_float(s)

    if re.search(r"(?<![eE])[~～至到]|(?<![eE])\s*[-–—]\s*", s):
        nums = re.findall(r"[-+]?\d*\.?\d+(?:[eE][-+]?\d+)?", s)
        if len(nums) > 1:
            return None

    if looks_like_ratio_only_token(s):
        return None

    m = re.search(r"[-+]?\d*\.?\d+(?:[eE][-+]?\d+)?", s)
    if not m:
        return None
    try:
        return float(m.group(0))
    except Exception:
        return None



def format_float_for_join(x: Any, ndigits: int = 6) -> str:
    if x is None:
        return ""
    try:
        if pd.isna(x):
            return ""
    except Exception:
        pass
    try:
        val = float(x)
        return f"{round(val, ndigits):g}"
    except Exception:
        return str(x)




def repair_unit_text(raw_unit: Any) -> str:
    """
    先做乱码修复/写法归一：
    - L?1 -> L-1
    - mg.L-1 -> mg/L
    - g L-1 -> g/L
    - w/v.% -> w/v%
    - wt/vol% -> wt/v%
    - % (aqueous) -> %   （只保留“这是百分号”的信息，语义类型后面再判）
    - w/v% of acetone -> w/v%
    """
    s = norm_text(raw_unit)
    if not s:
        return ""

    s = s.replace("％", "%")
    s = s.replace("μ", "u").replace("µ", "u")
    s = s.replace("−", "-").replace("–", "-").replace("—", "-").replace("⁻", "-")
    s = s.replace("·", ".").replace("．", ".").replace("／", "/")

    # 常见乱码：L?1 / mL?1
    s = re.sub(r'([A-Za-z])\?1\b', r'\1-1', s)

    # 点号或空格分隔的每升/每毫升写法 -> slash 形式
    s = re.sub(r'\b(g|mg|ug|ng|mol|mmol|monomoles)\.L-1\b', lambda m: f"{m.group(1)}/L", s, flags=re.I)
    s = re.sub(r'\b(g|mg|ug|ng|mol|mmol|monomoles)\s+L-1\b', lambda m: f"{m.group(1)}/L", s, flags=re.I)
    s = re.sub(r'\b(mg|g)\s+mL-1\b', lambda m: f"{m.group(1)}/mL", s, flags=re.I)
    s = re.sub(r'\b(mg|g)\.mL-1\b', lambda m: f"{m.group(1)}/mL", s, flags=re.I)

    # 单位尾巴里的介质说明，先砍掉，保留浓度类型本身
    s = re.sub(r'\b(of|in)\b.*$', '', s, flags=re.I).strip()

    # 变体收敛
    s = re.sub(r'w\s*/\s*v\s*\.\s*%', 'w/v%', s, flags=re.I)
    s = re.sub(r'wt\s*/\s*vol\s*%', 'wt/v%', s, flags=re.I)
    s = re.sub(r'wt\s*/\s*v\s*%', 'wt/v%', s, flags=re.I)

    # 这类写法本质仍然只是“百分号”，不是一个独立可换算单位
    if re.fullmatch(r'%\s*\(aqueous\)', s, flags=re.I):
        s = '%'

    s = re.sub(r'\s+', ' ', s).strip()
    return s


def load_json_cache(path: str) -> Dict[str, Any]:
    p = Path(path)
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {}



def save_json_cache(path: str, data: Dict[str, Any]) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def save_review_checkpoint(path: str, review_df: pd.DataFrame) -> None:
    """保存 build_review_sheet 的完整结果，供写 Excel 失败时跳过重跑。"""
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    review_df.to_pickle(p)


def load_review_checkpoint(path: str) -> pd.DataFrame:
    return pd.read_pickle(path)


def flush_json_caches(
    cfg: AppConfig,
    pubchem_cache: Dict[str, Any],
    llm_cache: Dict[str, Any],
    llm_unit_aux_cache: Dict[str, Any],
) -> None:
    """尽早 / 写 Excel 前将 PubChem、LLM 相关 JSON 落盘。"""
    save_json_cache(cfg.pubchem_cache_json, pubchem_cache)
    save_json_cache(cfg.llm_cache_json, llm_cache)
    save_json_cache(cfg.llm_unit_aux_cache_json, llm_unit_aux_cache)



def unique_keep_order(items: List[str]) -> List[str]:
    seen = set()
    out = []
    for x in items:
        if not x:
            continue
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out



def read_text_safely(path: Path) -> str:
    for enc in ("utf-8", "utf-8-sig", "latin-1", "gbk"):
        try:
            return path.read_text(encoding=enc, errors="ignore")
        except Exception:
            continue
    return ""



def find_column(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    if df is None or df.empty:
        return None
    mapping = {normalize_key(c): c for c in df.columns}
    for cand in candidates:
        key = normalize_key(cand)
        if key in mapping:
            return mapping[key]
    for cand in candidates:
        key = normalize_key(cand)
        for col in df.columns:
            ckey = normalize_key(col)
            if key == ckey or key in ckey or ckey in key:
                return col
    return None



def get_row_id(row: pd.Series, idx: int) -> Any:
    for c in ["row_id", "RowID", "RowId", "RowIndex", "ID", "id"]:
        if c in row.index:
            v = row.get(c)
            if norm_text(v):
                return v
    return idx + 1


# =========================================================
# 4) 输入读取
# =========================================================

def read_excel_with_engine(path: str, sheet_name: Optional[Any] = None) -> pd.DataFrame:
    """
    显式指定 Excel engine，避免 pandas 报：
    ValueError: Excel file format cannot be determined, you must specify an engine manually.
    常见于：无扩展名、扩展名异常、或部分环境下无法根据路径推断格式。
    """
    suf = Path(str(path)).suffix.lower()
    engine = "xlrd" if suf == ".xls" else "openpyxl"
    if sheet_name is None:
        return pd.read_excel(path, engine=engine)
    return pd.read_excel(path, sheet_name=sheet_name, engine=engine)


def read_csv_flexible(path: str) -> pd.DataFrame:
    """
    读取 CSV：依次尝试常见编码（含中文 Windows 下 gbk）。
    默认逗号分隔；若解析失败，再尝试制表符分隔。
    """
    encodings = ("utf-8-sig", "utf-8", "gbk", "gb18030", "latin-1")
    last_err: Optional[BaseException] = None
    for enc in encodings:
        for sep in (",", "\t"):
            try:
                return pd.read_csv(path, encoding=enc, sep=sep)
            except Exception as e:
                last_err = e
                continue
    raise RuntimeError(f"无法解析为 CSV: {path}") from last_err


def read_tabular_file(path: str, sheet_name: Optional[Any] = None) -> pd.DataFrame:
    """
    按扩展名自动选择：.csv -> read_csv；否则按 Excel 读取。
    CSV 无「工作表」，若传入 sheet_name 会忽略（仅 Excel 生效）。
    """
    suf = Path(str(path)).suffix.lower()
    if suf == ".csv":
        if sheet_name is not None:
            warnings.warn(
                f"[read_tabular_file] CSV 无 sheet，已忽略 sheet_name={sheet_name!r}（{path}）",
                UserWarning,
                stacklevel=2,
            )
        return read_csv_flexible(path)
    return read_excel_with_engine(path, sheet_name=sheet_name)


def read_inputs(cfg: AppConfig) -> Tuple[pd.DataFrame, Optional[pd.DataFrame], pd.DataFrame]:
    if cfg.input_main_sheet is None:
        main_df = read_tabular_file(cfg.input_main_excel)
    else:
        main_df = read_tabular_file(cfg.input_main_excel, sheet_name=cfg.input_main_sheet)

    unit_df = None
    if cfg.unit_catalog_excel and Path(cfg.unit_catalog_excel).exists():
        if cfg.unit_catalog_sheet is None:
            unit_df = read_tabular_file(cfg.unit_catalog_excel)
        else:
            unit_df = read_tabular_file(cfg.unit_catalog_excel, sheet_name=cfg.unit_catalog_sheet)

    if cfg.identity_table_sheet is None:
        identity_df = read_tabular_file(cfg.identity_table_excel)
    else:
        identity_df = read_tabular_file(cfg.identity_table_excel, sheet_name=cfg.identity_table_sheet)

    return main_df, unit_df, identity_df


# =========================================================
# 5) 单位归一化
# =========================================================

def _unit_out(
    original: str,
    canonical: str,
    family: str,
    unsupported_detail: str = "",
) -> Dict[str, str]:
    """统一单位字典结构，便于附带 unsupported 原因与下游统计。"""
    return {
        "original_unit": original,
        "canonical_unit": canonical,
        "unit_family": family,
        "unsupported_detail": unsupported_detail,
    }


def normalize_unit(raw_unit: Any) -> Dict[str, str]:
    raw = repair_unit_text(norm_text(raw_unit))
    if not raw:
        return _unit_out("", "", "unsupported", "empty unit string")

    s = raw.lower().strip()
    s = s.replace("％", "%").replace("μ", "u").replace("µ", "u").replace("／", "/")
    s_nospace = re.sub(r"\s+", "", s)

    # 统一括号 / 标点表达，方便处理 % (w/v) 这种写法
    s_compact = s_nospace
    s_compact = s_compact.replace("[", "(").replace("]", ")")
    s_compact = s_compact.replace("{", "(").replace("}", ")")

    # ---------- 裸 % ----------
    if s_compact in {"%", "percent"}:
        return _unit_out(raw, "%", "percent_ambiguous", "")

    # ---------- mass_fraction ----------
    mass_fraction_patterns = {
        "wt%", "wt.%", "weight%", "mass%", "wtpercent", "masspercent", "w/w%", "%w/w",
        "m/m%", "%m/m", "weightpercent", "%(w/w)", "%(m/m)", "%(wt)", "%(mass)",
        "wt", "w/w", "m/m"
    }
    if (
        s_compact in mass_fraction_patterns
        or "bymass" in s_compact
        or "w/w" in s_compact
        or "m/m" in s_compact
        or s_compact in {"massfraction", "weightfraction"}
    ):
        return _unit_out(raw, "wt%", "mass_fraction", "")

    # ---------- mass_volume_percent ----------
    mass_volume_patterns = {
        "w/v%", "%w/v", "wt/v%", "%wt/v", "w/v", "wt/v", "wv", "wtv",
        "%(w/v)", "%(wt/v)", "%(m/v)", "m/v", "g/100ml", "g/100milliliter",
        "g/100milliliters", "g/dl", "wt/vol%", "w/v.%"
    }
    if s_compact in mass_volume_patterns or re.fullmatch(r"g/100m?l", s_compact):
        canon = "g/100mL" if s_compact in {"g/100ml", "g/dl"} or re.fullmatch(r"g/100m?l", s_compact) else "w/v%"
        return _unit_out(raw, canon, "mass_volume_percent", "")

    # ---------- mass_concentration ----------
    mass_conc_map = {
        "g/l": "g/L", "gl-1": "g/L", "g·l-1": "g/L", "g.l-1": "g/L",
        "mg/l": "mg/L", "mgl-1": "mg/L", "mg·l-1": "mg/L", "mg.l-1": "mg/L",
        "ug/l": "ug/L", "ugl-1": "ug/L", "u g/l": "ug/L", "ug.l-1": "ug/L",
        "ng/l": "ng/L", "ngl-1": "ng/L", "ng.l-1": "ng/L",
        "mg/ml": "mg/mL", "mgml-1": "mg/mL", "mg.ml-1": "mg/mL", "g/ml": "g/mL", "g.ml-1": "g/mL",
        "ug/ml": "ug/mL", "ugml-1": "ug/mL", "ug·ml-1": "ug/mL", "ug.ml-1": "ug/mL",
        "μg/ml": "ug/mL",
    }
    if s_compact in mass_conc_map:
        return _unit_out(raw, mass_conc_map[s_compact], "mass_concentration", "")

    # ---------- molarity ----------
    if raw.strip() == "M":
        return _unit_out(raw, "M", "molarity", "")
    if raw.strip() == "mM":
        return _unit_out(raw, "mM", "molarity", "")
    molarity_map = {
        "mol/l": "mol/L", "moll-1": "mol/L", "mol·l-1": "mol/L", "mol.l-1": "mol/L",
        "mmol/l": "mmol/L", "mmoll-1": "mmol/L", "mmol·l-1": "mmol/L", "mmol.l-1": "mmol/L",
        "monomoles/l": "mol/L", "monomolesl-1": "mol/L",  # 容错：历史脏数据里常见奇怪写法
    }
    if s_compact in molarity_map:
        return _unit_out(raw, molarity_map[s_compact], "molarity", "")

    # ---------- volume_fraction ----------
    volume_fraction_map = {
        "v/v%": "v/v%", "%v/v": "v/v%", "v/v": "v/v%", "%(v/v)": "v/v%",
        "vol%": "vol%", "vol.%": "vol%", "vol": "vol%", "ml/l": "mL/L"
    }
    if s_compact in volume_fraction_map:
        return _unit_out(raw, volume_fraction_map[s_compact], "volume_fraction", "")

    # ---------- ppm family ----------
    ppm_map = {"ppm": "ppm", "ppb": "ppb", "g/kg": "g/kg", "mg/kg": "mg/kg"}
    if s_compact in ppm_map:
        return _unit_out(raw, ppm_map[s_compact], "ppm_family", "")

    # ---------- 明确不适用 wt% 换算（配方比例、聚合度等） ----------
    low_raw = raw.lower()
    if re.search(r"\b(molar\s*ratio|monomer\s*ratio|feed\s*ratio|mol\s*ratio|mole\s*ratio)\b", low_raw):
        return _unit_out(raw, raw, "unsupported", "ratio_label_not_a_concentration_unit")
    if re.search(r"\b(degree\s*of\s*polymeri[sz]ation|dp\b)\b", low_raw):
        return _unit_out(raw, raw, "unsupported", "polymerization_degree_not_concentration")

    # ---------- unsupported ----------
    if re.fullmatch(r"\d+\s*:\s*\d+", raw.strip()):
        return _unit_out(raw, raw, "unsupported", "numeric_ratio_not_concentration")

    if s_compact in {"drops", "drop", "g", "ml", "mmol", "mol", "l", "ul"}:
        return _unit_out(
            raw,
            raw,
            "unsupported",
            "bare_mass_or_volume_without_per_solution_not_concentration",
        )

    return _unit_out(raw, raw, "unsupported", "unrecognized_unit_spelling")


# =========================================================
# 6) 拆分 slot 子项
# =========================================================

def split_cell_multi(x: Any) -> List[str]:
    s = norm_text(x)
    if not s:
        return []
    parts = re.split(r"[;\n；]+", s)
    return [p.strip() for p in parts if norm_text(p)]



def split_inline_value_and_unit(value_str: str, unit_str: str) -> Tuple[str, str]:
    value_str = norm_text(value_str)
    unit_str = norm_text(unit_str)
    if unit_str:
        return value_str, unit_str

    m = re.match(r"^\s*([-+]?\d*\.?\d+(?:[eE][-+]?\d+)?)\s*(.+?)\s*$", value_str)
    if m:
        num_part = m.group(1)
        unit_part = m.group(2)
        if re.search(r"[A-Za-z%/]", unit_part):
            return num_part, unit_part.strip()
    return value_str, unit_str



def _align_items(lst: List[str], n: int, fill: str, warnings: List[str], field_name: str, allow_broadcast: bool = True) -> List[str]:
    if len(lst) == 0:
        warnings.append(f"{field_name}:empty_to_{n}")
        return [fill] * n
    if len(lst) == n:
        return lst
    if allow_broadcast and len(lst) == 1 and n > 1:
        warnings.append(f"{field_name}:broadcast_1_to_{n}")
        return lst * n
    if len(lst) < n:
        warnings.append(f"{field_name}:padded_{len(lst)}_to_{n}")
        return lst + [fill] * (n - len(lst))
    warnings.append(f"{field_name}:truncated_{len(lst)}_to_{n}")
    return lst[:n]



def split_slot_items(row: pd.Series, slot_spec: SlotSpec) -> List[Dict[str, Any]]:
    solute_raw = row.get(slot_spec.solute_col, "")
    value_raw = row.get(slot_spec.value_col, "")
    unit_raw = row.get(slot_spec.unit_col, "")

    # test_nacl：固定溶质为 NaCl，不再把浓度值当成溶质
    if slot_spec.slot_name == "test_nacl":
        solutes = ["NaCl"]
    else:
        solutes = split_cell_multi(solute_raw)

    values = split_cell_multi(value_raw)
    units = split_cell_multi(unit_raw)

    if not solutes and not values and not units:
        return []

    n = max(len(solutes), len(values), len(units), 1)
    warnings: List[str] = []

    # 对 test_nacl，允许 NaCl 广播；对普通 slot，solute 不轻易广播成多个不同化学物
    solutes_e = _align_items(
        solutes,
        n=n,
        fill=("NaCl" if slot_spec.slot_name == "test_nacl" else ""),
        warnings=warnings,
        field_name="solute",
        allow_broadcast=(slot_spec.slot_name == "test_nacl"),
    )
    values_e = _align_items(values, n=n, fill="", warnings=warnings, field_name="value", allow_broadcast=True)
    units_e = _align_items(units, n=n, fill="", warnings=warnings, field_name="unit", allow_broadcast=True)

    out = []
    for i in range(n):
        v, u = split_inline_value_and_unit(values_e[i], units_e[i])
        parse_error = ""
        if not norm_text(solutes_e[i]) and not norm_text(v) and not norm_text(u):
            parse_error = "empty_aligned_subitem"
        elif norm_text(solutes_e[i]) and not norm_text(v) and not norm_text(u):
            parse_error = "missing_value_and_unit"
        elif norm_text(solutes_e[i]) and not norm_text(v) and norm_text(u):
            parse_error = "missing_value"
        elif not norm_text(solutes_e[i]) and (norm_text(v) or norm_text(u)):
            parse_error = "missing_solute"

        confidence = "high"
        if warnings:
            confidence = "medium"
        if parse_error:
            confidence = "low"

        out.append({
            "item_index": i + 1,
            "original_solute": norm_text(solutes_e[i]),
            "original_value": norm_text(v),
            "original_unit": norm_text(u),
            "split_parse_error": parse_error,
            "split_parse_warning": ";".join(unique_keep_order(warnings)),
            "alignment_confidence": confidence,
        })
    return out


# =========================================================
# 7) 身份表构建与溶质匹配
# =========================================================

def _empty_identity_result(source: str) -> Dict[str, Any]:
    return {
        "standardized_solute": "",
        "IUPAC_Name": "",
        "formula": "",
        "CID": "",
        "MW_from_table": None,
        "identity_source": source,
        "identity_confidence": "none",
        "identity_reason": source,
    }



def build_identity_lookup(identity_df: pd.DataFrame) -> Dict[str, Dict[str, Any]]:
    col_original = find_column(identity_df, ["Original_Name", "original_name", "name", "raw_name"])
    col_formula = find_column(identity_df, ["Molecular_Formula", "molecular_formula", "formula"])
    col_iupac = find_column(identity_df, ["IUPAC_Name", "iupac_name", "IUPAC name"])
    col_cid = find_column(identity_df, ["CID", "cid"])
    col_std = find_column(identity_df, ["standardized_name", "Standardized_Name", "canonical_name", "Canonical_Name", "normalized_name"])
    col_mw = find_column(identity_df, ["MW", "MolecularWeight", "Molecular_Weight", "molecular weight"])

    exact_lookup: Dict[str, Dict[str, Any]] = {}
    abbr_lookup: Dict[str, Dict[str, Any]] = {}
    simplify_lookup: Dict[str, Dict[str, Any]] = {}

    for _, r in identity_df.iterrows():
        original_name = norm_text(r[col_original]) if col_original else ""
        formula = norm_text(r[col_formula]) if col_formula else ""
        iupac = norm_text(r[col_iupac]) if col_iupac else ""
        cid = norm_text(r[col_cid]) if col_cid else ""
        std_name = norm_text(r[col_std]) if col_std else ""
        mw_table = parse_float_maybe(r[col_mw]) if col_mw else None

        standardized_solute = std_name or iupac or original_name or formula
        rec = {
            "standardized_solute": standardized_solute,
            "IUPAC_Name": iupac,
            "formula": formula,
            "CID": cid,
            "MW_from_table": mw_table,
            "identity_source": "IDENTITY_TABLE",
            "identity_confidence": "high",
            "identity_reason": "Resolved from local identity table.",
        }

        for key in [original_name, standardized_solute, iupac, formula]:
            if not key:
                continue

            nk = normalize_key(key)
            if nk and nk not in exact_lookup:
                exact_lookup[nk] = rec.copy()

            if is_short_abbreviation(key, max_len=CFG.short_abbrev_max_len):
                ak = normalize_abbr_key(key)
                if ak and ak not in abbr_lookup:
                    abbr_lookup[ak] = rec.copy()

            if allow_simplified_alias(key):
                sk = simplify_key(key)
                if sk and sk not in simplify_lookup:
                    simplify_lookup[sk] = rec.copy()

    return {
        "__exact__": exact_lookup,
        "__abbr__": abbr_lookup,
        "__simplified__": simplify_lookup,
    }



def resolve_solute_identity(original_solute: str, identity_lookup: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    raw = norm_text(original_solute)
    if not raw:
        return _empty_identity_result("EMPTY_SOLUTE")

    exact_lookup = identity_lookup.get("__exact__", {})
    abbr_lookup = identity_lookup.get("__abbr__", {})
    simplify_lookup = identity_lookup.get("__simplified__", {})

    # 规则 1：短缩写只允许查本地映射表，不允许模糊猜测
    if is_short_abbreviation(raw, max_len=CFG.short_abbrev_max_len):
        ak = normalize_abbr_key(raw)
        if ak in abbr_lookup:
            rec = abbr_lookup[ak].copy()
            rec["identity_source"] = "IDENTITY_TABLE_ABBR_EXACT"
            rec["identity_confidence"] = "high"
            rec["identity_reason"] = f"Short abbreviation '{raw}' resolved by exact local-table abbreviation match."
            return rec

        nk = normalize_key(raw)
        if nk in exact_lookup:
            rec = exact_lookup[nk].copy()
            rec["identity_source"] = "IDENTITY_TABLE_EXACT"
            rec["identity_confidence"] = "high"
            rec["identity_reason"] = f"Short abbreviation '{raw}' resolved by exact local-table match."
            return rec

        return {
            "standardized_solute": raw,
            "IUPAC_Name": "",
            "formula": "",
            "CID": "",
            "MW_from_table": None,
            "identity_source": "UNRESOLVED_ABBREVIATION",
            "identity_confidence": "low",
            "identity_reason": "Short abbreviation not found in local identity table; no external guessing applied.",
        }

    # 非短缩写：先 exact，再 simplified
    nk = normalize_key(raw)
    if nk in exact_lookup:
        rec = exact_lookup[nk].copy()
        rec["identity_source"] = "IDENTITY_TABLE_EXACT"
        rec["identity_confidence"] = "high"
        rec["identity_reason"] = "Resolved by exact local identity-table match."
        return rec

    sk = simplify_key(raw)
    if sk in simplify_lookup:
        rec = simplify_lookup[sk].copy()
        rec["identity_source"] = "IDENTITY_TABLE_SIMPLIFIED"
        rec["identity_confidence"] = "medium"
        rec["identity_reason"] = "Resolved by simplified local identity-table alias match."
        return rec

    return {
        "standardized_solute": raw,
        "IUPAC_Name": "",
        "formula": "",
        "CID": "",
        "MW_from_table": None,
        "identity_source": "UNRESOLVED",
        "identity_confidence": "low",
        "identity_reason": "Not found in local identity table.",
    }


# =========================================================
# 8) PubChem 查询分子量（带缓存）
# =========================================================

def query_pubchem_mw_by_cid(cid: str, timeout_seconds: int = 30) -> Optional[float]:
    url = f"https://pubchem.ncbi.nlm.nih.gov/rest/pug/compound/cid/{quote(str(cid))}/property/MolecularWeight/JSON"
    r = requests.get(url, timeout=timeout_seconds)
    r.raise_for_status()
    data = r.json()
    props = data.get("PropertyTable", {}).get("Properties", [])
    if not props:
        return None
    mw = props[0].get("MolecularWeight")
    return float(mw) if mw is not None else None



def query_pubchem_mw_by_name(name: str, timeout_seconds: int = 30) -> Optional[float]:
    url = f"https://pubchem.ncbi.nlm.nih.gov/rest/pug/compound/name/{quote(name)}/property/MolecularWeight/JSON"
    r = requests.get(url, timeout=timeout_seconds)
    r.raise_for_status()
    data = r.json()
    props = data.get("PropertyTable", {}).get("Properties", [])
    if not props:
        return None
    mw = props[0].get("MolecularWeight")
    return float(mw) if mw is not None else None



def fetch_molecular_weight(record: Dict[str, Any], cfg: AppConfig, pubchem_cache: Dict[str, Any]) -> Tuple[Optional[float], str]:
    # 0) identity 被拒绝或不可信时，不使用 identity 表里残留的 MW（防误匹配）
    mw_table = record.get("MW_from_table")
    if record.get("identity_trust_for_mw") is False:
        mw_table = None

    # 1) 映射表已有 MW
    if mw_table is not None:
        return float(mw_table), "IDENTITY_TABLE"

    cid = norm_text(record.get("CID", ""))
    iupac = norm_text(record.get("IUPAC_Name", ""))
    std_name = norm_text(record.get("standardized_solute", ""))
    original_solute = norm_text(record.get("original_solute", ""))
    identity_source = norm_text(record.get("identity_source", ""))

    cache_keys: List[str] = []
    if cid:
        cache_keys.append(f"cid::{cid}")
    for nm in unique_keep_order([iupac, std_name, original_solute]):
        if nm:
            cache_keys.append(f"name::{normalize_key(nm)}")

    for ck in cache_keys:
        if ck in pubchem_cache:
            hit = pubchem_cache[ck]
            return hit.get("mw"), hit.get("source", "PUBCHEM_CACHE")

    if not cfg.pubchem_enabled:
        return None, "PUBCHEM_DISABLED"

    # 规则 2：PubChem 只负责补 MW，不负责猜短缩写意思
    if identity_source == "UNRESOLVED_ABBREVIATION":
        return None, "MW_SKIPPED_UNRESOLVED_ABBREVIATION"

    mw = None
    source = "PUBCHEM_NOT_FOUND"
    cid_error = None

    # 2) 先 CID
    if cid:
        try:
            mw = query_pubchem_mw_by_cid(cid, timeout_seconds=cfg.timeout_seconds)
            if mw is not None:
                source = "PUBCHEM_CID"
        except Exception as e:
            cid_error = f"PUBCHEM_CID_ERROR::{type(e).__name__}"

    # 3) 再 IUPAC / standardized / original name
    if mw is None:
        for name, label in [
            (iupac, "PUBCHEM_IUPAC_NAME"),
            (std_name, "PUBCHEM_STANDARDIZED_NAME"),
            (original_solute, "PUBCHEM_ORIGINAL_NAME"),
        ]:
            if not name:
                continue
            if is_short_abbreviation(name, max_len=cfg.short_abbrev_max_len):
                # 短缩写不做公网名称搜索
                continue
            try:
                mw = query_pubchem_mw_by_name(name, timeout_seconds=cfg.timeout_seconds)
                if mw is not None:
                    source = label
                    break
            except Exception:
                continue

    time.sleep(cfg.sleep_between_pubchem_calls)

    if mw is not None:
        payload = {"mw": mw, "source": source}
        for ck in cache_keys:
            pubchem_cache[ck] = payload
        return mw, source

    miss_source = cid_error or source
    for ck in cache_keys:
        pubchem_cache.setdefault(ck, {"mw": None, "source": miss_source})
    return None, miss_source


# =========================================================
# 9) 相别 / 溶剂识别
# =========================================================

def canonicalize_solvent_name(text: str) -> str:
    t = normalize_key(text)
    if not t:
        return ""
    for canon, aliases in SOLVENT_ALIASES.items():
        for alias in aliases:
            if normalize_key(alias) in t:
                return canon
    return ""



def extract_row_text(row: pd.Series) -> str:
    pieces = []
    for c in row.index:
        v = norm_text(row.get(c))
        if v:
            pieces.append(f"{c}: {v}")
    return " | ".join(pieces)



def infer_solvent_from_row(row: pd.Series, slot_spec: SlotSpec) -> str:
    aq_solvent = ""
    org_solvent = ""
    general_solvents: List[str] = []

    for col in row.index:
        ckey = normalize_key(col)
        val = norm_text(row.get(col))
        if not val:
            continue

        maybe_solvent = ""
        if "solvent" in ckey or "溶剂" in ckey:
            maybe_solvent = canonicalize_solvent_name(val)
            if maybe_solvent:
                if any(k in ckey for k in ["aqueous", "water", "水相"]):
                    aq_solvent = maybe_solvent
                elif any(k in ckey for k in ["organic", "oil", "油相"]):
                    org_solvent = maybe_solvent
                else:
                    general_solvents.append(maybe_solvent)

    row_text = extract_row_text(row)
    row_text_solvent = canonicalize_solvent_name(row_text)
    if row_text_solvent:
        general_solvents.append(row_text_solvent)

    general_solvents = unique_keep_order(general_solvents)

    if slot_spec.default_phase == "aqueous":
        return aq_solvent or "water"
    if slot_spec.default_phase == "organic":
        if org_solvent:
            return org_solvent
        for s in general_solvents:
            if s != "water":
                return s
        return ""

    # additive / modifier 保守：不要因为整行出现油相溶剂就强行判定 organic
    if aq_solvent:
        return aq_solvent
    if len(general_solvents) == 1 and general_solvents[0] == "water":
        return "water"
    return ""



def infer_phase_for_record(row: pd.Series, slot_spec: SlotSpec, record: Dict[str, Any]) -> Tuple[str, str, str]:
    """
    返回 phase, phase_confidence, phase_reason
    """
    if slot_spec.default_phase in {"aqueous", "organic"}:
        return slot_spec.default_phase, "high", f"Fixed by slot definition: {slot_spec.slot_name}."

    solvent_identified = normalize_key(record.get("solvent_identified", ""))
    std = normalize_key(record.get("standardized_solute", ""))
    raw = normalize_key(record.get("original_solute", ""))
    keys = {std, raw}

    if keys & COMMON_AQUEOUS_PHASE_SOLUTES:
        return "aqueous", "high", "Matched aqueous-phase solute heuristic whitelist."

    if keys & COMMON_ORGANIC_PHASE_SOLUTES:
        return "organic", "high", "Matched organic-phase solute heuristic whitelist."

    if solvent_identified == "water":
        return "aqueous", "medium", "Only aqueous solvent identified for this slot."

    # 对 additive / modifier，如果只有非水溶剂线索，不直接硬判 organic，避免 TEA/CSA/DSS 类误判
    if solvent_identified and solvent_identified != "water":
        return "unknown", "low", f"Non-water solvent '{solvent_identified}' seen in row, but additive/modifier kept conservative."

    return "unknown", "low", "No reliable phase evidence for additive/modifier slot."


# =========================================================
# 10) MD traceback
# =========================================================

def ensure_md_extracted(cfg: AppConfig) -> Optional[Path]:
    if not cfg.use_traceback:
        return None
    if not cfg.md_zip_path:
        return None

    zip_path = Path(cfg.md_zip_path)
    if not zip_path.exists():
        return None

    extract_dir = Path(cfg.md_extract_dir)
    extract_dir.mkdir(parents=True, exist_ok=True)

    if not any(extract_dir.iterdir()):
        with zipfile.ZipFile(zip_path, "r") as zf:
            zf.extractall(extract_dir)

    return extract_dir




def get_sample_name_text(row: pd.Series) -> str:
    """
    从样品编号/文件名/题目等字段里取一个适合做“配方补救解析”的文本。
    """
    candidates = []
    for c in row.index:
        ckey = normalize_key(c)
        v = norm_text(row.get(c))
        if not v:
            continue
        if any(k in ckey for k in ["样品", "sample", "filename", "file", "title", "题目", "文献标题"]):
            candidates.append(v)
    return " | ".join(unique_keep_order(candidates))


def extract_concentration_centered_snippets(text: str, keywords: List[str], window: int, limit: int = 3) -> List[str]:
    """
    优先抓“浓度表达附近 + 当前溶质关键词附近”的片段，而不是只围绕关键词截取。
    对判别裸 % 特别重要。
    """
    local_pattern = TRACEBACK_CONC_PATTERN
    low = text.lower()
    hits: List[Tuple[int, str]] = []

    keyword_positions: List[Tuple[str, int]] = []
    for kw in keywords:
        kw_low = kw.lower()
        start = 0
        while kw_low and (pos := low.find(kw_low, start)) >= 0:
            keyword_positions.append((kw, pos))
            start = pos + max(1, len(kw_low))

    for m in local_pattern.finditer(text):
        score = 0
        nearest = None
        if keyword_positions:
            nearest = min(abs(m.start() - pos) for _, pos in keyword_positions)
            if nearest <= window:
                score += max(1, window - nearest)
        # 明确类型说明更优先
        unit = norm_text(m.group("unit")).lower()
        if any(tag in unit for tag in ["w/v", "m/v", "v/v", "w/w", "wt"]):
            score += 40
        if score <= 0:
            continue
        start = max(0, m.start() - window)
        end = min(len(text), m.end() + window)
        snip = re.sub(r"\s+", " ", text[start:end].replace("\n", " ")).strip()
        hits.append((score, snip))

    hits.sort(key=lambda x: x[0], reverse=True)
    out = []
    seen = set()
    for _, snip in hits:
        if snip not in seen:
            seen.add(snip)
            out.append(snip)
        if len(out) >= limit:
            break
    return out


SAMPLE_RATIO_PATTERN = re.compile(
    r"\((?P<vals>[-+]?\d*\.?\d+(?:\s*[:：]\s*[-+]?\d*\.?\d+){1,5})\s*(?P<unit>%(?:\s*\((?:w/v|m/v|w/w|v/v|wt)\))?)\)",
    flags=re.I
)


def recover_concentration_from_sample_name(row: pd.Series, record: Dict[str, Any], item_index: int) -> Dict[str, Any]:
    """
    解析样品编号/样品名中的配方表达。
    例如:
      "(0.1:0.013%) N-IPAAm/MBAAm/CA/CTA-RO membrane"
    会把 item_index=1 -> 0.1 %, item_index=2 -> 0.013 % 取出来。
    """
    sample_text = get_sample_name_text(row)
    if not sample_text:
        return {"found": False, "reason": "No sample/title text available."}

    m = SAMPLE_RATIO_PATTERN.search(sample_text)
    if not m:
        return {"found": False, "reason": "No ratio-like concentration pattern found in sample/title text."}

    vals = [norm_text(x) for x in re.split(r"\s*[:：]\s*", m.group("vals")) if norm_text(x)]
    unit = norm_text(m.group("unit"))

    if not vals:
        return {"found": False, "reason": "Ratio pattern found but no numeric values extracted."}

    idx = max(0, int(item_index) - 1)
    if idx >= len(vals):
        return {"found": False, "reason": f"Sample/title ratio pattern has only {len(vals)} value(s), item_index={item_index} out of range."}

    # 与 traceback 一致的数值 sanity（样品名也可能含异常数字）
    canon = normalize_unit(unit)
    sane, sreason = _traceback_numeric_sanity(vals[idx], canon["unit_family"], canon["canonical_unit"])
    if not sane:
        return {
            "found": False,
            "reason": f"Sample/title candidate failed sanity ({sreason}).",
            "snippet": sample_text,
        }

    return {
        "found": True,
        "value": vals[idx],
        "unit": unit,
        "reason": f"Recovered from sample/title ratio expression: {m.group(0)}",
        "snippet": sample_text,
        "sanity_passed": True,
        "sanity_reason": sreason,
    }


def infer_percent_type_from_context(record: Dict[str, Any], row: pd.Series, snippets: List[str]) -> Dict[str, Any]:
    """
    给裸 % 加一层比 heuristic 更强的上下文判别。
    目标不是“全都转”，而是把明显可推断的 % 从 PERCENT_UNKNOWN 往前推进。
    """
    if record.get("unit_family") != "percent_ambiguous":
        return {
            "percent_type_inferred": record.get("percent_type_inferred", ""),
            "percent_type_confidence": "n/a",
            "percent_type_reason": "Unit family is not percent_ambiguous.",
        }

    slot_name = record.get("slot_name", "")
    phase = record.get("phase", "unknown")
    physical_form = record.get("physical_form_inferred", "unknown")
    solvent_identified = canonicalize_solvent_name(record.get("solvent_identified", ""))
    original_unit = normalize_key(record.get("original_unit", ""))
    sample_text = get_sample_name_text(row).lower()
    snippet_text = " ".join(snippets).lower()

    # 1) 文本里有明确标签，最高优先
    if re.search(r"%\s*\((w/v|m/v)\)|\bw/v\b|\bm/v\b|wt/vol|wt\s*/\s*v", original_unit + " " + snippet_text + " " + sample_text):
        return {
            "percent_type_inferred": "WV_PERCENT",
            "percent_type_confidence": "high",
            "percent_type_reason": "Explicit w/v or m/v style text found in unit/snippet/sample text.",
        }
    if re.search(r"%\s*\(v/v\)|\bv/v\b|\bvol\.?%?\b", original_unit + " " + snippet_text + " " + sample_text):
        return {
            "percent_type_inferred": "VOL_PERCENT",
            "percent_type_confidence": "high",
            "percent_type_reason": "Explicit v/v or vol% style text found in unit/snippet/sample text.",
        }
    if re.search(r"%\s*\((w/w|wt)\)|\bw/w\b|\bwt%\b|\bmass%\b|%\s*by\s*mass", original_unit + " " + snippet_text + " " + sample_text):
        return {
            "percent_type_inferred": "WT_PERCENT",
            "percent_type_confidence": "high",
            "percent_type_reason": "Explicit wt/w-w/mass style text found in unit/snippet/sample text.",
        }

    # 2) 固定 slot
    if slot_name == "test_nacl":
        return {
            "percent_type_inferred": "WT_PERCENT",
            "percent_type_confidence": "medium",
            "percent_type_reason": "test_nacl bare % treated as wt% in balanced mode.",
        }

    # 3) 样品编号中出现共享百分号时，结合 slot/phase/形态做推断
    if "%" in sample_text:
        if slot_name in {"aqueous_monomer", "additive", "modifier"} and phase in {"aqueous", "unknown"} and physical_form != "liquid":
            return {
                "percent_type_inferred": "WV_PERCENT",
                "percent_type_confidence": "medium",
                "percent_type_reason": "Sample/title contains shared % pattern; aqueous-side non-liquid solute is treated as likely w/v%.",
            }
        # 对“样品名里共享 % + graft/crosslink/polymerization 语境”的非液体项，允许较保守地视为 w/v%
        if physical_form != "liquid" and any(tok in (sample_text + " " + snippet_text) for tok in ["graft", "crosslink", "polymeriz", "persulfate", "naoh"]):
            return {
                "percent_type_inferred": "WV_PERCENT",
                "percent_type_confidence": "low",
                "percent_type_reason": "Shared % pattern under graft/crosslink/polymerization context is provisionally treated as w/v% for non-liquid solute.",
            }
        if phase == "organic" and physical_form == "liquid":
            return {
                "percent_type_inferred": "VOL_PERCENT",
                "percent_type_confidence": "medium",
                "percent_type_reason": "Sample/title contains shared % pattern; organic liquid solute is treated as likely v/v%.",
            }

    # 4) 上下文启发式：这是让裸 % 更容易“落地”的关键
    if slot_name in {"aqueous_monomer", "additive", "modifier"} and phase == "aqueous" and physical_form != "liquid":
        return {
            "percent_type_inferred": "WV_PERCENT",
            "percent_type_confidence": "medium",
            "percent_type_reason": "Aqueous-side non-liquid bare % is treated as likely w/v%.",
        }

    if slot_name in {"aqueous_monomer", "additive", "modifier"} and phase == "aqueous" and solvent_identified == "water":
        return {
            "percent_type_inferred": "WV_PERCENT",
            "percent_type_confidence": "low",
            "percent_type_reason": "Aqueous slot + water solvent suggests likely w/v% for bare %.",
        }

    if phase == "organic" and physical_form == "liquid":
        return {
            "percent_type_inferred": "VOL_PERCENT",
            "percent_type_confidence": "medium",
            "percent_type_reason": "Organic liquid bare % is treated as likely v/v%.",
        }

    if phase == "organic" and physical_form == "solid":
        return {
            "percent_type_inferred": "WT_PERCENT",
            "percent_type_confidence": "low",
            "percent_type_reason": "Organic solid bare % is provisionally treated as wt% rather than blocked; keep manual review on.",
        }

    return {
        "percent_type_inferred": "PERCENT_UNKNOWN",
        "percent_type_confidence": "low",
        "percent_type_reason": "Still insufficient context to disambiguate bare %.",
    }


def extract_row_metadata(row: pd.Series) -> Dict[str, str]:
    doi = ""
    title = ""
    filename_hint = ""

    for c in row.index:
        ckey = normalize_key(c)
        v = norm_text(row.get(c))
        if not v:
            continue
        if not doi and "doi" in ckey:
            doi = v
        if not title and any(k in ckey for k in ["title", "题目", "文献标题", "paper title"]):
            title = v
        if not filename_hint and any(k in ckey for k in ["file", "filename", "文件名"]):
            filename_hint = v

    return {"doi": doi, "title": title, "filename_hint": filename_hint}



def extract_snippet(text: str, keyword: str, window: int) -> str:
    lower_text = text.lower()
    lower_kw = keyword.lower()
    pos = lower_text.find(lower_kw)
    if pos < 0:
        return ""
    start = max(0, pos - window)
    end = min(len(text), pos + len(keyword) + window)
    snippet = text[start:end].replace("\n", " ")
    snippet = re.sub(r"\s+", " ", snippet)
    return snippet.strip()



def traceback_md_context(row: pd.Series, record: Dict[str, Any], cfg: AppConfig) -> List[str]:
    extract_dir = ensure_md_extracted(cfg)
    if extract_dir is None or not extract_dir.exists():
        return []

    md_files = list(extract_dir.rglob("*.md"))
    if not md_files:
        return []

    meta = extract_row_metadata(row)
    keywords = unique_keep_order([
        meta.get("doi", ""),
        meta.get("title", ""),
        meta.get("filename_hint", ""),
        record.get("original_solute", ""),
        record.get("standardized_solute", ""),
        record.get("IUPAC_Name", ""),
    ])
    keywords = [k for k in keywords if len(norm_text(k)) >= 2]
    if not keywords:
        return []

    scored_files = []
    for fp in md_files:
        fname = fp.name.lower()
        stem_s = simplify_key(fp.stem)
        score = 0
        for kw in keywords:
            kw_n = normalize_key(kw)
            kw_s = simplify_key(kw)
            if kw_n and kw_n in fname:
                score += 60
            if kw_s and kw_s in stem_s:
                score += 45
        if score > 0:
            scored_files.append((score, fp))

    if not scored_files:
        for fp in md_files:
            text_local = read_text_safely(fp)
            low = text_local.lower()
            score = 0
            for kw in keywords:
                kw_n = normalize_key(kw)
                if kw_n and kw_n in low:
                    score += 12
            if score > 0:
                scored_files.append((score, fp))

    scored_files.sort(key=lambda x: x[0], reverse=True)
    top_files = [fp for _, fp in scored_files[:8]]

    snippets: List[str] = []
    for fp in top_files:
        text_local = read_text_safely(fp)
        if not text_local:
            continue

        # 优先抓“浓度表达附近 + 关键词附近”的配方句
        centered = extract_concentration_centered_snippets(
            text_local,
            keywords=keywords,
            window=cfg.traceback_window_chars,
            limit=2,
        )
        for snip in centered:
            snippets.append(f"[{fp.name}] {snip}")
            if len(snippets) >= cfg.max_traceback_snippets:
                break
        if len(snippets) >= cfg.max_traceback_snippets:
            break

        # 回退到老的关键词截取
        for kw in keywords:
            snip = extract_snippet(text_local, kw, cfg.traceback_window_chars)
            if snip:
                snippets.append(f"[{fp.name}] {snip}")
                break
        if len(snippets) >= cfg.max_traceback_snippets:
            break

    return snippets[:cfg.max_traceback_snippets]


# 修复：原模式中裸 “M” 会匹配参考文献/PMID 等处的数字+字母 M；改为非词边界内的独立 M，并保留 mmol/mol 优先匹配
TRACEBACK_CONC_PATTERN = re.compile(
    r"(?P<value>[-+]?\d*\.?\d+(?:[eE][-+]?\d+)?)\s*"
    r"(?P<unit>"
    r"%\s*\((?:w/v|m/v|w/w|v/v|wt)\)|"
    r"wt\s*/?\s*v%?|w\s*/?\s*v%?|v\s*/?\s*v%?|"
    r"vol\.?\s*%?|wt%|w/w%|m/m%|mass%|"
    r"g\s*/\s*100\s*mL|g\s*/\s*L|mg\s*/\s*L|ug\s*/\s*L|ng\s*/\s*L|"
    r"mg\s*/\s*mL|ug\s*/\s*mL|"
    r"mmol\s*/\s*L|mol\s*/\s*L|"
    r"mM\b|"
    r"(?<![A-Za-z0-9])M(?![A-Za-z])|"
    r"mL\s*/\s*L|"
    r"ppm|ppb|g\s*/\s*kg|mg\s*/\s*kg|"
    r"%)",
    flags=re.I,
)


def _traceback_is_reference_noise_window(snip: str, match_start: int, match_end: int, window: int = 100) -> bool:
    """
    修复：DOI/PMID/页码/收稿日期等附近的数字极易被误当成浓度。
    宁可少抓，也不要把参考文献噪声当配方浓度。
    """
    lo = max(0, match_start - window)
    hi = min(len(snip), match_end + window)
    ctx = snip[lo:hi].lower()
    noise_markers = [
        r"\bdoi\b",
        r"doi:",
        r"pmid",
        r"isbn",
        r"\bpp\.\s*\d",
        r"\bpage\b",
        r"\bpages\b",
        r"\bvol\.?\s*\d",
        r"issue\s*\d",
        r"fig\.?\s*\d",
        r"table\s*\d",
        r"\bref\.?\s*\d",
        r"references?\b",
        r"\[\d+\]",
        r"copyright",
        r"accepted\b",
        r"published\b",
        r"received\b",
        r"revised\b",
        r"available online",
        r"http",
        r"www\.",
        r"crossref",
        r"elsvier",
        r"wiley",
        r"springer",
    ]
    return any(re.search(p, ctx) for p in noise_markers)


def _traceback_numeric_sanity(
    value_str: str,
    unit_family: str,
    canonical_unit: str,
) -> Tuple[bool, str]:
    """
    对 traceback 候选做保守数值范围过滤（防 114169 M 类荒谬值）。
    """
    v = parse_float_maybe(value_str)
    if v is None:
        return False, "unparseable_value"

    av = abs(v)
    s_int = norm_text(value_str).lstrip("+-")
    is_integer_like = bool(re.fullmatch(r"\d+", s_int))

    uf = unit_family or ""
    cu = (canonical_unit or "").lower()

    if uf == "molarity" or cu in {"m", "mm", "mol/l", "mmol/l"}:
        if is_integer_like and len(s_int) >= 5:
            return False, "huge_integer_with_molarity_unit_likely_ref_id"
        if cu in {"m", "mol/l"}:
            if av > 55.0:
                return False, "molarity_M_out_of_sane_lab_range"
        if cu in {"mm", "mmol/l"}:
            if av > 5e5:
                return False, "mmolarity_out_of_range"

    if uf == "ppm_family" and cu == "ppm" and av > 1e9:
        return False, "ppm_absurdly_high"

    if uf in {"mass_fraction", "mass_volume_percent", "volume_fraction"} or cu in {"%", "wt%"}:
        if av > 100.0 + 1e-6:
            return False, "percent_over_100"

    if uf == "mass_concentration":
        if av > 1e12:
            return False, "mass_conc_absurdly_high"

    return True, "ok"


def _traceback_keyword_variants(record: Dict[str, Any]) -> List[str]:
    kws = []
    for key in ["original_solute", "standardized_solute", "IUPAC_Name"]:
        val = norm_text(record.get(key, ""))
        if val and len(val) >= 2:
            kws.append(val)
    return unique_keep_order(kws)


def recover_concentration_from_traceback(record: Dict[str, Any], snippets: List[str]) -> Dict[str, Any]:
    """
    对“有溶质名但浓度缺失”的情况，尝试从 MD snippet 中回捞一个浓度候选。
    这里只做 regex + 就近关键词启发式，不直接覆盖原始值；会返回 candidate 供 review。
    修复：增加参考文献/噪声窗口过滤 + 数值 sanity；未通过 sanity 的候选不视为 found。
    """
    if not snippets:
        return {
            "found": False,
            "reason": "No traceback snippets available.",
            "candidate_count": 0,
            "sanity_passed": False,
            "sanity_reason": "no_snippets",
        }

    keywords = _traceback_keyword_variants(record)
    candidates: List[Dict[str, Any]] = []

    for snip in snippets:
        low = snip.lower()
        keyword_pos = []
        for kw in keywords:
            pos = low.find(kw.lower())
            if pos >= 0:
                keyword_pos.append((kw, pos))

        for m in TRACEBACK_CONC_PATTERN.finditer(snip):
            value = normalize_scientific_text_for_float(norm_text(m.group("value")))
            unit = norm_text(m.group("unit"))
            canon = normalize_unit(unit)
            score = 1
            hit_kw = ""

            if _traceback_is_reference_noise_window(snip, m.start(), m.end()):
                continue

            if canon["unit_family"] != "unsupported":
                score += 8
            if canon["unit_family"] == "percent_ambiguous":
                score += 1
            if canon["canonical_unit"] != "%":
                score += 2

            local_ctx = snip[max(0, m.start()-40): min(len(snip), m.end()+40)].lower()
            if any(bad in local_ctx for bad in ["sigma", "aldrich", "purity", "purchased", "supplied by", ">", "≥"]):
                score -= 8

            if keyword_pos:
                nearest = min(abs(m.start() - pos) for _, pos in keyword_pos)
                score += max(0, 18 - min(nearest, 18))
                hit_kw = min(keyword_pos, key=lambda x: abs(m.start() - x[1]))[0]

            sane, sane_reason = _traceback_numeric_sanity(value, canon["unit_family"], canon["canonical_unit"])

            candidates.append({
                "value": value,
                "unit": unit,
                "canonical_unit": canon["canonical_unit"],
                "unit_family": canon["unit_family"],
                "score": score,
                "keyword": hit_kw,
                "snippet": snip,
                "sanity_passed": sane,
                "sanity_reason": sane_reason,
            })

    if not candidates:
        return {
            "found": False,
            "reason": "No concentration-like pattern found in traceback snippets.",
            "candidate_count": 0,
            "sanity_passed": False,
            "sanity_reason": "no_regex_hits",
        }

    # 优先：通过 sanity 的候选；否则整组视为未恢复（found=False），避免荒谬换算
    sane_pool = [c for c in candidates if c.get("sanity_passed")]
    pool = sane_pool if sane_pool else []

    if not pool:
        best_bad = sorted(candidates, key=lambda x: x["score"], reverse=True)[0]
        return {
            "found": False,
            "reason": (
                f"Traceback regex hit(s) failed sanity check (best candidate filtered: {best_bad.get('sanity_reason')}). "
                f"Likely reference/page/DOI noise, not formulation concentration."
            ),
            "candidate_count": len(candidates),
            "sanity_passed": False,
            "sanity_reason": best_bad.get("sanity_reason", "failed"),
            "value": best_bad.get("value", ""),
            "unit": best_bad.get("unit", ""),
            "canonical_unit": best_bad.get("canonical_unit", ""),
            "unit_family": best_bad.get("unit_family", ""),
            "snippet": best_bad.get("snippet", ""),
        }

    pool.sort(key=lambda x: x["score"], reverse=True)
    best = pool[0]
    return {
        "found": True,
        "value": best["value"],
        "unit": best["unit"],
        "canonical_unit": best["canonical_unit"],
        "unit_family": best["unit_family"],
        "candidate_count": len(candidates),
        "reason": f"Recovered from MD snippet by regex proximity search (keyword='{best['keyword'] or 'N/A'}', score={best['score']}, sanity={best.get('sanity_reason')}).",
        "snippet": best["snippet"],
        "sanity_passed": True,
        "sanity_reason": best.get("sanity_reason", "ok"),
    }


# =========================================================
# 11) LLM 判断（只做语义判断）
# =========================================================

def extract_json_from_text(text: str) -> Dict[str, Any]:
    text = text.strip()
    text = re.sub(r"^```json\s*", "", text, flags=re.I)
    text = re.sub(r"^```\s*", "", text, flags=re.I)
    text = re.sub(r"\s*```$", "", text)
    first = text.find("{")
    last = text.rfind("}")
    if first >= 0 and last > first:
        text = text[first:last + 1]
    return json.loads(text)



def heuristic_judge_slot(record: Dict[str, Any], snippets: List[str]) -> Dict[str, Any]:
    std = normalize_key(record.get("standardized_solute", ""))
    raw = normalize_key(record.get("original_solute", ""))
    slot_name = record.get("slot_name", "")
    unit_family = record.get("unit_family", "")
    phase = record.get("phase", "unknown") or "unknown"

    merged_text = " | ".join([std, raw])

    # physical form
    if std in COMMON_LIQUIDS or raw in COMMON_LIQUIDS or std in SOLUTE_DENSITY_DICT or raw in SOLUTE_DENSITY_DICT:
        physical_form = "liquid"
    elif std in COMMON_SOLIDS or raw in COMMON_SOLIDS:
        physical_form = "solid"
    elif any(tok in merged_text for tok in SOLID_HINT_PATTERNS) and not any(tok in merged_text for tok in LIQUID_HINT_PATTERNS):
        physical_form = "solid"
    else:
        physical_form = "unknown"

    # percent type: 这里先给基础判断，更强的上下文判断由 infer_percent_type_from_context() 再补
    if unit_family in PERCENT_TYPE_BY_FAMILY:
        percent_type = PERCENT_TYPE_BY_FAMILY[unit_family]
    elif unit_family == "percent_ambiguous":
        snippet_text = " ".join(snippets).lower()
        if re.search(r"w/v|g/100 ?ml|% ?\(w/v\)|% ?\(m/v\)", snippet_text):
            percent_type = "WV_PERCENT"
        elif re.search(r"v/v|vol%|% ?\(v/v\)", snippet_text):
            percent_type = "VOL_PERCENT"
        elif re.search(r"w/w|wt%|mass%|% ?\(w/w\)|% ?\(wt\)", snippet_text):
            percent_type = "WT_PERCENT"
        elif slot_name == "test_nacl" and ("nacl" in std or "sodium chloride" in std or "nacl" in raw):
            percent_type = "WT_PERCENT"
        else:
            percent_type = "PERCENT_UNKNOWN"
    else:
        percent_type = ""

    need_traceback = unit_family == "percent_ambiguous" and percent_type == "PERCENT_UNKNOWN"

    return {
        "physical_form_inferred": physical_form,
        "phase": phase,
        "percent_type_inferred": percent_type,
        "solute_for_this_value": record.get("standardized_solute", "") or record.get("original_solute", ""),
        "need_traceback": need_traceback,
        "judgement_source": "heuristic",
        "reason": "heuristic_fallback",
    }


def should_use_llm(record: Dict[str, Any]) -> bool:
    unit_family = record.get("unit_family", "")
    phase = record.get("phase", "unknown")
    standardized_solute = norm_text(record.get("standardized_solute", ""))
    return (
        unit_family == "percent_ambiguous"
        or phase == "unknown"
        or (unit_family == "molarity" and not standardized_solute)
    )



def llm_judge_slot(record: Dict[str, Any], snippets: List[str], cfg: AppConfig, llm_cache: Dict[str, Any]) -> Dict[str, Any]:
    heuristic = heuristic_judge_slot(record, snippets)

    if not cfg.llm_enabled or not cfg.llm_api_key or not should_use_llm(record):
        return heuristic

    cache_key_payload = {
        "slot_name": record.get("slot_name"),
        "original_solute": record.get("original_solute"),
        "standardized_solute": record.get("standardized_solute"),
        "original_value": record.get("original_value"),
        "original_unit": record.get("original_unit"),
        "canonical_unit": record.get("canonical_unit"),
        "unit_family": record.get("unit_family"),
        "phase": record.get("phase"),
        "snippets": snippets,
    }
    cache_key = json.dumps(cache_key_payload, ensure_ascii=False, sort_keys=True)
    if cache_key in llm_cache:
        return llm_cache[cache_key]

    system_prompt = """
You are helping normalize membrane-experiment concentration metadata.
Rules:
1) Return STRICT JSON only.
2) Never do arithmetic.
3) Prefer UNKNOWN over over-guessing.
4) Determine:
   - physical_form_inferred: solid | liquid | unknown
   - phase: aqueous | organic | unknown
   - percent_type_inferred:
       WT_PERCENT | VOL_PERCENT | WV_PERCENT | PERCENT_UNKNOWN | ""
   - solute_for_this_value: short string
   - need_traceback: true | false
   - reason: short reason
5) If raw unit is bare %, do NOT default to wt% unless evidence strongly supports it.
"""

    user_payload = {
        "record": {
            "slot_name": record.get("slot_name"),
            "original_solute": record.get("original_solute"),
            "standardized_solute": record.get("standardized_solute"),
            "IUPAC_Name": record.get("IUPAC_Name"),
            "original_value": record.get("original_value"),
            "original_unit": record.get("original_unit"),
            "canonical_unit": record.get("canonical_unit"),
            "unit_family": record.get("unit_family"),
            "phase_current_guess": record.get("phase"),
            "solvent_identified": record.get("solvent_identified"),
        },
        "traceback_snippets": snippets,
        "heuristic_baseline": heuristic,
    }

    try:
        url = cfg.llm_base_url.rstrip("/") + "/chat/completions"
        headers = {
            "Authorization": f"Bearer {cfg.llm_api_key}",
            "Content-Type": "application/json",
        }
        body = {
            "model": cfg.llm_model,
            "temperature": 0,
            "messages": [
                {"role": "system", "content": system_prompt.strip()},
                {"role": "user", "content": json.dumps(user_payload, ensure_ascii=False)},
            ],
        }
        r = requests.post(url, headers=headers, json=body, timeout=cfg.timeout_seconds)
        r.raise_for_status()
        data = r.json()
        content = data["choices"][0]["message"]["content"]
        parsed = extract_json_from_text(content)
        out = {
            "physical_form_inferred": parsed.get("physical_form_inferred", heuristic["physical_form_inferred"]),
            "phase": parsed.get("phase", heuristic["phase"]),
            "percent_type_inferred": parsed.get("percent_type_inferred", heuristic["percent_type_inferred"]),
            "solute_for_this_value": parsed.get("solute_for_this_value", heuristic["solute_for_this_value"]),
            "need_traceback": bool(parsed.get("need_traceback", heuristic["need_traceback"])),
            "judgement_source": "llm",
            "reason": norm_text(parsed.get("reason", "")) or "llm_judgement",
        }
        llm_cache[cache_key] = out
        return out
    except Exception:
        return heuristic


# =========================================================
# 11b) LLM 辅助单位修复与换算（保守、可审计；不执行 LLM 给出的任意公式字符串）
# =========================================================

ALLOWED_LLM_UNIT_FAMILIES = frozenset({
    "mass_fraction",
    "mass_volume_percent",
    "mass_concentration",
    "molarity",
    "volume_fraction",
    "ppm_family",
    "percent_ambiguous",
})


def _deepseek_chat_json_payload(system: str, user_obj: Dict[str, Any], cfg: AppConfig) -> Dict[str, Any]:
    """统一 DeepSeek 调用，返回解析后的 JSON；失败抛异常由上层捕获。"""
    url = cfg.llm_base_url.rstrip("/") + "/chat/completions"
    headers = {
        "Authorization": f"Bearer {cfg.llm_api_key}",
        "Content-Type": "application/json",
    }
    body = {
        "model": cfg.llm_model,
        "temperature": 0,
        "messages": [
            {"role": "system", "content": system.strip()},
            {"role": "user", "content": json.dumps(user_obj, ensure_ascii=False)},
        ],
    }
    r = requests.post(url, headers=headers, json=body, timeout=cfg.timeout_seconds)
    r.raise_for_status()
    data = r.json()
    content = data["choices"][0]["message"]["content"]
    return extract_json_from_text(content)


def wt_percent_passes_sanity(wt: Optional[float], allow_slight_over: bool = False) -> bool:
    """程序侧结果校验：默认 0–100 wt% 为合理实验范围。"""
    if wt is None:
        return False
    try:
        x = float(wt)
    except Exception:
        return False
    if x < 0:
        return False
    if not allow_slight_over and x > 100.0 + 1e-4:
        return False
    if allow_slight_over and x > 10000.0:
        return False
    return True


def _llm_confidence_ok(conf: Any, cfg: AppConfig) -> bool:
    c = norm_text(str(conf)).lower()
    if c == "high":
        return True
    if c == "medium" and cfg.llm_unit_aux_min_auto_confidence == "medium":
        return True
    return False


def _normalize_unit_string_through_rules(raw: str) -> Dict[str, str]:
    """规则链：repair_unit_text → normalize_unit（与主流程一致）。"""
    s = repair_unit_text(norm_text(raw))
    return normalize_unit(s)


def normalize_or_repair_unit_with_llm(
    raw_unit: str,
    context_text: Optional[str],
    cfg: AppConfig,
    llm_unit_aux_cache: Dict[str, Any],
) -> Dict[str, Any]:
    """
    类型 A：单位符号疑似 OCR/编码错误。
    必须先走规则；此处仅在规则已返回 unsupported 时由上层调用。
    返回结构化 JSON + 程序侧对 cleaned 的再归一化结果。
    """
    out: Dict[str, Any] = {
        "applied": False,
        "normalized_unit_info": None,
        "cleaned_unit": "",
        "confidence": "",
        "reason": "",
        "whether_safe_to_convert": False,
        "interpretation": "",
        "raw_llm": {},
    }
    if not norm_text(raw_unit):
        out["reason"] = "empty_raw_unit"
        return out

    cache_key = json.dumps({"task": "unit_repair", "u": raw_unit, "ctx": context_text or ""}, sort_keys=True)
    if cache_key in llm_unit_aux_cache:
        cached = llm_unit_aux_cache[cache_key]
        return dict(cached)

    if not cfg.llm_enabled or not norm_text(cfg.llm_api_key):
        out["reason"] = "llm_disabled_or_no_api_key"
        llm_unit_aux_cache[cache_key] = out
        return out

    system = """
You repair chemistry concentration UNIT strings only (OCR/encoding/typo). Return STRICT JSON.
Rules:
1) Do NOT invent numeric values. Do NOT output arithmetic.
2) Map the unit to a standard chemical notation string (e.g. mg/L, g/L, μg/mL, µg/mL, M, mM, wt%, w/v%, v/v%, ppm).
3) If unsure, set whether_safe_to_convert=false and confidence=low.
4) suspected_unit_family must be one of:
   mass_fraction | mass_volume_percent | mass_concentration | molarity | volume_fraction | ppm_family | percent_ambiguous | unsupported
5) suspected_canonical_unit MUST be a short string you believe is standard (e.g. "mg/L"), not a sentence.
6) Fields:
   original_unit, cleaned_unit, suspected_canonical_unit, suspected_unit_family,
   interpretation, confidence (high|medium|low),
   whether_safe_to_convert (boolean), reason (short)
"""

    user_obj = {"original_unit": raw_unit, "context_text": context_text or ""}
    try:
        parsed = _deepseek_chat_json_payload(system, user_obj, cfg)
        out["raw_llm"] = parsed
        out["cleaned_unit"] = norm_text(parsed.get("cleaned_unit", ""))
        out["confidence"] = norm_text(parsed.get("confidence", "")).lower()
        out["reason"] = norm_text(parsed.get("reason", ""))
        out["whether_safe_to_convert"] = bool(parsed.get("whether_safe_to_convert", False))
        out["interpretation"] = norm_text(parsed.get("interpretation", ""))

        cand = norm_text(parsed.get("suspected_canonical_unit", "")) or out["cleaned_unit"]
        if not cand:
            out["reason"] = out["reason"] or "llm_no_candidate_unit"
            llm_unit_aux_cache[cache_key] = out
            return out

        u2 = _normalize_unit_string_through_rules(cand)
        # 家族以程序 normalize_unit 为准；LLM 的 suspected_unit_family 仅作提示，不据此否决

        if u2["unit_family"] != "unsupported" and out["whether_safe_to_convert"] and _llm_confidence_ok(out["confidence"], cfg):
            out["applied"] = True
            out["normalized_unit_info"] = u2
            out["reason"] = out["reason"] or "llm_repair_normalized_ok"
        else:
            out["reason"] = out["reason"] or "llm_repair_not_reliable_or_normalize_failed"
    except Exception as e:
        out["reason"] = f"llm_repair_exception::{type(e).__name__}"

    llm_unit_aux_cache[cache_key] = out
    return out


def infer_conversion_formula_with_llm(
    unit_text: str,
    value_str: Optional[str],
    context_text: Optional[str],
    cfg: AppConfig,
    llm_unit_aux_cache: Dict[str, Any],
) -> Dict[str, Any]:
    """
    类型 B：疑似新单位/变体。LLM 仅输出结构化语义与「建议 canonical」；
    程序不执行 conversion_formula_to_wt_percent 字符串，只作审计字段。
    """
    out: Dict[str, Any] = {
        "applied": False,
        "normalized_unit_info": None,
        "confidence": "",
        "whether_safe_to_convert": False,
        "interpretation": "",
        "conversion_formula_to_wt_percent": "",
        "required_parameters": [],
        "reason": "",
        "raw_llm": {},
    }
    if not norm_text(unit_text):
        out["reason"] = "empty_unit_text"
        return out

    cache_key = json.dumps(
        {"task": "unit_formula", "u": unit_text, "v": value_str or "", "ctx": context_text or ""},
        sort_keys=True,
    )
    if cache_key in llm_unit_aux_cache:
        return dict(llm_unit_aux_cache[cache_key])

    if not cfg.llm_enabled or not norm_text(cfg.llm_api_key):
        out["reason"] = "llm_disabled_or_no_api_key"
        llm_unit_aux_cache[cache_key] = out
        return out

    system = """
You classify a concentration UNIT string for membrane chemistry datasets. Return STRICT JSON only.
Rules:
1) Do NOT evaluate numbers. Do NOT claim numeric conversion results.
2) suggested_conversion_formula_to_wt_percent is ONLY a human-readable hint (string). The downstream program will NOT eval it.
3) suspected_unit_family MUST be one of:
   mass_fraction | mass_volume_percent | mass_concentration | molarity | volume_fraction | ppm_family | percent_ambiguous | unsupported
4) If physical meaning is unclear, set whether_safe_to_convert=false and confidence=low.
5) required_parameters: list of strings among:
   MW | rho_solution | rho_solute | phase | temperature | (add short notes if needed)
6) Fields:
   original_unit, suspected_canonical_unit, suspected_unit_family,
   interpretation,
   suggested_conversion_formula_to_wt_percent,
   required_parameters (array of strings),
   confidence (high|medium|low),
   whether_safe_to_convert (boolean),
   reason (short)
"""

    user_obj = {"original_unit": unit_text, "value": value_str or "", "context_text": context_text or ""}
    try:
        parsed = _deepseek_chat_json_payload(system, user_obj, cfg)
        out["raw_llm"] = parsed
        out["confidence"] = norm_text(parsed.get("confidence", "")).lower()
        out["whether_safe_to_convert"] = bool(parsed.get("whether_safe_to_convert", False))
        out["interpretation"] = norm_text(parsed.get("interpretation", ""))
        out["conversion_formula_to_wt_percent"] = norm_text(parsed.get("suggested_conversion_formula_to_wt_percent", ""))
        rp = parsed.get("required_parameters", [])
        if isinstance(rp, list):
            out["required_parameters"] = [norm_text(x) for x in rp if norm_text(x)]
        out["reason"] = norm_text(parsed.get("reason", ""))

        cand = norm_text(parsed.get("suspected_canonical_unit", ""))

        if cand:
            u2 = _normalize_unit_string_through_rules(cand)
        else:
            u2 = normalize_unit("")

        fam_ok = u2["unit_family"] in (ALLOWED_LLM_UNIT_FAMILIES | {"percent_ambiguous"})
        if (
            u2["unit_family"] != "unsupported"
            and fam_ok
            and out["whether_safe_to_convert"]
            and _llm_confidence_ok(out["confidence"], cfg)
        ):
            out["applied"] = True
            out["normalized_unit_info"] = u2
            out["reason"] = out["reason"] or "llm_formula_classify_ok"
        else:
            out["reason"] = out["reason"] or "llm_formula_not_applied_conservative"
    except Exception as e:
        out["reason"] = f"llm_formula_exception::{type(e).__name__}"

    llm_unit_aux_cache[cache_key] = out
    return out


def classify_conversion_parameters(
    rec: Dict[str, Any],
    density_info: Dict[str, Any],
    llm_required: List[str],
) -> Tuple[List[str], List[str], List[str]]:
    """
    将 LLM 声明的 required_parameters 与当前记录已有字段对齐（保守：不凭空假设密度）。
    """
    found: List[str] = []
    missing: List[str] = []
    assumed: List[str] = []

    mw = rec.get("MW")
    rho_s = density_info.get("rho_solution")
    rho_sol = density_info.get("rho_solute")
    dens_assumed = bool(density_info.get("density_assumed", False))

    for raw in llm_required:
        t = normalize_key(raw)
        if not t:
            continue
        if "mw" in t or "molecular" in t or "molar mass" in t:
            if mw is not None:
                found.append(raw)
            else:
                missing.append(raw)
        elif "rho_solution" in t or "solution density" in t or "density of solution" in t:
            if rho_s is not None:
                found.append(raw)
                if dens_assumed:
                    assumed.append(f"{raw} (rho_solution assumed)")
            else:
                missing.append(raw)
        elif "rho_solute" in t or "solute density" in t:
            if rho_sol is not None:
                found.append(raw)
            else:
                missing.append(raw)
        elif "temperature" in t:
            if "temp" in normalize_key(rec.get("original_unit", "")):
                found.append(raw)
            else:
                missing.append(raw)
        else:
            missing.append(raw)

    return found, missing, assumed


def apply_llm_unit_resolution_to_record(
    rec: Dict[str, Any],
    row: pd.Series,
    cfg: AppConfig,
    llm_unit_aux_cache: Dict[str, Any],
) -> Dict[str, str]:
    """
    在首次 normalize_unit 之后调用：必要时走 LLM 修复/分类，
    返回应写回 rec 的 **新 unit_info**（canonical_* / unit_family / unsupported_detail）。
    同时在 rec 上填充审计字段（扁平）。
    """
    raw = norm_text(rec.get("original_unit", ""))
    u0 = normalize_unit(rec["original_unit"])

    # 审计默认值
    rec["raw_unit"] = raw
    rec["cleaned_unit"] = ""
    rec["canonical_unit_before_llm"] = u0.get("canonical_unit", "")
    rec["canonical_unit_after_llm"] = u0.get("canonical_unit", "")
    rec["unit_family_before_llm"] = u0.get("unit_family", "")
    rec["unit_family_after_llm"] = u0.get("unit_family", "")
    rec["llm_unit_fix_used"] = False
    rec["llm_formula_used"] = False
    rec["llm_unit_aux_path"] = "RULE"
    rec["llm_unit_aux_attempted"] = False
    rec["llm_formula_used_text"] = ""
    rec["llm_required_parameters"] = ""
    rec["llm_confidence"] = ""
    rec["llm_reason"] = ""
    rec["llm_safe_to_convert"] = False
    rec["conversion_parameters_found"] = ""
    rec["conversion_parameters_missing"] = ""
    rec["conversion_parameters_assumed"] = ""
    rec["final_conversion_formula"] = ""
    rec["final_value_wt_percent"] = None
    rec["llm_manual_review_for_unit"] = False

    if not raw:
        return u0

    if u0["unit_family"] != "unsupported":
        return u0

    ctx = extract_row_text(row)
    if len(ctx) > 4000:
        ctx = ctx[:4000]

    # ---------- 类型 A：修复 ----------
    rec["llm_unit_aux_attempted"] = True
    repair = normalize_or_repair_unit_with_llm(raw, ctx, cfg, llm_unit_aux_cache)
    rec["llm_reason"] = repair.get("reason", "")
    rec["llm_confidence"] = repair.get("confidence", "")
    rec["llm_safe_to_convert"] = bool(repair.get("whether_safe_to_convert", False))
    rec["cleaned_unit"] = repair.get("cleaned_unit", "")

    if repair.get("applied") and repair.get("normalized_unit_info"):
        u1 = repair["normalized_unit_info"]
        rec["llm_unit_fix_used"] = True
        rec["llm_unit_aux_path"] = "LLM_REPAIR"
        if repair.get("confidence") == "medium":
            rec["llm_manual_review_for_unit"] = True
        rec["canonical_unit_after_llm"] = u1.get("canonical_unit", "")
        rec["unit_family_after_llm"] = u1.get("unit_family", "")
        return u1

    # ---------- 类型 B：公式/语义分类（仅映射到已有换算分支） ----------
    infer = infer_conversion_formula_with_llm(raw, rec.get("original_value"), ctx, cfg, llm_unit_aux_cache)
    rec["llm_formula_used_text"] = infer.get("conversion_formula_to_wt_percent", "")
    rec["llm_required_parameters"] = json.dumps(infer.get("required_parameters", []), ensure_ascii=False)
    if infer.get("reason"):
        rec["llm_reason"] = (rec.get("llm_reason") + " | " if rec.get("llm_reason") else "") + infer.get("reason", "")
    if infer.get("confidence"):
        rec["llm_confidence"] = infer.get("confidence", "")
    rec["llm_safe_to_convert"] = bool(infer.get("whether_safe_to_convert", False))

    if infer.get("applied") and infer.get("normalized_unit_info"):
        u2 = infer["normalized_unit_info"]
        rec["llm_formula_used"] = True
        rec["llm_unit_aux_path"] = "LLM_FORMULA"
        if infer.get("confidence") == "medium":
            rec["llm_manual_review_for_unit"] = True
        rec["canonical_unit_after_llm"] = u2.get("canonical_unit", "")
        rec["unit_family_after_llm"] = u2.get("unit_family", "")
        return u2

    rec["unit_family_after_llm"] = u0.get("unit_family", "")
    rec["canonical_unit_after_llm"] = u0.get("canonical_unit", "")
    rec["llm_unit_aux_path"] = "LLM_FAILED"
    return u0


def remap_success_status_by_unit_path(rec: Dict[str, Any]) -> None:
    """
    将纯规则/LLM 单位路径映射为用户要求的最终状态枚举（不覆盖失败类状态）。
    """
    st = rec.get("suggested_status", "")
    path = rec.get("llm_unit_aux_path", "RULE")
    if rec.get("suggested_wtpercent") is None:
        return
    if st in {"FAILED_PARSE", "NEED_TRACEBACK", "CANNOT_CONVERT", "NEED_MANUAL_REVIEW"}:
        return
    if st not in SUCCESS_STATUS and st not in {"DIRECT_WT", "SAFE_CONVERTED", "ASSUMED_CONVERTED"}:
        return

    if path == "RULE":
        if st in {"DIRECT_WT", "SAFE_CONVERTED", "ASSUMED_CONVERTED"}:
            rec["suggested_status"] = "RULE_NORMALIZED_CONVERTED"
    elif path == "LLM_REPAIR":
        rec["suggested_status"] = "LLM_UNIT_REPAIRED_CONVERTED"
    elif path == "LLM_FORMULA":
        rec["suggested_status"] = "LLM_FORMULA_CONVERTED"


# =========================================================
# 12) 密度解析
# =========================================================

def find_explicit_density_in_row(row: pd.Series, slot_name: str) -> Tuple[Optional[float], str]:
    preferred_keywords = {
        "aqueous_monomer": ["aqueous", "water", "水相"],
        "organic_monomer": ["organic", "oil", "油相"],
        "additive": ["solution", "solvent", "溶液", "density", "密度"],
        "modifier": ["solution", "solvent", "溶液", "density", "密度"],
        "test_nacl": ["test", "nacl", "solution", "水相", "aqueous"],
    }

    best_score = None
    best_source = ""
    best_rho = None

    for col in row.index:
        ckey = normalize_key(col)
        if "density" not in ckey and "密度" not in ckey:
            continue
        v = row.get(col)
        rho = parse_float_maybe(v)
        if rho is None or rho <= 0 or rho > 5:
            continue
        score = 1
        for kw in preferred_keywords.get(slot_name, []):
            if normalize_key(kw) in ckey:
                score += 2
        if best_score is None or score > best_score:
            best_score = score
            best_source = f"ROW_COLUMN::{col}"
            best_rho = rho

    if best_score is None:
        return None, ""
    return best_rho, best_source



def resolve_density(record: Dict[str, Any], row: pd.Series) -> Dict[str, Any]:
    slot_name = record.get("slot_name", "")
    phase = record.get("phase", "unknown")
    solvent = canonicalize_solvent_name(record.get("solvent_identified", ""))
    std = normalize_key(record.get("standardized_solute", ""))
    raw = normalize_key(record.get("original_solute", ""))

    # rho_solute：优先专用 dict；若溶质本身也是常见溶剂，则复用 SOLVENT_DENSITY_DICT
    rho_solute = SOLUTE_DENSITY_DICT.get(std)
    if rho_solute is None:
        rho_solute = SOLUTE_DENSITY_DICT.get(raw)
    if rho_solute is None:
        canon_std = canonicalize_solvent_name(std) or canonicalize_solvent_name(raw)
        if canon_std:
            rho_solute = SOLVENT_DENSITY_DICT.get(canon_std)

    rho_explicit, rho_source = find_explicit_density_in_row(row, slot_name)
    if rho_explicit is not None:
        return {
            "rho_solution": rho_explicit,
            "rho_solute": rho_solute,
            "density_source": rho_source,
            "density_assumed": False,
            "density_reason": f"Explicit density taken from row column: {rho_source}.",
        }

    if phase == "aqueous" or slot_name == "test_nacl":
        return {
            "rho_solution": 1.0,
            "rho_solute": rho_solute,
            "density_source": "ASSUMED_AQUEOUS_RHO_1.0",
            "density_assumed": True,
            "density_reason": "Aqueous/test solution assumed rho≈1.0 g/mL.",
        }

    # organic 只在相别已经较明确时，才使用 solvent density 近似 solution density
    if phase == "organic" and solvent:
        rho_solution = SOLVENT_DENSITY_DICT.get(solvent)
        if rho_solution is not None:
            return {
                "rho_solution": rho_solution,
                "rho_solute": rho_solute,
                "density_source": f"SOLVENT_DICT::{solvent}",
                "density_assumed": True,
                "density_reason": f"Organic low-concentration solution approximated by solvent density of {solvent}.",
            }

    # 若 phase 仍 unknown，但 v/v% 场景至少先把 rho_solute 暴露给 review；rho_solution 不硬猜
    return {
        "rho_solution": None,
        "rho_solute": rho_solute,
        "density_source": "NO_RELIABLE_SOLUTION_DENSITY",
        "density_assumed": True if rho_solute is not None else False,
        "density_reason": "No reliable solution density; only solute density may be available.",
    }


# =========================================================
# 13) wt% 换算
# =========================================================

def convert_to_wtpercent(record: Dict[str, Any], density_info: Dict[str, Any], ndigits: int = 6) -> Dict[str, Any]:
    if record.get("_ratio_semantics_unclear"):
        return {
            "suggested_wtpercent": None,
            "suggested_status": "NEED_MANUAL_REVIEW",
            "suggested_reason": "Ratio-like value; semantics unclear. " + "; ".join(record.get("_parse_notes") or []),
            "conversion_formula_used": "RATIO_SEMANTICS_UNCLEAR",
        }
    if record.get("_block_numeric_conversion"):
        return {
            "suggested_wtpercent": None,
            "suggested_status": "NEED_TRACEBACK",
            "suggested_reason": "; ".join(record.get("_parse_notes") or ["Suspicious date-like/OCR-like value"]),
            "conversion_formula_used": "BLOCKED_DATE_LIKE",
        }

    original_value = record.get("original_value", "")
    value = parse_float_maybe(original_value)
    if value is None:
        return {
            "suggested_wtpercent": None,
            "suggested_status": "FAILED_PARSE",
            "suggested_reason": f"Cannot parse numeric value from: {original_value}",
            "conversion_formula_used": "PARSE_FAIL",
        }

    # 优先使用 effective_*（traceback 回填后与换算口径一致）
    unit_family = record.get("effective_unit_family") or record.get("unit_family", "")
    canonical_unit = record.get("effective_canonical_unit") or record.get("canonical_unit", "")
    orig_family = record.get("original_unit_family", record.get("unit_family", ""))
    phase = record.get("phase", "unknown")
    percent_type = record.get("percent_type_inferred", "")
    mw = record.get("MW")
    rho_solution = density_info.get("rho_solution")
    rho_solute = density_info.get("rho_solute")
    density_source = density_info.get("density_source", "")
    density_assumed = bool(density_info.get("density_assumed", False))
    judgement_source = record.get("judgement_source", "heuristic")
    slot_name = record.get("slot_name", "")

    effective_family = unit_family
    effective_unit = canonical_unit

    if unit_family == "percent_ambiguous":
        if percent_type == "WT_PERCENT":
            effective_family = "mass_fraction"
            effective_unit = "wt%"
        elif percent_type == "WV_PERCENT":
            effective_family = "mass_volume_percent"
            effective_unit = "w/v%"
        elif percent_type == "VOL_PERCENT":
            effective_family = "volume_fraction"
            effective_unit = "v/v%"
        else:
            return {
                "suggested_wtpercent": None,
                "suggested_status": "NEED_TRACEBACK",
                "suggested_reason": "Raw % is ambiguous; percent type still unresolved.",
                "conversion_formula_used": "AMBIGUOUS_PERCENT",
            }

    # A. mass fraction
    if effective_family == "mass_fraction":
        if orig_family == "mass_fraction":
            return {
                "suggested_wtpercent": round(value, ndigits),
                "suggested_status": "DIRECT_WT",
                "suggested_reason": "Already mass fraction / wt% equivalent.",
                "conversion_formula_used": "DIRECT_WT",
            }
        status = "SAFE_CONVERTED" if judgement_source == "llm" else "ASSUMED_CONVERTED"
        return {
            "suggested_wtpercent": round(value, ndigits),
            "suggested_status": status,
            "suggested_reason": f"Raw % inferred as WT_PERCENT via {judgement_source}.",
            "conversion_formula_used": "PERCENT_TO_WT",
        }

    # B. ppm family
    if effective_family == "ppm_family":
        if effective_unit == "ppm":
            if phase == "aqueous" or slot_name == "test_nacl":
                wt = value / 10000.0
                return {
                    "suggested_wtpercent": round(wt, ndigits),
                    "suggested_status": "ASSUMED_CONVERTED",
                    "suggested_reason": "ppm -> wt% using dilute aqueous approximation.",
                    "conversion_formula_used": "PPM_TO_WT",
                }
            return {
                "suggested_wtpercent": None,
                "suggested_status": "CANNOT_CONVERT",
                "suggested_reason": "ppm without aqueous context is not converted in balanced mode.",
                "conversion_formula_used": "PPM_BLOCKED",
            }

        if effective_unit == "ppb":
            if phase == "aqueous" or slot_name == "test_nacl":
                wt = value / 1e7
                return {
                    "suggested_wtpercent": round(wt, ndigits),
                    "suggested_status": "ASSUMED_CONVERTED",
                    "suggested_reason": "ppb -> wt% using dilute aqueous approximation.",
                    "conversion_formula_used": "PPB_TO_WT",
                }
            return {
                "suggested_wtpercent": None,
                "suggested_status": "CANNOT_CONVERT",
                "suggested_reason": "ppb without aqueous context is not converted in balanced mode.",
                "conversion_formula_used": "PPB_BLOCKED",
            }

        if effective_unit == "g/kg":
            wt = value / 10.0
            return {
                "suggested_wtpercent": round(wt, ndigits),
                "suggested_status": "SAFE_CONVERTED",
                "suggested_reason": "g/kg is directly mass-based.",
                "conversion_formula_used": "G_PER_KG_TO_WT",
            }

        if effective_unit == "mg/kg":
            wt = value / 10000.0
            return {
                "suggested_wtpercent": round(wt, ndigits),
                "suggested_status": "SAFE_CONVERTED",
                "suggested_reason": "mg/kg is directly mass-based.",
                "conversion_formula_used": "MG_PER_KG_TO_WT",
            }

    # mass concentration
    if effective_family == "mass_concentration":
        if rho_solution is None:
            return {
                "suggested_wtpercent": None,
                "suggested_status": "NEED_TRACEBACK",
                "suggested_reason": "Mass concentration conversion requires solution density.",
                "conversion_formula_used": "MASS_CONC_NEEDS_RHO",
            }

        if effective_unit == "g/L":
            wt = value / (10.0 * rho_solution)
        elif effective_unit == "mg/L":
            wt = value / (10000.0 * rho_solution)
        elif effective_unit == "ug/L":
            wt = value / (10000000.0 * rho_solution)
        elif effective_unit == "ng/L":
            wt = value / (10000000000.0 * rho_solution)
        elif effective_unit == "mg/mL":
            wt = value / (10.0 * rho_solution)
        elif effective_unit == "g/mL":
            wt = value * 100.0 / rho_solution
        elif effective_unit == "ug/mL":
            # x ug/mL 数值上等同 x mg/L（稀溶液近似）
            wt = value / (10000.0 * rho_solution)
        else:
            return {
                "suggested_wtpercent": None,
                "suggested_status": "CANNOT_CONVERT",
                "suggested_reason": f"Unsupported mass concentration unit: {effective_unit}",
                "conversion_formula_used": "MASS_CONC_UNSUPPORTED",
            }

        status = "ASSUMED_CONVERTED" if density_assumed else "SAFE_CONVERTED"
        return {
            "suggested_wtpercent": round(wt, ndigits),
            "suggested_status": status,
            "suggested_reason": f"{effective_unit} -> wt% using rho_solution={rho_solution} from {density_source}.",
            "conversion_formula_used": "MASS_CONC_TO_WT",
        }

    # C. w/v%
    if effective_family == "mass_volume_percent":
        if rho_solution is None:
            return {
                "suggested_wtpercent": None,
                "suggested_status": "NEED_TRACEBACK",
                "suggested_reason": "w/v% conversion requires solution density.",
                "conversion_formula_used": "WV_NEEDS_RHO",
            }
        wt = value / rho_solution
        status = "ASSUMED_CONVERTED" if density_assumed else "SAFE_CONVERTED"
        return {
            "suggested_wtpercent": round(wt, ndigits),
            "suggested_status": status,
            "suggested_reason": f"{effective_unit} -> wt% using rho_solution={rho_solution} from {density_source}.",
            "conversion_formula_used": "WV_TO_WT",
        }

    # D. molarity
    if effective_family == "molarity":
        if mw is None:
            return {
                "suggested_wtpercent": None,
                "suggested_status": "NEED_TRACEBACK",
                "suggested_reason": "Molarity conversion requires molecular weight.",
                "conversion_formula_used": "MOLARITY_NEEDS_MW",
            }
        if rho_solution is None:
            return {
                "suggested_wtpercent": None,
                "suggested_status": "NEED_TRACEBACK",
                "suggested_reason": "Molarity conversion requires solution density.",
                "conversion_formula_used": "MOLARITY_NEEDS_RHO",
            }

        if effective_unit in {"M", "mol/L"}:
            c = value
        elif effective_unit in {"mM", "mmol/L"}:
            c = value / 1000.0
        else:
            return {
                "suggested_wtpercent": None,
                "suggested_status": "CANNOT_CONVERT",
                "suggested_reason": f"Unsupported molarity unit: {effective_unit}",
                "conversion_formula_used": "MOLARITY_UNSUPPORTED",
            }

        wt = c * mw / (10.0 * rho_solution)
        status = "ASSUMED_CONVERTED" if density_assumed else "SAFE_CONVERTED"
        return {
            "suggested_wtpercent": round(wt, ndigits),
            "suggested_status": status,
            "suggested_reason": f"{effective_unit} -> wt% using MW={mw} and rho_solution={rho_solution}.",
            "conversion_formula_used": "MOLARITY_TO_WT",
        }

    # E. v/v%
    if effective_family == "volume_fraction":
        if rho_solute is None:
            return {
                "suggested_wtpercent": None,
                "suggested_status": "NEED_TRACEBACK",
                "suggested_reason": "v/v% conversion requires solute density.",
                "conversion_formula_used": "VV_NEEDS_RHO_SOLUTE",
            }
        if rho_solution is None:
            return {
                "suggested_wtpercent": None,
                "suggested_status": "NEED_TRACEBACK",
                "suggested_reason": "v/v% conversion requires solution density.",
                "conversion_formula_used": "VV_NEEDS_RHO_SOLUTION",
            }

        if effective_unit in {"v/v%", "vol%"}:
            vv_percent = value
        elif effective_unit == "mL/L":
            vv_percent = value / 10.0
        else:
            return {
                "suggested_wtpercent": None,
                "suggested_status": "CANNOT_CONVERT",
                "suggested_reason": f"Unsupported volume fraction unit: {effective_unit}",
                "conversion_formula_used": "VV_UNSUPPORTED",
            }

        wt = vv_percent * rho_solute / rho_solution
        status = "ASSUMED_CONVERTED" if density_assumed else "SAFE_CONVERTED"
        return {
            "suggested_wtpercent": round(wt, ndigits),
            "suggested_status": status,
            "suggested_reason": f"{effective_unit} -> wt% using rho_solute={rho_solute}, rho_solution={rho_solution}.",
            "conversion_formula_used": "VV_TO_WT",
        }

    if effective_family == "unsupported":
        detail = norm_text(record.get("unsupported_unit_detail", "")) or norm_text(record.get("unsupported_detail", ""))
        reason = f"Unsupported unit family: {unit_family}"
        if detail:
            reason += f" ({detail})"
        return {
            "suggested_wtpercent": None,
            "suggested_status": "CANNOT_CONVERT",
            "suggested_reason": reason,
            "conversion_formula_used": "UNSUPPORTED_UNIT",
        }

    return {
        "suggested_wtpercent": None,
        "suggested_status": "CANNOT_CONVERT",
        "suggested_reason": f"Unhandled unit family: {unit_family}",
        "conversion_formula_used": "UNHANDLED_UNIT_FAMILY",
    }


# =========================================================
# 14) 主 review 表构建
# =========================================================

def annotate_duplicate_md_recoveries(review_df: pd.DataFrame) -> None:
    """
    同一 run 内大量行从 MD 回溯到同一 (slot, value, unit) 时，在 final_reason 末尾追加提示（防「全员 1 M」误覆盖）。
    """
    if review_df.empty or "_md_pair_key" not in review_df.columns:
        return
    keys = review_df["_md_pair_key"].fillna("").astype(str)
    nz = keys[keys != ""]
    if nz.empty:
        return
    vc = nz.value_counts()
    hot = {k for k, n in vc.items() if int(n) >= 25}
    if not hot:
        return
    for idx in review_df.index:
        k = str(review_df.at[idx, "_md_pair_key"])
        if k in hot:
            n = int(vc[k])
            extra = (
                f" NOTE: same MD traceback pair appears {n} times in this run (possible generic snippet); verify row-specific context."
            )
            fr = norm_text(review_df.at[idx, "final_reason"])
            review_df.at[idx, "final_reason"] = (fr + extra).strip()


def build_review_sheet(
    main_df: pd.DataFrame,
    identity_lookup: Dict[str, Dict[str, Any]],
    cfg: AppConfig,
    pubchem_cache: Dict[str, Any],
    llm_cache: Dict[str, Any],
    llm_unit_aux_cache: Dict[str, Any],
) -> pd.DataFrame:
    review_rows: List[Dict[str, Any]] = []

    n_rows = len(main_df)
    row_iter = main_df.iterrows()
    if cfg.show_progress and n_rows:
        row_iter = tqdm(
            row_iter,
            total=n_rows,
            desc="浓度审查",
            unit="行",
        )

    for idx, row in row_iter:
        row_id = get_row_id(row, idx)

        for slot_spec in SLOT_SPECS:
            items = split_slot_items(row, slot_spec)

            for item in items:
                rec: Dict[str, Any] = {
                    "row_id": row_id,
                    "slot_name": slot_spec.slot_name,
                    "item_index": item.get("item_index", 1),
                    "original_solute": item.get("original_solute", ""),
                    "original_value": item.get("original_value", ""),
                    "original_unit": item.get("original_unit", ""),
                    "split_parse_error": item.get("split_parse_error", ""),
                    "split_parse_warning": item.get("split_parse_warning", ""),
                    "alignment_confidence": item.get("alignment_confidence", ""),
                }
                _preprocess_concentration_value_cell(rec)

                identity = resolve_solute_identity(rec["original_solute"], identity_lookup)
                rec.update(identity)
                # 修复：identity 表误匹配（如 GO -> methane）时回退并禁止错误 MW
                apply_identity_plausibility_filter(rec)

                # 先规则归一化，再（若 unsupported）LLM 辅助修复/分类；保留 original_* 为规则-only 结果
                unit_info = normalize_unit(rec["original_unit"])
                rec["original_unit_family"] = unit_info["unit_family"]
                rec["original_canonical_unit"] = unit_info["canonical_unit"]
                unit_info = apply_llm_unit_resolution_to_record(rec, row, cfg, llm_unit_aux_cache)
                rec["canonical_unit"] = unit_info["canonical_unit"]
                rec["unit_family"] = unit_info["unit_family"]
                rec["unsupported_unit_detail"] = unit_info.get("unsupported_detail", "")
                rec["effective_unit_family"] = unit_info["unit_family"]
                rec["effective_canonical_unit"] = unit_info["canonical_unit"]

                rec["solvent_identified"] = infer_solvent_from_row(row, slot_spec)
                phase, phase_confidence, phase_reason = infer_phase_for_record(row, slot_spec, rec)
                rec["phase"] = phase
                rec["phase_confidence"] = phase_confidence
                rec["phase_reason"] = phase_reason

                missing_concentration = bool(norm_text(rec["original_solute"])) and not bool(norm_text(rec["original_value"]))
                need_snippets = cfg.use_traceback and (
                    rec["unit_family"] == "percent_ambiguous"
                    or rec["phase"] == "unknown"
                    or missing_concentration
                    or bool(rec.get("_force_md_traceback"))
                    or bool(rec.get("_block_numeric_conversion"))
                    or bool(rec.get("_ratio_semantics_unclear"))
                )
                snippets = traceback_md_context(row, rec, cfg) if need_snippets else []
                judge = llm_judge_slot(rec, snippets, cfg, llm_cache)

                rec["physical_form_inferred"] = judge.get("physical_form_inferred", "unknown")
                rec["percent_type_inferred"] = judge.get("percent_type_inferred", "")
                rec["percent_type_confidence"] = "high" if rec["percent_type_inferred"] in {"WT_PERCENT", "WV_PERCENT", "VOL_PERCENT"} and rec["unit_family"] != "percent_ambiguous" else ""
                rec["percent_type_reason"] = "Directly implied by normalized unit family." if rec["percent_type_confidence"] else ""
                rec["judgement_source"] = judge.get("judgement_source", "heuristic")
                rec["judgement_reason"] = judge.get("reason", "")
                rec["need_traceback_flag"] = bool(judge.get("need_traceback", False))
                rec["traceback_snippets"] = "\n---\n".join(snippets) if snippets else ""

                if rec["unit_family"] == "percent_ambiguous":
                    percent_ctx = infer_percent_type_from_context(rec, row, snippets)
                    if percent_ctx.get("percent_type_inferred") and percent_ctx.get("percent_type_inferred") != "PERCENT_UNKNOWN":
                        rec["percent_type_inferred"] = percent_ctx["percent_type_inferred"]
                    elif not rec.get("percent_type_inferred"):
                        rec["percent_type_inferred"] = percent_ctx.get("percent_type_inferred", "")
                    rec["percent_type_confidence"] = percent_ctx.get("percent_type_confidence", "")
                    rec["percent_type_reason"] = percent_ctx.get("percent_type_reason", "")

                # 允许 LLM 覆盖 unknown phase，但不覆盖 slot 固定相别
                if slot_spec.default_phase == "unknown" and judge.get("phase") in {"aqueous", "organic", "unknown"}:
                    llm_phase = judge.get("phase", rec["phase"])
                    if llm_phase != rec["phase"]:
                        rec["phase"] = llm_phase
                        rec["phase_confidence"] = "medium" if rec["judgement_source"] == "llm" else rec["phase_confidence"]
                        rec["phase_reason"] = f"Phase updated by {rec['judgement_source']}: {rec['judgement_reason']}"

                mw, mw_source = fetch_molecular_weight(rec, cfg, pubchem_cache)
                rec["MW"] = mw
                rec["MW_source"] = mw_source

                density_info = resolve_density(rec, row)
                rec["density_source"] = density_info.get("density_source", "")
                rec["density_assumed"] = density_info.get("density_assumed", False)
                rec["density_reason"] = density_info.get("density_reason", "")
                rec["rho_solution"] = density_info.get("rho_solution")
                rec["rho_solute"] = density_info.get("rho_solute")

                # LLM 公式路径：审计「公式文字」+ 参数是否已在记录中找到（不执行公式字符串）
                rec["final_conversion_formula"] = norm_text(rec.get("llm_formula_used_text", ""))
                try:
                    req_list = json.loads(rec.get("llm_required_parameters") or "[]")
                except Exception:
                    req_list = []
                if not isinstance(req_list, list):
                    req_list = []
                ff, mm, aa = classify_conversion_parameters(rec, density_info, req_list)
                rec["conversion_parameters_found"] = json.dumps(ff, ensure_ascii=False)
                rec["conversion_parameters_missing"] = json.dumps(mm, ensure_ascii=False)
                rec["conversion_parameters_assumed"] = json.dumps(aa, ensure_ascii=False)

                rec["traceback_candidate_value"] = ""
                rec["traceback_candidate_unit"] = ""
                rec["traceback_candidate_canonical_unit"] = ""
                rec["traceback_candidate_count"] = 0
                rec["traceback_candidate_reason"] = ""
                rec["traceback_candidate_snippet"] = ""
                rec["traceback_candidate_sanity_passed"] = False
                rec["traceback_candidate_sanity_reason"] = ""
                rec["resolved_value_for_conversion"] = rec["original_value"]
                rec["resolved_unit_for_conversion"] = rec["original_unit"]

                parse_error = rec["split_parse_error"]
                missing_concentration_for_traceback = parse_error in {"missing_value_and_unit", "missing_value"} and bool(norm_text(rec["original_solute"]))
                need_traceback_recovery = (
                    missing_concentration_for_traceback
                    or bool(rec.get("_block_numeric_conversion"))
                    or bool(rec.get("_ratio_semantics_unclear"))
                )

                if parse_error in {"empty_aligned_subitem", "missing_solute"}:
                    rec["suggested_wtpercent"] = None
                    rec["suggested_status"] = "FAILED_PARSE"
                    rec["conversion_formula_used"] = "SPLIT_FAIL"
                    rec["suggested_reason"] = f"Split/alignment issue: {parse_error}"
                elif need_traceback_recovery:
                    recovered_sample = recover_concentration_from_sample_name(row, rec, rec.get("item_index", 1))
                    recovered_md = recover_concentration_from_traceback(rec, snippets)

                    rec["traceback_candidate_count"] = recovered_md.get("candidate_count", 0)
                    if recovered_sample.get("found"):
                        rec["traceback_candidate_sanity_passed"] = bool(recovered_sample.get("sanity_passed", True))
                        rec["traceback_candidate_sanity_reason"] = norm_text(recovered_sample.get("sanity_reason", "ok"))
                    else:
                        rec["traceback_candidate_sanity_passed"] = bool(recovered_md.get("sanity_passed"))
                        rec["traceback_candidate_sanity_reason"] = norm_text(recovered_md.get("sanity_reason", ""))

                    chosen = recovered_sample if recovered_sample.get("found") else recovered_md
                    rec["traceback_candidate_reason"] = chosen.get("reason", "")
                    rec["traceback_candidate_snippet"] = chosen.get("snippet", "")

                    # MD 路径若未通过 sanity，仍保留“最佳噪声候选”供人工看，但不进入换算
                    if not recovered_sample.get("found") and not recovered_md.get("found"):
                        rec["traceback_candidate_value"] = norm_text(recovered_md.get("value", ""))
                        rec["traceback_candidate_unit"] = norm_text(recovered_md.get("unit", ""))
                        rec["traceback_candidate_canonical_unit"] = norm_text(recovered_md.get("canonical_unit", ""))

                    if chosen.get("found"):
                        rec["_block_numeric_conversion"] = False
                        rec["_ratio_semantics_unclear"] = False
                        rec["traceback_candidate_value"] = chosen.get("value", "")
                        rec["traceback_candidate_unit"] = chosen.get("unit", "")
                        rec["traceback_candidate_canonical_unit"] = normalize_unit(chosen.get("unit", "")).get("canonical_unit", "")
                        rec["resolved_value_for_conversion"] = chosen.get("value", "")
                        rec["resolved_unit_for_conversion"] = chosen.get("unit", "")
                        rec["traceback_recovered_pair"] = (
                            f"{norm_text(rec['resolved_value_for_conversion'])} {norm_text(rec['resolved_unit_for_conversion'])}".strip()
                        )
                        if not recovered_sample.get("found"):
                            rec["_md_pair_key"] = (
                                f"{rec['slot_name']}||{norm_text(rec['resolved_value_for_conversion'])}||{norm_text(rec['resolved_unit_for_conversion'])}"
                            )

                        rec_for_conversion = rec.copy()
                        rec_for_conversion["original_value"] = rec["resolved_value_for_conversion"]
                        rec_for_conversion["original_unit"] = rec["resolved_unit_for_conversion"]
                        recovered_unit_info = normalize_unit(rec["resolved_unit_for_conversion"])
                        rec_for_conversion["canonical_unit"] = recovered_unit_info["canonical_unit"]
                        rec_for_conversion["unit_family"] = recovered_unit_info["unit_family"]
                        rec_for_conversion["unsupported_unit_detail"] = recovered_unit_info.get("unsupported_detail", "")
                        # 修复：traceback 成功后，effective_* 与用于换算的口径一致
                        rec_for_conversion["effective_canonical_unit"] = recovered_unit_info["canonical_unit"]
                        rec_for_conversion["effective_unit_family"] = recovered_unit_info["unit_family"]
                        if rec_for_conversion["unit_family"] == "percent_ambiguous":
                            percent_ctx2 = infer_percent_type_from_context(rec_for_conversion, row, snippets)
                            rec_for_conversion["percent_type_inferred"] = percent_ctx2.get("percent_type_inferred", rec.get("percent_type_inferred", ""))

                        conv = convert_to_wtpercent(rec_for_conversion, density_info, ndigits=cfg.round_digits)
                        rec["suggested_wtpercent"] = conv["suggested_wtpercent"]
                        rec["suggested_status"] = conv["suggested_status"] if conv["suggested_status"] != "FAILED_PARSE" else "NEED_TRACEBACK"
                        rec["conversion_formula_used"] = conv.get("conversion_formula_used", "")
                        rec["effective_canonical_unit"] = rec_for_conversion["effective_canonical_unit"]
                        rec["effective_unit_family"] = rec_for_conversion["effective_unit_family"]
                        rec["unsupported_unit_detail"] = rec_for_conversion.get("unsupported_unit_detail", rec.get("unsupported_unit_detail", ""))
                        source_label = "sample/title" if recovered_sample.get("found") else "MD"
                        tb_kind = "proportion/ratio context unclear" if rec.get("_ratio_semantics_unclear") else (
                            "date-like/OCR blocked field" if rec.get("_parse_notes") and "date-like" in "; ".join(rec["_parse_notes"]).lower() else "missing structured concentration"
                        )
                        reason_parts = [
                            f"Traceback used ({source_label}); {tb_kind}. Recovered pair: {rec['traceback_recovered_pair']}.",
                            f"Missing concentration in structured field; recovered candidate from {source_label}: {rec['resolved_value_for_conversion']} {rec['resolved_unit_for_conversion']}",
                            chosen.get("reason", ""),
                            conv["suggested_reason"],
                        ]
                        if rec.get("_parse_notes"):
                            reason_parts.insert(0, "; ".join(rec["_parse_notes"]))
                        if rec.get("percent_type_reason"):
                            reason_parts.append(f"percent_context={rec['percent_type_reason']}")
                        if rec.get("split_parse_warning"):
                            reason_parts.append(f"split_warning={rec['split_parse_warning']}")
                        rec["suggested_reason"] = " | ".join([p for p in reason_parts if p])
                    else:
                        rec["suggested_wtpercent"] = None
                        rec["suggested_status"] = "NEED_TRACEBACK"
                        rec["conversion_formula_used"] = "TRACEBACK_REQUIRED"
                        rec["traceback_recovered_pair"] = ""
                        reason_parts = [
                            "Traceback failed or no acceptable candidate.",
                            f"Solute present but concentration missing or unusable in structured field ({parse_error}).",
                            recovered_sample.get("reason", ""),
                            recovered_md.get("reason", ""),
                            "Go back to MD / original text for manual confirmation.",
                        ]
                        if rec.get("_parse_notes"):
                            reason_parts.insert(0, "; ".join(rec["_parse_notes"]))
                        if rec.get("split_parse_warning"):
                            reason_parts.append(f"split_warning={rec['split_parse_warning']}")
                        rec["suggested_reason"] = " | ".join([p for p in reason_parts if p])
                else:
                    conv = convert_to_wtpercent(rec, density_info, ndigits=cfg.round_digits)
                    rec["suggested_wtpercent"] = conv["suggested_wtpercent"]
                    rec["suggested_status"] = conv["suggested_status"]
                    rec["conversion_formula_used"] = conv.get("conversion_formula_used", "")
                    reason_parts = [conv["suggested_reason"]]
                    if rec.get("_parse_notes"):
                        reason_parts.insert(0, "; ".join(rec["_parse_notes"]))
                    if rec.get("percent_type_reason"):
                        reason_parts.append(f"percent_context={rec['percent_type_reason']}")
                    if rec.get("split_parse_warning"):
                        reason_parts.append(f"split_warning={rec['split_parse_warning']}")
                    rec["suggested_reason"] = " | ".join([p for p in reason_parts if p])

                rec["manual_review_flag"] = (
                    rec["suggested_status"] in {"NEED_TRACEBACK", "FAILED_PARSE"}
                    or rec["identity_confidence"] == "low"
                    or rec["phase_confidence"] == "low"
                    or rec.get("percent_type_confidence") in {"low", ""}
                    or (rec.get("unit_family") == "percent_ambiguous" and rec.get("percent_type_inferred") in {"WT_PERCENT", "WV_PERCENT", "VOL_PERCENT"})
                    or bool(norm_text(rec.get("traceback_candidate_value", "")))
                    or bool(rec.get("identity_match_rejected"))
                    or (bool(norm_text(rec.get("traceback_candidate_value", ""))) and rec.get("traceback_candidate_sanity_passed") is False)
                    or bool(rec.get("llm_manual_review_for_unit"))
                )

                # 最后防线：identity 已拒绝仍出现极端 wt%（历史错 MW / 噪声 traceback 残留）
                if rec.get("identity_match_rejected") and rec.get("suggested_wtpercent") is not None:
                    try:
                        sw = float(rec["suggested_wtpercent"])
                    except Exception:
                        sw = None
                    if sw is not None and (sw > 10000.0 or sw < 0.0):
                        rec["suggested_wtpercent"] = None
                        rec["suggested_status"] = "CANNOT_CONVERT"
                        rec["conversion_formula_used"] = "ABSURD_WT_SUPPRESSED"
                        rec["suggested_reason"] = (
                            f"Suppressed non-physical wt% estimate ({sw}) after identity-table rejection. "
                            + norm_text(rec.get("suggested_reason", ""))
                        )
                        rec["manual_review_flag"] = True

                # 成功换算后按单位解析路径映射最终状态码；不覆盖失败类状态
                remap_success_status_by_unit_path(rec)

                # 程序侧 wt% 合理性（不 eval LLM 公式，仅校验数值）
                if rec.get("suggested_wtpercent") is not None and not wt_percent_passes_sanity(rec.get("suggested_wtpercent")):
                    rec["suggested_wtpercent"] = None
                    rec["suggested_status"] = "NEED_MANUAL_REVIEW"
                    rec["conversion_formula_used"] = "WT_PERCENT_SANITY_FAIL"
                    rec["manual_review_flag"] = True

                # 规则+LLM 仍无法识别单位：由 CANNOT_CONVERT 提升为需人工（保留原始单位可追溯）
                if (
                    rec.get("unit_family") == "unsupported"
                    and norm_text(rec.get("original_unit"))
                    and rec.get("llm_unit_aux_attempted")
                    and rec.get("suggested_status") == "CANNOT_CONVERT"
                    and rec.get("conversion_formula_used") == "UNSUPPORTED_UNIT"
                ):
                    rec["suggested_status"] = "NEED_MANUAL_REVIEW"

                rec["final_value_wt_percent"] = rec["suggested_wtpercent"]
                rec["final_value"] = rec["suggested_wtpercent"]
                rec["final_unit"] = "wt%" if rec["suggested_wtpercent"] is not None else ""
                rec["final_status"] = rec["suggested_status"]
                rec["final_reason"] = rec["suggested_reason"]

                review_rows.append(rec)

    review_df = pd.DataFrame(review_rows)
    annotate_duplicate_md_recoveries(review_df)

    desired_cols = [
        "row_id", "slot_name", "item_index",
        "original_solute", "standardized_solute", "IUPAC_Name", "formula", "CID",
        "identity_source", "identity_confidence", "identity_reason",
        "identity_match_rejected", "identity_trust_for_mw",
        "original_value", "original_unit", "canonical_unit", "unit_family",
        "original_canonical_unit", "original_unit_family",
        "effective_canonical_unit", "effective_unit_family",
        "unsupported_unit_detail",
        "raw_unit", "cleaned_unit",
        "canonical_unit_before_llm", "canonical_unit_after_llm",
        "unit_family_before_llm", "unit_family_after_llm",
        "llm_unit_fix_used", "llm_formula_used", "llm_formula_used_text",
        "llm_unit_aux_path", "llm_unit_aux_attempted",
        "llm_required_parameters", "llm_confidence", "llm_reason", "llm_safe_to_convert",
        "conversion_parameters_found", "conversion_parameters_missing", "conversion_parameters_assumed",
        "final_conversion_formula", "final_value_wt_percent",
        "llm_manual_review_for_unit",
        "split_parse_error", "split_parse_warning", "alignment_confidence",
        "resolved_value_for_conversion", "resolved_unit_for_conversion",
        "traceback_candidate_value", "traceback_candidate_unit", "traceback_candidate_canonical_unit",
        "traceback_candidate_count", "traceback_candidate_reason", "traceback_candidate_snippet",
        "traceback_candidate_sanity_passed", "traceback_candidate_sanity_reason",
        "phase", "phase_confidence", "phase_reason",
        "solvent_identified", "physical_form_inferred", "percent_type_inferred", "percent_type_confidence", "percent_type_reason",
        "MW", "MW_source", "rho_solution", "rho_solute",
        "density_source", "density_assumed", "density_reason",
        "judgement_source", "judgement_reason", "need_traceback_flag", "traceback_snippets",
        "suggested_wtpercent", "suggested_status", "conversion_formula_used", "suggested_reason",
        "manual_review_flag",
        "traceback_recovered_pair",
        "final_value", "final_unit", "final_status", "final_reason",
        "MW_from_table",
    ]
    cols = [c for c in desired_cols if c in review_df.columns] + [c for c in review_df.columns if c not in desired_cols]
    return review_df[cols]


# =========================================================
# 15) 交付主表构建
# =========================================================

def join_series_in_order(values: List[Any]) -> str:
    vals = [format_float_for_join(v) for v in values if norm_text(v) != ""]
    return ";".join([v for v in vals if v])



def join_texts_in_order(values: List[Any]) -> str:
    vals = [norm_text(v) for v in values if norm_text(v)]
    return ";".join(vals)


def _item_has_successful_wt(r: Any) -> bool:
    """子项是否换算成功（有 final_value 且 final_status 属于成功族）。"""
    v = r.get("final_value") if hasattr(r, "get") else getattr(r, "final_value", None)
    if v is None:
        return False
    try:
        if pd.isna(v):
            return False
    except Exception:
        pass
    st = norm_text(r.get("final_status", "") if hasattr(r, "get") else "")
    return st in SUCCESS_STATUS


def _item_soft_quality_review(r: Any) -> bool:
    """成功子项中偏近似/LLM 路径的结果，主表汇总标为 REVIEW。"""
    if not _item_has_successful_wt(r):
        return False
    st = norm_text(r.get("final_status", ""))
    # RULE_NORMALIZED_CONVERTED 仍视为日常可接受的成功；REVIEW 留给近似/LLM 等路径
    if st in {
        "ASSUMED_CONVERTED",
        "LLM_UNIT_REPAIRED_CONVERTED",
        "LLM_FORMULA_CONVERTED",
    }:
        return True
    if "ASSUMED" in st and "CONVERTED" in st:
        return True
    return False


def _compute_slot_delivery_summary(grp: pd.DataFrame) -> Dict[str, Any]:
    """
    主表每个 slot×row 汇总：
    - 只拼接成功项的 wt% 数值，无空分号占位
    - 简化状态优先级（注释即规范）：
      1) total_count==0 → 全空
      2) success_count==0 → FAIL
      3) 任一子项 manual_review_flag → REVIEW
      4) success_count < total_count → PARTIAL
      5) 全部成功但存在 ASSUMED/LLM 等 soft_quality → REVIEW
      6) 否则 OK
    """
    grp = grp.sort_values("item_index")
    total = int(len(grp))
    success_vals: List[str] = []
    failed_indices: List[str] = []
    any_manual = False

    for _, r in grp.iterrows():
        try:
            ix = int(r.get("item_index", 0) or 0)
        except Exception:
            ix = 0
        if bool(r.get("manual_review_flag", False)):
            any_manual = True
        if _item_has_successful_wt(r):
            success_vals.append(format_float_for_join(r.get("final_value"), ndigits=6))
        else:
            failed_indices.append(str(ix))

    sc = len(success_vals)
    wt_joined = ";".join([x for x in success_vals if norm_text(x)])
    failed_joined = ";".join(failed_indices) if failed_indices else ""

    if total == 0:
        return {
            "wt": "",
            "simple": "",
            "success_count": 0,
            "total_count": 0,
            "failed_index": "",
            "manual_review": False,
        }

    if sc == 0:
        simple = "FAIL"
    elif any_manual:
        simple = "REVIEW"
    elif sc < total:
        simple = "PARTIAL"
    elif any(_item_soft_quality_review(r) for _, r in grp.iterrows()):
        simple = "REVIEW"
    else:
        simple = "OK"

    # manual_review：子项人工复核标记，或汇总状态为 REVIEW（含近似/LLM 等软质量路径）
    slot_mr = bool(any_manual) or (simple == "REVIEW")

    return {
        "wt": wt_joined,
        "simple": simple,
        "success_count": sc,
        "total_count": total,
        "failed_index": failed_joined,
        "manual_review": slot_mr,
    }


def build_delivery_main(main_df: pd.DataFrame, review_df: pd.DataFrame) -> pd.DataFrame:
    """
    交付主表：在原始列基础上**新增**规范化列，不覆盖 value_col / unit_col 等原始浓度列。
    仅写入成功换算的 wt% 数值（分号分隔），无空段位；逐项详细状态在 review_audit。
    """
    out = main_df.copy()

    if "__row_id__" not in out.columns:
        out["__row_id__"] = [get_row_id(r, i) for i, (_, r) in enumerate(out.iterrows())]

    for slot_spec in SLOT_SPECS:
        base = slot_spec.output_value_col
        col_wt = base
        col_simple = f"{base}_status_simple"
        col_sc = f"{base}_success_count"
        col_tc = f"{base}_total_count"
        col_fail = f"{base}_failed_index"
        col_mrf = f"{base}_manual_review"

        out[col_wt] = ""
        out[col_simple] = ""
        out[col_sc] = 0
        out[col_tc] = 0
        out[col_fail] = ""
        out[col_mrf] = False

        slot_df = review_df[review_df["slot_name"] == slot_spec.slot_name].copy()
        if slot_df.empty:
            continue

        slot_df = slot_df.sort_values(["row_id", "item_index"])

        for rid, grp in slot_df.groupby("row_id", sort=False):
            sm = _compute_slot_delivery_summary(grp)
            m = out["__row_id__"] == rid
            out.loc[m, col_wt] = sm["wt"]
            out.loc[m, col_simple] = sm["simple"]
            out.loc[m, col_sc] = sm["success_count"]
            out.loc[m, col_tc] = sm["total_count"]
            out.loc[m, col_fail] = sm["failed_index"]
            out.loc[m, col_mrf] = sm["manual_review"]

    return out.drop(columns=["__row_id__"])


# =========================================================
# 15b) review 拆分为 core / audit
# =========================================================

REVIEW_CORE_COLUMNS: List[str] = [
    "row_id",
    "slot_name",
    "item_index",
    "original_solute",
    "standardized_solute",
    "original_value",
    "original_unit",
    "effective_canonical_unit",
    "effective_unit_family",
    "MW",
    "rho_solution",
    "rho_solute",
    "final_value",
    "final_unit",
    "final_status",
    "manual_review_flag",
    "traceback_recovered_pair",
    "final_reason",
]


def split_review_core_audit(review_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """日常筛查用 core + 底稿 audit；audit 仍带 row_id/slot_name/item_index 便于与 core 对齐。"""
    core_cols = [c for c in REVIEW_CORE_COLUMNS if c in review_df.columns]
    join_keys = [c for c in ("row_id", "slot_name", "item_index") if c in review_df.columns]
    rest = [c for c in review_df.columns if c not in core_cols]
    seen: set = set()
    audit_cols: List[str] = []
    for c in join_keys + rest:
        if c not in seen:
            seen.add(c)
            audit_cols.append(c)
    core_df = review_df[core_cols].copy() if core_cols else pd.DataFrame()
    audit_df = review_df[audit_cols].copy() if audit_cols else pd.DataFrame()
    return core_df, audit_df


# =========================================================
# 16) summary / unit_catalog
# =========================================================

def build_summary(review_df: pd.DataFrame) -> pd.DataFrame:
    rows = []

    total = len(review_df)
    converted = review_df["final_status"].isin(SUCCESS_STATUS).sum() if total else 0
    coverage = converted / total if total else 0.0

    rows.append({"section": "overall", "metric": "total_items", "value": total})
    rows.append({"section": "overall", "metric": "converted_items", "value": converted})
    rows.append({"section": "overall", "metric": "conversion_coverage", "value": coverage})

    status_counts = review_df["final_status"].fillna("MISSING").value_counts(dropna=False)
    for status, cnt in status_counts.items():
        rows.append({"section": "status_count", "metric": status, "value": int(cnt)})

    family_counts = review_df["unit_family"].fillna("MISSING").value_counts(dropna=False)
    for fam, cnt in family_counts.items():
        rows.append({"section": "unit_family_count", "metric": fam, "value": int(cnt)})

    if "effective_unit_family" in review_df.columns:
        eff_counts = review_df["effective_unit_family"].fillna("MISSING").value_counts(dropna=False)
        for fam, cnt in eff_counts.items():
            rows.append({"section": "effective_unit_family_count", "metric": fam, "value": int(cnt)})

    manual_review = int(review_df["manual_review_flag"].fillna(False).sum()) if "manual_review_flag" in review_df.columns else 0
    rows.append({"section": "overall", "metric": "manual_review_flag_count", "value": manual_review})

    return pd.DataFrame(rows)



def build_unit_catalog(unit_df: Optional[pd.DataFrame], review_df: pd.DataFrame, main_df: Optional[pd.DataFrame] = None) -> pd.DataFrame:
    counts: Dict[str, int] = {}

    # 1) 可选外部单位表：兼容长表 / 宽表
    if unit_df is not None and not unit_df.empty:
        unit_col = find_column(unit_df, ["unit", "单位", "original_unit"])
        count_col = find_column(unit_df, ["count", "counts", "频次", "次数", "数量"])
        if unit_col is not None:
            for _, r in unit_df.iterrows():
                unit_val = norm_text(r.get(unit_col, ""))
                if not unit_val:
                    continue
                cnt = parse_float_maybe(r.get(count_col)) if count_col else None
                parts = split_cell_multi(unit_val)
                if not parts:
                    parts = [unit_val]
                for part in parts:
                    counts[part] = counts.get(part, 0) + int(cnt if cnt is not None else 1)
        else:
            for col in unit_df.columns:
                for cell in unit_df[col].tolist():
                    for part in split_cell_multi(cell):
                        if norm_text(part):
                            counts[part] = counts.get(part, 0) + 1

    # 2) 主表 5 个单位列
    if main_df is not None and not main_df.empty:
        for slot in SLOT_SPECS:
            if slot.unit_col in main_df.columns:
                for cell in main_df[slot.unit_col].tolist():
                    for part in split_cell_multi(cell):
                        if norm_text(part):
                            counts[part] = counts.get(part, 0) + 1

    # 3) review 表（原始单位 + 与换算一致的有效单位，避免 traceback 后统计口径分裂）
    if review_df is not None and not review_df.empty and "original_unit" in review_df.columns:
        for unit in review_df["original_unit"].fillna("").astype(str):
            for part in split_cell_multi(unit):
                if norm_text(part):
                    counts[part] = counts.get(part, 0) + 1
        if "resolved_unit_for_conversion" in review_df.columns:
            for unit in review_df["resolved_unit_for_conversion"].fillna("").astype(str):
                for part in split_cell_multi(unit):
                    if norm_text(part):
                        counts[part] = counts.get(part, 0) + 1

    temp = pd.DataFrame([
        {"original_unit": k, "count": v}
        for k, v in sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))
    ])

    if temp.empty:
        return pd.DataFrame(columns=["original_unit", "count", "canonical_unit", "unit_family"])

    normalized = temp["original_unit"].apply(normalize_unit)
    temp["canonical_unit"] = normalized.apply(lambda x: x["canonical_unit"])
    temp["unit_family"] = normalized.apply(lambda x: x["unit_family"])
    return temp


# =========================================================
# 17) 写出 Excel
# =========================================================

def _sanitize_cell_for_excel(val: Any, max_cell_chars: int) -> Any:
    """
    Excel / openpyxl 稳健写入：
    - 去除 XML 非法控制字符（IllegalCharacterError）
    - 超长文本截断（单格约 32767）
    - datetime / 数值 / 布尔保持可写标量；其他类型转 JSON 或 str
    """
    if val is None:
        return None

    try:
        if isinstance(val, float) and (math.isnan(val) or math.isinf(val)):
            if math.isinf(val):
                return str(val)
    except Exception:
        pass

    try:
        if pd.isna(val):
            return val
    except (TypeError, ValueError):
        pass

    if isinstance(val, bool):
        return val

    if isinstance(val, pd.Timestamp):
        return val

    if isinstance(val, (dt.datetime, dt.date, dt.time)):
        return val

    if isinstance(val, (bytes, bytearray)):
        try:
            val = val.decode("utf-8", errors="replace")
        except Exception:
            val = str(val)

    if isinstance(val, numbers.Integral) and not isinstance(val, bool):
        try:
            return int(val)
        except Exception:
            pass

    if isinstance(val, numbers.Real) and not isinstance(val, bool):
        try:
            x = float(val)
            if math.isinf(x):
                return str(x)
            return x
        except Exception:
            pass

    if isinstance(val, str):
        s = ILLEGAL_CHARACTERS_RE.sub("", val)
        if len(s) > max_cell_chars:
            s = s[: max(0, max_cell_chars - 48)] + "\n...[truncated for Excel]"
        return s

    if isinstance(val, (dict, list, tuple)) or isinstance(val, set):
        try:
            s = json.dumps(val, ensure_ascii=False, default=str)
        except Exception:
            s = str(val)
        s = ILLEGAL_CHARACTERS_RE.sub("", s)
        if len(s) > max_cell_chars:
            s = s[: max(0, max_cell_chars - 48)] + "\n...[truncated for Excel]"
        return s

    try:
        s = str(val)
    except Exception:
        s = "<unrepresentable>"
    s = ILLEGAL_CHARACTERS_RE.sub("", s)
    if len(s) > max_cell_chars:
        s = s[: max(0, max_cell_chars - 48)] + "\n...[truncated for Excel]"
    return s


def sanitize_dataframe_for_excel(df: pd.DataFrame, max_cell_chars: int = 32700) -> pd.DataFrame:
    if df.empty:
        return df

    def _clean(x: Any) -> Any:
        return _sanitize_cell_for_excel(x, max_cell_chars)

    try:
        return df.map(_clean)
    except AttributeError:
        return df.applymap(_clean)


def _excel_grid_row_warnings(df: pd.DataFrame, sheet: str) -> List[str]:
    """Excel 2007+ 行上限约 1048576，列上限 16384（XFD）。"""
    out: List[str] = []
    n, m = len(df), len(df.columns)
    if n > 1_048_570:
        out.append(f"{sheet}: rows={n} may exceed Excel row limit (~1048576)")
    if m > 16_380:
        out.append(f"{sheet}: cols={m} may exceed Excel column limit (~16384)")
    return out


def sample_excel_write_risks(
    frames: List[Tuple[str, pd.DataFrame]],
    max_cell_chars: int,
    max_samples: int = 150,
) -> List[Dict[str, Any]]:
    """对原始 DataFrame 抽样：超长串 / 非法控制符（清洗前），便于诊断。"""
    samples: List[Dict[str, Any]] = []
    for sheet_name, df in frames:
        if df is None or df.empty:
            continue
        scan_rows = min(len(df), 80_000)
        for col in df.columns:
            if len(samples) >= max_samples:
                return samples
            try:
                ser = df[col].iloc[:scan_rows]
            except Exception:
                continue
            for pos, cell in ser.items():
                if len(samples) >= max_samples:
                    return samples
                if not isinstance(cell, str):
                    continue
                if ILLEGAL_CHARACTERS_RE.search(cell):
                    samples.append(
                        {
                            "sheet": sheet_name,
                            "column": str(col),
                            "row_label": str(pos),
                            "issue": "illegal_control_char",
                            "prefix": cell[:240],
                        }
                    )
                elif len(cell) > max_cell_chars:
                    samples.append(
                        {
                            "sheet": sheet_name,
                            "column": str(col),
                            "row_label": str(pos),
                            "issue": "string_too_long",
                            "length": len(cell),
                        }
                    )
    return samples


def write_excel_failure_diagnostic(
    exc: BaseException,
    cfg: AppConfig,
    *,
    last_sheet_attempted: Optional[str] = None,
    frames_for_audit: Optional[List[Tuple[str, pd.DataFrame]]] = None,
) -> None:
    path = Path(cfg.excel_failure_diag_json)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload: Dict[str, Any] = {
        "error_type": type(exc).__name__,
        "error_message": str(exc),
        "traceback": traceback.format_exc(),
        "last_sheet_attempted": last_sheet_attempted,
        "review_checkpoint_path": str(cfg.review_checkpoint_path),
        "output_excel": str(cfg.output_excel),
        "json_caches": {
            "pubchem": cfg.pubchem_cache_json,
            "llm_judgements": cfg.llm_cache_json,
            "llm_unit_aux": cfg.llm_unit_aux_cache_json,
        },
    }
    gw: List[str] = []
    if frames_for_audit:
        for name, df in frames_for_audit:
            gw.extend(_excel_grid_row_warnings(df, name))
        payload["risk_cell_samples"] = sample_excel_write_risks(
            frames_for_audit, cfg.excel_max_cell_chars, max_samples=160
        )
    payload["grid_warnings"] = gw
    try:
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f">>> Wrote Excel failure diagnostic: {path}")
    except Exception as e2:
        warnings.warn(f"无法写入 Excel 诊断文件 {path}: {e2}", UserWarning, stacklevel=2)


def write_workbook(
    delivery_main_df: pd.DataFrame,
    review_core_df: pd.DataFrame,
    review_audit_df: pd.DataFrame,
    summary_df: pd.DataFrame,
    unit_catalog_df: pd.DataFrame,
    cfg: AppConfig,
) -> None:
    out_path = Path(cfg.output_excel)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    mx = int(cfg.excel_max_cell_chars)

    raw_audit_frames: List[Tuple[str, pd.DataFrame]] = [
        ("delivery_main", delivery_main_df),
        ("concentration_review_core", review_core_df),
        ("concentration_review_audit", review_audit_df),
        ("summary", summary_df),
        ("unit_catalog", unit_catalog_df),
    ]

    dm = sanitize_dataframe_for_excel(delivery_main_df, mx)
    rc = sanitize_dataframe_for_excel(review_core_df, mx)
    ra = sanitize_dataframe_for_excel(review_audit_df, mx)
    sm = sanitize_dataframe_for_excel(summary_df, mx)
    uc = sanitize_dataframe_for_excel(unit_catalog_df, mx)

    sheets: List[Tuple[str, pd.DataFrame]] = [
        ("delivery_main", dm),
        ("concentration_review_core", rc),
        ("concentration_review_audit", ra),
        ("summary", sm),
        ("unit_catalog", uc),
    ]

    last_sheet: Optional[str] = None
    try:
        with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
            for sheet_name, sdf in sheets:
                last_sheet = sheet_name
                sdf.to_excel(writer, sheet_name=sheet_name, index=False)
    except Exception as exc:
        write_excel_failure_diagnostic(
            exc,
            cfg,
            last_sheet_attempted=last_sheet,
            frames_for_audit=raw_audit_frames,
        )
        raise RuntimeError(
            f"Excel 写出失败（sheet={last_sheet!r}）: {exc}。已写入诊断: {cfg.excel_failure_diag_json}"
        ) from exc


# =========================================================
# 18) 主程序
# =========================================================

def warn_if_missing_llm_api_key(cfg: AppConfig) -> None:
    """未配置 DEEPSEEK_API_KEY 时给出明确警告，避免误以为已启用 LLM。"""
    if norm_text(cfg.llm_api_key):
        return
    if cfg.llm_enabled:
        warnings.warn(
            "[LLM] 环境变量 DEEPSEEK_API_KEY 未设置或为空，但 llm_enabled=True："
            "无法调用 DeepSeek API，将始终回退为启发式规则。请先配置 API 密钥。",
            UserWarning,
            stacklevel=2,
        )
    else:
        warnings.warn(
            "[LLM] 环境变量 DEEPSEEK_API_KEY 未设置或为空："
            "若将 llm_enabled 设为 True，需先在系统中设置 DEEPSEEK_API_KEY。",
            UserWarning,
            stacklevel=2,
        )


def main() -> None:
    warn_if_missing_llm_api_key(CFG)
    print(">>> Reading inputs...")
    main_df, unit_df, identity_df = read_inputs(CFG)

    print(">>> Building identity lookup...")
    identity_lookup = build_identity_lookup(identity_df)

    ckpt = Path(CFG.review_checkpoint_path)

    if CFG.resume_from_review_checkpoint and ckpt.is_file():
        print(f">>> Resuming from review checkpoint: {ckpt}")
        review_df = load_review_checkpoint(str(ckpt))
        print(">>> Loading JSON caches (for save step)...")
        pubchem_cache = load_json_cache(CFG.pubchem_cache_json)
        llm_cache = load_json_cache(CFG.llm_cache_json)
        llm_unit_aux_cache = load_json_cache(CFG.llm_unit_aux_cache_json)
    else:
        if CFG.resume_from_review_checkpoint and not ckpt.is_file():
            warnings.warn(
                f"resume_from_review_checkpoint=True 但找不到 {ckpt}，将重新执行 build_review_sheet。",
                UserWarning,
                stacklevel=2,
            )

        print(">>> Loading caches...")
        pubchem_cache = load_json_cache(CFG.pubchem_cache_json)
        llm_cache = load_json_cache(CFG.llm_cache_json)
        llm_unit_aux_cache = load_json_cache(CFG.llm_unit_aux_cache_json)

        print(">>> Building concentration_review...")
        review_df = build_review_sheet(
            main_df=main_df,
            identity_lookup=identity_lookup,
            cfg=CFG,
            pubchem_cache=pubchem_cache,
            llm_cache=llm_cache,
            llm_unit_aux_cache=llm_unit_aux_cache,
        )

        if CFG.save_review_checkpoint:
            save_review_checkpoint(str(ckpt), review_df)
            print(f">>> Saved review checkpoint: {ckpt}")

        print(">>> Flushing JSON caches after review (early persist)...")
        flush_json_caches(CFG, pubchem_cache, llm_cache, llm_unit_aux_cache)

    print(">>> Building delivery_main...")
    delivery_main_df = build_delivery_main(main_df, review_df)

    print(">>> Splitting concentration_review into core / audit...")
    review_core_df, review_audit_df = split_review_core_audit(review_df)

    print(">>> Building summary and unit_catalog...")
    summary_df = build_summary(review_df)
    unit_catalog_df = build_unit_catalog(unit_df, review_df, main_df=main_df)

    print(">>> Saving caches (final flush before Excel)...")
    flush_json_caches(CFG, pubchem_cache, llm_cache, llm_unit_aux_cache)

    print(">>> Writing workbook...")
    try:
        write_workbook(
            delivery_main_df=delivery_main_df,
            review_core_df=review_core_df,
            review_audit_df=review_audit_df,
            summary_df=summary_df,
            unit_catalog_df=unit_catalog_df,
            cfg=CFG,
        )
    except Exception as e:
        print(
            f">>> [FATAL] Excel write failed: {e}\n"
            f">>> Review checkpoint: {CFG.review_checkpoint_path} (resume_from_review_checkpoint=True to skip review)\n"
            f">>> JSON caches should already be on disk.\n"
            f">>> See diagnostic: {CFG.excel_failure_diag_json}"
        )
        raise

    print(f">>> Done. Output written to: {CFG.output_excel}")


if __name__ == "__main__":
    main()
