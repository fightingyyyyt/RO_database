# -*- coding: utf-8 -*-
"""
concentration_cleaner_balanced.py

目标：
- 清洗膜材料数据库中的 5 类浓度字段
- 尽可能统一换算为 wt%
- 但绝不为了整齐而做物理上不可靠的硬转换
- 输出 delivery_main / concentration_review / summary / unit_catalog

依赖：
    pip install pandas openpyxl requests

说明：
1) LLM 使用 OpenAI-compatible /chat/completions 接口；
   DeepSeek 可直接用；
   Claude 若经兼容网关或中转层，也可复用同样结构。
2) PubChem 只用于查询 Molecular Weight，不作为通用溶液密度来源。
3) 程序做数学换算，LLM 只做语义判断。
"""

from __future__ import annotations

import os
import re
import json
import math
import time
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote

import pandas as pd
import requests


# =========================================================
# 0) 固定配置区：改这里即可
# =========================================================

@dataclass
class AppConfig:
    # ---------- 输入 ----------
    input_main_excel: str = "./test3/test1.xlsx"
    input_main_sheet: Optional[str] = None  # None=默认第一个sheet

    unit_catalog_excel: Optional[str] = "./test3/统计单位.xlsx"  # 可选
    unit_catalog_sheet: Optional[str] = None

    identity_table_excel: str = "./test3/task1.xlsx"
    identity_table_sheet: Optional[str] = None

    md_zip_path: Optional[str] = "./test3/反渗透膜_output.zip"
    md_extract_dir: str = "./md_extracted"
    use_traceback: bool = True
    max_traceback_snippets: int = 3
    traceback_window_chars: int = 260

    # ---------- 输出 ----------
    output_excel: str = "./test3/test1_output.xlsx"

    # ---------- 缓存 ----------
    pubchem_cache_json: str = "./cache_pubchem_mw.json"
    llm_cache_json: str = "./cache_llm_judgements.json"

    # ---------- 功能开关 ----------
    pubchem_enabled: bool = True
    llm_enabled: bool = False

    # ---------- API ----------
    llm_base_url: str = "https://api.deepseek.com/v1"
    llm_api_key: str = os.getenv("DEEPSEEK_API_KEY", "")
    llm_model: str = "deepseek-chat"
    timeout_seconds: int = 30

    # ---------- 运行策略 ----------
    sleep_between_pubchem_calls: float = 0.15
    round_digits: int = 6


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
    output_status_col: str


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
        solute_col="测试NaCl浓度",
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

# 常见溶剂密度（g/mL），用于“近似把溶液密度视为溶剂密度”
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

# 溶质纯物质密度（只用于 v/v% 等情况；越保守越好）
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
}

# 常见液体/固体提示，用于 heuristic fallback
COMMON_LIQUIDS = {
    "triethylamine", "piperidine", "aniline", "ethanol", "methanol", "isopropanol",
    "glycerol", "nmp", "dmf", "dmac", "toluene", "hexane", "cyclohexane"
}
COMMON_SOLIDS = {
    "sodium chloride", "nacl", "m-phenylenediamine", "mpd", "piperazine",
    "trimesoyl chloride", "tmc", "camphorsulfonic acid", "sodium hydroxide",
    "sodium carbonate", "sodium dodecyl sulfate", "sds", "potassium persulfate"
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
    "dmac": ["dmac", "dimethylacetamide", "dmac", "二甲基乙酰胺"],
    "nmp": ["nmp", "n-methyl-2-pyrrolidone", "甲基吡咯烷酮"],
    "chloroform": ["chloroform", "三氯甲烷"],
    "dichloromethane": ["dichloromethane", "methylene chloride", "二氯甲烷"],
}

PERCENT_TYPE_BY_FAMILY = {
    "mass_fraction": "WT_PERCENT",
    "mass_volume_percent": "WV_PERCENT",
    "volume_fraction": "VOL_PERCENT",
}

SUCCESS_STATUS = {"DIRECT_WT", "SAFE_CONVERTED", "ASSUMED_CONVERTED"}


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


def parse_float_maybe(x: Any) -> Optional[float]:
    s = norm_text(x)
    if not s:
        return None

    s = s.replace(",", "")
    s = s.replace("×10^", "e")
    s = s.replace("x10^", "e")
    s = s.replace("X10^", "e")

    # 避免把区间值误当成单值
    if re.search(r"(?<![eE])[~～至到]|(?<![eE])\s*[-–—]\s*", s):
        nums = re.findall(r"[-+]?\d*\.?\d+(?:[eE][-+]?\d+)?", s)
        if len(nums) > 1:
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
    # 再做宽松匹配
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

def read_inputs(cfg: AppConfig) -> Tuple[pd.DataFrame, Optional[pd.DataFrame], pd.DataFrame]:
    # 主表
    if cfg.input_main_sheet is None:
        main_df = pd.read_excel(cfg.input_main_excel)
    else:
        main_df = pd.read_excel(cfg.input_main_excel, sheet_name=cfg.input_main_sheet)

    # 单位统计表（可选）
    unit_df = None
    if cfg.unit_catalog_excel and Path(cfg.unit_catalog_excel).exists():
        if cfg.unit_catalog_sheet is None:
            unit_df = pd.read_excel(cfg.unit_catalog_excel)
        else:
            unit_df = pd.read_excel(cfg.unit_catalog_excel, sheet_name=cfg.unit_catalog_sheet)

    # 身份映射表
    if cfg.identity_table_sheet is None:
        identity_df = pd.read_excel(cfg.identity_table_excel)
    else:
        identity_df = pd.read_excel(cfg.identity_table_excel, sheet_name=cfg.identity_table_sheet)

    return main_df, unit_df, identity_df


# =========================================================
# 5) 单位归一化
# =========================================================

def normalize_unit(raw_unit: Any) -> Dict[str, str]:
    """
    返回：
    {
        "original_unit": ...,
        "canonical_unit": ...,
        "unit_family": ...
    }
    """
    raw = norm_text(raw_unit)
    if not raw:
        return {
            "original_unit": "",
            "canonical_unit": "",
            "unit_family": "unsupported",
        }

    s = raw.lower()
    s_nospace = re.sub(r"\s+", "", s)
    s_nospace = s_nospace.replace("％", "%").replace("μ", "u").replace("µ", "u")

    # ---------- 裸 % ----------
    if s_nospace in {"%", "percent"}:
        return {
            "original_unit": raw,
            "canonical_unit": "%",
            "unit_family": "percent_ambiguous",
        }

    # ---------- mass_fraction ----------
    mass_fraction_patterns = {
        "wt%", "wt.%", "weight%", "mass%", "wtpercent", "masspercent",
        "w/w%", "%w/w", "m/m%", "%m/m", "%bymass", "%bymass", "wtpercent.", "weightpercent"
    }
    if s_nospace in mass_fraction_patterns or "w/w%" in s_nospace or "m/m%" in s_nospace:
        return {
            "original_unit": raw,
            "canonical_unit": "wt%",
            "unit_family": "mass_fraction",
        }
    if "bymass" in s_nospace:
        return {
            "original_unit": raw,
            "canonical_unit": "wt%",
            "unit_family": "mass_fraction",
        }

    # ---------- mass_volume_percent ----------
    if s_nospace in {"w/v%", "%w/v", "wt/v%", "%wt/v", "g/100ml", "g/100milliliter", "g/100milliliters", "g/dl"}:
        return {
            "original_unit": raw,
            "canonical_unit": "w/v%" if "w/v" in s_nospace or "wt/v" in s_nospace else "g/100mL",
            "unit_family": "mass_volume_percent",
        }

    # ---------- mass_concentration ----------
    mass_conc_map = {
        "g/l": "g/L",
        "gl-1": "g/L",
        "mg/l": "mg/L",
        "mgl-1": "mg/L",
        "ug/l": "ug/L",
        "ugl-1": "ug/L",
        "ng/l": "ng/L",
        "ngl-1": "ng/L",
        "mg/ml": "mg/mL",
        "mgml-1": "mg/mL",
    }
    if s_nospace in mass_conc_map:
        return {
            "original_unit": raw,
            "canonical_unit": mass_conc_map[s_nospace],
            "unit_family": "mass_concentration",
        }

    # ---------- molarity ----------
    raw_strip = raw.strip()
    if raw_strip == "M":
        return {"original_unit": raw, "canonical_unit": "M", "unit_family": "molarity"}
    if raw_strip == "mM":
        return {"original_unit": raw, "canonical_unit": "mM", "unit_family": "molarity"}

    molarity_map = {
        "mol/l": "mol/L",
        "moll-1": "mol/L",
        "mmol/l": "mmol/L",
        "mmoll-1": "mmol/L",
    }
    if s_nospace in molarity_map:
        return {
            "original_unit": raw,
            "canonical_unit": molarity_map[s_nospace],
            "unit_family": "molarity",
        }

    # ---------- volume_fraction ----------
    volume_fraction_map = {
        "v/v%": "v/v%",
        "%v/v": "v/v%",
        "vol%": "vol%",
        "vol.%": "vol%",
        "ml/l": "mL/L",
    }
    if s_nospace in volume_fraction_map:
        return {
            "original_unit": raw,
            "canonical_unit": volume_fraction_map[s_nospace],
            "unit_family": "volume_fraction",
        }

    # ---------- ppm_family ----------
    ppm_map = {
        "ppm": "ppm",
        "ppb": "ppb",
        "g/kg": "g/kg",
        "mg/kg": "mg/kg",
    }
    if s_nospace in ppm_map:
        return {
            "original_unit": raw,
            "canonical_unit": ppm_map[s_nospace],
            "unit_family": "ppm_family",
        }

    # ---------- unsupported ----------
    if re.fullmatch(r"\d+\s*:\s*\d+", raw_strip):
        return {
            "original_unit": raw,
            "canonical_unit": raw,
            "unit_family": "unsupported",
        }

    unsupported_keywords = {"drops", "drop", "g", "ml", "mmol", "mol", "l", "ul"}
    if s_nospace in unsupported_keywords:
        return {
            "original_unit": raw,
            "canonical_unit": raw,
            "unit_family": "unsupported",
        }

    return {
        "original_unit": raw,
        "canonical_unit": raw,
        "unit_family": "unsupported",
    }


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
    """
    支持像：
      value='0.1 M', unit=''
      value='2 wt%', unit=''
    """
    value_str = norm_text(value_str)
    unit_str = norm_text(unit_str)
    if unit_str:
        return value_str, unit_str

    m = re.match(r"^\s*([-+]?\d*\.?\d+(?:[eE][-+]?\d+)?)\s*(.+?)\s*$", value_str)
    if m:
        num_part = m.group(1)
        unit_part = m.group(2)
        # unit_part 至少得像单位
        if re.search(r"[A-Za-z%/]", unit_part):
            return num_part, unit_part.strip()

    return value_str, unit_str


def split_slot_items(row: pd.Series, slot_spec: SlotSpec) -> List[Dict[str, Any]]:
    """
    把一个 slot 拆成若干 item。
    若长度不一致且无法广播，则返回一个 FAILED_PARSE 占位项。
    """
    solute_raw = row.get(slot_spec.solute_col, "")
    value_raw = row.get(slot_spec.value_col, "")
    unit_raw = row.get(slot_spec.unit_col, "")

    solutes = split_cell_multi(solute_raw)
    values = split_cell_multi(value_raw)
    units = split_cell_multi(unit_raw)

    # 全空 -> 无子项
    if not solutes and not values and not units:
        return []

    lengths = [len(x) for x in [solutes, values, units] if len(x) > 0]
    n = max(lengths) if lengths else 1

    def expand(lst: List[str]) -> Optional[List[str]]:
        if len(lst) == 0:
            return [""] * n
        if len(lst) == 1 and n > 1:
            return lst * n
        if len(lst) == n:
            return lst
        return None

    solutes_e = expand(solutes)
    values_e = expand(values)
    units_e = expand(units)

    if solutes_e is None or values_e is None or units_e is None:
        return [{
            "item_index": 1,
            "original_solute": norm_text(solute_raw),
            "original_value": norm_text(value_raw),
            "original_unit": norm_text(unit_raw),
            "split_parse_error": "length_mismatch_cannot_broadcast",
        }]

    out = []
    for i in range(n):
        v, u = split_inline_value_and_unit(values_e[i], units_e[i])
        out.append({
            "item_index": i + 1,
            "original_solute": norm_text(solutes_e[i]),
            "original_value": norm_text(v),
            "original_unit": norm_text(u),
            "split_parse_error": "",
        })
    return out


# =========================================================
# 7) 身份表构建与溶质匹配
# =========================================================

def build_identity_lookup(identity_df: pd.DataFrame) -> Dict[str, Dict[str, Any]]:
    col_original = find_column(identity_df, ["Original_Name", "original_name", "name", "raw_name"])
    col_formula = find_column(identity_df, ["Molecular_Formula", "molecular_formula", "formula"])
    col_iupac = find_column(identity_df, ["IUPAC_Name", "iupac_name", "IUPAC name"])
    col_cid = find_column(identity_df, ["CID", "cid"])
    col_std = find_column(identity_df, ["standardized_name", "Standardized_Name", "canonical_name", "Canonical_Name", "normalized_name"])
    col_mw = find_column(identity_df, ["MW", "MolecularWeight", "Molecular_Weight", "molecular weight"])

    lookup: Dict[str, Dict[str, Any]] = {}

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
        }

        candidate_keys = [
            original_name,
            standardized_solute,
            iupac,
            formula,
        ]
        for key in candidate_keys:
            if not key:
                continue
            for k in [normalize_key(key), simplify_key(key)]:
                if k and k not in lookup:
                    lookup[k] = rec.copy()

    return lookup


def resolve_solute_identity(original_solute: str, identity_lookup: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    raw = norm_text(original_solute)
    if not raw:
        return {
            "standardized_solute": "",
            "IUPAC_Name": "",
            "formula": "",
            "CID": "",
            "MW_from_table": None,
            "identity_source": "EMPTY_SOLUTE",
        }

    for k in [normalize_key(raw), simplify_key(raw)]:
        if k in identity_lookup:
            return identity_lookup[k].copy()

    return {
        "standardized_solute": raw,
        "IUPAC_Name": "",
        "formula": "",
        "CID": "",
        "MW_from_table": None,
        "identity_source": "UNRESOLVED",
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
    # 1) 身份表已有
    mw_table = record.get("MW_from_table")
    if mw_table is not None:
        return float(mw_table), "IDENTITY_TABLE"

    # 2) 先读 cache
    cid = norm_text(record.get("CID", ""))
    iupac = norm_text(record.get("IUPAC_Name", ""))
    std_name = norm_text(record.get("standardized_solute", ""))
    original_solute = norm_text(record.get("original_solute", ""))

    cache_keys = []
    if cid:
        cache_keys.append(f"cid::{cid}")
    for nm in [iupac, std_name, original_solute]:
        if nm:
            cache_keys.append(f"name::{normalize_key(nm)}")

    for ck in cache_keys:
        if ck in pubchem_cache:
            hit = pubchem_cache[ck]
            return hit.get("mw"), hit.get("source", "PUBCHEM_CACHE")

    # 3) 禁用 PubChem
    if not cfg.pubchem_enabled:
        return None, "PUBCHEM_DISABLED"

    # 4) 正式查 PubChem
    try:
        mw = None
        source = ""

        if cid:
            mw = query_pubchem_mw_by_cid(cid, timeout_seconds=cfg.timeout_seconds)
            source = "PUBCHEM_CID"

        if mw is None:
            for name in [iupac, std_name, original_solute]:
                if name:
                    try:
                        mw = query_pubchem_mw_by_name(name, timeout_seconds=cfg.timeout_seconds)
                        source = "PUBCHEM_NAME"
                        if mw is not None:
                            break
                    except Exception:
                        continue

        time.sleep(cfg.sleep_between_pubchem_calls)

        if mw is not None:
            for ck in cache_keys:
                pubchem_cache[ck] = {"mw": mw, "source": source}
            return mw, source

        return None, "PUBCHEM_NOT_FOUND"

    except Exception as e:
        return None, f"PUBCHEM_ERROR::{type(e).__name__}"


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
    """
    粗粒度溶剂识别：
    - 优先看含 solvent/溶剂 的列
    - 再从整行文本中找已知溶剂别名
    """
    aq_solvent = ""
    org_solvent = ""
    general_solvents = []

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
        # 优先非水溶剂
        if org_solvent:
            return org_solvent
        for s in general_solvents:
            if s != "water":
                return s
        return ""

    # additive / modifier
    if aq_solvent:
        return aq_solvent
    if org_solvent:
        return org_solvent
    return general_solvents[0] if general_solvents else ""


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

    # 若目录为空，则解压
    if not any(extract_dir.iterdir()):
        with zipfile.ZipFile(zip_path, "r") as zf:
            zf.extractall(extract_dir)

    return extract_dir


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

    return {
        "doi": doi,
        "title": title,
        "filename_hint": filename_hint,
    }


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
    """
    目标：为 LLM 提供短证据片段，不做全文喂入。
    优先依据 DOI / 标题 / 文件名 / 溶质名 / IUPAC 名检索。
    """
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
    keywords = [k for k in keywords if len(norm_text(k)) >= 3]
    if not keywords:
        return []

    scored_files = []
    for fp in md_files:
        fname = fp.name.lower()
        stem = fp.stem.lower()
        score = 0

        for kw in keywords:
            kw_n = normalize_key(kw)
            kw_s = simplify_key(kw)

            if kw_n and kw_n in fname:
                score += 50
            if kw_s and kw_s in simplify_key(stem):
                score += 40

        if score > 0:
            scored_files.append((score, fp))

    # 文件名没命中时，退化到内容搜
    if not scored_files:
        for fp in md_files:
            text = read_text_safely(fp)
            low = text.lower()
            score = 0
            for kw in keywords:
                kw_n = normalize_key(kw)
                if kw_n and kw_n in low:
                    score += 10
            if score > 0:
                scored_files.append((score, fp))

    scored_files.sort(key=lambda x: x[0], reverse=True)
    top_files = [fp for _, fp in scored_files[:8]]

    snippets: List[str] = []
    for fp in top_files:
        text = read_text_safely(fp)
        if not text:
            continue

        for kw in keywords:
            snip = extract_snippet(text, kw, cfg.traceback_window_chars)
            if snip:
                snippets.append(f"[{fp.name}] {snip}")
                break

        if len(snippets) >= cfg.max_traceback_snippets:
            break

    return snippets[:cfg.max_traceback_snippets]


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
        text = text[first:last+1]
    return json.loads(text)


def heuristic_judge_slot(record: Dict[str, Any], snippets: List[str]) -> Dict[str, Any]:
    std = normalize_key(record.get("standardized_solute", ""))
    raw = normalize_key(record.get("original_solute", ""))
    slot_name = record.get("slot_name", "")
    unit_family = record.get("unit_family", "")
    phase = record.get("phase", "unknown") or "unknown"

    # physical form
    if std in COMMON_LIQUIDS or raw in COMMON_LIQUIDS or std in SOLUTE_DENSITY_DICT:
        physical_form = "liquid"
    elif std in COMMON_SOLIDS or raw in COMMON_SOLIDS:
        physical_form = "solid"
    else:
        physical_form = "unknown"

    # percent type
    if unit_family in PERCENT_TYPE_BY_FAMILY:
        percent_type = PERCENT_TYPE_BY_FAMILY[unit_family]
    elif unit_family == "mass_fraction":
        percent_type = "WT_PERCENT"
    elif unit_family == "percent_ambiguous":
        snippet_text = " ".join(snippets).lower()
        if re.search(r"\bw/v\b|g/100 ?ml|% ?\(w/v\)", snippet_text):
            percent_type = "WV_PERCENT"
        elif re.search(r"\bv/v\b|vol%|% ?\(v/v\)", snippet_text):
            percent_type = "VOL_PERCENT"
        elif re.search(r"\bw/w\b|wt%|mass%|% ?\(w/w\)", snippet_text):
            percent_type = "WT_PERCENT"
        elif slot_name == "test_nacl" and ("nacl" in std or "sodium chloride" in std or "nacl" in raw):
            # 这是一个有意的“平衡版”启发式：测试 NaCl 裸 % 常常是 wt%
            percent_type = "WT_PERCENT"
        else:
            percent_type = "PERCENT_UNKNOWN"
    else:
        percent_type = ""

    need_traceback = False
    if unit_family == "percent_ambiguous" and percent_type == "PERCENT_UNKNOWN":
        need_traceback = True

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
    # 先 heuristic baseline
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
        "return_schema": {
            "physical_form_inferred": "solid|liquid|unknown",
            "phase": "aqueous|organic|unknown",
            "percent_type_inferred": "WT_PERCENT|VOL_PERCENT|WV_PERCENT|PERCENT_UNKNOWN|''",
            "solute_for_this_value": "string",
            "need_traceback": "boolean",
            "reason": "string"
        }
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
                {"role": "user", "content": json.dumps(user_payload, ensure_ascii=False)}
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
# 12) 密度解析
# =========================================================

def find_explicit_density_in_row(row: pd.Series, slot_name: str) -> Tuple[Optional[float], str]:
    """
    只做保守识别：
    - 列名里要有 density/密度
    - 优先带 solution/solvent/aqueous/organic/水相/油相 等上下文
    """
    preferred_keywords = {
        "aqueous_monomer": ["aqueous", "water", "水相"],
        "organic_monomer": ["organic", "oil", "油相"],
        "additive": ["solution", "solvent", "溶液", "density", "密度"],
        "modifier": ["solution", "solvent", "溶液", "density", "密度"],
        "test_nacl": ["test", "nacl", "solution", "水相", "aqueous"],
    }

    best_candidate = None
    best_source = ""

    for col in row.index:
        ckey = normalize_key(col)
        if "density" not in ckey and "密度" not in ckey:
            continue

        v = row.get(col)
        rho = parse_float_maybe(v)
        if rho is None:
            continue

        score = 1
        for kw in preferred_keywords.get(slot_name, []):
            if normalize_key(kw) in ckey:
                score += 2

        # 过于奇怪的值直接跳过
        if rho <= 0 or rho > 5:
            continue

        if best_candidate is None or score > best_candidate:
            best_candidate = score
            best_source = f"ROW_COLUMN::{col}"
            best_rho = rho

    if best_candidate is None:
        return None, ""
    return best_rho, best_source


def resolve_density(record: Dict[str, Any], row: pd.Series) -> Dict[str, Any]:
    """
    返回：
    {
        "rho_solution": Optional[float],
        "rho_solute": Optional[float],
        "density_source": str,
        "density_assumed": bool
    }
    """
    slot_name = record.get("slot_name", "")
    phase = record.get("phase", "unknown")
    solvent = normalize_key(record.get("solvent_identified", ""))
    std = normalize_key(record.get("standardized_solute", ""))

    # 1) 显式 solution density
    rho_explicit, rho_source = find_explicit_density_in_row(row, slot_name)
    if rho_explicit is not None:
        rho_solute = SOLUTE_DENSITY_DICT.get(std)
        return {
            "rho_solution": rho_explicit,
            "rho_solute": rho_solute,
            "density_source": rho_source,
            "density_assumed": False,
        }

    # 2) aqueous / test_nacl 允许 rho≈1
    if phase == "aqueous" or slot_name == "test_nacl":
        rho_solute = SOLUTE_DENSITY_DICT.get(std)
        return {
            "rho_solution": 1.0,
            "rho_solute": rho_solute,
            "density_source": "ASSUMED_AQUEOUS_RHO_1.0",
            "density_assumed": True,
        }

    # 3) organic 允许低浓度近似 rho≈rho_solvent（谨慎）
    if solvent:
        # 统一到 canonical solvent
        canon_solvent = canonicalize_solvent_name(solvent) or solvent
        rho_solution = SOLVENT_DENSITY_DICT.get(canon_solvent)
        rho_solute = SOLUTE_DENSITY_DICT.get(std)
        if rho_solution is not None:
            return {
                "rho_solution": rho_solution,
                "rho_solute": rho_solute,
                "density_source": f"SOLVENT_DICT::{canon_solvent}",
                "density_assumed": True,
            }

    # 4) 仅知道溶质纯物质密度
    rho_solute = SOLUTE_DENSITY_DICT.get(std)
    return {
        "rho_solution": None,
        "rho_solute": rho_solute,
        "density_source": "NO_RELIABLE_SOLUTION_DENSITY",
        "density_assumed": True if rho_solute is not None else False,
    }


# =========================================================
# 13) wt% 换算
# =========================================================

def convert_to_wtpercent(record: Dict[str, Any], density_info: Dict[str, Any], ndigits: int = 6) -> Dict[str, Any]:
    """
    只做程序换算，不让 LLM 算数。
    """

    original_value = record.get("original_value", "")
    value = parse_float_maybe(original_value)
    if value is None:
        return {
            "suggested_wtpercent": None,
            "suggested_status": "FAILED_PARSE",
            "suggested_reason": f"Cannot parse numeric value from: {original_value}",
        }

    unit_family = record.get("unit_family", "")
    canonical_unit = record.get("canonical_unit", "")
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

    # 先处理裸 %
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
            }

    # A. mass_fraction
    if effective_family == "mass_fraction":
        # 裸 % 经推断为 WT_PERCENT，不算 DIRECT_WT
        if unit_family == "mass_fraction":
            return {
                "suggested_wtpercent": round(value, ndigits),
                "suggested_status": "DIRECT_WT",
                "suggested_reason": "Already mass fraction / wt% equivalent.",
            }
        else:
            status = "SAFE_CONVERTED" if judgement_source == "llm" else "ASSUMED_CONVERTED"
            return {
                "suggested_wtpercent": round(value, ndigits),
                "suggested_status": status,
                "suggested_reason": f"Raw % inferred as WT_PERCENT via {judgement_source}.",
            }

    # B. ppm / ppb / g/L / mg/L / ug/L / ng/L / mg/mL
    if effective_family == "ppm_family":
        if effective_unit == "ppm":
            # aqueous/test_aqueous approximation
            if phase == "aqueous" or slot_name == "test_nacl":
                wt = value / 10000.0
                return {
                    "suggested_wtpercent": round(wt, ndigits),
                    "suggested_status": "ASSUMED_CONVERTED",
                    "suggested_reason": "ppm -> wt% using dilute aqueous approximation.",
                }
            return {
                "suggested_wtpercent": None,
                "suggested_status": "CANNOT_CONVERT",
                "suggested_reason": "ppm without aqueous context is not converted in balanced mode.",
            }

        if effective_unit == "ppb":
            if phase == "aqueous" or slot_name == "test_nacl":
                wt = value / 1e7
                return {
                    "suggested_wtpercent": round(wt, ndigits),
                    "suggested_status": "ASSUMED_CONVERTED",
                    "suggested_reason": "ppb -> wt% using dilute aqueous approximation.",
                }
            return {
                "suggested_wtpercent": None,
                "suggested_status": "CANNOT_CONVERT",
                "suggested_reason": "ppb without aqueous context is not converted in balanced mode.",
            }

        if effective_unit == "g/kg":
            wt = value / 10.0
            return {
                "suggested_wtpercent": round(wt, ndigits),
                "suggested_status": "SAFE_CONVERTED",
                "suggested_reason": "g/kg is directly mass-based.",
            }

        if effective_unit == "mg/kg":
            wt = value / 10000.0
            return {
                "suggested_wtpercent": round(wt, ndigits),
                "suggested_status": "SAFE_CONVERTED",
                "suggested_reason": "mg/kg is directly mass-based.",
            }

    if effective_family == "mass_concentration":
        if rho_solution is None:
            return {
                "suggested_wtpercent": None,
                "suggested_status": "NEED_TRACEBACK",
                "suggested_reason": "mass concentration needs solution density.",
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
            # 1 mg/mL = 1 g/L
            wt = value / (10.0 * rho_solution)
        else:
            return {
                "suggested_wtpercent": None,
                "suggested_status": "CANNOT_CONVERT",
                "suggested_reason": f"Unsupported mass concentration unit: {effective_unit}",
            }

        status = "ASSUMED_CONVERTED" if density_assumed else "SAFE_CONVERTED"
        return {
            "suggested_wtpercent": round(wt, ndigits),
            "suggested_status": status,
            "suggested_reason": f"{effective_unit} -> wt% using rho_solution={rho_solution} from {density_source}.",
        }

    # C. w/v%
    if effective_family == "mass_volume_percent":
        if rho_solution is None:
            return {
                "suggested_wtpercent": None,
                "suggested_status": "NEED_TRACEBACK",
                "suggested_reason": "w/v% requires solution density.",
            }
        # x w/v% = x g / 100 mL solution
        # wt% = x / rho_solution
        wt = value / rho_solution
        status = "ASSUMED_CONVERTED" if density_assumed else "SAFE_CONVERTED"
        return {
            "suggested_wtpercent": round(wt, ndigits),
            "suggested_status": status,
            "suggested_reason": f"{effective_unit} -> wt% using rho_solution={rho_solution} from {density_source}.",
        }

    # D. M / mol/L
    if effective_family == "molarity":
        if mw is None:
            return {
                "suggested_wtpercent": None,
                "suggested_status": "NEED_TRACEBACK",
                "suggested_reason": "Molarity conversion requires molecular weight.",
            }
        if rho_solution is None:
            return {
                "suggested_wtpercent": None,
                "suggested_status": "NEED_TRACEBACK",
                "suggested_reason": "Molarity conversion requires solution density.",
            }

        if effective_unit == "M" or effective_unit == "mol/L":
            c = value
        elif effective_unit == "mM" or effective_unit == "mmol/L":
            c = value / 1000.0
        else:
            return {
                "suggested_wtpercent": None,
                "suggested_status": "CANNOT_CONVERT",
                "suggested_reason": f"Unsupported molarity unit: {effective_unit}",
            }

        wt = c * mw / (10.0 * rho_solution)
        status = "ASSUMED_CONVERTED" if density_assumed else "SAFE_CONVERTED"
        return {
            "suggested_wtpercent": round(wt, ndigits),
            "suggested_status": status,
            "suggested_reason": f"{effective_unit} -> wt% using MW={mw} and rho_solution={rho_solution}.",
        }

    # E. v/v%
    if effective_family == "volume_fraction":
        if rho_solute is None or rho_solution is None:
            return {
                "suggested_wtpercent": None,
                "suggested_status": "NEED_TRACEBACK",
                "suggested_reason": "v/v% conversion requires both solute density and solution density.",
            }

        # 先判断 value 的基准
        if effective_unit == "v/v%" or effective_unit == "vol%":
            vv_percent = value
        elif effective_unit == "mL/L":
            vv_percent = value / 10.0
        else:
            return {
                "suggested_wtpercent": None,
                "suggested_status": "CANNOT_CONVERT",
                "suggested_reason": f"Unsupported volume fraction unit: {effective_unit}",
            }

        # wt% ≈ (v/v%) * rho_solute / rho_solution
        wt = vv_percent * rho_solute / rho_solution
        status = "ASSUMED_CONVERTED" if density_assumed else "SAFE_CONVERTED"
        return {
            "suggested_wtpercent": round(wt, ndigits),
            "suggested_status": status,
            "suggested_reason": f"{effective_unit} -> wt% using rho_solute={rho_solute}, rho_solution={rho_solution}.",
        }

    # unsupported
    if effective_family == "unsupported":
        return {
            "suggested_wtpercent": None,
            "suggested_status": "CANNOT_CONVERT",
            "suggested_reason": f"Unsupported unit family: {unit_family}",
        }

    return {
        "suggested_wtpercent": None,
        "suggested_status": "CANNOT_CONVERT",
        "suggested_reason": f"Unhandled unit family: {unit_family}",
    }


# =========================================================
# 14) 主 review 表构建
# =========================================================

def build_review_sheet(
    main_df: pd.DataFrame,
    identity_lookup: Dict[str, Dict[str, Any]],
    cfg: AppConfig,
    pubchem_cache: Dict[str, Any],
    llm_cache: Dict[str, Any],
) -> pd.DataFrame:
    review_rows: List[Dict[str, Any]] = []

    for idx, row in main_df.iterrows():
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
                }

                # 1) split 失败，直接落 FAILED_PARSE
                if rec["split_parse_error"]:
                    rec.update({
                        "standardized_solute": rec["original_solute"],
                        "IUPAC_Name": "",
                        "formula": "",
                        "CID": "",
                        "canonical_unit": "",
                        "unit_family": "unsupported",
                        "phase": slot_spec.default_phase,
                        "solvent_identified": infer_solvent_from_row(row, slot_spec),
                        "physical_form_inferred": "unknown",
                        "percent_type_inferred": "",
                        "MW": None,
                        "MW_source": "",
                        "density_source": "",
                        "suggested_wtpercent": None,
                        "suggested_status": "FAILED_PARSE",
                        "suggested_reason": f"split_slot_items error: {rec['split_parse_error']}",
                        "final_value": None,
                        "final_unit": "",
                        "final_status": "FAILED_PARSE",
                        "final_reason": f"split_slot_items error: {rec['split_parse_error']}",
                    })
                    review_rows.append(rec)
                    continue

                # 2) 身份匹配
                identity = resolve_solute_identity(rec["original_solute"], identity_lookup)
                rec.update(identity)

                # 3) 单位归一化
                unit_info = normalize_unit(rec["original_unit"])
                rec.update({
                    "canonical_unit": unit_info["canonical_unit"],
                    "unit_family": unit_info["unit_family"],
                })

                # 4) 初始相别与溶剂
                phase = slot_spec.default_phase
                solvent_identified = infer_solvent_from_row(row, slot_spec)

                # additive / modifier 初始 phase 若未知，先给 unknown
                if phase == "unknown":
                    if solvent_identified == "water":
                        phase = "aqueous"
                    elif solvent_identified:
                        phase = "organic"

                rec["phase"] = phase
                rec["solvent_identified"] = solvent_identified

                # 5) traceback + LLM
                need_snippets = (
                    cfg.use_traceback and (
                        rec["unit_family"] == "percent_ambiguous"
                        or rec["phase"] == "unknown"
                    )
                )
                snippets = traceback_md_context(row, rec, cfg) if need_snippets else []

                judge = llm_judge_slot(rec, snippets, cfg, llm_cache)
                rec["physical_form_inferred"] = judge.get("physical_form_inferred", "unknown")
                rec["phase"] = judge.get("phase", rec["phase"])
                rec["percent_type_inferred"] = judge.get("percent_type_inferred", "")
                rec["judgement_source"] = judge.get("judgement_source", "heuristic")
                rec["judgement_reason"] = judge.get("reason", "")
                rec["need_traceback_flag"] = bool(judge.get("need_traceback", False))
                rec["traceback_snippets"] = "\n---\n".join(snippets) if snippets else ""

                # 6) MW
                mw, mw_source = fetch_molecular_weight(rec, cfg, pubchem_cache)
                rec["MW"] = mw
                rec["MW_source"] = mw_source

                # 7) Density
                density_info = resolve_density(rec, row)
                rec["density_source"] = density_info.get("density_source", "")

                # 8) 换算
                conv = convert_to_wtpercent(rec, density_info, ndigits=cfg.round_digits)
                rec["suggested_wtpercent"] = conv["suggested_wtpercent"]
                rec["suggested_status"] = conv["suggested_status"]
                rec["suggested_reason"] = conv["suggested_reason"]

                # 9) 当前版本直接把 suggested 写为 final
                rec["final_value"] = rec["suggested_wtpercent"]
                rec["final_unit"] = "wt%" if rec["suggested_wtpercent"] is not None else ""
                rec["final_status"] = rec["suggested_status"]
                rec["final_reason"] = rec["suggested_reason"]

                review_rows.append(rec)

    review_df = pd.DataFrame(review_rows)

    # 按要求整理列顺序
    desired_cols = [
        "row_id",
        "slot_name",
        "item_index",
        "original_solute",
        "standardized_solute",
        "IUPAC_Name",
        "formula",
        "CID",
        "original_value",
        "original_unit",
        "canonical_unit",
        "unit_family",
        "phase",
        "solvent_identified",
        "physical_form_inferred",
        "percent_type_inferred",
        "MW",
        "MW_source",
        "density_source",
        "suggested_wtpercent",
        "suggested_status",
        "suggested_reason",
        "final_value",
        "final_unit",
        "final_status",
        "final_reason",
        "identity_source",
        "judgement_source",
        "judgement_reason",
        "need_traceback_flag",
        "traceback_snippets",
        "split_parse_error",
        "MW_from_table",
    ]
    cols = [c for c in desired_cols if c in review_df.columns] + [c for c in review_df.columns if c not in desired_cols]
    review_df = review_df[cols]
    return review_df


# =========================================================
# 15) 交付主表构建
# =========================================================

def join_series_in_order(values: List[Any]) -> str:
    vals = [format_float_for_join(v) for v in values if norm_text(v) != ""]
    return ";".join([v for v in vals if v])


def join_texts_in_order(values: List[Any]) -> str:
    vals = [norm_text(v) for v in values if norm_text(v)]
    return ";".join(vals)


def build_delivery_main(main_df: pd.DataFrame, review_df: pd.DataFrame) -> pd.DataFrame:
    out = main_df.copy()

    if "__row_id__" not in out.columns:
        out["__row_id__"] = [get_row_id(r, i) for i, (_, r) in enumerate(out.iterrows())]

    for slot_spec in SLOT_SPECS:
        out[slot_spec.output_value_col] = ""
        out[slot_spec.output_status_col] = ""

        slot_df = review_df[review_df["slot_name"] == slot_spec.slot_name].copy()
        if slot_df.empty:
            continue

        slot_df = slot_df.sort_values(["row_id", "item_index"])

        grouped_values = slot_df.groupby("row_id")["final_value"].apply(list).to_dict()
        grouped_status = slot_df.groupby("row_id")["final_status"].apply(list).to_dict()

        value_map = {rid: join_series_in_order(vals) for rid, vals in grouped_values.items()}
        status_map = {rid: join_texts_in_order(vals) for rid, vals in grouped_status.items()}

        out[slot_spec.output_value_col] = out["__row_id__"].map(value_map).fillna("")
        out[slot_spec.output_status_col] = out["__row_id__"].map(status_map).fillna("")

    return out.drop(columns=["__row_id__"])


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

    slot_counts = review_df.groupby("slot_name")["final_status"].apply(lambda s: s.isin(SUCCESS_STATUS).sum()).to_dict()
    slot_total = review_df["slot_name"].value_counts().to_dict()
    for slot, total_slot in slot_total.items():
        converted_slot = slot_counts.get(slot, 0)
        rows.append({"section": "slot_coverage", "metric": f"{slot}__total", "value": int(total_slot)})
        rows.append({"section": "slot_coverage", "metric": f"{slot}__converted", "value": int(converted_slot)})
        rows.append({
            "section": "slot_coverage",
            "metric": f"{slot}__coverage",
            "value": converted_slot / total_slot if total_slot else 0.0
        })

    return pd.DataFrame(rows)


def build_unit_catalog(unit_df: Optional[pd.DataFrame], review_df: pd.DataFrame) -> pd.DataFrame:
    if unit_df is not None and not unit_df.empty:
        unit_col = find_column(unit_df, ["unit", "单位", "original_unit"])
        count_col = find_column(unit_df, ["count", "counts", "频次", "次数", "数量"])

        if unit_col is None:
            temp = pd.DataFrame({"original_unit": sorted(review_df["original_unit"].dropna().astype(str).unique())})
        else:
            temp = unit_df.copy()
            temp = temp.rename(columns={unit_col: "original_unit"})
            if count_col and count_col != "count":
                temp = temp.rename(columns={count_col: "count"})
            if "count" not in temp.columns:
                temp["count"] = None
    else:
        temp = review_df["original_unit"].fillna("").astype(str)
        temp = temp[temp.str.strip() != ""].value_counts().reset_index()
        temp.columns = ["original_unit", "count"]

    normalized = temp["original_unit"].apply(normalize_unit)
    temp["canonical_unit"] = normalized.apply(lambda x: x["canonical_unit"])
    temp["unit_family"] = normalized.apply(lambda x: x["unit_family"])
    return temp


# =========================================================
# 17) 写出 Excel
# =========================================================

def write_workbook(
    delivery_main_df: pd.DataFrame,
    review_df: pd.DataFrame,
    summary_df: pd.DataFrame,
    unit_catalog_df: pd.DataFrame,
    cfg: AppConfig,
) -> None:
    out_path = Path(cfg.output_excel)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        delivery_main_df.to_excel(writer, sheet_name="delivery_main", index=False)
        review_df.to_excel(writer, sheet_name="concentration_review", index=False)
        summary_df.to_excel(writer, sheet_name="summary", index=False)
        unit_catalog_df.to_excel(writer, sheet_name="unit_catalog", index=False)


# =========================================================
# 18) 主程序
# =========================================================

def main() -> None:
    print(">>> Reading inputs...")
    main_df, unit_df, identity_df = read_inputs(CFG)

    print(">>> Building identity lookup...")
    identity_lookup = build_identity_lookup(identity_df)

    print(">>> Loading caches...")
    pubchem_cache = load_json_cache(CFG.pubchem_cache_json)
    llm_cache = load_json_cache(CFG.llm_cache_json)

    print(">>> Building concentration_review...")
    review_df = build_review_sheet(
        main_df=main_df,
        identity_lookup=identity_lookup,
        cfg=CFG,
        pubchem_cache=pubchem_cache,
        llm_cache=llm_cache,
    )

    print(">>> Building delivery_main...")
    delivery_main_df = build_delivery_main(main_df, review_df)

    print(">>> Building summary and unit_catalog...")
    summary_df = build_summary(review_df)
    unit_catalog_df = build_unit_catalog(unit_df, review_df)

    print(">>> Saving caches...")
    save_json_cache(CFG.pubchem_cache_json, pubchem_cache)
    save_json_cache(CFG.llm_cache_json, llm_cache)

    print(">>> Writing workbook...")
    write_workbook(
        delivery_main_df=delivery_main_df,
        review_df=review_df,
        summary_df=summary_df,
        unit_catalog_df=unit_catalog_df,
        cfg=CFG,
    )

    print(f">>> Done. Output written to: {CFG.output_excel}")


if __name__ == "__main__":
    main()