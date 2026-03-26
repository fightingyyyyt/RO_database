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
    pip install pandas openpyxl requests
"""

from __future__ import annotations

import json
import os
import re
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
    output_excel: str = "./test3/test1_3_output.xlsx"

    # ---------- 缓存 ----------
    pubchem_cache_json: str = "./test3/cache_pubchem_mw.json"
    llm_cache_json: str = "./test3/cache_llm_judgements.json"

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
    short_abbrev_max_len: int = 6


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



def parse_float_maybe(x: Any) -> Optional[float]:
    s = norm_text(x)
    if not s:
        return None

    s = s.replace(",", "")
    s = s.replace("×10^", "e")
    s = s.replace("x10^", "e")
    s = s.replace("X10^", "e")

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
    if cfg.input_main_sheet is None:
        main_df = pd.read_excel(cfg.input_main_excel)
    else:
        main_df = pd.read_excel(cfg.input_main_excel, sheet_name=cfg.input_main_sheet)

    unit_df = None
    if cfg.unit_catalog_excel and Path(cfg.unit_catalog_excel).exists():
        if cfg.unit_catalog_sheet is None:
            unit_df = pd.read_excel(cfg.unit_catalog_excel)
        else:
            unit_df = pd.read_excel(cfg.unit_catalog_excel, sheet_name=cfg.unit_catalog_sheet)

    if cfg.identity_table_sheet is None:
        identity_df = pd.read_excel(cfg.identity_table_excel)
    else:
        identity_df = pd.read_excel(cfg.identity_table_excel, sheet_name=cfg.identity_table_sheet)

    return main_df, unit_df, identity_df


# =========================================================
# 5) 单位归一化
# =========================================================

def normalize_unit(raw_unit: Any) -> Dict[str, str]:
    raw = norm_text(raw_unit)
    if not raw:
        return {"original_unit": "", "canonical_unit": "", "unit_family": "unsupported"}

    s = raw.lower().strip()
    s = s.replace("％", "%").replace("μ", "u").replace("µ", "u").replace("／", "/")
    s_nospace = re.sub(r"\s+", "", s)

    # 统一括号 / 标点表达，方便处理 % (w/v) 这种写法
    s_compact = s_nospace
    s_compact = s_compact.replace("[", "(").replace("]", ")")
    s_compact = s_compact.replace("{", "(").replace("}", ")")

    # ---------- 裸 % ----------
    if s_compact in {"%", "percent"}:
        return {"original_unit": raw, "canonical_unit": "%", "unit_family": "percent_ambiguous"}

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
        return {"original_unit": raw, "canonical_unit": "wt%", "unit_family": "mass_fraction"}

    # ---------- mass_volume_percent ----------
    mass_volume_patterns = {
        "w/v%", "%w/v", "wt/v%", "%wt/v", "w/v", "wt/v", "wv", "wtv",
        "%(w/v)", "%(wt/v)", "%(m/v)", "m/v", "g/100ml", "g/100milliliter",
        "g/100milliliters", "g/dl"
    }
    if s_compact in mass_volume_patterns or re.fullmatch(r"g/100m?l", s_compact):
        canon = "g/100mL" if s_compact in {"g/100ml", "g/dl"} or re.fullmatch(r"g/100m?l", s_compact) else "w/v%"
        return {"original_unit": raw, "canonical_unit": canon, "unit_family": "mass_volume_percent"}

    # ---------- mass_concentration ----------
    mass_conc_map = {
        "g/l": "g/L", "gl-1": "g/L", "g·l-1": "g/L",
        "mg/l": "mg/L", "mgl-1": "mg/L", "mg·l-1": "mg/L",
        "ug/l": "ug/L", "ugl-1": "ug/L", "u g/l": "ug/L",
        "ng/l": "ng/L", "ngl-1": "ng/L",
        "mg/ml": "mg/mL", "mgml-1": "mg/mL", "g/ml": "g/mL",
    }
    if s_compact in mass_conc_map:
        return {"original_unit": raw, "canonical_unit": mass_conc_map[s_compact], "unit_family": "mass_concentration"}

    # ---------- molarity ----------
    if raw.strip() == "M":
        return {"original_unit": raw, "canonical_unit": "M", "unit_family": "molarity"}
    if raw.strip() == "mM":
        return {"original_unit": raw, "canonical_unit": "mM", "unit_family": "molarity"}
    molarity_map = {
        "mol/l": "mol/L", "moll-1": "mol/L", "mol·l-1": "mol/L",
        "mmol/l": "mmol/L", "mmoll-1": "mmol/L", "mmol·l-1": "mmol/L",
        "monomoles/l": "mol/L",  # 容错：历史脏数据里常见奇怪写法
    }
    if s_compact in molarity_map:
        return {"original_unit": raw, "canonical_unit": molarity_map[s_compact], "unit_family": "molarity"}

    # ---------- volume_fraction ----------
    volume_fraction_map = {
        "v/v%": "v/v%", "%v/v": "v/v%", "v/v": "v/v%", "%(v/v)": "v/v%",
        "vol%": "vol%", "vol.%": "vol%", "vol": "vol%", "ml/l": "mL/L"
    }
    if s_compact in volume_fraction_map:
        return {"original_unit": raw, "canonical_unit": volume_fraction_map[s_compact], "unit_family": "volume_fraction"}

    # ---------- ppm family ----------
    ppm_map = {"ppm": "ppm", "ppb": "ppb", "g/kg": "g/kg", "mg/kg": "mg/kg"}
    if s_compact in ppm_map:
        return {"original_unit": raw, "canonical_unit": ppm_map[s_compact], "unit_family": "ppm_family"}

    # ---------- unsupported ----------
    if re.fullmatch(r"\d+\s*:\s*\d+", raw.strip()):
        return {"original_unit": raw, "canonical_unit": raw, "unit_family": "unsupported"}

    if s_compact in {"drops", "drop", "g", "ml", "mmol", "mol", "l", "ul"}:
        return {"original_unit": raw, "canonical_unit": raw, "unit_family": "unsupported"}

    return {"original_unit": raw, "canonical_unit": raw, "unit_family": "unsupported"}


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
    # 1) 映射表已有 MW
    mw_table = record.get("MW_from_table")
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
        text = text[first:last + 1]
    return json.loads(text)



def heuristic_judge_slot(record: Dict[str, Any], snippets: List[str]) -> Dict[str, Any]:
    std = normalize_key(record.get("standardized_solute", ""))
    raw = normalize_key(record.get("original_solute", ""))
    slot_name = record.get("slot_name", "")
    unit_family = record.get("unit_family", "")
    phase = record.get("phase", "unknown") or "unknown"

    # physical form
    if std in COMMON_LIQUIDS or raw in COMMON_LIQUIDS or std in SOLUTE_DENSITY_DICT or raw in SOLUTE_DENSITY_DICT:
        physical_form = "liquid"
    elif std in COMMON_SOLIDS or raw in COMMON_SOLIDS:
        physical_form = "solid"
    else:
        physical_form = "unknown"

    # percent type
    if unit_family in PERCENT_TYPE_BY_FAMILY:
        percent_type = PERCENT_TYPE_BY_FAMILY[unit_family]
    elif unit_family == "percent_ambiguous":
        snippet_text = " ".join(snippets).lower()
        if re.search(r"\bw/v\b|g/100 ?ml|% ?\(w/v\)|% ?\(m/v\)", snippet_text):
            percent_type = "WV_PERCENT"
        elif re.search(r"\bv/v\b|vol%|% ?\(v/v\)", snippet_text):
            percent_type = "VOL_PERCENT"
        elif re.search(r"\bw/w\b|wt%|mass%|% ?\(w/w\)|% ?\(wt\)", snippet_text):
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
    original_value = record.get("original_value", "")
    value = parse_float_maybe(original_value)
    if value is None:
        return {
            "suggested_wtpercent": None,
            "suggested_status": "FAILED_PARSE",
            "suggested_reason": f"Cannot parse numeric value from: {original_value}",
            "conversion_formula_used": "PARSE_FAIL",
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
        if unit_family == "mass_fraction":
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
        return {
            "suggested_wtpercent": None,
            "suggested_status": "CANNOT_CONVERT",
            "suggested_reason": f"Unsupported unit family: {unit_family}",
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
                    "split_parse_warning": item.get("split_parse_warning", ""),
                    "alignment_confidence": item.get("alignment_confidence", ""),
                }

                identity = resolve_solute_identity(rec["original_solute"], identity_lookup)
                rec.update(identity)

                unit_info = normalize_unit(rec["original_unit"])
                rec["canonical_unit"] = unit_info["canonical_unit"]
                rec["unit_family"] = unit_info["unit_family"]

                rec["solvent_identified"] = infer_solvent_from_row(row, slot_spec)
                phase, phase_confidence, phase_reason = infer_phase_for_record(row, slot_spec, rec)
                rec["phase"] = phase
                rec["phase_confidence"] = phase_confidence
                rec["phase_reason"] = phase_reason

                need_snippets = cfg.use_traceback and (rec["unit_family"] == "percent_ambiguous" or rec["phase"] == "unknown")
                snippets = traceback_md_context(row, rec, cfg) if need_snippets else []
                judge = llm_judge_slot(rec, snippets, cfg, llm_cache)

                rec["physical_form_inferred"] = judge.get("physical_form_inferred", "unknown")
                rec["percent_type_inferred"] = judge.get("percent_type_inferred", "")
                rec["judgement_source"] = judge.get("judgement_source", "heuristic")
                rec["judgement_reason"] = judge.get("reason", "")
                rec["need_traceback_flag"] = bool(judge.get("need_traceback", False))
                rec["traceback_snippets"] = "\n---\n".join(snippets) if snippets else ""

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

                if rec["split_parse_error"] in {"empty_aligned_subitem", "missing_value_and_unit", "missing_solute"}:
                    rec["suggested_wtpercent"] = None
                    rec["suggested_status"] = "FAILED_PARSE"
                    rec["conversion_formula_used"] = "SPLIT_FAIL"
                    rec["suggested_reason"] = f"Split/alignment issue: {rec['split_parse_error']}"
                else:
                    conv = convert_to_wtpercent(rec, density_info, ndigits=cfg.round_digits)
                    rec["suggested_wtpercent"] = conv["suggested_wtpercent"]
                    rec["suggested_status"] = conv["suggested_status"]
                    rec["conversion_formula_used"] = conv.get("conversion_formula_used", "")
                    reason_parts = [conv["suggested_reason"]]
                    if rec.get("split_parse_warning"):
                        reason_parts.append(f"split_warning={rec['split_parse_warning']}")
                    rec["suggested_reason"] = " | ".join([p for p in reason_parts if p])

                rec["manual_review_flag"] = (
                    rec["suggested_status"] in {"NEED_TRACEBACK", "FAILED_PARSE"}
                    or rec["identity_confidence"] == "low"
                    or rec["phase_confidence"] == "low"
                )

                rec["final_value"] = rec["suggested_wtpercent"]
                rec["final_unit"] = "wt%" if rec["suggested_wtpercent"] is not None else ""
                rec["final_status"] = rec["suggested_status"]
                rec["final_reason"] = rec["suggested_reason"]

                review_rows.append(rec)

    review_df = pd.DataFrame(review_rows)

    desired_cols = [
        "row_id", "slot_name", "item_index",
        "original_solute", "standardized_solute", "IUPAC_Name", "formula", "CID",
        "identity_source", "identity_confidence", "identity_reason",
        "original_value", "original_unit", "canonical_unit", "unit_family",
        "split_parse_error", "split_parse_warning", "alignment_confidence",
        "phase", "phase_confidence", "phase_reason",
        "solvent_identified", "physical_form_inferred", "percent_type_inferred",
        "MW", "MW_source", "rho_solution", "rho_solute",
        "density_source", "density_assumed", "density_reason",
        "judgement_source", "judgement_reason", "need_traceback_flag", "traceback_snippets",
        "suggested_wtpercent", "suggested_status", "conversion_formula_used", "suggested_reason",
        "manual_review_flag",
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

    identity_counts = review_df["identity_confidence"].fillna("MISSING").value_counts(dropna=False)
    for conf, cnt in identity_counts.items():
        rows.append({"section": "identity_confidence_count", "metric": conf, "value": int(cnt)})

    phase_counts = review_df["phase_confidence"].fillna("MISSING").value_counts(dropna=False)
    for conf, cnt in phase_counts.items():
        rows.append({"section": "phase_confidence_count", "metric": conf, "value": int(cnt)})

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
                counts[unit_val] = counts.get(unit_val, 0) + int(cnt if cnt is not None else 1)
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

    # 3) review 表
    if review_df is not None and not review_df.empty and "original_unit" in review_df.columns:
        for unit in review_df["original_unit"].fillna("").astype(str):
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
    unit_catalog_df = build_unit_catalog(unit_df, review_df, main_df=main_df)

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
