"""
膜材料数据库浓度字段清洗系统 - 完整版claude版本
"""

import pandas as pd
import numpy as np
import json
import re
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass, asdict
from enum import Enum
import zipfile
import os

# ============================================================================
# 配置区
# ============================================================================

CONFIG = {
    "input_database": "./test4/test1.xlsx",
    "unit_catalog": "./test4/统计单位.xlsx",
    "substance_mapping": "./test4/task1.xlsx",
    "md_archive": "./test4/反渗透膜_output.zip",
    "output_file": "./test4/delivery_cleaned.xlsx",
    
    # LLM 配置（OpenAI-compatible）
    "llm_api_key": "DEEPSEEK_API_KEY",
    "llm_base_url": "https://api.deepseek.com/v1",  # 或 Claude / OpenAI
    "llm_model": "deepseek-chat",
    
    # 本地物质库（补充）
    "substance_db": {
        "NaCl": {"formula": "NaCl", "MW": 58.44, "IUPAC": "sodium chloride"},
        "TMC": {"formula": "C9H6Cl3O3", "MW": 265.48, "IUPAC": "1,3,5-benzenetricarbonyl chloride"},
        "MPD": {"formula": "C5H14N2", "MW": 104.17, "IUPAC": "1,3-pentanediamine"},
    },
    
    # 溶剂密度表（g/mL @ 25°C）
    "solvent_density": {
        "water": 0.997,
        "hexane": 0.655,
        "toluene": 0.862,
        "acetone": 0.784,
        "ethanol": 0.789,
        "methanol": 0.791,
        "DCM": 1.325,
        "DMF": 0.944,
        "DMSO": 1.096,
    },
}

# ============================================================================
# 数据类定义
# ============================================================================

class UnitFamily(Enum):
    """单位家族"""
    MASS_FRACTION = "mass_fraction"  # wt%, w/w%, mass%
    MASS_VOLUME_PERCENT = "mass_volume_percent"  # w/v%, wt/v%
    MASS_CONCENTRATION = "mass_concentration"  # g/L, mg/L, mg/mL
    MOLARITY = "molarity"  # M, mM, mol/L
    VOLUME_FRACTION = "volume_fraction"  # v/v%, vol%
    PPM_FAMILY = "ppm_family"  # ppm, ppb, g/kg, mg/kg
    PERCENT_AMBIGUOUS = "percent_ambiguous"  # 裸 %
    UNSUPPORTED = "unsupported"  # 不支持的单位

class Phase(Enum):
    """相位"""
    AQUEOUS = "aqueous"
    ORGANIC = "organic"
    MIXED = "mixed"
    UNKNOWN = "unknown"

class Status(Enum):
    """状态码"""
    DIRECT_WT = "DIRECT_WT"  # 原始单位明确就是 wt%
    SAFE_CONVERTED = "SAFE_CONVERTED"  # 参数充分、程序可靠换算
    ASSUMED_CONVERTED = "ASSUMED_CONVERTED"  # 基于合理近似
    NEED_TRACEBACK = "NEED_TRACEBACK"  # 缺证据，需回溯
    CANNOT_CONVERT = "CANNOT_CONVERT"  # 本质不适合转为 wt%
    FAILED_PARSE = "FAILED_PARSE"  # 解析失败

@dataclass
class ConcentrationItem:
    """单个浓度项"""
    row_id: int
    slot_name: str  # aqueous_monomer, organic_monomer, additive, modifier, test_nacl
    
    # 原始信息
    original_solute: str
    original_value: Optional[float]
    original_unit: str
    
    # 标准化后
    standardized_solute: Optional[str] = None
    IUPAC_name: Optional[str] = None
    formula: Optional[str] = None
    CID: Optional[str] = None
    
    # 单位
    canonical_unit: Optional[str] = None
    unit_family: Optional[UnitFamily] = None
    
    # 上下文
    phase: Phase = Phase.UNKNOWN
    solvent_identified: Optional[str] = None
    physical_form_inferred: Optional[str] = None  # solid/liquid/unknown
    percent_type_inferred: Optional[str] = None  # WT_PERCENT/VOL_PERCENT/WV_PERCENT/UNKNOWN
    
    # 物理参数
    MW: Optional[float] = None
    MW_source: Optional[str] = None
    density_solution: Optional[float] = None
    density_source: Optional[str] = None
    
    # 转换结果
    suggested_wtpercent: Optional[float] = None
    suggested_status: Status = Status.FAILED_PARSE
    suggested_reason: str = ""
    
    # 最终结果
    final_value: Optional[float] = None
    final_unit: str = "wt%"
    final_status: Status = Status.FAILED_PARSE
    final_reason: str = ""

# ============================================================================
# 单位归一化
# ============================================================================

UNIT_NORMALIZATION_MAP = {
    # mass_fraction
    "wt%": (UnitFamily.MASS_FRACTION, "wt%"),
    "w/w%": (UnitFamily.MASS_FRACTION, "wt%"),
    "mass%": (UnitFamily.MASS_FRACTION, "wt%"),
    "% by mass": (UnitFamily.MASS_FRACTION, "wt%"),
    "% m/m": (UnitFamily.MASS_FRACTION, "wt%"),
    "wt. %": (UnitFamily.MASS_FRACTION, "wt%"),
    
    # mass_volume_percent
    "w/v%": (UnitFamily.MASS_VOLUME_PERCENT, "w/v%"),
    "wt/v%": (UnitFamily.MASS_VOLUME_PERCENT, "w/v%"),
    "g/100ml": (UnitFamily.MASS_VOLUME_PERCENT, "w/v%"),
    "g/100 ml": (UnitFamily.MASS_VOLUME_PERCENT, "w/v%"),
    
    # mass_concentration
    "g/l": (UnitFamily.MASS_CONCENTRATION, "g/L"),
    "g/L": (UnitFamily.MASS_CONCENTRATION, "g/L"),
    "mg/l": (UnitFamily.MASS_CONCENTRATION, "mg/L"),
    "mg/L": (UnitFamily.MASS_CONCENTRATION, "mg/L"),
    "ug/l": (UnitFamily.MASS_CONCENTRATION, "μg/L"),
    "μg/l": (UnitFamily.MASS_CONCENTRATION, "μg/L"),
    "μg/L": (UnitFamily.MASS_CONCENTRATION, "μg/L"),
    "ng/l": (UnitFamily.MASS_CONCENTRATION, "ng/L"),
    "ng/L": (UnitFamily.MASS_CONCENTRATION, "ng/L"),
    "mg/ml": (UnitFamily.MASS_CONCENTRATION, "mg/mL"),
    "mg/mL": (UnitFamily.MASS_CONCENTRATION, "mg/mL"),
    "g/ml": (UnitFamily.MASS_CONCENTRATION, "g/mL"),
    "g/mL": (UnitFamily.MASS_CONCENTRATION, "g/mL"),
    
    # molarity
    "M": (UnitFamily.MOLARITY, "M"),
    "mol/L": (UnitFamily.MOLARITY, "M"),
    "mol/l": (UnitFamily.MOLARITY, "M"),
    "mM": (UnitFamily.MOLARITY, "mM"),
    "mmol/L": (UnitFamily.MOLARITY, "mM"),
    "mmol/l": (UnitFamily.MOLARITY, "mM"),
    "mol/dm3": (UnitFamily.MOLARITY, "M"),
    
    # volume_fraction
    "v/v%": (UnitFamily.VOLUME_FRACTION, "v/v%"),
    "vol%": (UnitFamily.VOLUME_FRACTION, "v/v%"),
    "ml/l": (UnitFamily.VOLUME_FRACTION, "v/v%"),
    "mL/L": (UnitFamily.VOLUME_FRACTION, "v/v%"),
    
    # ppm_family
    "ppm": (UnitFamily.PPM_FAMILY, "ppm"),
    "ppb": (UnitFamily.PPM_FAMILY, "ppb"),
    "g/kg": (UnitFamily.PPM_FAMILY, "g/kg"),
    "mg/kg": (UnitFamily.PPM_FAMILY, "mg/kg"),
    
    # percent_ambiguous
    "%": (UnitFamily.PERCENT_AMBIGUOUS, "%"),
    
    # unsupported
    "ratio": (UnitFamily.UNSUPPORTED, "ratio"),
    "drops": (UnitFamily.UNSUPPORTED, "drops"),
    "cycles": (UnitFamily.UNSUPPORTED, "cycles"),
}

def normalize_unit(unit_str: str) -> Tuple[Optional[UnitFamily], Optional[str]]:
    """
    将原始单位字符串归一化为单位家族和规范单位
    """
    if not unit_str or pd.isna(unit_str):
        return None, None
    
    unit_str = str(unit_str).strip().lower()
    
    # 直接查表
    if unit_str in UNIT_NORMALIZATION_MAP:
        return UNIT_NORMALIZATION_MAP[unit_str]
    
    # 模糊匹配
    for key, (family, canonical) in UNIT_NORMALIZATION_MAP.items():
        if key in unit_str or unit_str in key:
            return family, canonical
    
    return UnitFamily.UNSUPPORTED, unit_str

# ============================================================================
# 读取输入
# ============================================================================

def read_inputs(config: Dict) -> Tuple[pd.DataFrame, Dict, Dict, Dict]:
    """
    读取所有输入文件
    返回：(主数据库, 单位统计表, 物质映射表, MD 文件内容)
    """
    # 主数据库
    db = pd.read_excel(config["input_database"])
    
    # 单位统计表
    unit_catalog = {}
    if Path(config["unit_catalog"]).exists():
        uc_df = pd.read_excel(config["unit_catalog"])
        unit_catalog = uc_df.to_dict(orient="records")
    
    # 物质映射表
    substance_map = {}
    if Path(config["substance_mapping"]).exists():
        sm_df = pd.read_excel(config["substance_mapping"])
        for _, row in sm_df.iterrows():
            key = str(row.get("原始名称", "")).strip().lower()
            if key:
                substance_map[key] = {
                    "standardized_name": row.get("标准名称"),
                    "formula": row.get("分子式"),
                    "IUPAC_name": row.get("IUPAC_Name"),
                    "CID": row.get("CID"),
                    "MW": row.get("MW"),
                }
    
    # MD 文件
    md_content = {}
    if Path(config["md_archive"]).exists():
        with zipfile.ZipFile(config["md_archive"], "r") as z:
            for name in z.namelist():
                if name.endswith(".md"):
                    md_content[name] = z.read(name).decode("utf-8", errors="ignore")
    
    return db, unit_catalog, substance_map, md_content

# ============================================================================
# Slot 拆分
# ============================================================================

SLOT_DEFINITIONS = {
    "aqueous_monomer": {
        "solute_col": "水相单体",
        "value_col": "水相单体浓度",
        "unit_col": "水相单体浓度_单位",
        "phase": Phase.AQUEOUS,
    },
    "organic_monomer": {
        "solute_col": "油相单体",
        "value_col": "油相单体浓度",
        "unit_col": "油相单体浓度_单位",
        "phase": Phase.ORGANIC,
    },
    "additive": {
        "solute_col": "添加剂",
        "value_col": "添加剂浓度",
        "unit_col": "添加剂浓度_单位",
        "phase": Phase.UNKNOWN,
    },
    "modifier": {
        "solute_col": "改性剂",
        "value_col": "改性剂浓度",
        "unit_col": "改性剂浓度_单位",
        "phase": Phase.UNKNOWN,
    },
    "test_nacl": {
        "solute_col": None,  # 固定为 NaCl
        "value_col": "测试NaCl浓度",
        "unit_col": "测试NaCl浓度_单位",
        "phase": Phase.AQUEOUS,
    },
}

def split_slot_items(row: pd.Series, row_id: int) -> List[ConcentrationItem]:
    """
    从一行数据中拆分出所有浓度项
    """
    items = []
    
    for slot_name, slot_def in SLOT_DEFINITIONS.items():
        solute_col = slot_def["solute_col"]
        value_col = slot_def["value_col"]
        unit_col = slot_def["unit_col"]
        phase = slot_def["phase"]
        
        # 读取原始值
        if slot_name == "test_nacl":
            solutes = ["NaCl"]
        else:
            solute_raw = row.get(solute_col)
            if pd.isna(solute_raw) or not str(solute_raw).strip():
                continue
            solutes = [s.strip() for s in str(solute_raw).split(";")]
        
        value_raw = row.get(value_col)
        if pd.isna(value_raw) or not str(value_raw).strip():
            continue
        values = [v.strip() for v in str(value_raw).split(";")]
        
        unit_raw = row.get(unit_col)
        if pd.isna(unit_raw) or not str(unit_raw).strip():
            units = [""] * len(values)
        else:
            units = [u.strip() for u in str(unit_raw).split(";")]
        
        # 对齐长度
        max_len = max(len(solutes), len(values), len(units))
        solutes = (solutes * ((max_len // len(solutes)) + 1))[:max_len]
        values = (values * ((max_len // len(values)) + 1))[:max_len]
        units = (units * ((max_len // len(units)) + 1))[:max_len]
        
        # 创建 item
        for solute, value, unit in zip(solutes, values, units):
            try:
                value_float = float(value)
            except (ValueError, TypeError):
                value_float = None
            
            item = ConcentrationItem(
                row_id=row_id,
                slot_name=slot_name,
                original_solute=solute,
                original_value=value_float,
                original_unit=unit,
                phase=phase,
            )
            items.append(item)
    
    return items

# ============================================================================
# 溶质标准化
# ============================================================================

def resolve_solute_identity(
    item: ConcentrationItem,
    substance_map: Dict,
    config: Dict,
) -> None:
    """
    标准化溶质身份
    """
    if not item.original_solute:
        return
    
    # 清洗原始名称
    cleaned = item.original_solute.strip().lower()
    cleaned = re.sub(r"\s+", "", cleaned)  # 去空格
    cleaned = re.sub(r"[（()）]", "", cleaned)  # 去括号
    
    # 查本地物质库
    if cleaned in CONFIG["substance_db"]:
        db_entry = CONFIG["substance_db"][cleaned]
        item.standardized_solute = cleaned
        item.formula = db_entry.get("formula")
        item.IUPAC_name = db_entry.get("IUPAC")
        item.MW = db_entry.get("MW")
        item.MW_source = "local_db"
        return
    
    # 查物质映射表
    if cleaned in substance_map:
        map_entry = substance_map[cleaned]
        item.standardized_solute = map_entry.get("standardized_name", cleaned)
        item.formula = map_entry.get("formula")
        item.IUPAC_name = map_entry.get("IUPAC_name")
        item.CID = map_entry.get("CID")
        item.MW = map_entry.get("MW")
        item.MW_source = "substance_map"
        return
    
    # 无法匹配
    item.standardized_solute = cleaned
    item.MW_source = "unknown"

# ============================================================================
# 分子量查询
# ============================================================================

def fetch_molecular_weight(item: ConcentrationItem, config: Dict) -> None:
    """
    获取分子量
    """
    if item.MW is not None:
        return
    
    if not item.IUPAC_name and not item.CID:
        return
    
    # 这里可以调用 PubChem API
    # 为简化，这里只做占位
    # 实际应该调用 requests 查询 PubChem
    pass

# ============================================================================
# 密度查询
# ============================================================================

def resolve_density(
    item: ConcentrationItem,
    config: Dict,
) -> None:
    """
    确定溶液密度
    """
    # 如果不需要密度，直接返回
    if item.unit_family not in [
        UnitFamily.MASS_VOLUME_PERCENT,
        UnitFamily.MASS_CONCENTRATION,
        UnitFamily.MOLARITY,
        UnitFamily.VOLUME_FRACTION,
    ]:
        return
    
    # 优先级 1：原文明确给出（这里需要从 MD 回溯，暂时跳过）
    
    # 优先级 2：稀水溶液近似 rho ≈ 1.0
    if item.phase == Phase.AQUEOUS and item.original_value and item.original_value < 10:
        item.density_solution = 1.0
        item.density_source = "aqueous_dilute_approx"
        return
    
    # 优先级 3：油相，用溶剂密度
    if item.phase == Phase.ORGANIC:
        if item.solvent_identified:
            solvent_key = item.solvent_identified.lower()
            if solvent_key in config["solvent_density"]:
                item.density_solution = config["solvent_density"][solvent_key]
                item.density_source = f"solvent_density_{solvent_key}"
                return
        # 油相但溶剂不明确，标记为需要回溯
        item.density_source = "organic_solvent_unknown"
        return
    
    # 其他情况
    item.density_source = "unknown"

# ============================================================================
# 单位和值的转换
# ============================================================================

def convert_to_wtpercent(item: ConcentrationItem, config: Dict) -> None:
    """
    将浓度转换为 wt%
    """
    if item.original_value is None:
        item.suggested_status = Status.FAILED_PARSE
        item.suggested_reason = "original_value is None"
        return
    
    if item.unit_family is None:
        item.suggested_status = Status.FAILED_PARSE
        item.suggested_reason = "unit_family is None"
        return
    
    value = item.original_value
    
    # ========== A. mass_fraction ==========
    if item.unit_family == UnitFamily.MASS_FRACTION:
        item.suggested_wtpercent = value
        item.suggested_status = Status.DIRECT_WT
        item.suggested_reason = "original unit is mass fraction"
        return
    
    # ========== B. ppm / ppb / g/kg / mg/kg ==========
    if item.unit_family == UnitFamily.PPM_FAMILY:
        if item.canonical_unit == "ppm":
            # ppm -> wt% ≈ ppm / 10000
            item.suggested_wtpercent = value / 10000
            item.suggested_status = Status.ASSUMED_CONVERTED
            item.suggested_reason = "ppm to wt% (aqueous dilute assumption)"
            return
        elif item.canonical_unit == "ppb":
            # ppb -> wt% ≈ ppb / 10000000
            item.suggested_wtpercent = value / 10000000
            item.suggested_status = Status.ASSUMED_CONVERTED
            item.suggested_reason = "ppb to wt%"
            return
        elif item.canonical_unit == "g/kg":
            # g/kg = wt%
            item.suggested_wtpercent = value / 10
            item.suggested_status = Status.ASSUMED_CONVERTED
            item.suggested_reason = "g/kg to wt%"
            return
        elif item.canonical_unit == "mg/kg":
            # mg/kg = ppm
            item.suggested_wtpercent = value / 10000
            item.suggested_status = Status.ASSUMED_CONVERTED
            item.suggested_reason = "mg/kg to wt%"
            return
    
    # ========== C. mass_concentration (g/L, mg/L, mg/mL) ==========
    if item.unit_family == UnitFamily.MASS_CONCENTRATION:
        if not item.density_solution:
            item.suggested_status = Status.NEED_TRACEBACK
            item.suggested_reason = "mass_concentration requires density_solution"
            return
        
        rho = item.density_solution
        
        if item.canonical_unit == "g/L":
            # wt% = (g/L) / (10 * rho_solution)
            item.suggested_wtpercent = value / (10 * rho)
            item.suggested_status = Status.SAFE_CONVERTED if item.density_source != "unknown" else Status.ASSUMED_CONVERTED
            item.suggested_reason = f"g/L to wt% (rho={rho} from {item.density_source})"
            return
        
        elif item.canonical_unit == "mg/L":
            # mg/L = g/L / 1000
            # wt% = (mg/L / 1000) / (10 * rho)
            item.suggested_wtpercent = value / (10000 * rho)
            item.suggested_status = Status.SAFE_CONVERTED if item.density_source != "unknown" else Status.ASSUMED_CONVERTED
            item.suggested_reason = f"mg/L to wt% (rho={rho})"
            return
        
        elif item.canonical_unit == "μg/L":
            # μg/L = mg/L / 1000
            item.suggested_wtpercent = value / (10000000 * rho)
            item.suggested_status = Status.ASSUMED_CONVERTED
            item.suggested_reason = f"μg/L to wt%"
            return
        
        elif item.canonical_unit == "ng/L":
            item.suggested_wtpercent = value / (10000000000 * rho)
            item.suggested_status = Status.ASSUMED_CONVERTED
            item.suggested_reason = f"ng/L to wt%"
            return
        
        elif item.canonical_unit == "mg/mL":
            # 关键修复：mg/mL = g/L
            # 1 mg/mL = 1 g/L
            # wt% = (mg/mL) / (10 * rho)
            item.suggested_wtpercent = value / (10 * rho)
            item.suggested_status = Status.SAFE_CONVERTED if item.density_source != "unknown" else Status.ASSUMED_CONVERTED
            item.suggested_reason = f"mg/mL to wt% (1 mg/mL = 1 g/L, rho={rho})"
            return
        
        elif item.canonical_unit == "g/mL":
            # g/mL = 1000 g/L
            item.suggested_wtpercent = (value * 1000) / (10 * rho)
            item.suggested_status = Status.SAFE_CONVERTED if item.density_source != "unknown" else Status.ASSUMED_CONVERTED
            item.suggested_reason = f"g/mL to wt%"
            return
    
    # ========== D. w/v% ==========
    if item.unit_family == UnitFamily.MASS_VOLUME_PERCENT:
        if not item.density_solution:
            item.suggested_status = Status.NEED_TRACEBACK
            item.suggested_reason = "w/v% requires density_solution"
            return
        
        rho = item.density_solution
        # x w/v% = x g / 100 mL solution
        # wt% = (x / 100) / rho
        item.suggested_wtpercent = value / (100 * rho)
        
        if item.phase == Phase.ORGANIC and item.density_source == "organic_solvent_unknown":
            item.suggested_status = Status.NEED_TRACEBACK
            item.suggested_reason = "w/v% in organic phase but solvent density unknown"
        else:
            item.suggested_status = Status.SAFE_CONVERTED if item.density_source != "unknown" else Status.ASSUMED_CONVERTED
            item.suggested_reason = f"w/v% to wt% (rho={rho} from {item.density_source})"
        return
    
    # ========== E. molarity (M / mM / mol/L) ==========
    if item.unit_family == UnitFamily.MOLARITY:
        if not item.MW:
            item.suggested_status = Status.NEED_TRACEBACK
            item.suggested_reason = "molarity requires MW"
            return
        
        if not item.density_solution:
            item.suggested_status = Status.NEED_TRACEBACK
            item.suggested_reason = "molarity requires density_solution"
            return
        
        rho = item.density_solution
        MW = item.MW
        
        # 转换为 mol/L
        if item.canonical_unit == "mM":
            c_mol_per_L = value / 1000
        else:  # M 或 mol/L
            c_mol_per_L = value
        
        # wt% = (c * MW) / (10 * rho)
        item.suggested_wtpercent = (c_mol_per_L * MW) / (10 * rho)
        item.suggested_status = Status.SAFE_CONVERTED if (item.MW_source != "unknown" and item.density_source != "unknown") else Status.ASSUMED_CONVERTED
        item.suggested_reason = f"M to wt% (MW={MW} from {item.MW_source}, rho={rho} from {item.density_source})"
        return
    
    # ========== F. v/v% ==========
    if item.unit_family == UnitFamily.VOLUME_FRACTION:
        # v/v% 需要溶质密度，通常不建议转为 wt%
        item.suggested_status = Status.NEED_TRACEBACK
        item.suggested_reason = "v/v% requires solute density (not recommended for conversion)"
        return
    
    # ========== G. 裸 % ==========
    if item.unit_family == UnitFamily.PERCENT_AMBIGUOUS:
        # 需要判断是 wt% / v/v% / w/v%
        # 这里暂时保守处理
        item.suggested_status = Status.NEED_TRACEBACK
        item.suggested_reason = "bare % requires context to determine type (wt/v/w/v)"
        return
    
    # ========== H. unsupported ==========
    if item.unit_family == UnitFamily.UNSUPPORTED:
        item.suggested_status = Status.CANNOT_CONVERT
        item.suggested_reason = f"unsupported unit: {item.original_unit}"
        return

# ============================================================================
# 主处理流程
# ============================================================================

def process_database(config: Dict) -> Tuple[pd.DataFrame, List[ConcentrationItem]]:
    """
    处理整个数据库
    """
    # 读取输入
    db, unit_catalog, substance_map, md_content = read_inputs(config)
    
    # 处理每一行
    all_items = []
    
    for idx, row in db.iterrows():
        row_id = idx
        
        # 拆分 slot
        items = split_slot_items(row, row_id)
        
        for item in items:
            # 标准化单位
            unit_family, canonical_unit = normalize_unit(item.original_unit)
            item.unit_family = unit_family
            item.canonical_unit = canonical_unit
            
            # 标准化溶质
            resolve_solute_identity(item, substance_map, config)
            
            # 获取分子量
            fetch_molecular_weight(item, config)
            
            # 确定密度
            resolve_density(item, config)
            
            # 转换为 wt%
            convert_to_wtpercent(item, config)
            
            all_items.append(item)
    
    return db, all_items

# ============================================================================
# 主表聚合
# ============================================================================

def build_delivery_main(db: pd.DataFrame, items: List[ConcentrationItem]) -> pd.DataFrame:
    """
    构建主表，在原始列旁边插入新列
    """
    result = db.copy()
    
    # 为每个 slot 构建聚合结果
    slot_results = {}
    for item in items:
        key = (item.row_id, item.slot_name)
        if key not in slot_results:
            slot_results[key] = {
                "values": [],
                "statuses": [],
            }
        
        if item.suggested_wtpercent is not None:
            slot_results[key]["values"].append(f"{item.suggested_wtpercent:.6g}")
        else:
            slot_results[key]["values"].append("")
        
        slot_results[key]["statuses"].append(item.suggested_status.value)
    
    # 定义要插入的列
    insert_specs = [
        ("aqueous_monomer", "水相单体浓度_单位", "水相单体浓度_wt%", "水相单体浓度_wt%_status"),
        ("organic_monomer", "油相单体浓度_单位", "油相单体浓度_wt%", "油相单体浓度_wt%_status"),
        ("additive", "添加剂浓度_单位", "添加剂浓度_wt%", "添加剂浓度_wt%_status"),
        ("modifier", "改性剂浓度_单位", "改性剂浓度_wt%", "改性剂浓度_wt%_status"),
        ("test_nacl", "测试NaCl浓度_单位", "测试NaCl浓度_wt%", "测试NaCl浓度_wt%_status"),
    ]
    
    # 按照原始列顺序插入新列
    for slot_name, after_col, wt_col, status_col in insert_specs:
        if after_col not in result.columns:
            continue
        
        insert_pos = result.columns.get_loc(after_col) + 1
        
        # 构建新列数据
        wt_values = []
        status_values = []
        
        for row_id in range(len(result)):
            key = (row_id, slot_name)
            if key in slot_results:
                wt_values.append(";".join(slot_results[key]["values"]))
                status_values.append(";".join(slot_results[key]["statuses"]))
            else:
                wt_values.append("")
                status_values.append("")
        
        # 插入列
        result.insert(insert_pos, wt_col, wt_values)
        result.insert(insert_pos + 1, status_col, status_values)
    
    return result

# ============================================================================
# Review 表构建
# ============================================================================

def build_concentration_review(items: List[ConcentrationItem]) -> pd.DataFrame:
    """
    构建 review 表
    """
    records = []
    for item in items:
        records.append({
            "row_id": item.row_id,
            "slot_name": item.slot_name,
            "original_solute": item.original_solute,
            "standardized_solute": item.standardized_solute,
            "IUPAC_Name": item.IUPAC_name,
            "formula": item.formula,
            "original_value": item.original_value,
            "original_unit": item.original_unit,
            "canonical_unit": item.canonical_unit,
            "unit_family": item.unit_family.value if item.unit_family else None,
            "phase": item.phase.value,
            "solvent_identified": item.solvent_identified,
            "physical_form_inferred": item.physical_form_inferred,
            "percent_type_inferred": item.percent_type_inferred,
            "MW": item.MW,
            "MW_source": item.MW_source,
            "density_solution": item.density_solution,
            "density_source": item.density_source,
            "suggested_wtpercent": item.suggested_wtpercent,
            "suggested_status": item.suggested_status.value,
            "suggested_reason": item.suggested_reason,
            "final_value": item.final_value,
            "final_unit": item.final_unit,
            "final_status": item.final_status.value,
            "final_reason": item.final_reason,
        })
    
    return pd.DataFrame(records)

# ============================================================================
# Summary 表构建
# ============================================================================

def build_summary(items: List[ConcentrationItem]) -> pd.DataFrame:
    """
    构建统计摘要
    """
    # 按状态统计
    status_counts = {}
    for item in items:
        status = item.suggested_status.value
        status_counts[status] = status_counts.get(status, 0) + 1
    
    # 按单位家族统计
    unit_family_counts = {}
    for item in items:
        if item.unit_family:
            family = item.unit_family.value
            unit_family_counts[family] = unit_family_counts.get(family, 0) + 1
    
    # 按 slot 统计
    slot_counts = {}
    for item in items:
        slot = item.slot_name
        slot_counts[slot] = slot_counts.get(slot, 0) + 1
    
    summary_data = {
        "Category": [],
        "Count": [],
    }
    
    summary_data["Category"].append("=== Status Distribution ===")
    summary_data["Count"].append("")
    for status, count in sorted(status_counts.items()):
        summary_data["Category"].append(status)
        summary_data["Count"].append(count)
    
    summary_data["Category"].append("")
    summary_data["Count"].append("")
    
    summary_data["Category"].append("=== Unit Family Distribution ===")
    summary_data["Count"].append("")
    for family, count in sorted(unit_family_counts.items()):
        summary_data["Category"].append(family)
        summary_data["Count"].append(count)
    
    summary_data["Category"].append("")
    summary_data["Count"].append("")
    
    summary_data["Category"].append("=== Slot Distribution ===")
    summary_data["Count"].append("")
    for slot, count in sorted(slot_counts.items()):
        summary_data["Category"].append(slot)
        summary_data["Count"].append(count)
    
    summary_data["Category"].append("")
    summary_data["Count"].append("")
    
    # 覆盖率统计
    total = len(items)
    converted = sum(1 for item in items if item.suggested_status in [
        Status.DIRECT_WT,
        Status.SAFE_CONVERTED,
        Status.ASSUMED_CONVERTED,
    ])
    coverage = (converted / total * 100) if total > 0 else 0
    
    summary_data["Category"].append("Total Items")
    summary_data["Count"].append(total)
    summary_data["Category"].append("Successfully Converted")
    summary_data["Count"].append(converted)
    summary_data["Category"].append("Coverage Rate (%)")
    summary_data["Count"].append(f"{coverage:.2f}%")
    
    return pd.DataFrame(summary_data)

# ============================================================================
# 输出
# ============================================================================

def write_workbook(
    config: Dict,
    delivery_main: pd.DataFrame,
    concentration_review: pd.DataFrame,
    summary: pd.DataFrame,
) -> None:
    """
    写入 Excel 文件
    """
    output_path = config["output_file"]
    
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        delivery_main.to_excel(writer, sheet_name="delivery_main", index=False)
        concentration_review.to_excel(writer, sheet_name="concentration_review", index=False)
        summary.to_excel(writer, sheet_name="summary", index=False)
    
    print(f"✓ Output written to {output_path}")

# ============================================================================
# 主函数
# ============================================================================

def main():
    """
    主处理流程
    """
    print("=" * 80)
    print("膜材料数据库浓度字段清洗系统")
    print("=" * 80)
    
    # 处理数据库
    print("\n[1/4] Reading inputs...")
    db, all_items = process_database(CONFIG)
    print(f"  ✓ Processed {len(all_items)} concentration items from {len(db)} rows")
    
    # 构建主表
    print("\n[2/4] Building delivery_main...")
    delivery_main = build_delivery_main(db, all_items)
    print(f"  ✓ Main table: {delivery_main.shape[0]} rows × {delivery_main.shape[1]} columns")
    
    # 构建 review 表
    print("\n[3/4] Building concentration_review...")
    concentration_review = build_concentration_review(all_items)
    print(f"  ✓ Review table: {concentration_review.shape[0]} rows × {concentration_review.shape[1]} columns")
    
    # 构建 summary
    print("\n[4/4] Building summary...")
    summary = build_summary(all_items)
    print(f"  ✓ Summary table: {summary.shape[0]} rows")
    
    # 输出
    print("\n[5/5] Writing output...")
    write_workbook(CONFIG, delivery_main, concentration_review, summary)
    
    print("\n" + "=" * 80)
    print("Processing complete!")
    print("=" * 80)

if __name__ == "__main__":
    main()
