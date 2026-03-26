import os
import re
import json
import zipfile
from pathlib import Path
from dataclasses import dataclass, asdict, field
from enum import Enum
from typing import Dict, List, Tuple, Optional, Any
from datetime import datetime
import logging

import pandas as pd
import numpy as np
import requests
from openpyxl import Workbook
from openpyxl.utils.dataframe import dataframe_to_rows

# ============================================================================
# 配置区
# ============================================================================

CONFIG = {
    "main_excel_path": "./test4/test1.xlsx",
    "unit_catalog_path": "./test4/统计单位.xlsx",
    "solute_mapping_path": "./test4/task1.xlsx",
    "md_archive_path":"./test4/反渗透膜_output.zip",
    "output_excel_path":"./test4/test1_output.xlsx",
    
    # LLM 配置（OpenAI-compatible）
    "llm_provider": "deepseek",  # "deepseek" 或 "openai" 或 "claude"
    "llm_api_key": os.getenv("DEEPSEEK_API_KEY", "your-api-key-here"),
    "llm_base_url": "https://api.deepseek.com/v1",
    "llm_model": "deepseek-chat",
    
    # PubChem 缓存
    "pubchem_cache_path": "./cache/pubchem_mw_cache.json",
    
    # 溶剂密度字典（g/mL，20°C）
    "solvent_density": {
        "water": 0.998,
        "ethanol": 0.789,
        "methanol": 0.792,
        "acetone": 0.784,
        "dmso": 1.101,
        "dmf": 0.944,
        "toluene": 0.867,
        "hexane": 0.655,
        "dcm": 1.325,
        "chloroform": 1.489,
    },
    
    # 日志
    "log_level": logging.INFO,
}

# ============================================================================
# 日志设置
# ============================================================================

logging.basicConfig(
    level=CONFIG["log_level"],
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# ============================================================================
# 枚举定义
# ============================================================================

class UnitFamily(Enum):
    MASS_FRACTION = "mass_fraction"
    MASS_VOLUME_PERCENT = "mass_volume_percent"
    MASS_CONCENTRATION = "mass_concentration"
    MOLARITY = "molarity"
    VOLUME_FRACTION = "volume_fraction"
    PPN_FAMILY = "ppm_family"
    PERCENT_AMBIGUOUS = "percent_ambiguous"
    UNSUPPORTED = "unsupported"

class ConversionStatus(Enum):
    DIRECT_WT = "DIRECT_WT"
    SAFE_CONVERTED = "SAFE_CONVERTED"
    ASSUMED_CONVERTED = "ASSUMED_CONVERTED"
    NEED_TRACEBACK = "NEED_TRACEBACK"
    CANNOT_CONVERT = "CANNOT_CONVERT"
    FAILED_PARSE = "FAILED_PARSE"

# ============================================================================
# 数据结构
# ============================================================================

@dataclass
class ConcentrationItem:
    row_id: int
    slot_name: str
    original_solute: str
    standardized_solute: str = None
    iupac_name: str = None
    formula: str = None
    cid: str = None
    original_value: float = None
    original_unit: str = None
    canonical_unit: str = None
    unit_family: UnitFamily = None
    phase: str = None
    solvent_identified: str = None
    physical_form_inferred: str = None
    percent_type_inferred: str = None
    mw: float = None
    mw_source: str = None
    density_source: str = None
    density_value: float = None
    suggested_wtpercent: float = None
    suggested_status: ConversionStatus = None
    suggested_reason: str = None
    final_value: float = None
    final_unit: str = None
    final_status: ConversionStatus = None
    final_reason: str = None

# ============================================================================
# 输入管理器
# ============================================================================

class InputManager:
    def __init__(self, config: Dict):
        self.config = config
        self.main_df = None
        self.unit_catalog_df = None
        self.solute_mapping_df = None
        self.md_archive = None
        
    def read_main_excel(self) -> pd.DataFrame:
        """读取主数据库 Excel"""
        path = self.config["main_excel_path"]
        logger.info(f"Reading main Excel: {path}")
        self.main_df = pd.read_excel(path)
        logger.info(f"Loaded {len(self.main_df)} rows")
        return self.main_df
    
    def read_unit_catalog(self) -> pd.DataFrame:
        """读取单位统计表"""
        path = self.config["unit_catalog_path"]
        if not os.path.exists(path):
            logger.warning(f"Unit catalog not found: {path}")
            return None
        logger.info(f"Reading unit catalog: {path}")
        self.unit_catalog_df = pd.read_excel(path)
        return self.unit_catalog_df
    
    def read_solute_mapping(self) -> pd.DataFrame:
        """读取溶质对应表"""
        path = self.config["solute_mapping_path"]
        if not os.path.exists(path):
            logger.warning(f"Solute mapping not found: {path}")
            return None
        logger.info(f"Reading solute mapping: {path}")
        self.solute_mapping_df = pd.read_excel(path)
        return self.solute_mapping_df
    
    def load_md_archive(self) -> Dict[str, str]:
        """加载 MD 文件存档"""
        path = self.config["md_archive_path"]
        if not os.path.exists(path):
            logger.warning(f"MD archive not found: {path}")
            return {}
        
        logger.info(f"Loading MD archive: {path}")
        md_files = {}
        try:
            with zipfile.ZipFile(path, 'r') as zf:
                for name in zf.namelist():
                    if name.endswith('.md'):
                        md_files[name] = zf.read(name).decode('utf-8', errors='ignore')
            logger.info(f"Loaded {len(md_files)} MD files")
        except Exception as e:
            logger.error(f"Failed to load MD archive: {e}")
        
        self.md_archive = md_files
        return md_files

# ============================================================================
# 单位规范化器
# ============================================================================

class UnitNormalizer:
    # 单位到规范形式的映射
    UNIT_MAPPING = {
        # mass_fraction
        'wt%': 'wt%',
        'w/w%': 'wt%',
        'mass%': 'wt%',
        '% by mass': 'wt%',
        '% m/m': 'wt%',
        'wt. %': 'wt%',
        'weight %': 'wt%',
        
        # mass_volume_percent
        'w/v%': 'w/v%',
        'wt/v%': 'w/v%',
        'g/100ml': 'w/v%',
        'g/100 ml': 'w/v%',
        
        # mass_concentration
        'g/l': 'g/L',
        'g/L': 'g/L',
        'mg/l': 'mg/L',
        'mg/L': 'mg/L',
        'μg/l': 'μg/L',
        'μg/L': 'μg/L',
        'ug/l': 'μg/L',
        'ug/L': 'μg/L',
        'ng/l': 'ng/L',
        'ng/L': 'ng/L',
        'mg/ml': 'mg/mL',
        'mg/mL': 'mg/mL',
        
        # molarity
        'M': 'M',
        'mol/L': 'M',
        'mol/l': 'M',
        'mM': 'mM',
        'mmol/L': 'mM',
        'mmol/l': 'mM',
        'μM': 'μM',
        'umol/L': 'μM',
        
        # volume_fraction
        'v/v%': 'v/v%',
        'vol%': 'v/v%',
        'ml/l': 'v/v%',
        'mL/L': 'v/v%',
        
        # ppm family
        'ppm': 'ppm',
        'ppb': 'ppb',
        'g/kg': 'g/kg',
        'mg/kg': 'mg/kg',
    }
    
    @staticmethod
    def normalize_unit_string(unit_str: str) -> str:
        """规范化单位字符串"""
        if not unit_str or pd.isna(unit_str):
            return None
        
        unit_str = str(unit_str).strip().lower()
        
        # 直接查表
        if unit_str in UnitNormalizer.UNIT_MAPPING:
            return UnitNormalizer.UNIT_MAPPING[unit_str]
        
        # 模糊匹配
        for key, val in UnitNormalizer.UNIT_MAPPING.items():
            if key in unit_str or unit_str in key:
                return val
        
        # 裸 %
        if unit_str == '%':
            return '%'
        
        return None
    
    @staticmethod
    def classify_unit_family(canonical_unit: str) -> UnitFamily:
        """分类单位家族"""
        if not canonical_unit:
            return UnitFamily.UNSUPPORTED
        
        canonical_unit = canonical_unit.lower()
        
        if canonical_unit == 'wt%':
            return UnitFamily.MASS_FRACTION
        elif canonical_unit == 'w/v%':
            return UnitFamily.MASS_VOLUME_PERCENT
        elif canonical_unit in ['g/l', 'mg/l', 'μg/l', 'ng/l', 'mg/ml']:
            return UnitFamily.MASS_CONCENTRATION
        elif canonical_unit in ['m', 'mm', 'μm']:
            return UnitFamily.MOLARITY
        elif canonical_unit == 'v/v%':
            return UnitFamily.VOLUME_FRACTION
        elif canonical_unit in ['ppm', 'ppb', 'g/kg', 'mg/kg']:
            return UnitFamily.PPN_FAMILY
        elif canonical_unit == '%':
            return UnitFamily.PERCENT_AMBIGUOUS
        else:
            return UnitFamily.UNSUPPORTED

# ============================================================================
# 溶质解析器
# ============================================================================

class SoluteResolver:
    def __init__(self, solute_mapping_df: pd.DataFrame, config: Dict):
        self.solute_mapping_df = solute_mapping_df
        self.config = config
        self.mw_cache = self._load_mw_cache()
    
    def _load_mw_cache(self) -> Dict:
        """加载分子量缓存"""
        cache_path = self.config["pubchem_cache_path"]
        if os.path.exists(cache_path):
            with open(cache_path, 'r') as f:
                return json.load(f)
        return {}
    
    def _save_mw_cache(self):
        """保存分子量缓存"""
        cache_path = self.config["pubchem_cache_path"]
        os.makedirs(os.path.dirname(cache_path), exist_ok=True)
        with open(cache_path, 'w') as f:
            json.dump(self.mw_cache, f, indent=2)
    
    def match_solute_identity(self, solute_name: str) -> Optional[Dict]:
        """从对应表匹配溶质身份"""
        if not self.solute_mapping_df is not None or not solute_name:
            return None
        
        solute_name_lower = str(solute_name).lower().strip()
        
        # 精确匹配
        for _, row in self.solute_mapping_df.iterrows():
            original_name = str(row.get('Original_Name', '')).lower().strip()
            if original_name == solute_name_lower:
                return {
                    'standardized_solute': row.get('Standardized_Name', solute_name),
                    'iupac_name': row.get('IUPAC_Name'),
                    'formula': row.get('Molecular_Formula'),
                    'cid': row.get('CID'),
                    'mw': row.get('Molecular_Weight'),
                }
        
        # 模糊匹配
        for _, row in self.solute_mapping_df.iterrows():
            original_name = str(row.get('Original_Name', '')).lower().strip()
            if solute_name_lower in original_name or original_name in solute_name_lower:
                return {
                    'standardized_solute': row.get('Standardized_Name', solute_name),
                    'iupac_name': row.get('IUPAC_Name'),
                    'formula': row.get('Molecular_Formula'),
                    'cid': row.get('CID'),
                    'mw': row.get('Molecular_Weight'),
                }
        
        return None
    
    def fetch_molecular_weight(self, iupac_name: str = None, cid: str = None, formula: str = None) -> Tuple[Optional[float], str]:
        """从 PubChem 查询分子量"""
        
        # 先查缓存
        cache_key = iupac_name or cid or formula
        if cache_key and cache_key in self.mw_cache:
            return self.mw_cache[cache_key], "cache"
        
        try:
            # 优先用 CID
            if cid:
                url = f"https://pubchem.ncbi.nlm.nih.gov/rest/pug/compound/cid/{cid}/property/MolecularWeight/JSON"
                resp = requests.get(url, timeout=5)
                if resp.status_code == 200:
                    data = resp.json()
                    mw = data['properties'][0]['MolecularWeight']
                    self.mw_cache[cache_key] = mw
                    self._save_mw_cache()
                    return mw, "pubchem_cid"
            
            # 用 IUPAC 名称查询
            if iupac_name:
                url = f"https://pubchem.ncbi.nlm.nih.gov/rest/pug/compound/name/{iupac_name}/property/MolecularWeight/JSON"
                resp = requests.get(url, timeout=5)
                if resp.status_code == 200:
                    data = resp.json()
                    mw = data['properties'][0]['MolecularWeight']
                    self.mw_cache[cache_key] = mw
                    self._save_mw_cache()
                    return mw, "pubchem_iupac"
        
        except Exception as e:
            logger.warning(f"Failed to fetch MW for {cache_key}: {e}")
        
        return None, "failed"

# ============================================================================
# 密度解析器
# ============================================================================

class DensityResolver:
    def __init__(self, config: Dict):
        self.config = config
        self.solvent_density = config.get("solvent_density", {})
    
    def resolve_solution_density(
        self,
        solute_name: str,
        solvent_name: str,
        concentration_value: float,
        concentration_unit: str,
        phase: str
    ) -> Tuple[Optional[float], str]:
        """
        解析溶液密度
        优先级：
        1. 原文明确给出（需要外部传入）
        2. 本地溶剂密度字典
        3. 稀水溶液用 rho≈1.0 g/mL
        4. 已知低浓度油相可近似 rho≈rho_solvent
        """
        
        # 如果是水相且浓度较低，近似为 1.0
        if phase == "aqueous" and concentration_value and concentration_value < 10:
            return 1.0, "assumed_dilute_aqueous"
        
        # 查本地溶剂密度字典
        solvent_lower = str(solvent_name).lower().strip() if solvent_name else ""
        for key, val in self.solvent_density.items():
            if key in solvent_lower or solvent_lower in key:
                return val, f"solvent_dict_{key}"
        
        # 油相低浓度近似
        if phase == "organic" and concentration_value and concentration_value < 5:
            # 尝试找油相溶剂密度
            if solvent_lower:
                for key, val in self.solvent_density.items():
                    if key in solvent_lower:
                        return val, f"assumed_organic_solvent_{key}"
            return 0.8, "assumed_organic_default"
        
        return None, "unknown"
    
    def get_solvent_density(self, solvent_name: str) -> Tuple[Optional[float], str]:
        """获取纯溶剂密度"""
        solvent_lower = str(solvent_name).lower().strip() if solvent_name else ""
        
        for key, val in self.solvent_density.items():
            if key in solvent_lower or solvent_lower in key:
                return val, f"solvent_dict_{key}"
        
        return None, "unknown"

# ============================================================================
# 上下文追踪器
# ============================================================================

class ContextTracer:
    def __init__(self, md_archive: Dict[str, str]):
        self.md_archive = md_archive
    
    def search_md_files(
        self,
        doi: str = None,
        solute_name: str = None,
        iupac_name: str = None,
        keywords: List[str] = None
    ) -> List[Tuple[str, str]]:
        """
        在 MD 文件中搜索相关文献
        返回 [(filename, content), ...]
        """
        results = []
        keywords = keywords or []
        
        for filename, content in self.md_archive.items():
            content_lower = content.lower()
            
            # DOI 匹配
            if doi and doi.lower() in content_lower:
                results.append((filename, content))
                continue
            
            # 溶质名称匹配
            if solute_name and solute_name.lower() in content_lower:
                results.append((filename, content))
                continue
            
            # IUPAC 名称匹配
            if iupac_name and iupac_name.lower() in content_lower:
                results.append((filename, content))
                continue
            
            # 关键词匹配
            if keywords and any(kw.lower() in content_lower for kw in keywords):
                results.append((filename, content))
        
        return results
    
    def extract_evidence(self, content: str, keywords: List[str], context_lines: int = 3) -> List[str]:
        """
        从文本中提取包含关键词的证据片段
        """
        lines = content.split('\n')
        evidence = []
        
        for i, line in enumerate(lines):
            if any(kw.lower() in line.lower() for kw in keywords):
                start = max(0, i - context_lines)
                end = min(len(lines), i + context_lines + 1)
                snippet = '\n'.join(lines[start:end])
                evidence.append(snippet)
        
        return evidence

# ============================================================================
# LLM 判断器
# ============================================================================

class LLMJudge:
    def __init__(self, config: Dict):
        self.config = config
        self.api_key = config["llm_api_key"]
        self.base_url = config["llm_base_url"]
        self.model = config["llm_model"]
    
    def _call_llm(self, prompt: str) -> str:
        """调用 LLM API（OpenAI-compatible）"""
        try:
            import openai
            client = openai.OpenAI(
                api_key=self.api_key,
                base_url=self.base_url
            )
            
            response = client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "You are a chemistry expert assistant. Answer concisely and precisely."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.3,
                max_tokens=200
            )
            
            return response.choices[0].message.content.strip()
        
        except Exception as e:
            logger.error(f"LLM call failed: {e}")
            return None
    
    def judge_physical_form(self, solute_name: str, iupac_name: str = None) -> str:
        """判断溶质物理形态：solid / liquid / unknown"""
        prompt = f"""
Based on chemistry knowledge, what is the physical form of {solute_name}?
{f'IUPAC name: {iupac_name}' if iupac_name else ''}

Answer with only one word: solid, liquid, or unknown.
"""
        result = self._call_llm(prompt)
        if result:
            result = result.lower().strip()
            if result in ['solid', 'liquid', 'unknown']:
                return result
        return 'unknown'
    
    def judge_phase(self, solute_name: str, slot_name: str, context: str = None) -> str:
        """判断相位：aqueous / organic / unknown"""
        prompt = f"""
In the context of membrane materials, is {solute_name} typically used in aqueous or organic phase?
Slot: {slot_name}
{f'Context: {context}' if context else ''}

Answer with only one word: aqueous, organic, or unknown.
"""
        result = self._call_llm(prompt)
        if result:
            result = result.lower().strip()
            if result in ['aqueous', 'organic', 'unknown']:
                return result
        return 'unknown'
    
    def judge_percent_type(self, percent_value: float, solute_name: str, context: str = None) -> str:
        """判断裸 % 的类型：WT_PERCENT / VOL_PERCENT / WV_PERCENT / UNKNOWN"""
        prompt = f"""
A concentration is given as {percent_value}% for {solute_name}.
{f'Context: {context}' if context else ''}

Is this most likely:
- WT_PERCENT (weight/weight percent)
- VOL_PERCENT (volume/volume percent)
- WV_PERCENT (weight/volume percent)
- UNKNOWN

Answer with only the type name.
"""
        result = self._call_llm(prompt)
        if result:
            result = result.upper().strip()
            if result in ['WT_PERCENT', 'VOL_PERCENT', 'WV_PERCENT', 'UNKNOWN']:
                return result
        return 'UNKNOWN'
    
    def judge_conversion_necessity(self, item: ConcentrationItem) -> bool:
        """判断是否需要回溯原文"""
        prompt = f"""
Given the following concentration data:
- Solute: {item.original_solute}
- Original value: {item.original_value}
- Original unit: {item.original_unit}
- Unit family: {item.unit_family.value if item.unit_family else 'unknown'}
- Phase: {item.phase}

Do we need to check the original paper/document to confirm the conversion?
Answer with only: yes or no.
"""
        result = self._call_llm(prompt)
        return result and result.lower().strip() == 'yes'

# ============================================================================
# 转换引擎
# ============================================================================

class ConversionEngine:
    def __init__(self, config: Dict):
        self.config = config
    
    def convert_to_wtpercent(self, item: ConcentrationItem) -> Tuple[Optional[float], ConversionStatus, str]:
        """
        将浓度转换为 wt%
        返回 (value, status, reason)
        """
        
        if not item.original_value or item.original_value <= 0:
            return None, ConversionStatus.FAILED_PARSE, "Invalid value"
        
        if not item.unit_family:
            return None, ConversionStatus.FAILED_PARSE, "Unknown unit family"
        
        # A. 直接质量分数
        if item.unit_family == UnitFamily.MASS_FRACTION:
            return item.original_value, ConversionStatus.DIRECT_WT, "Already wt%"
        
        # B. ppm / ppb / g/L / mg/L
        if item.unit_family == UnitFamily.PPN_FAMILY:
            if item.canonical_unit == 'ppm':
                # ppm -> wt% ≈ ppm / 10000
                if item.phase == 'aqueous':
                    wtpct = item.original_value / 10000
                    return wtpct, ConversionStatus.SAFE_CONVERTED, "ppm to wt% (aqueous)"
            elif item.canonical_unit == 'ppb':
                # ppb -> wt% ≈ ppb / 10000000
                if item.phase == 'aqueous':
                    wtpct = item.original_value / 10000000
                    return wtpct, ConversionStatus.SAFE_CONVERTED, "ppb to wt% (aqueous)"
            elif item.canonical_unit == 'g/kg':
                # g/kg = wt%
                return item.original_value / 10, ConversionStatus.SAFE_CONVERTED, "g/kg to wt%"
            elif item.canonical_unit == 'mg/kg':
                # mg/kg = ppm
                wtpct = item.original_value / 10000
                return wtpct, ConversionStatus.SAFE_CONVERTED, "mg/kg to wt%"
        
        # C. 质量浓度 (g/L, mg/L, etc.)
        if item.unit_family == UnitFamily.MASS_CONCENTRATION:
            if not item.density_value:
                return None, ConversionStatus.NEED_TRACEBACK, "Need solution density"
            
            if item.canonical_unit == 'g/L':
                # wt% = (g/L) / (10 * rho_solution)
                wtpct = item.original_value / (10 * item.density_value)
                if item.density_source and 'assumed' not in item.density_source:
                    return wtpct, ConversionStatus.SAFE_CONVERTED, f"g/L to wt% (rho from {item.density_source})"
                else:
                    return wtpct, ConversionStatus.ASSUMED_CONVERTED, f"g/L to wt% (assumed rho={item.density_value})"
            
            elif item.canonical_unit == 'mg/L':
                # wt% = (mg/L) / (10000 * rho_solution)
                wtpct = item.original_value / (10000 * item.density_value)
                if item.density_source and 'assumed' not in item.density_source:
                    return wtpct, ConversionStatus.SAFE_CONVERTED, f"mg/L to wt% (rho from {item.density_source})"
                else:
                    return wtpct, ConversionStatus.ASSUMED_CONVERTED, f"mg/L to wt% (assumed rho={item.density_value})"
            
            elif item.canonical_unit == 'mg/mL':
                # mg/mL = g/mL = 1000 g/L
                wtpct = (item.original_value * 1000) / (10 * item.density_value)
                return wtpct, ConversionStatus.ASSUMED_CONVERTED, f"mg/mL to wt% (assumed rho={item.density_value})"
        
        # D. w/v%
        if item.unit_family == UnitFamily.MASS_VOLUME_PERCENT:
            # x w/v% = x g / 100 mL solution
            # wt% = x / rho_solution
            if not item.density_value:
                return None, ConversionStatus.NEED_TRACEBACK, "Need solution density for w/v%"
            
            wtpct = item.original_value / item.density_value
            if item.density_source and 'assumed' not in item.density_source:
                return wtpct, ConversionStatus.SAFE_CONVERTED, f"w/v% to wt% (rho from {item.density_source})"
            else:
                return wtpct, ConversionStatus.ASSUMED_CONVERTED, f"w/v% to wt% (assumed rho={item.density_value})"
        
        # E. 摩尔浓度 (M, mM, etc.)
        if item.unit_family == UnitFamily.MOLARITY:
            if not item.mw or not item.density_value:
                return None, ConversionStatus.NEED_TRACEBACK, "Need MW and solution density for molarity"
            
            # wt% = c * MW / (10 * rho_solution)
            # 先转换到 M
            if item.canonical_unit == 'mM':
                c_M = item.original_value / 1000
            elif item.canonical_unit == 'μM':
                c_M = item.original_value / 1000000
            else:
                c_M = item.original_value
            
            wtpct = (c_M * item.mw) / (10 * item.density_value)
            
            mw_source = item.mw_source or "unknown"
            rho_source = item.density_source or "unknown"
            
            if 'assumed' not in mw_source and 'assumed' not in rho_source:
                return wtpct, ConversionStatus.SAFE_CONVERTED, f"M to wt% (MW from {mw_source}, rho from {rho_source})"
            else:
                return wtpct, ConversionStatus.ASSUMED_CONVERTED, f"M to wt% (MW from {mw_source}, rho from {rho_source})"
        
        # F. v/v%
        if item.unit_family == UnitFamily.VOLUME_FRACTION:
            if not item.density_value:
                return None, ConversionStatus.NEED_TRACEBACK, "Need solute and solution density for v/v%"
            
            # 需要溶质密度，这里假设无法获得
            return None, ConversionStatus.NEED_TRACEBACK, "v/v% requires solute density (not available)"
        
        # G. 裸 %
        if item.unit_family == UnitFamily.PERCENT_AMBIGUOUS:
            if item.percent_type_inferred == 'WT_PERCENT':
                return item.original_value, ConversionStatus.ASSUMED_CONVERTED, "Bare % inferred as wt%"
            elif item.percent_type_inferred == 'VOL_PERCENT':
                return None, ConversionStatus.NEED_TRACEBACK, "Bare % inferred as v/v%, need density"
            elif item.percent_type_inferred == 'WV_PERCENT':
                if not item.density_value:
                    return None, ConversionStatus.NEED_TRACEBACK, "Bare % inferred as w/v%, need density"
                wtpct = item.original_value / item.density_value
                return wtpct, ConversionStatus.ASSUMED_CONVERTED, f"Bare % inferred as w/v%, rho={item.density_value}"
            else:
                return None, ConversionStatus.NEED_TRACEBACK, "Bare % type unknown"
        
        # H. 不支持的单位
        if item.unit_family == UnitFamily.UNSUPPORTED:
            return None, ConversionStatus.CANNOT_CONVERT, f"Unsupported unit: {item.original_unit}"
        
        return None, ConversionStatus.FAILED_PARSE, "Unknown conversion path"

# ============================================================================
# 槽位分割器
# ============================================================================

class SlotSplitter:
    """处理多值字段的分割"""
    
    SLOT_COLUMNS = {
        'aqueous_monomer': ['水相单体', '水相单体浓度', '水相单体浓度_单位'],
        'organic_monomer': ['油相单体', '油相单体浓度', '油相单体浓度_单位'],
        'additive': ['添加剂', '添加剂浓度', '添加剂浓度_单位'],
        'modifier': ['改性剂', '改性剂浓度', '改性剂浓度_单位'],
        'test_nacl': ['测试NaCl浓度', '测试NaCl浓度_单位'],
    }
    
    @staticmethod
    def split_slot_items(row: pd.Series, slot_name: str) -> List[Dict]:
        """
        从一行数据中提取某个 slot 的所有项
        处理多值情况（用 ; 分隔）
        返回 [{'solute': ..., 'value': ..., 'unit': ...}, ...]
        """
        cols = SlotSplitter.SLOT_COLUMNS.get(slot_name, [])
        if len(cols) < 1:
            return []
        
        solute_col = cols[0]
        value_col = cols[1] if len(cols) > 1 else None
        unit_col = cols[2] if len(cols) > 2 else None
        
        solutes = str(row.get(solute_col, '')).strip()
        values = str(row.get(value_col, '')).strip() if value_col else ''
        units = str(row.get(unit_col, '')).strip() if unit_col else ''
        
        if not solutes or solutes.lower() == 'nan':
            return []
        
        # 分割多值
        solute_list = [s.strip() for s in solutes.split(';') if s.strip()]
        value_list = [v.strip() for v in values.split(';') if v.strip()] if values else []
        unit_list = [u.strip() for u in units.split(';') if u.strip()] if units else []
        
        # 补齐列表长度
        while len(value_list) < len(solute_list):
            value_list.append(None)
        while len(unit_list) < len(solute_list):
            unit_list.append(None)
        
        items = []
        for solute, value, unit in zip(solute_list, value_list, unit_list):
            try:
                value_float = float(value) if value else None
            except:
                value_float = None
            
            items.append({
                'solute': solute,
                'value': value_float,
                'unit': unit,
            })
        
        return items

# ============================================================================
# 主处理器
# ============================================================================

class ConcentrationCleaner:
    def __init__(self, config: Dict):
        self.config = config
        self.input_manager = InputManager(config)
        self.unit_normalizer = UnitNormalizer()
        self.solute_resolver = None
        self.density_resolver = DensityResolver(config)
        self.context_tracer = None
        self.llm_judge = LLMJudge(config)
        self.conversion_engine = ConversionEngine(config)
        
        self.main_df = None
        self.concentration_items = []
        self.results = []
    
    def run(self):
        """执行完整清洗流程"""
        logger.info("=" * 80)
        logger.info("Starting Concentration Cleaner")
        logger.info("=" * 80)
        
        # 1. 读取输入
        self._load_inputs()
        
        # 2. 处理每一行
        self._process_rows()
        
        # 3. 生成输出
        self._generate_outputs()
        
        logger.info("=" * 80)
        logger.info("Concentration Cleaner completed")
        logger.info("=" * 80)
    
    def _load_inputs(self):
        """加载所有输入数据"""
        logger.info("Loading inputs...")
        
        self.main_df = self.input_manager.read_main_excel()
        
        solute_mapping_df = self.input_manager.read_solute_mapping()
        if solute_mapping_df is not None:
            self.solute_resolver = SoluteResolver(solute_mapping_df, self.config)
        
        md_archive = self.input_manager.load_md_archive()
        if md_archive:
            self.context_tracer = ContextTracer(md_archive)
        
        unit_catalog_df = self.input_manager.read_unit_catalog()
        if unit_catalog_df is not None:
            logger.info(f"Unit catalog loaded: {len(unit_catalog_df)} rows")
    
    def _process_rows(self):
        """处理每一行数据"""
        logger.info(f"Processing {len(self.main_df)} rows...")
        
        for idx, row in self.main_df.iterrows():
            logger.debug(f"Processing row {idx}")
            
            # 处理 5 个 slot
            for slot_name in ['aqueous_monomer', 'organic_monomer', 'additive', 'modifier', 'test_nacl']:
                items = SlotSplitter.split_slot_items(row, slot_name)
                
                for item_dict in items:
                    item = self._process_single_item(idx, slot_name, item_dict)
                    if item:
                        self.concentration_items.append(item)
    
    def _process_single_item(self, row_id: int, slot_name: str, item_dict: Dict) -> Optional[ConcentrationItem]:
        """处理单个浓度项"""
        
        item = ConcentrationItem(
            row_id=row_id,
            slot_name=slot_name,
            original_solute=item_dict['solute'],
            original_value=item_dict['value'],
            original_unit=item_dict['unit'],
        )
        
        # 1. 规范化单位
        item.canonical_unit = self.unit_normalizer.normalize_unit_string(item.original_unit)
        item.unit_family = self.unit_normalizer.classify_unit_family(item.canonical_unit)
        
        if item.unit_family == UnitFamily.UNSUPPORTED:
            item.suggested_status = ConversionStatus.CANNOT_CONVERT
            item.suggested_reason = f"Unsupported unit: {item.original_unit}"
            return item
        
        # 2. 解析溶质身份
        if self.solute_resolver:
            solute_info = self.solute_resolver.match_solute_identity(item.original_solute)
            if solute_info:
                item.standardized_solute = solute_info.get('standardized_solute')
                item.iupac_name = solute_info.get('iupac_name')
                item.formula = solute_info.get('formula')
                item.cid = solute_info.get('cid')
                if solute_info.get('mw'):
                    item.mw = solute_info['mw']
                    item.mw_source = "solute_mapping"
        
        # 3. 推断物理形态
        if item.standardized_solute or item.iupac_name:
            item.physical_form_inferred = self.llm_judge.judge_physical_form(
                item.standardized_solute or item.original_solute,
                item.iupac_name
            )
        
        # 4. 推断相位
        item.phase = self._infer_phase(slot_name, item)
        
        # 5. 获取分子量（如果需要）
        if item.unit_family == UnitFamily.MOLARITY and not item.mw:
            if item.iupac_name or item.cid:
                mw, source = self.solute_resolver.fetch_molecular_weight(
                    iupac_name=item.iupac_name,
                    cid=item.cid,
                    formula=item.formula
                )
                if mw:
                    item.mw = mw
                    item.mw_source = source
        
        # 6. 解析溶液密度
        solvent_name = self._infer_solvent(slot_name, item)
        item.solvent_identified = solvent_name
        
        rho, rho_source = self.density_resolver.resolve_solution_density(
            item.standardized_solute or item.original_solute,
            solvent_name,
            item.original_value,
            item.original_unit,
            item.phase
        )
        item.density_value = rho
        item.density_source = rho_source
        
        # 7. 处理裸 %
        if item.unit_family == UnitFamily.PERCENT_AMBIGUOUS:
            item.percent_type_inferred = self.llm_judge.judge_percent_type(
                item.original_value,
                item.standardized_solute or item.original_solute
            )
        
        # 8. 尝试转换
        wtpct, status, reason = self.conversion_engine.convert_to_wtpercent(item)
        item.suggested_wtpercent = wtpct
        item.suggested_status = status
        item.suggested_reason = reason
        
        # 9. 判断是否需要回溯
        if status == ConversionStatus.NEED_TRACEBACK:
            if self.context_tracer:
                self._traceback_context(item)
        
        # 10. 最终值
        if status in [ConversionStatus.DIRECT_WT, ConversionStatus.SAFE_CONVERTED, ConversionStatus.ASSUMED_CONVERTED]:
            item.final_value = wtpct
            item.final_unit = 'wt%'
            item.final_status = status
            item.final_reason = reason
        else:
            item.final_status = status
            item.final_reason = reason
        
        return item
    
    def _infer_phase(self, slot_name: str, item: ConcentrationItem) -> str:
        """推断相位"""
        if 'aqueous' in slot_name or 'nacl' in slot_name.lower():
            return 'aqueous'
        elif 'organic' in slot_name:
            return 'organic'
        else:
            # 用 LLM 判断
            return self.llm_judge.judge_phase(
                item.standardized_solute or item.original_solute,
                slot_name
            )
    
    def _infer_solvent(self, slot_name: str, item: ConcentrationItem) -> str:
        """推断溶剂"""
        if 'aqueous' in slot_name or 'nacl' in slot_name.lower():
            return 'water'
        elif 'organic' in slot_name:
            # 需要从原文或其他信息推断
            return None
        return None
    
    def _traceback_context(self, item: ConcentrationItem):
        """回溯原文获取更多信息"""
        if not self.context_tracer:
            return
        
        logger.info(f"Tracing back context for {item.original_solute}...")
        
        keywords = [
            item.original_solute,
            item.standardized_solute,
            item.iupac_name,
            item.formula,
        ]
        keywords = [k for k in keywords if k]
        
        md_results = self.context_tracer.search_md_files(keywords=keywords)
        
        if md_results:
            logger.info(f"Found {len(md_results)} relevant MD files")
            
            # 提取证据片段
            for filename, content in md_results[:3]:  # 只看前 3 个
                evidence = self.context_tracer.extract_evidence(content, keywords)
                if evidence:
                    logger.debug(f"Evidence from {filename}: {evidence[0][:200]}")
    
    def _generate_outputs(self):
        """生成输出 Excel"""
        logger.info("Generating output Excel...")
        
        output_path = self.config["output_excel_path"]
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            # Sheet 1: delivery_main
            self._build_delivery_main(writer)
            
            # Sheet 2: concentration_review
            self._build_concentration_review(writer)
            
            # Sheet 3: summary
            self._build_summary(writer)
        
        logger.info(f"Output saved to {output_path}")
    
    def _build_delivery_main(self, writer):
        """构建 delivery_main sheet"""
        logger.info("Building delivery_main sheet...")
        
        # 复制原始数据
        output_df = self.main_df.copy()
        
        # 为每个 slot 添加 wt% 列
        for slot_name in ['aqueous_monomer', 'organic_monomer', 'additive', 'modifier', 'test_nacl']:
            col_name_value = f"{slot_name}_wt%"
            col_name_status = f"{slot_name}_wt%_status"
            
            output_df[col_name_value] = None
            output_df[col_name_status] = None
        
        # 填充数据
        for item in self.concentration_items:
            row_idx = item.row_id
            slot_name = item.slot_name
            
            col_name_value = f"{slot_name}_wt%"
            col_name_status = f"{slot_name}_wt%_status"
            
            if item.final_value is not None:
                output_df.at[row_idx, col_name_value] = round(item.final_value, 6)
            
            if item.final_status:
                output_df.at[row_idx, col_name_status] = item.final_status.value
        
        output_df.to_excel(writer, sheet_name='delivery_main', index=False)
    
    def _build_concentration_review(self, writer):
        """构建 concentration_review sheet"""
        logger.info("Building concentration_review sheet...")
        
        review_data = []
        for item in self.concentration_items:
            review_data.append({
                'row_id': item.row_id,
                'slot_name': item.slot_name,
                'original_solute': item.original_solute,
                'standardized_solute': item.standardized_solute,
                'iupac_name': item.iupac_name,
                'formula': item.formula,
                'cid': item.cid,
                'original_value': item.original_value,
                'original_unit': item.original_unit,
                'canonical_unit': item.canonical_unit,
                'unit_family': item.unit_family.value if item.unit_family else None,
                'phase': item.phase,
                'solvent_identified': item.solvent_identified,
                'physical_form_inferred': item.physical_form_inferred,
                'percent_type_inferred': item.percent_type_inferred,
                'mw': item.mw,
                'mw_source': item.mw_source,
                'density_value': item.density_value,
                'density_source': item.density_source,
                'suggested_wtpercent': item.suggested_wtpercent,
                'suggested_status': item.suggested_status.value if item.suggested_status else None,
                'suggested_reason': item.suggested_reason,
                'final_value': item.final_value,
                'final_unit': item.final_unit,
                'final_status': item.final_status.value if item.final_status else None,
                'final_reason': item.final_reason,
            })
        
        review_df = pd.DataFrame(review_data)
        review_df.to_excel(writer, sheet_name='concentration_review', index=False)
    
    def _build_summary(self, writer):
        """构建 summary sheet"""
        logger.info("Building summary sheet...")
        
        summary_data = []
        
        # 按状态统计
        status_counts = {}
        for item in self.concentration_items:
            status = item.final_status.value if item.final_status else 'UNKNOWN'
            status_counts[status] = status_counts.get(status, 0) + 1
        
        summary_data.append(['Status Distribution', ''])
        for status, count in sorted(status_counts.items()):
            summary_data.append([status, count])
        
        summary_data.append(['', ''])
        
        # 按单位家族统计
        family_counts = {}
        for item in self.concentration_items:
            family = item.unit_family.value if item.unit_family else 'UNKNOWN'
            family_counts[family] = family_counts.get(family, 0) + 1
        
        summary_data.append(['Unit Family Distribution', ''])
        for family, count in sorted(family_counts.items()):
            summary_data.append([family, count])
        
        summary_data.append(['', ''])
        
        # 总体统计
        total = len(self.concentration_items)
        converted = sum(1 for item in self.concentration_items if item.final_value is not None)
        coverage = (converted / total * 100) if total > 0 else 0
        
        summary_data.append(['Overall Statistics', ''])
        summary_data.append(['Total items', total])
        summary_data.append(['Successfully converted', converted])
        summary_data.append(['Conversion coverage (%)', round(coverage, 2)])
        
        summary_df = pd.DataFrame(summary_data, columns=['Metric', 'Value'])
        summary_df.to_excel(writer, sheet_name='summary', index=False)

# ============================================================================
# 主函数
# ============================================================================

def main():
    """主入口"""
    cleaner = ConcentrationCleaner(CONFIG)
    cleaner.run()

if __name__ == '__main__':
    main()