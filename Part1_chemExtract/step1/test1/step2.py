import pandas as pd
import pubchempy as pcp
from deep_translator import GoogleTranslator
import requests
from urllib.parse import quote
import time
import re
import os
from difflib import SequenceMatcher

class ChemicalNormalizer:
    def __init__(self):
        self.translator = GoogleTranslator(source='auto', target='en')
        self.common_keywords = {
            'acid': ['acid', '酸'],
            'salt': ['salt', '盐'],
            'oxide': ['oxide', '氧化物'],
            'chloride': ['chloride', '氯化物'],
            'bromide': ['bromide', '溴化物'],
            'nitrate': ['nitrate', '硝酸盐'],
            'sulfate': ['sulfate', '硫酸盐'],
            'phosphate': ['phosphate', '磷酸盐'],
        }
        
    def preprocess_name(self, raw_name):
        """预处理：去除浓度、括号注释等干扰信息"""
        if pd.isna(raw_name):
            return None
            
        text = str(raw_name).strip()
        
        # 去除常见的浓度标记
        text = re.sub(r'\d+(\.\d+)?%', '', text)
        text = re.sub(r'\d+(\.\d+)?\s*(mg|g|ml|mM|μM|nM|L|nm)', '', text, flags=re.IGNORECASE)
        
        # 去除"溶液"、"solution"等词
        text = re.sub(r'溶液|solution|水溶液|aqueous|suspension|dispersion', '', text, flags=re.IGNORECASE)
        
        # 处理括号：保留括号内的英文，去除中文注释
        bracket_match = re.search(r'\(([A-Za-z\s\-,\.0-9]+)\)', text)
        if bracket_match:
            english_part = bracket_match.group(1).strip()
            if len(english_part) > 2:
                text = english_part
        
        # 去除所有括号及其内容
        text = re.sub(r'[\(（].*?[\)）]', '', text)
        
        # 去除商品标记
        text = re.sub(r'™|®|©', '', text)
        
        # 去除型号信息（Model No., etc）
        text = re.sub(r'Model\s+No\..*', '', text, flags=re.IGNORECASE)
        
        return text.strip()
    
    def translate_to_english(self, text):
        """翻译中文到英文"""
        try:
            if any('\u4e00' <= char <= '\u9fff' for char in text):
                translated = self.translator.translate(text)
                return translated
        except Exception as e:
            pass
        return text
    
    def extract_main_component(self, name):
        """从复杂名称中提取主要成分"""
        # 移除修饰词
        modifiers = ['modified', 'crosslinked', 'sulfonated', 'nitro', 'anhydrous', 
                     'free', 'mixed', 'aromatic', 'thin-film', 'composite', 'support',
                     'membrane', 'nanoparticles', 'netting', 'fabric', 'substrate',
                     'bilayer', 'self-standing', 'amorphous', 'porous']
        
        result = name
        for mod in modifiers:
            result = re.sub(rf'\b{mod}\b', '', result, flags=re.IGNORECASE)
        
        # 移除 @ 符号后的内容（复合材料）
        result = re.sub(r'@.*', '', result)
        
        # 移除 / 后的内容（混合物）
        result = re.sub(r'/.*', '', result)
        
        # 移除数字和特殊后缀
        result = re.sub(r'-\d+[A-Z]?$', '', result)
        result = re.sub(r'\s+\d+$', '', result)
        
        return result.strip()
    
    def query_pubchem_by_name(self, name):
        """直接通过名称查询 PubChem"""
        try:
            compounds = pcp.get_compounds(name, 'name')
            if compounds:
                return compounds[0]
        except:
            pass
        return None
    
    def query_cir_smiles(self, name):
        """使用 CIR 服务将名称转为 SMILES"""
        try:
            url = f"https://cactus.nci.nih.gov/chemical/structure/{quote(name)}/smiles"
            response = requests.get(url, timeout=5)
            if response.status_code == 200 and response.text.strip():
                return response.text.strip()
        except:
            pass
        return None
    
    def fuzzy_match_pubchem(self, name):
        """模糊匹配：尝试查询相似的化学物名称"""
        try:
            # 尝试搜索相关化合物
            compounds = pcp.get_compounds(name, 'name')
            if compounds:
                return compounds[0]
            
            # 如果直接搜索失败，尝试搜索部分名称
            words = name.split()
            for word in sorted(words, key=len, reverse=True):
                if len(word) > 3:
                    try:
                        compounds = pcp.get_compounds(word, 'name')
                        if compounds:
                            return compounds[0]
                    except:
                        pass
        except:
            pass
        return None
    
    def normalize_single_chemical(self, raw_name):
        """完整的标准化流程，包含多层次查询"""
        result = {
            'Original_Name': raw_name,
            'Molecular_Formula': None,
            'IUPAC_Name': None,
            'Status': 'Failed',
            'Attempted_Name': None
        }
        
        # 步骤 1: 预处理
        cleaned = self.preprocess_name(raw_name)
        if not cleaned or len(cleaned) < 2:
            result['Status'] = 'Invalid_Input'
            print(f"   处理: {raw_name[:35]}... -> ❌ 输入无效")
            return result
        
        # 步骤 2: 翻译（如果是中文）
        english_name = self.translate_to_english(cleaned)
        result['Attempted_Name'] = english_name
        print(f"   处理: {raw_name[:35]}... -> {english_name[:35]}...", end=" ")
        
        compound = None
        
        # 步骤 3: 尝试直接查询 PubChem（方法1）
        compound = self.query_pubchem_by_name(english_name)
        if compound:
            result['Status'] = 'Found_Direct'
            result['Molecular_Formula'] = compound.molecular_formula
            result['IUPAC_Name'] = compound.iupac_name if compound.iupac_name else (compound.synonyms[0] if compound.synonyms else None)
            print("✅ (Direct)")
            return result
        
        # 步骤 4: 尝试 CIR 服务（方法2）
        smiles = self.query_cir_smiles(english_name)
        if smiles:
            try:
                compounds = pcp.get_compounds(smiles, 'smiles')
                if compounds:
                    compound = compounds[0]
                    result['Status'] = 'Found_via_CIR'
                    result['Molecular_Formula'] = compound.molecular_formula
                    result['IUPAC_Name'] = compound.iupac_name if compound.iupac_name else (compound.synonyms[0] if compound.synonyms else None)
                    print("✅ (CIR)")
                    return result
            except:
                pass
        
        # 步骤 5: 尝试去除特殊字符后重新查询（方法3）
        simplified_name = re.sub(r'[^\w\s]', '', english_name).strip()
        if simplified_name != english_name and len(simplified_name) > 2:
            compound = self.query_pubchem_by_name(simplified_name)
            if compound:
                result['Status'] = 'Found_Simplified'
                result['Attempted_Name'] = simplified_name
                result['Molecular_Formula'] = compound.molecular_formula
                result['IUPAC_Name'] = compound.iupac_name if compound.iupac_name else (compound.synonyms[0] if compound.synonyms else None)
                print("✅ (Simplified)")
                return result
        
        # 步骤 6: 提取主要成分并重新查询（方法4）
        main_component = self.extract_main_component(english_name)
        if main_component != english_name and len(main_component) > 2:
            compound = self.query_pubchem_by_name(main_component)
            if compound:
                result['Status'] = 'Found_MainComponent'
                result['Attempted_Name'] = main_component
                result['Molecular_Formula'] = compound.molecular_formula
                result['IUPAC_Name'] = compound.iupac_name if compound.iupac_name else (compound.synonyms[0] if compound.synonyms else None)
                print("✅ (Main Component)")
                return result
            
            # 尝试 CIR
            smiles = self.query_cir_smiles(main_component)
            if smiles:
                try:
                    compounds = pcp.get_compounds(smiles, 'smiles')
                    if compounds:
                        compound = compounds[0]
                        result['Status'] = 'Found_MainComponent_CIR'
                        result['Attempted_Name'] = main_component
                        result['Molecular_Formula'] = compound.molecular_formula
                        result['IUPAC_Name'] = compound.iupac_name if compound.iupac_name else (compound.synonyms[0] if compound.synonyms else None)
                        print("✅ (Main Component CIR)")
                        return result
                except:
                    pass
        
        # 步骤 7: 模糊匹配（方法5）
        compound = self.fuzzy_match_pubchem(english_name)
        if compound:
            result['Status'] = 'Found_Fuzzy'
            result['Molecular_Formula'] = compound.molecular_formula
            result['IUPAC_Name'] = compound.iupac_name if compound.iupac_name else (compound.synonyms[0] if compound.synonyms else None)
            print("✅ (Fuzzy Match)")
            return result
        
        # 所有方法都失败了
        result['Status'] = 'Not_Found'
        print("❌ (Not Found)")
        return result

def main():
    input_file = 'step1.xlsx'
    output_file = 'step2.xlsx'
    
    if not os.path.exists(input_file):
        print(f"错误: 找不到文件 {input_file}")
        return
    
    print(f"正在读取 {input_file}...")
    df = pd.read_excel(input_file)
    
    column_name = df.columns[0]
    chemical_names = df[column_name].tolist()
    
    print(f"共找到 {len(chemical_names)} 个化学物名称")
    print("开始标准化处理...\n")
    
    normalizer = ChemicalNormalizer()
    
    results = []
    for i, name in enumerate(chemical_names, 1):
        print(f"[{i}/{len(chemical_names)}]", end=" ")
        result = normalizer.normalize_single_chemical(name)
        results.append(result)
        time.sleep(0.3)
    
    output_df = pd.DataFrame(results)
    output_df.to_excel(output_file, index=False)
    
    # 统计结果
    success_count = len(output_df[output_df['Molecular_Formula'].notna()])
    failed_df = output_df[output_df['Molecular_Formula'].isna()]
    
    print(f"\n{'='*60}")
    print(f"处理完成！")
    print(f"成功: {success_count}/{len(chemical_names)}")
    print(f"失败: {len(failed_df)}/{len(chemical_names)}")
    print(f"结果已保存至: {output_file}")
    print(f"{'='*60}")
    
    # 输出失败的物质列表
    if len(failed_df) > 0:
        print("\n失败的物质列表:")
        for idx, row in failed_df.iterrows():
            print(f"  - {row['Original_Name']} (尝试: {row['Attempted_Name']}, 原因: {row['Status']})")

if __name__ == "__main__":
    main()