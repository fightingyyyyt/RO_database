import re
import pandas as pd

# ====== 配置区：按你的表改这里 ======
INPUT_FILE = "./step1/test7_part_polymer/step0.xlsx"
OUTPUT_FILE = "./step1/test7_part_polymer/merged_one_column.xlsx"

# 你截图里这些列名（不存在就自动退化为“使用全部列”）
COLUMNS_TO_USE = [ "水相单体", "油相单体", "有机溶剂", "添加剂", "改性剂"]
# ====================================

# 识别“纯数值”（支持整数/小数/科学计数法）
NUMERIC_RE = re.compile(r"^[+-]?(\d+(\.\d*)?|\.\d+)([eE][+-]?\d+)?$")

# 去掉 token 首尾多余标点（避免出现 “，-COOH” 这种）
EDGE_PUNCT_RE = re.compile(r"^[\s,，;；、:：\.\-–—]+|[\s,，;；、:：\.\-–—]+$")

def clean_token(x: str) -> str:
    x = "" if x is None else str(x)
    x = x.strip()
    x = re.sub(r"\s+", " ", x)          # 合并空格
    x = EDGE_PUNCT_RE.sub("", x)        # 剥掉首尾标点
    return x.strip()

def is_numeric_only(x: str) -> bool:
    return bool(NUMERIC_RE.fullmatch(x.strip()))

def is_symbol_only(x: str) -> bool:
    """
    只含符号：不包含任何 英文字母/数字/中文
    """
    x = x.strip()
    return re.search(r"[A-Za-z0-9\u4e00-\u9fff]", x) is None

def main():
    df = pd.read_excel(INPUT_FILE)

    # 1) 选列：指定列存在就用指定列，否则用全部列
    cols = [c for c in COLUMNS_TO_USE if c in df.columns]
    if not cols:
        cols = list(df.columns)

    # 2) 合并成长表（每个单元格一行）
    long_df = df[cols].melt(var_name="Source_Field", value_name="Raw").dropna()

    # 3) 按中英文分号拆分，并展开为一列
    # 支持连续分号：A；；B
    split_series = long_df["Raw"].astype(str).str.split(r"[;；]+")
    exploded = pd.DataFrame({"Token": split_series}).explode("Token", ignore_index=True)

    # 4) 清理空白
    exploded["Token"] = exploded["Token"].map(clean_token)
    exploded = exploded[exploded["Token"] != ""]
    exploded = exploded[~exploded["Token"].isin(["nan", "None", "NULL"])]

    # 5) 去重（先去重！保留首次出现）
    exploded["dedup_key"] = (
        exploded["Token"]
        .str.lower()
        .str.replace(r"\s+", " ", regex=True)
        .str.strip()
    )
    exploded = exploded.drop_duplicates(subset=["dedup_key"], keep="first")

    # 6) 再过滤：纯数字 / 纯符号（按你要求“去重后再过滤”）
    exploded = exploded[~exploded["Token"].map(is_numeric_only)]
    exploded = exploded[~exploded["Token"].map(is_symbol_only)]

    # 7) 输出：一列
    out = exploded[["Token"]].rename(columns={"Token": "Name"}).reset_index(drop=True)
    out.to_excel(OUTPUT_FILE, index=False)

    print(f"Done! rows = {len(out)}")
    print(f"Saved to: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()