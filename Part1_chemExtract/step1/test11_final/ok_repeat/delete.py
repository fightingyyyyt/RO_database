from pathlib import Path
import pandas as pd
'''
帮我写一个代码，在这一个表格里面，删除RowIndex和CID相同的重复项。也就是说遇到RowIndex和CID相同的，删除其余的项，最后只保留一个。
'''

# 当前 py 文件所在目录
base_dir = Path(__file__).resolve().parent

# 输入输出文件
input_file = base_dir / "task1_chemical_entities_final_0310_ok.xlsx"
output_file = base_dir / "task1_chemical_entities_final_0310_ok_dedup.xlsx"

print("当前脚本目录：", base_dir)
print("准备读取文件：", input_file)

# 如果文件不存在，就把当前目录下所有 xlsx 打印出来
if not input_file.exists():
    print("\n未找到目标文件。当前目录下的 xlsx 文件有：")
    for f in base_dir.glob("*.xlsx"):
        print(" -", f.name)
    raise FileNotFoundError(f"\n找不到文件：{input_file}")

# 读取 Excel
df = pd.read_excel(input_file)

# 删除 RowIndex 和 CID 都相同的重复项，只保留第一条
df_dedup = df.drop_duplicates(subset=["RowIndex", "CID"], keep="first")

# 保存结果
df_dedup.to_excel(output_file, index=False)

print(f"\n原始行数: {len(df)}")
print(f"去重后行数: {len(df_dedup)}")
print(f"结果已保存到: {output_file}")