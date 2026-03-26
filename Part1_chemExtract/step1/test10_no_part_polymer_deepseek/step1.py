import pandas as pd
import re
from pathlib import Path

# 读取Excel文件
df = pd.read_excel('step0.xlsx')

# 收集所有化学物质
chemicals = []

# 遍历所有单元格
for col in df.columns:
    for value in df[col]:
        if pd.notna(value):
            # 用中英文分号分割
            items = re.split(r'[;；]', str(value))
            # 清理空白并添加
            for item in items:
                item = item.strip()
                # 去掉单引号
                item = item.replace("'", "")
                # 检查是否为纯数字，如果是则跳过
                if item and not item.isdigit():
                    chemicals.append(item)

# 去重并排序
chemicals = sorted(list(set(chemicals)))

# 创建新的DataFrame
result_df = pd.DataFrame({'化学物质': chemicals})

# 保存到Excel
base_dir = Path(__file__).resolve().parent
save_path = base_dir / "step1.xlsx"

result_df.to_excel(save_path, index=False)
print("保存到：", save_path)