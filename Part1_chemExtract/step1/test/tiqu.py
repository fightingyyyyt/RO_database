import pandas as pd
import numpy as np

# 读取表格数据（支持Excel、CSV等）
df = pd.read_excel('./test/step1.xlsx')  # 或 pd.read_csv('your_file.csv')

# 方法1：随机提取80行
random_80 = df.sample(n=80, random_state=42)  # random_state确保可重复结果

# 方法2：随机提取80行（不放回）
random_80_no_replace = df.sample(n=80, replace=False)

# 方法3：如果数据不足80行，允许重复抽取
if len(df) < 80:
    random_80 = df.sample(n=80, replace=True)

# 保存结果
random_80.to_excel('random_80_rows.xlsx', index=False)