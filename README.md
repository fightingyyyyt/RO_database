# RO_database

反渗透（RO）相关数据处理与浓度规范化脚本仓库。根目录下按子任务分文件夹。

## 目录结构

| 路径 | 说明 |
|------|------|
| `Part1_chemExtract/` | 任务一：化学实体 / 文献与表格抽取等相关脚本与数据 |
| `Part2_clean/` | 任务二：膜片浓度字段清洗、单位换算、MD 回溯与交付表生成 |

## 任务二（Part2_clean）— 浓度清洗主流程

主脚本位于 `Part2_clean/test3/`，当前推荐使用 **`test3_6.py`**（在 `test3_5` 基础上增强数值解析、回溯与 Excel 稳健写出等逻辑）。

### 环境依赖

```bash
pip install pandas openpyxl requests tqdm
```

（若使用 DeepSeek 等 LLM 辅助，需配置环境变量 `DEEPSEEK_API_KEY` 等，见脚本内 `AppConfig`。）

### 运行方式

在仓库中进入脚本目录后执行（工作目录影响相对路径）：

```bash
cd Part2_clean/test3
python test3_6.py
```

输入/输出路径在脚本顶部 **`AppConfig`** 中配置，例如：

- 主表 CSV/Excel
- `test3/output/`：导出 Excel、`review_checkpoint.pkl` 等
- `test3/cache_*.json`：PubChem / LLM 等缓存（可加速重复运行）

### 输出说明（典型）

- **`delivery_main`**：主表 + 规范化 wt% 及简化状态列  
- **`concentration_review_core` / `concentration_review_audit`**：审查明细（core 精简、audit 调试）  
- **`summary`**、**`unit_catalog`**

具体列名与逻辑以脚本内实现为准。

## Git 与大体量文件

- 单个文件 **>100MB** 无法直接推送到 GitHub；大体积中间结果、整库 `md_extracted`、运行输出等建议留在本地或通过 **`.gitignore`** 排除。  
- 根目录 `.gitignore` 已忽略部分生成物与缓存目录，提交前可用 `git status` 检查。

## 许可证与引用

（如需发表论文或开源，请在此补充许可证与引用方式。）
