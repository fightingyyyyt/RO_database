import re
import json
from collections import Counter, defaultdict
from pathlib import Path

import pandas as pd


# =========================
# 1. 你可以先维护一份种子词典
# =========================
SEED_DICTIONARY = [
    {"alias": "MPD", "canonical_name": "m-phenylenediamine", "category": "monomer"},
    {"alias": "PIP", "canonical_name": "piperazine", "category": "monomer"},
    {"alias": "PEI", "canonical_name": "polyethyleneimine", "category": "monomer"},
    {"alias": "EDA", "canonical_name": "ethane-1,2-diamine", "category": "monomer"},
    {"alias": "TMC", "canonical_name": "benzene-1,3,5-tricarbonyl trichloride", "category": "monomer"},
    {"alias": "TPC", "canonical_name": "terephthaloyl chloride", "category": "monomer"},
    {"alias": "PSF", "canonical_name": "polysulfone", "category": "substrate"},
    {"alias": "PES", "canonical_name": "polyethersulfone", "category": "substrate"},
    {"alias": "PAN", "canonical_name": "polyacrylonitrile", "category": "substrate"},
    {"alias": "PVDF", "canonical_name": "poly(vinylidene fluoride)", "category": "substrate"},
    {"alias": "CA", "canonical_name": "cellulose acetate", "category": "substrate"},
    {"alias": "CTA", "canonical_name": "cellulose triacetate", "category": "substrate"},
    {"alias": "PA", "canonical_name": "polyamide", "category": "polymer"},
    {"alias": "PEG", "canonical_name": "polyethylene glycol", "category": "additive"},
    {"alias": "PVP", "canonical_name": "polyvinylpyrrolidone", "category": "additive"},
    {"alias": "SDS", "canonical_name": "sodium dodecyl sulfate", "category": "additive"},
    {"alias": "TEA", "canonical_name": "triethylamine", "category": "additive"},
    {"alias": "DMAc", "canonical_name": "N,N-dimethylacetamide", "category": "solvent"},
    {"alias": "DMF", "canonical_name": "N,N-dimethylformamide", "category": "solvent"},
    {"alias": "DMSO", "canonical_name": "dimethyl sulfoxide", "category": "solvent"},
    {"alias": "NMP", "canonical_name": "1-methyl-2-pyrrolidinone", "category": "solvent"},
]


# =========================
# 2. 一些辅助函数
# =========================
def normalize_text(text: str) -> str:
    if pd.isna(text):
        return ""
    text = str(text).strip()
    text = text.replace("（", "(").replace("）", ")")
    text = text.replace("，", ",").replace("；", ";").replace("：", ":")
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def looks_like_abbreviation(token: str) -> bool:
    """
    判断一个 token 是否像缩写：
    - 长度 2~10
    - 主要由大写字母 / 数字 / 连字符组成
    - 至少包含两个大写字母
    """
    if not token:
        return False
    token = token.strip()

    if len(token) < 2 or len(token) > 10:
        return False

    if not re.fullmatch(r"[A-Za-z0-9\-]+", token):
        return False

    uppercase_count = sum(1 for c in token if c.isupper())
    if uppercase_count < 2:
        return False

    return True


def clean_full_name(text: str) -> str:
    """
    清理作为 full name 的文本，但不要破坏内部括号结构。
    只去掉末尾明显的注释性内容。
    """
    text = normalize_text(text)

    # 去掉常见上下文词
    text = re.sub(
        r"\b(aqueous solution|solution|suspension|dispersion|membrane|substrate)\b",
        "",
        text,
        flags=re.IGNORECASE,
    )

    # 去掉多余空格和标点
    text = re.sub(r"\s+", " ", text).strip(" ,;:-")
    return text


def mine_fullname_abbr_pairs(text: str):
    """
    从文本中挖掘：
    Full Name (ABBR)
    """
    text = normalize_text(text)
    pairs = []

    # 匹配末尾括号缩写: full name (ABBR)
    # 例如: 1,4-cyclohexanediamine (CHDA)
    m = re.match(r"^(.*?)[\s]*\(([A-Za-z0-9\-]{2,10})\)\s*$", text)
    if m:
        full_name = clean_full_name(m.group(1))
        abbr = m.group(2).strip()
        if looks_like_abbreviation(abbr) and len(full_name) >= 4:
            pairs.append((abbr, full_name))

    return pairs


def extract_abbreviation_tokens(text: str):
    """
    从文本中提取所有可能的缩写 token
    """
    text = normalize_text(text)
    tokens = re.findall(r"\b[A-Za-z][A-Za-z0-9\-]{1,9}\b", text)
    result = []
    for t in tokens:
        if looks_like_abbreviation(t):
            result.append(t)
    return result


def infer_category(name: str) -> str:
    lower = name.lower()

    if "poly" in lower:
        return "polymer"
    if any(k in lower for k in ["chloride", "diamine", "amine", "acid", "acyl", "anhydride"]):
        return "monomer"
    if any(k in lower for k in ["sulfone", "vinylidene fluoride", "acrylonitrile", "cellulose"]):
        return "substrate"
    if any(k in lower for k in ["glycol", "surfactant", "triethylamine"]):
        return "additive"
    return "other"


# =========================
# 3. 主逻辑
# =========================
def build_alias_dictionary(input_excel: str, output_excel: str, output_json: str):
    df = pd.read_excel(input_excel)

    first_col = df.columns[0]
    names = df[first_col].dropna().astype(str).tolist()

    pair_counter = Counter()
    pair_examples = defaultdict(list)
    abbr_counter = Counter()

    for raw in names:
        text = normalize_text(raw)

        # 3.1 挖 full name (ABBR)
        pairs = mine_fullname_abbr_pairs(text)
        for abbr, full_name in pairs:
            pair_counter[(abbr, full_name)] += 1
            if len(pair_examples[(abbr, full_name)]) < 3:
                pair_examples[(abbr, full_name)].append(text)

        # 3.2 提取高频缩写 token
        abbrs = extract_abbreviation_tokens(text)
        abbr_counter.update(abbrs)

    # =========================
    # 4. 组装自动挖掘结果
    # =========================
    auto_rows = []
    for (abbr, full_name), freq in pair_counter.most_common():
        auto_rows.append({
            "alias": abbr,
            "canonical_name": full_name,
            "category": infer_category(full_name),
            "source": "auto_mined",
            "frequency": freq,
            "status": "pending",
            "note": "; ".join(pair_examples[(abbr, full_name)]),
        })

    auto_df = pd.DataFrame(auto_rows)

    # 同一个 alias 可能对应多个 full_name，优先取频次最高的
    if not auto_df.empty:
        auto_df = (
            auto_df.sort_values(["alias", "frequency"], ascending=[True, False])
                  .drop_duplicates(subset=["alias"], keep="first")
        )

    # =========================
    # 5. 高频缩写候选（没有 full name 的）
    # =========================
    known_aliases = set(auto_df["alias"].tolist()) if not auto_df.empty else set()
    seed_aliases = {row["alias"] for row in SEED_DICTIONARY}

    abbr_candidate_rows = []
    for abbr, freq in abbr_counter.most_common():
        if freq < 3:
            continue
        if abbr in known_aliases:
            continue
        if abbr in seed_aliases:
            continue

        abbr_candidate_rows.append({
            "alias": abbr,
            "canonical_name": "",
            "category": "unknown",
            "source": "high_freq_abbr",
            "frequency": freq,
            "status": "pending",
            "note": "Need manual confirmation",
        })

    abbr_candidate_df = pd.DataFrame(abbr_candidate_rows)

    # =========================
    # 6. 种子词典
    # =========================
    seed_rows = []
    for row in SEED_DICTIONARY:
        seed_rows.append({
            "alias": row["alias"],
            "canonical_name": row["canonical_name"],
            "category": row["category"],
            "source": "seed_manual",
            "frequency": "",
            "status": "confirmed",
            "note": "",
        })
    seed_df = pd.DataFrame(seed_rows)

    # =========================
    # 7. 合并总词典
    # 优先级：seed_manual > auto_mined > high_freq_abbr
    # =========================
    all_df = pd.concat([seed_df, auto_df, abbr_candidate_df], ignore_index=True)

    source_priority = {"seed_manual": 0, "auto_mined": 1, "high_freq_abbr": 2}
    all_df["source_priority"] = all_df["source"].map(source_priority).fillna(99)

    all_df = (
        all_df.sort_values(["alias", "source_priority", "frequency"], ascending=[True, True, False])
              .drop_duplicates(subset=["alias"], keep="first")
              .drop(columns=["source_priority"])
              .reset_index(drop=True)
    )

    # =========================
    # 8. 输出 Excel
    # =========================
    with pd.ExcelWriter(output_excel, engine="openpyxl") as writer:
        all_df.to_excel(writer, sheet_name="merged_dictionary", index=False)
        seed_df.to_excel(writer, sheet_name="seed_dictionary", index=False)
        auto_df.to_excel(writer, sheet_name="auto_mined_pairs", index=False)
        abbr_candidate_df.to_excel(writer, sheet_name="high_freq_abbr", index=False)

    # =========================
    # 9. 输出 JSON
    # =========================
    json_records = all_df.fillna("").to_dict(orient="records")
    with open(output_json, "w", encoding="utf-8") as f:
        json.dump(json_records, f, ensure_ascii=False, indent=2)

    print(f"Done! Excel saved to: {output_excel}")
    print(f"Done! JSON saved to: {output_json}")
    print(f"Total dictionary size: {len(all_df)}")
    print(f"Auto-mined pairs: {len(auto_df)}")
    print(f"High-frequency abbreviation candidates: {len(abbr_candidate_df)}")


if __name__ == "__main__":
    input_excel = "./dictionary/step1.xlsx"
    output_excel = "./dictionary/alias_dictionary_candidates.xlsx"
    output_json = "./dictionary/alias_dictionary_candidates.json"

    build_alias_dictionary(input_excel, output_excel, output_json)