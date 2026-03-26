import os
import re
import json
import time
import math
import argparse
from typing import Dict, Any, List, Optional

import pandas as pd
import requests
from tqdm import tqdm
from openai import OpenAI

'''
1.脚本先从你指定的 Excel（--input，可选 --col）读入化学/材料名称，
去掉空行并对名称去重；

2.然后用 DeepSeek 按批次把每个名称做“清洗 + 标准化 + 分类”，并判断 是否应该查 
PubChem（小分子/无机盐才查，聚合物/复合材料/品牌名一般不查），结果会写入缓存；

3.接着只对 should_query_pubchem=True 的条目去 PubChem 查询 CID、分子式、
IUPAC、SMILES 等信息（也有缓存）；

4.最后输出 3 个表：full_annotated.xlsx（全量带分类+PubChem结果）、resolved_compounds.xlsx
（成功查到的小分子）、materials_queue.xlsx（聚合物/材料/品牌/查不到的待后续处理）。

LLM 负责：清洗 + 分类 + 决定查不查 PubChem
PubChem 负责：对小分子回填结构信息（CID/Formula/IUPAC/SMILES）
聚合物/复合材料/品牌名通常不查 PubChem → 会进 materials_queue
有缓存：中断了也能续跑，不会重复花钱
'''
# =============================
# Config
# =============================
PUBCHEM_BASE = "https://pubchem.ncbi.nlm.nih.gov/rest/pug"

DEFAULT_MODEL = os.getenv("DEEPSEEK_MODEL", "deepseek-chat")  # deepseek-chat / deepseek-reasoner
DEFAULT_BATCH_SIZE = int(os.getenv("LLM_BATCH_SIZE", "25"))
DEFAULT_SLEEP_BETWEEN_PUBCHEM = float(os.getenv("PUBCHEM_SLEEP", "0.12"))
DEFAULT_MAX_RETRIES = int(os.getenv("MAX_RETRIES", "5"))

ENTITY_TYPES = [
    "small_molecule",
    "salt_or_inorganic",
    "polymer",
    "composite_material",
    "surface_modified_material",
    "nanomaterial",
    "brand_or_product",
    "membrane_material",
    "unknown",
]
LANGS = ["zh", "en", "mixed", "unknown"]

REQUIRED_KEYS = [
    "raw_name", "clean_name", "canonical_name",
    "entity_type", "language", "modifier", "components",
    "should_query_pubchem", "pubchem_query_name",
    "confidence", "review_flag", "notes"
]


# =============================
# Utility
# =============================
def safe_mkdir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def normalize_text(x: Any) -> str:
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return ""
    s = str(x).strip()
    s = re.sub(r"\s+", " ", s)
    return s


def chunk_list(lst: List[Any], n: int) -> List[List[Any]]:
    return [lst[i:i + n] for i in range(0, len(lst), n)]


def load_jsonl_cache(path: str) -> Dict[str, Any]:
    cache = {}
    if not os.path.exists(path):
        return cache
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            key = obj.get("key")
            val = obj.get("value")
            if key is not None:
                cache[key] = val
    return cache


def append_jsonl_cache(path: str, key: str, value: Any) -> None:
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps({"key": key, "value": value}, ensure_ascii=False) + "\n")


# =============================
# DeepSeek JSON-mode cleaning
# =============================
def build_llm_system_prompt() -> str:
    # DeepSeek JSON mode requires mention of "json"
    return f"""You are a chemistry data-cleaning assistant.

Return STRICT JSON ONLY (no markdown, no extra text).

Task:
Given raw chemical/material strings (Chinese/English, abbreviations, product names, coatings, generations, nanoparticles),
return a clean structured classification suitable for downstream PubChem lookup ONLY when appropriate.

Rules:
1) Do NOT invent IUPAC names, molecular formulas, or structures.
2) entity_type must be one of: {ENTITY_TYPES}
3) language must be one of: {LANGS}
4) should_query_pubchem = true ONLY for small_molecule or salt_or_inorganic.
5) For polymer/composite/nanomaterial/brand/membrane_material/surface_modified_material: usually should_query_pubchem=false.
6) pubchem_query_name: if should_query_pubchem=true, provide best English search string; else empty string.
7) components: list core components if composite/modified; else [].
8) review_flag=true if ambiguous/truncated/translation uncertain.

Output JSON format EXACTLY:
{{
  "items": [
    {{
      "raw_name": "",
      "clean_name": "",
      "canonical_name": "",
      "entity_type": "",
      "language": "",
      "modifier": "",
      "components": [],
      "should_query_pubchem": false,
      "pubchem_query_name": "",
      "confidence": 0.0,
      "review_flag": true,
      "notes": ""
    }}
  ]
}}
"""


def coerce_item(raw_name: str, item: Dict[str, Any]) -> Dict[str, Any]:
    out = {k: item.get(k) for k in REQUIRED_KEYS}

    out["raw_name"] = str(out.get("raw_name") or raw_name)
    out["clean_name"] = str(out.get("clean_name") or raw_name)
    out["canonical_name"] = str(out.get("canonical_name") or out["clean_name"])

    if out.get("entity_type") not in ENTITY_TYPES:
        out["entity_type"] = "unknown"
    if out.get("language") not in LANGS:
        out["language"] = "unknown"

    out["modifier"] = str(out.get("modifier") or "")

    comps = out.get("components")
    if not isinstance(comps, list):
        out["components"] = []
    else:
        out["components"] = [str(x) for x in comps if str(x).strip()]

    out["should_query_pubchem"] = bool(out.get("should_query_pubchem"))
    out["pubchem_query_name"] = str(out.get("pubchem_query_name") or "")

    try:
        out["confidence"] = float(out.get("confidence") or 0.0)
    except Exception:
        out["confidence"] = 0.0

    out["review_flag"] = bool(out.get("review_flag", True))
    out["notes"] = str(out.get("notes") or "")

    return out


def llm_clean_batch(
    client: OpenAI,
    model: str,
    raw_names: List[str],
    max_retries: int = DEFAULT_MAX_RETRIES,
) -> List[Dict[str, Any]]:
    user_content = {"raw_names": raw_names}

    for attempt in range(max_retries):
        try:
            resp = client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": build_llm_system_prompt()},
                    {
                        "role": "user",
                        "content": (
                            "Clean and classify these raw names. Return one item per input name in the same order.\n\n"
                            + json.dumps(user_content, ensure_ascii=False)
                        ),
                    },
                ],
                response_format={"type": "json_object"},
                temperature=0,
                max_tokens=4000,
                stream=False,
            )

            content = (resp.choices[0].message.content or "").strip()
            if not content:
                raise RuntimeError("Empty JSON content from model")

            data = json.loads(content)
            items = data.get("items", [])
            if not isinstance(items, list):
                raise ValueError("JSON missing 'items' list")

            # align by order if possible
            out: List[Dict[str, Any]] = []
            if len(items) == len(raw_names):
                for rn, it in zip(raw_names, items):
                    out.append(coerce_item(rn, it if isinstance(it, dict) else {}))
                return out

            # fallback: match by raw_name
            by_raw = {}
            for it in items:
                if isinstance(it, dict) and "raw_name" in it:
                    by_raw[str(it["raw_name"])] = it

            for rn in raw_names:
                out.append(coerce_item(rn, by_raw.get(rn, {})))
            return out

        except Exception as e:
            wait = 1.2 * (2 ** attempt)
            print(f"[DeepSeek LLM] attempt {attempt+1}/{max_retries} failed: {e} | sleep {wait:.1f}s")
            time.sleep(wait)

    return [coerce_item(rn, {}) for rn in raw_names]


# =============================
# PubChem helpers
# =============================
def http_get_json(url: str, max_retries: int = DEFAULT_MAX_RETRIES, timeout: int = 20) -> Optional[Dict[str, Any]]:
    for attempt in range(max_retries):
        try:
            r = requests.get(url, timeout=timeout)
            if r.status_code == 200:
                return r.json()
            if r.status_code in (400, 404):
                return None
            if r.status_code == 429:
                time.sleep(1.2 * (2 ** attempt))
                continue
            time.sleep(1.0 * (2 ** attempt))
        except Exception:
            time.sleep(1.0 * (2 ** attempt))
    return None


def pubchem_name_to_cids(name: str) -> List[int]:
    q = requests.utils.quote(name, safe="")
    url = f"{PUBCHEM_BASE}/compound/name/{q}/cids/JSON"
    data = http_get_json(url)
    if not data:
        return []
    try:
        return data["IdentifierList"]["CID"]
    except Exception:
        return []


def pubchem_cid_properties(cid: int) -> Dict[str, Any]:
    props = "MolecularFormula,IUPACName,CanonicalSMILES,IsomericSMILES,InChIKey"
    url = f"{PUBCHEM_BASE}/compound/cid/{cid}/property/{props}/JSON"
    data = http_get_json(url)
    if not data:
        return {}
    try:
        row = data["PropertyTable"]["Properties"][0]
        return {
            "cid": cid,
            "molecular_formula": row.get("MolecularFormula", ""),
            "iupac_name": row.get("IUPACName", ""),
            "canonical_smiles": row.get("CanonicalSMILES", ""),
            "isomeric_smiles": row.get("IsomericSMILES", ""),
            "inchikey": row.get("InChIKey", ""),
        }
    except Exception:
        return {}


def pubchem_lookup_best(name_candidates: List[str], sleep_s: float = DEFAULT_SLEEP_BETWEEN_PUBCHEM) -> Dict[str, Any]:
    tried = []
    for nm in name_candidates:
        nm = normalize_text(nm)
        if not nm:
            continue
        if nm.lower() in tried:
            continue
        tried.append(nm.lower())

        cids = pubchem_name_to_cids(nm)
        time.sleep(sleep_s)
        if not cids:
            continue

        cid = int(cids[0])
        props = pubchem_cid_properties(cid)
        time.sleep(sleep_s)

        props.update({
            "pubchem_hit": True,
            "pubchem_query_used": nm,
            "pubchem_cid_candidates": ",".join(map(str, cids[:10])),
        })
        return props

    return {
        "pubchem_hit": False,
        "pubchem_query_used": "",
        "pubchem_cid_candidates": "",
        "cid": "",
        "molecular_formula": "",
        "iupac_name": "",
        "canonical_smiles": "",
        "isomeric_smiles": "",
        "inchikey": "",
    }


# =============================
# Main
# =============================
def main():
    parser = argparse.ArgumentParser(description="DeepSeek clean/classify chemical names + PubChem lookup")
    parser.add_argument("--input", default="./test4_LLM/step1.xlsx", help="Input Excel file path")
    parser.add_argument("--sheet", default=None, help="Sheet name (optional). If None, reads all sheets -> first sheet used.")
    parser.add_argument("--col", default=None, help="Column name containing raw names (default: first column)")
    parser.add_argument("--outdir", default="out_deepseek", help="Output directory")
    parser.add_argument("--model", default=DEFAULT_MODEL, help=f"DeepSeek model (default: {DEFAULT_MODEL})")
    parser.add_argument("--batch", type=int, default=DEFAULT_BATCH_SIZE, help=f"LLM batch size (default: {DEFAULT_BATCH_SIZE})")
    parser.add_argument("--force", action="store_true", help="Force rerun LLM even if cached")
    args = parser.parse_args()

    safe_mkdir(args.outdir)
    llm_cache_path = os.path.join(args.outdir, "cache_llm.jsonl")
    pubchem_cache_path = os.path.join(args.outdir, "cache_pubchem.jsonl")

    llm_cache = load_jsonl_cache(llm_cache_path)
    pubchem_cache = load_jsonl_cache(pubchem_cache_path)

    # -------- Load Excel (supports multi-sheet) --------
    df_obj = pd.read_excel(args.input, sheet_name=args.sheet)

    if isinstance(df_obj, dict):
        if len(df_obj) == 0:
            raise ValueError("Excel has no sheets.")
        first_sheet_name = list(df_obj.keys())[0]
        print(f"[Info] read_excel returned multiple sheets, using first sheet: {first_sheet_name}")
        df = df_obj[first_sheet_name]
    else:
        df = df_obj

    if df.shape[1] == 0:
        raise ValueError("Excel has no columns.")

    raw_col = args.col or df.columns[0]
    if raw_col not in df.columns:
        raise ValueError(f"Column '{raw_col}' not found. Available: {list(df.columns)}")

    df["raw_name"] = df[raw_col].apply(normalize_text)
    df = df[df["raw_name"] != ""].copy()

    unique_names = sorted(df["raw_name"].unique().tolist())
    print(f"[Info] Loaded rows: {len(df)} | Unique names: {len(unique_names)} | Using column: {raw_col}")

    # -------- DeepSeek client --------
    deepseek_key = os.getenv("DEEPSEEK_API_KEY", "").strip()
    if not deepseek_key:
        raise RuntimeError("Missing environment variable DEEPSEEK_API_KEY")

    client = OpenAI(api_key=deepseek_key, base_url="https://api.deepseek.com")

    # -------- LLM cleaning --------
    results_llm: Dict[str, Dict[str, Any]] = {}
    todo = []
    for rn in unique_names:
        if (not args.force) and (rn in llm_cache):
            results_llm[rn] = llm_cache[rn]
        else:
            todo.append(rn)

    print(f"[DeepSeek LLM] cached: {len(results_llm)} | to_run: {len(todo)} | model: {args.model}")

    for batch in tqdm(chunk_list(todo, args.batch), desc="LLM cleaning"):
        batch_out = llm_clean_batch(client, args.model, batch)
        for item in batch_out:
            rn = item["raw_name"]
            results_llm[rn] = item
            append_jsonl_cache(llm_cache_path, rn, item)

    llm_df = pd.DataFrame([results_llm[rn] for rn in unique_names])
    df = df.merge(llm_df, on="raw_name", how="left")

    # -------- PubChem lookup --------
    df["should_query_pubchem"] = df["should_query_pubchem"].fillna(False)
    query_names = df[df["should_query_pubchem"] == True]["raw_name"].unique().tolist()

    pubchem_results: Dict[str, Dict[str, Any]] = {}
    pubchem_todo = []
    for rn in query_names:
        if rn in pubchem_cache:
            pubchem_results[rn] = pubchem_cache[rn]
        else:
            pubchem_todo.append(rn)

    print(f"[PubChem] to_query: {len(query_names)} | cached: {len(pubchem_results)} | to_run: {len(pubchem_todo)}")

    for rn in tqdm(pubchem_todo, desc="PubChem lookup"):
        row = results_llm.get(rn, {})
        candidates = []
        for k in ["pubchem_query_name", "clean_name", "canonical_name"]:
            v = row.get(k, "")
            if v:
                candidates.append(v)
        candidates.append(rn)

        hit = pubchem_lookup_best(candidates, sleep_s=DEFAULT_SLEEP_BETWEEN_PUBCHEM)
        pubchem_results[rn] = hit
        append_jsonl_cache(pubchem_cache_path, rn, hit)

    pubchem_df = pd.DataFrame([{"raw_name": rn, **pubchem_results.get(rn, {})} for rn in unique_names])
    df = df.merge(pubchem_df, on="raw_name", how="left")

    # -------- Outputs --------
    full_path = os.path.join(args.outdir, "full_annotated.xlsx")
    resolved_path = os.path.join(args.outdir, "resolved_compounds.xlsx")
    materials_path = os.path.join(args.outdir, "materials_queue.xlsx")

    df.to_excel(full_path, index=False)

    resolved = df[(df["should_query_pubchem"] == True) & (df["pubchem_hit"] == True)].copy()
    resolved.to_excel(resolved_path, index=False)

    materials = df[(df["should_query_pubchem"] == False) | (df["pubchem_hit"] != True)].copy()
    materials.to_excel(materials_path, index=False)

    print("\nDone ✅")
    print(f"- Full:      {full_path}")
    print(f"- Resolved:  {resolved_path}")
    print(f"- Materials: {materials_path}")
    print(f"- LLM cache: {llm_cache_path}")
    print(f"- PubChem cache: {pubchem_cache_path}")


if __name__ == "__main__":
    main()