import argparse
import json
import os
import re
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests

'''
这一版是我让一些组合的混合物能够通过LLM给他们分开 分别查询pubchem信息
结果依然不好，该分开的物质没有分开，后续还需要改进，以及原来本身可以获取的物质变少了？？？还需要改进。。。
不知道咋改。。。。。明天重新写一版
'''

# -----------------------
# input file
# -----------------------
from pathlib import Path
BASE_DIR = Path(__file__).resolve().parent
DEFAULT_INPUT = str(BASE_DIR / "step1.xlsx")   # 或者写绝对路径

# -----------------------
# DeepSeek (OpenAI-compatible)
# -----------------------
DEEPSEEK_API_URL = "https://api.deepseek.com/chat/completions"

# -----------------------
# PubChem PUG REST
# -----------------------
PUBCHEM_BASE = "https://pubchem.ncbi.nlm.nih.gov/rest/pug"

# -----------------------
# Process-condition filter (rule-based)
# -----------------------
PROCESS_PATTERNS = [
    r"\bdrying\b", r"\bblowing\b", r"\bwithout\b", r"\bwith\b",
    r"\bpost[- ]treatment\b", r"\bpretreatment\b",
    r"\bcuring\b", r"\baging\b", r"\bannealing\b", r"\bcalcination\b",
    r"\bsoaking\b", r"\bimmersion\b", r"\brinsing\b", r"\bwashing\b",
    r"\bstir(ring)?\b", r"\bsonication\b", r"\bultrasonic\b",
    r"\bvacuum\b", r"\bair\b", r"\bnatural\b",
    r"°c", r"℃", r"\btemperature\b", r"\brt\b", r"\broom temperature\b",
    r"\bmin\b", r"\bh\b", r"\bhour(s)?\b", r"\bday(s)?\b",
]
# 这些像 "Drying at 20°C ..."、"Blowing dry at 50°C" 直接删
def is_process_condition(s: str) -> bool:
    s2 = (s or "").strip().lower()
    if not s2:
        return True
    return any(re.search(p, s2) for p in PROCESS_PATTERNS)


# -----------------------
# Helpers
# -----------------------
def normalize_text(x: Any) -> str:
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return ""
    s = str(x).strip()
    s = re.sub(r"\s+", " ", s)
    return s

def choose_name_column(df: pd.DataFrame, preferred: Optional[str]) -> str:
    # 你不传列名：直接用第一列
    if not preferred:
        col = str(df.columns[0])
        print(f"[Info] --column not provided, using first column: {col}")
        return col

    # 你传了列名：存在就用
    if preferred in df.columns:
        print(f"[Info] using provided column: {preferred}")
        return preferred

    # 大小写不敏感匹配
    lower_map = {str(c).lower(): str(c) for c in df.columns}
    if preferred.lower() in lower_map:
        col = lower_map[preferred.lower()]
        print(f"[Info] using case-insensitive matched column: {col}")
        return col

    # 传了但找不到：回退第一列
    col = str(df.columns[0])
    print(f"[Warn] Column '{preferred}' not found. Fallback to first column: {col}. Existing columns: {list(df.columns)}")
    return col

def safe_getenv(name: str) -> str:
    v = os.getenv(name, "").strip()
    return v

def chunk_list(lst: List[Any], n: int) -> List[List[Any]]:
    return [lst[i:i+n] for i in range(0, len(lst), n)]

def ensure_object_column(df: pd.DataFrame, col: str) -> None:
    if col not in df.columns:
        df[col] = pd.Series([None] * len(df), dtype="object")
    else:
        df[col] = df[col].astype("object")

def write_jsonl(path: str, key: str, value: Dict[str, Any]) -> None:
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps({"key": key, "value": value}, ensure_ascii=False) + "\n")

def load_jsonl(path: str) -> Dict[str, Any]:
    if not os.path.exists(path):
        return {}
    out = {}
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            out[obj["key"]] = obj["value"]
    return out


# -----------------------
# DeepSeek prompt (重点：领域 + 严格 JSON + 给 PubChem 查询名)
# -----------------------
SYSTEM_PROMPT = """
You are a chemistry entity cleaning & abbreviation-expansion assistant specialized in membrane materials literature:
- RO/NF membranes, polyamide interfacial polymerization, supports (PSf/PES/PVDF), additives/surfactants, organosilica sol-gel precursors, silanes.
Your job is NOT to invent facts. You MUST be conservative when uncertain.

Return STRICT JSON ONLY (no markdown, no extra text).
Output JSON object with:
{
  "items": [
    {
        "raw_name": "...",
        "action": "drop|query_pubchem|queue_material|split_and_query",
        "normalized_input": "...",
        "entity_type": "...",
        "modifier": "...",
        "components": [
            {"name": "glycerol", "pubchem": "glycerol"},
            {"name": "sodium dodecyl sulfate", "pubchem": "sodium dodecyl sulfate"}
        ],
        "confidence": 0.0,
        "review_flag": true,
        "note": ""
    }
  ]
}

Guidelines:
- action="drop" for process/conditions/procedures like "Drying at 20°C with blowing", "curing at ...", "washing/rinsing", etc.
- action="query_pubchem" ONLY for discrete chemicals (solvents, monomers, salts, reagents, silane precursors). Provide best English pubchem_query_name.
- action="queue_material" for polymers, composites, modified materials (e.g., PDA-coated PE), nanomaterials, brands/products, membrane names, POSS derivatives, "X nanoparticles", "halloysite nanotubes", "graphene oxide nanoparticles".
- For mixtures/solutions like "25% glycerol aqueous solution": normalize to solute if clear; set action=query_pubchem and pubchem_query_name="glycerol" and note="solution". If unclear, queue_material.
- Abbreviations: expand when high confidence in membrane-materials context. If ambiguous, leave expanded_abbreviation empty and set review_flag=true.

Multi-component parsing (VERY IMPORTANT):
- If the input describes multiple chemicals/materials (mixtures, solutions, layer stacks), you MUST split them into separate components.
- Treat separators as multi-component signals: "+", "/", "-", "&", ",", ";", "with", "and", "in", "aqueous", "solution", "wt%", "%", "layers".
- Output MUST include a "components" array. Each component must be a discrete chemical/material name (expanded to full name when possible in membrane-materials context).
- Put concentrations/processing words into "modifier" or "note", NOT inside component names.

Rules for solutions/mixtures:
- "25% glycerol aqueous solution + 1% SDS" => components: glycerol; sodium dodecyl sulfate. (Do NOT include water as a component unless it is the only meaningful chemical.)
- "25% glycerol aqueous solution + 2.5% CaCl2" => components: glycerol; calcium chloride.

Rules for layer stacks/coatings:
- "PEI/DA (1.5 layers) healing" => components: polyethyleneimine; dopamine. modifier: "1.5 layers healing".
- If a slash "/" indicates multiple materials, split them.
- Expand abbreviations when high confidence in membrane literature (e.g., PEI=polyethyleneimine, DA=dopamine, SDS=sodium dodecyl sulfate, TEA=triethylamine, SLS=sodium lauryl sulfate, CSA=camphorsulfonic acid).

PubChem routing:
- For each component, also provide "component_pubchem_query_name" (best English query string).
- The parent item can be composite_material, but components should be individually queryable.

Examples (membrane context):
- "Drying at 40°C without blowing" -> action drop
- "BTESE" -> action query_pubchem, expanded_abbreviation="1,2-bis(triethoxysilyl)ethane", pubchem_query_name same
- "BTESPA" -> action query_pubchem, expanded_abbreviation="bis(triethoxysilylpropyl)amine"
- "MPD" -> action query_pubchem, expanded_abbreviation="m-phenylenediamine"
- "PDA-coated PE" -> action queue_material
- "FilmTec" -> action queue_material (brand_or_product)
"""

def deepseek_clean_batch(
    api_key: str,
    names: List[str],
    model: str = "deepseek-chat",
    temperature: float = 0.0,
    timeout: int = 60,
    max_retries: int = 4,
) -> List[Dict[str, Any]]:
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}

    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": json.dumps({"raw_names": names}, ensure_ascii=False)},
        ],
        "temperature": temperature,
        "response_format": {"type": "json_object"},
    }

    last_err = ""
    for attempt in range(max_retries):
        try:
            resp = requests.post(DEEPSEEK_API_URL, headers=headers, json=payload, timeout=timeout)
            print(f"[DeepSeek HTTP] status={resp.status_code}", flush=True)
            if resp.status_code >= 400:
                raise RuntimeError(f"HTTP {resp.status_code}: {resp.text[:500]}")

            data = resp.json()
            content = data["choices"][0]["message"]["content"] or ""
            content = content.strip()
            if not content:
                raise RuntimeError("Empty model content (json mode)")

            obj = json.loads(content)
            items = obj.get("items", [])
            if not isinstance(items, list):
                raise ValueError("JSON missing items list")

            # 对齐：尽量按 raw_name 匹配
            by_raw = {}
            for it in items:
                if isinstance(it, dict) and it.get("raw_name"):
                    by_raw[str(it["raw_name"]).strip()] = it

            out = []
            for rn in names:
                it = by_raw.get(rn, {})
                out.append(_coerce_llm_item(rn, it))
            return out

        except Exception as e:
            last_err = str(e)
            time.sleep(2 ** attempt)

    # fallback
    return [_coerce_llm_item(rn, {}) for rn in names]

def _coerce_llm_item(raw_name: str, item: Dict[str, Any]) -> Dict[str, Any]:
    # 强制字段齐全，避免后续 merge 崩
    entity_types = {
        "small_molecule","salt_or_inorganic","polymer","composite_material","surface_modified_material",
        "nanomaterial","brand_or_product","membrane_material","unknown"
    }
    actions = {"drop","query_pubchem","queue_material"}

    out = {
        "raw_name": raw_name,
        "action": str(item.get("action","queue_material")).strip(),
        "normalized_input": str(item.get("normalized_input","") or raw_name).strip(),
        "entity_type": str(item.get("entity_type","unknown")).strip(),
        "expanded_abbreviation": str(item.get("expanded_abbreviation","") or "").strip(),
        "pubchem_query_name": str(item.get("pubchem_query_name","") or "").strip(),
        "confidence": float(item.get("confidence", 0.0) or 0.0),
        "review_flag": bool(item.get("review_flag", True)),
        "note": str(item.get("note","") or "").strip(),
    }
    if out["action"] not in actions:
        out["action"] = "queue_material"
    if out["entity_type"] not in entity_types:
        out["entity_type"] = "unknown"
    # drop 的不允许带 pubchem_query
    if out["action"] != "query_pubchem":
        out["pubchem_query_name"] = ""
    return out


# -----------------------
# components table
# -----------------------

import pandas as pd

IGNORE_COMPONENTS = {"water", "h2o", "air"}

def build_component_table(df_parent: pd.DataFrame) -> pd.DataFrame:
    """
    df_parent: 含 raw_name + components（list[dict]） 的表
    输出：一行一个component，带 parent_raw_name 方便回溯
    """
    rows = []
    for _, r in df_parent.iterrows():
        parent = str(r.get("raw_name", "")).strip()
        comps = r.get("components", []) or []
        if not isinstance(comps, list):
            continue
        for c in comps:
            if isinstance(c, dict):
                name = str(c.get("name", "")).strip()
                q = str(c.get("pubchem", "")).strip() or name
            else:
                name = str(c).strip()
                q = name
            if not name:
                continue
            if name.lower() in IGNORE_COMPONENTS:
                continue
            rows.append({
                "parent_raw_name": parent,
                "component_name": name,
                "component_query": q,
            })
    return pd.DataFrame(rows).drop_duplicates()

    # === 在 main() 里，LLM merge 完成后调用 ===
    comp_df = build_component_table(kept2)  # kept2是你LLM后保留的主表
    comp_df_path = os.path.join(args.outdir, "components_to_query.xlsx")
    comp_df.to_excel(comp_df_path, index=False)
    print(f"[Info] component table: {len(comp_df)} rows -> {comp_df_path}")

# -----------------------
# PubChem
# -----------------------
def http_get_json(url: str, timeout: int = 20, max_retries: int = 5) -> Optional[Dict[str, Any]]:
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

def pubchem_lookup_best(candidates: List[str], sleep_s: float = 0.12) -> Dict[str, Any]:
    tried = set()
    for nm in candidates:
        nm = (nm or "").strip()
        if not nm:
            continue
        k = nm.lower()
        if k in tried:
            continue
        tried.add(k)

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


# -----------------------
# Main
# -----------------------
def main():
    parser = argparse.ArgumentParser(description="DeepSeek clean/expand abbreviations (membrane-domain) + PubChem validate/fill")
    parser.add_argument("--input", default=DEFAULT_INPUT, help=f"Input Excel path (default: {DEFAULT_INPUT})")
    parser.add_argument("--outdir", default="out_membrane2", help="Output directory")
    parser.add_argument("--sheet", default=None, help="Sheet name (optional). Default: first sheet")
    parser.add_argument("--column", default=None, help="Name column (optional). If not provided, use the first column.")
    parser.add_argument("--model", default="deepseek-chat", help="DeepSeek model (default: deepseek-chat)")
    parser.add_argument("--batch", type=int, default=25, help="DeepSeek batch size (default: 25)")
    parser.add_argument("--temperature", type=float, default=0.0, help="DeepSeek temperature (default: 0.0)")
    parser.add_argument("--timeout", type=int, default=60, help="DeepSeek HTTP timeout (default: 60s)")
    parser.add_argument("--max-retries", type=int, default=4, help="DeepSeek retries per batch (default: 4)")
    parser.add_argument("--sleep", type=float, default=0.15, help="Sleep between DeepSeek batches (default: 0.15s)")
    parser.add_argument("--pubchem_sleep", type=float, default=0.12, help="Sleep between PubChem calls (default: 0.12s)")
    args = parser.parse_args()

    #定义outdir =
    from pathlib import Path
    outdir_str = getattr(args, "outdir", None) or getattr(args, "output_dir", None) or "out_membrane2"
    outdir = Path(outdir_str)
    outdir.mkdir(parents=True, exist_ok=True)

    llm_cache_path = str(outdir / "cache_llm.jsonl")
    pubchem_cache_path = str(outdir / "cache_pubchem.jsonl")
    llm_cache = load_jsonl(llm_cache_path)
    pubchem_cache = load_jsonl(pubchem_cache_path)

    api_key = safe_getenv("DEEPSEEK_API_KEY")
    if not api_key:
        raise RuntimeError("Missing environment variable DEEPSEEK_API_KEY")


    llm_cache_path = str(outdir / "cache_llm.jsonl")
    pubchem_cache_path = str(outdir / "cache_pubchem.jsonl")
    llm_cache = load_jsonl(llm_cache_path)
    pubchem_cache = load_jsonl(pubchem_cache_path)

    # -------- Load Excel (multi-sheet safe) --------
    sheet_arg = 0 if args.sheet is None else args.sheet
    df_obj = pd.read_excel(args.input, sheet_name=sheet_arg)
    if isinstance(df_obj, dict):
        first_sheet_name = next(iter(df_obj))
        df = df_obj[first_sheet_name]
    else:
        df = df_obj

    name_col = choose_name_column(df, args.column)
    df["raw_name"] = df[name_col].apply(normalize_text)

    # -------- Rule-based drop: process conditions --------
    df["rule_drop"] = df["raw_name"].apply(is_process_condition)
    filtered_out = df[df["rule_drop"]].copy()
    kept = df[~df["rule_drop"]].copy()

    filtered_out_path = str(outdir / "filtered_out_process_conditions.xlsx")
    filtered_out.to_excel(filtered_out_path, index=False)
    print(f"[Info] filtered out (process conditions): {len(filtered_out)} -> {filtered_out_path}")

    kept = kept[kept["raw_name"] != ""].copy()
    unique_names = sorted(kept["raw_name"].unique().tolist())
    print(f"[Info] kept rows: {len(kept)} | unique names: {len(unique_names)} | col: {name_col}")

    # -------- DeepSeek cleaning (batched + cache) --------
    llm_results: Dict[str, Dict[str, Any]] = {}

    to_run = []
    for rn in unique_names:
        if rn in llm_cache:
            llm_results[rn] = llm_cache[rn]
        else:
            to_run.append(rn)

    print(f"[DeepSeek] cached: {len(llm_results)} | to_run: {len(to_run)} | model: {args.model}")

    for bi, batch in enumerate(chunk_list(to_run, args.batch), start=1):
        print(f"[DeepSeek] start batch {bi} | size={len(batch)} | example={batch[:3]}", flush=True)

        batch_out = deepseek_clean_batch(
            api_key=api_key,
            names=batch,
            model=args.model,
            temperature=args.temperature,
            timeout=args.timeout,
            max_retries=args.max_retries,
        )

        print(f"[DeepSeek] done batch {bi} | got={len(batch_out)}", flush=True)

        for item in batch_out:
            rn = item["raw_name"]
            llm_results[rn] = item
            write_jsonl(llm_cache_path, rn, item)

        time.sleep(args.sleep)

    llm_df = pd.DataFrame([llm_results[rn] for rn in unique_names])
    kept = kept.merge(llm_df, left_on="raw_name", right_on="raw_name", how="left")

    # LLM can still mark drop (extra safety)
    llm_drop = kept[kept["action"] == "drop"].copy()
    llm_drop_path = str(outdir / "filtered_out_llm_drop.xlsx")
    llm_drop.to_excel(llm_drop_path, index=False)
    print(f"[Info] LLM drop: {len(llm_drop)} -> {llm_drop_path}")

    kept2 = kept[kept["action"] != "drop"].copy()

    # -------- components table --------
    comp_df = build_component_table(kept2)
    comp_df_path = os.path.join(outdir, "components_to_query.xlsx")
    comp_df.to_excel(comp_df_path, index=False)
    print(f"[Info] component table: {len(comp_df)} rows -> {comp_df_path}", flush=True)

    # === PubChem for components (skip if empty) ===
    if len(comp_df) > 0:
        comp_results = []
        for q in comp_df["component_query"].unique().tolist():
            hit = pubchem_lookup_best([q], sleep_s=args.pubchem_sleep)
            comp_results.append({"component_query": q, **hit})

        comp_hit_df = pd.DataFrame(comp_results)
        comp_out = comp_df.merge(comp_hit_df, on="component_query", how="left")

        comp_resolved_path = os.path.join(outdir, "components_resolved.xlsx")
        comp_out.to_excel(comp_resolved_path, index=False)
        print(f"[Done] components resolved -> {comp_resolved_path}", flush=True)
    else:
        print("[Info] no components extracted; skipping component PubChem.", flush=True)


    # -------- PubChem lookup only for action=query_pubchem --------
    for col in ["pubchem_hit","cid","molecular_formula","iupac_name","canonical_smiles","isomeric_smiles","inchikey","pubchem_query_used","pubchem_cid_candidates"]:
        ensure_object_column(kept2, col)

    query_rows = kept2[kept2["action"] == "query_pubchem"].copy()
    print(f"[PubChem] candidates to query: {len(query_rows)}")

    # query per unique raw_name
    for rn in query_rows["raw_name"].unique().tolist():
        row = llm_results.get(rn, {})
        q = row.get("pubchem_query_name","") or row.get("expanded_abbreviation","") or row.get("normalized_input","") or rn
        q = str(q).strip()

        # cache by query string (more useful than raw_name)
        cache_key = f"Q::{q}"
        if cache_key in pubchem_cache:
            hit = pubchem_cache[cache_key]
        else:
            candidates = [q]
            # fallback candidates
            if row.get("expanded_abbreviation"):
                candidates.append(row["expanded_abbreviation"])
            if row.get("normalized_input"):
                candidates.append(row["normalized_input"])
            candidates.append(rn)

            hit = pubchem_lookup_best(candidates, sleep_s=args.pubchem_sleep)
            pubchem_cache[cache_key] = hit
            write_jsonl(pubchem_cache_path, cache_key, hit)

        # fill all rows with same raw_name
        mask = kept2["raw_name"] == rn
        for k, v in hit.items():
            kept2.loc[mask, k] = v

    # -------- Split outputs --------
    full = pd.concat([filtered_out, llm_drop, kept2], ignore_index=True)
    full_path = str(outdir / "full_annotated.xlsx")
    full.to_excel(full_path, index=False)

    resolved = kept2[(kept2["action"] == "query_pubchem") & (kept2["pubchem_hit"] == True)].copy()
    resolved_path = str(outdir / "resolved_compounds.xlsx")
    resolved.to_excel(resolved_path, index=False)

    materials = kept2[(kept2["action"] != "query_pubchem") | (kept2["pubchem_hit"] != True)].copy()
    materials_path = str(outdir / "materials_queue.xlsx")
    materials.to_excel(materials_path, index=False)

    # -------- Abbreviation review table (重点：你要的缩写扩写复核) --------
    # 规则：LLM说要查PubChem但没命中、且看起来像缩写/或LLM给了 expanded_abbreviation
    abbr_like = materials[
        (materials["action"] == "query_pubchem")
        & (materials["pubchem_hit"] != True)
        & (
            materials["raw_name"].str.fullmatch(r"[A-Z][A-Z0-9\-]{1,20}", na=False)
            | (materials["expanded_abbreviation"].fillna("") != "")
            | (materials["review_flag"] == True)
        )
    ].copy()

    abbr_review = abbr_like[[
        "raw_name", "expanded_abbreviation", "pubchem_query_name",
        "normalized_input", "entity_type", "confidence", "review_flag", "note",
        "pubchem_query_used", "pubchem_cid_candidates"
    ]].drop_duplicates()

    abbr_review_path = str(outdir / "abbr_review.xlsx")
    abbr_review.to_excel(abbr_review_path, index=False)

    print("\nDone ✅")
    print(f"- Full:        {full_path}")
    print(f"- Resolved:    {resolved_path}")
    print(f"- Materials:   {materials_path}")
    print(f"- Dropped(rule): {filtered_out_path}")
    print(f"- Dropped(LLM):  {llm_drop_path}")
    print(f"- Abbr review: {abbr_review_path}")


if __name__ == "__main__":
    main()