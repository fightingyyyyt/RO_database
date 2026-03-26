"""
Patch md_backtrace_then_pubchem_8.py to insert run_full_enrichment_pipeline()
and replace try_lookup_query() with a thin wrapper that calls it.
"""
import re

TARGET = r"d:\graduate_work\RO_database\Part1_chemExtract\step1\test12\md_backtrace_then_pubchem_8.py"

with open(TARGET, "r", encoding="utf-8") as f:
    content = f.read()

# ------------------------------------------------------------------
# Locate the slice: from "def try_lookup_query(" to "def init_bt_result("
# ------------------------------------------------------------------
START_MARKER = "\ndef try_lookup_query("
END_MARKER = "\ndef init_bt_result("

start_idx = content.find(START_MARKER)
end_idx = content.find(END_MARKER)

if start_idx == -1:
    raise RuntimeError("Could not find 'def try_lookup_query(' in the file")
if end_idx == -1:
    raise RuntimeError("Could not find 'def init_bt_result(' in the file")

print(f"Replacing lines at byte offsets {start_idx}..{end_idx}")

# ------------------------------------------------------------------
# New code to inject
# ------------------------------------------------------------------
NEW_CODE = r'''

def run_full_enrichment_pipeline(
    query: str,
    original_name: str,
    base_mod,
    pubchem_cache,
    rewrite_cache,
    material_cache,
    api_key: str,
    model: str,
    base_notes: str,
):
    """
    Mirror the full code_extract_name_fixed.py pipeline for a single candidate query:
      1. call_deepseek_batch() to classify (SINGLE / MIXTURE / POLYMER / ABBR / ...)
      2. For MIXTURE: split components, lookup each, pick best
      3. For ABBR: use abbr_candidates from DeepSeek, resolve each, pick best
      4. For SINGLE / SINGLE_NOISY / fallback: use single_query then q_norm
      5. apply_no_hit_reclassification() on every PubChem result
    Returns (chem_dict, notes_str).
    """
    q_norm = normalize_pubchem_query(query)

    # Score helper
    def status_score(s):
        return {"OK": 10, "ABBR_EXPANDED_OK": 10, "CID_ONLY": 6,
                "NO_SPECIFIC_CHEMICAL": 2, "Polymer": 2,
                "NO_HIT": 1, "ABBR_EXPANDED_NO_HIT": 1, "EMPTY_QUERY": 0}.get(str(s), 1)

    def do_pubchem(q, record_type="backtrace"):
        """Lookup q in PubChem then reclassify NO_HIT."""
        q = normalize_pubchem_query(q)
        if not q:
            return {"Lookup_Status": "EMPTY_QUERY", "Standardized_Query": ""}, base_notes
        chem = base_mod.pubchem_lookup(
            q, pubchem_cache,
            api_key=api_key, model=model,
            rewrite_cache=rewrite_cache, material_cache=material_cache,
        )
        chem = dict(chem)
        chem.setdefault("Standardized_Query", q)
        chem["_PubChem_Query"] = q
        if hasattr(base_mod, "apply_no_hit_reclassification"):
            chem, notes = base_mod.apply_no_hit_reclassification(
                chem=chem, query=q, original_name=original_name,
                record_type=record_type,
                api_key=api_key, model=model,
                pubchem_cache=pubchem_cache,
                rewrite_cache=rewrite_cache,
                material_cache=material_cache,
                base_notes=base_notes,
            )
        else:
            notes = base_notes
        return chem, notes

    # Step 1: DeepSeek classification
    ds_result = None
    if api_key and hasattr(base_mod, "call_deepseek_batch"):
        try:
            batch = base_mod.call_deepseek_batch([(1, q_norm)], api_key=api_key, model=model)
            ds_result = batch.get(1)
        except Exception as e:
            pass  # fall through to direct lookup

    if not ds_result:
        # No DeepSeek result — fall back to direct lookup
        return do_pubchem(q_norm, "backtrace")

    category = str(ds_result.get("category", "SINGLE")).upper()
    single_query = normalize_pubchem_query(str(ds_result.get("single_query", "") or ""))
    components = ds_result.get("components", [])
    abbr_candidates = ds_result.get("abbr_candidates", [])
    ds_notes = str(ds_result.get("notes", "") or "")

    best_chem = {"Lookup_Status": "EMPTY_QUERY", "Standardized_Query": q_norm}
    best_notes = "; ".join(x for x in [base_notes, f"ds_cat={category}", ds_notes] if x)
    best_score = -1

    def update_best(chem, notes):
        nonlocal best_chem, best_notes, best_score
        sc = status_score(chem.get("Lookup_Status"))
        if sc > best_score:
            best_score = sc
            best_chem = chem
            best_notes = notes
        return sc >= status_score("OK")  # True if we got a hit → can short-circuit

    # ------------------------------------------------------------------
    # Step 2: Category-specific handling
    # ------------------------------------------------------------------

    if category in ("POLYMER", "PROCESS", "NON_ENTITY"):
        # These are not directly lookup-able as single compounds
        lstat = "Polymer" if category == "POLYMER" else "NO_SPECIFIC_CHEMICAL"
        best_chem = {"Lookup_Status": lstat, "Standardized_Query": q_norm}
        best_notes = "; ".join(x for x in [base_notes, f"ds_cat={category}", ds_notes] if x)
        return best_chem, best_notes

    elif category == "MIXTURE":
        # Use LLM-provided components first; fall back to simple split
        comp_list = components if components else []
        if not comp_list and hasattr(base_mod, "split_mixture_components"):
            try:
                comp_list = base_mod.split_mixture_components(q_norm, {}, set())
            except Exception:
                comp_list = []

        if not comp_list:
            # single fallback
            comp_list = [{"name": single_query or q_norm, "role": "primary", "rank": 1}]

        for comp in sorted(comp_list, key=lambda c: c.get("rank", 99)):
            cname = normalize_pubchem_query(str(comp.get("name", "") or ""))
            if not cname:
                continue
            # Check if this component looks like an abbreviation
            is_abbr_comp = (
                hasattr(base_mod, "is_likely_abbr_token") and base_mod.is_likely_abbr_token(cname)
            )
            if is_abbr_comp and api_key and hasattr(base_mod, "resolve_abbr_candidate_once"):
                try:
                    resolved = base_mod.resolve_abbr_candidate_once(
                        cname, api_key=api_key, model=model, context_name=original_name
                    )
                    if resolved.get("status") == "CHEMICAL" and resolved.get("query"):
                        cname = normalize_pubchem_query(resolved["query"])
                except Exception:
                    pass
            chem, notes = do_pubchem(cname, "backtrace_mixture")
            if update_best(chem, notes):
                return best_chem, best_notes

    elif category == "ABBR":
        # Try each abbr_candidate from DeepSeek
        candidates = sorted(abbr_candidates, key=lambda c: -float(c.get("confidence", 0) or 0))
        if not candidates:
            # Ask DeepSeek to resolve the abbreviation directly
            if api_key and hasattr(base_mod, "resolve_abbr_candidate_once"):
                try:
                    resolved = base_mod.resolve_abbr_candidate_once(
                        q_norm, api_key=api_key, model=model, context_name=original_name
                    )
                    if resolved.get("status") == "CHEMICAL" and resolved.get("query"):
                        candidates = [{"query": resolved["query"], "confidence": 0.8}]
                except Exception:
                    pass

        for cand in candidates[:3]:
            cq = normalize_pubchem_query(str(cand.get("query", "") or ""))
            if not cq:
                continue
            chem, notes = do_pubchem(cq, "backtrace_abbr")
            if update_best(chem, notes):
                return best_chem, best_notes

        # Also try the raw q_norm as fallback
        chem, notes = do_pubchem(q_norm, "backtrace_abbr")
        update_best(chem, notes)

    else:
        # SINGLE, SINGLE_NOISY, or unknown — try single_query first, then q_norm
        queries_to_try = []
        if single_query and single_query.lower() != q_norm.lower():
            queries_to_try.append(single_query)
        queries_to_try.append(q_norm)
        # Also build simple variants (head before comma, strip suffix, etc.)
        for raw_q in list(queries_to_try):
            if "," in raw_q:
                head = normalize_pubchem_query(raw_q.split(",", 1)[0].strip())
                if head and head.lower() not in [x.lower() for x in queries_to_try]:
                    queries_to_try.append(head)

        for q_try in queries_to_try:
            chem, notes = do_pubchem(q_try, "backtrace")
            if update_best(chem, notes):
                return best_chem, best_notes

    return best_chem, best_notes


def try_lookup_query(
    query: str,
    original_name: str,
    base_mod,
    pubchem_cache,
    rewrite_cache,
    material_cache,
    api_key: str,
    model: str,
    base_notes: str,
):
    """Thin wrapper — delegates to run_full_enrichment_pipeline()."""
    return run_full_enrichment_pipeline(
        query=query,
        original_name=original_name,
        base_mod=base_mod,
        pubchem_cache=pubchem_cache,
        rewrite_cache=rewrite_cache,
        material_cache=material_cache,
        api_key=api_key,
        model=model,
        base_notes=base_notes,
    )

'''

# ------------------------------------------------------------------
# Stitch together: before + new_code + from END_MARKER onwards
# ------------------------------------------------------------------
new_content = content[:start_idx] + NEW_CODE + content[end_idx:]

with open(TARGET, "w", encoding="utf-8") as f:
    f.write(new_content)

print("Patch applied successfully.")

# Quick sanity check
with open(TARGET, "r", encoding="utf-8") as f:
    check = f.read()

assert "def run_full_enrichment_pipeline(" in check, "run_full_enrichment_pipeline not found!"
assert "def try_lookup_query(" in check, "try_lookup_query not found!"
assert "def init_bt_result(" in check, "init_bt_result not found!"
print("Sanity checks passed.")
