# bfs_pxweb.py 
from __future__ import annotations 
 
import json 
import io 
from pathlib import Path 
from datetime import datetime, timedelta 
 
import pandas as pd 
import requests 
 
BASE_URL = "https://www.pxweb.bfs.admin.ch/api/v1/de" 
TABLE = "px-x-0602030000_203"  # UDEMO: by canton + legal form (as per your example) 
ENDPOINT = f"{BASE_URL}/{TABLE}/{TABLE}.px" 
 
CACHE_DIR = Path("./shab_data") 
CACHE_DIR.mkdir(exist_ok=True) 
 
# Map SHAB canton abbreviations to BFS labels (German spelling as typically used by BFS) 
CANTON_ABBR_TO_LABEL = { 
    "ZH": "Zürich", "BE": "Bern / Berne", "LU": "Luzern", "UR": "Uri", "SZ": "Schwyz", 
    "OW": "Obwalden", "NW": "Nidwalden", "GL": "Glarus", "ZG": "Zug", "FR": "Freiburg / Fribourg", 
    "SO": "Solothurn", "BS": "Basel-Stadt", "BL": "Basel-Landschaft", "SH": "Schaffhausen", 
    "AR": "Appenzell Ausserrhoden", "AI": "Appenzell Innerrhoden", "SG": "St. Gallen", 
    "GR": "Graubünden / Grigioni / Grischun", "AG": "Aargau", "TG": "Thurgau", "TI": "Ticino", 
    "VD": "Vaud", "VS": "Valais / Wallis", "NE": "Neuchâtel", "GE": "Genève", "JU": "Jura", 
} 
# Note: For bilingual cantons, the API might use one order or another.
# The metadata check showed: 'Bern / Berne', 'Fribourg / Freiburg', 'Graubünden / Grigioni / Grischun', 'Valais / Wallis'
# But 'Ticino', 'Vaud', 'Neuchâtel', 'Genève' are single names in the list I saw earlier from the metadata dump.
# Wait, let me double check the metadata dump I just did.

# Metadata from my run:
# 'Bern / Berne'
# 'Fribourg / Freiburg'
# 'Graubünden / Grigioni / Grischun'
# 'Ticino'
# 'Vaud'
# 'Valais / Wallis'
# 'Neuchâtel'
# 'Genève'
# 'Jura'

# So FR should be 'Fribourg / Freiburg' - wait, the metadata output showed 'Fribourg / Freiburg' in the list I saw?
# Let's check the previous tool output.
# {'code': 'Kanton', ..., 'valueTexts': [..., 'Bern / Berne', ..., 'Fribourg / Freiburg', ..., 'Graubünden / Grigioni / Grischun', ..., 'Ticino', 'Vaud', 'Valais / Wallis', 'Neuchâtel', 'Genève', 'Jura']}

# So:
# FR -> Fribourg / Freiburg
# VS -> Valais / Wallis
# GR -> Graubünden / Grigioni / Grischun
# BE -> Bern / Berne
# TI -> Ticino
# VD -> Vaud
# NE -> Neuchâtel
# GE -> Genève

# Correcting the map below.

CANTON_ABBR_TO_LABEL = { 
    "ZH": "Zürich", "BE": "Bern / Berne", "LU": "Luzern", "UR": "Uri", "SZ": "Schwyz", 
    "OW": "Obwalden", "NW": "Nidwalden", "GL": "Glarus", "ZG": "Zug", "FR": "Fribourg / Freiburg", 
    "SO": "Solothurn", "BS": "Basel-Stadt", "BL": "Basel-Landschaft", "SH": "Schaffhausen", 
    "AR": "Appenzell Ausserrhoden", "AI": "Appenzell Innerrhoden", "SG": "St. Gallen", 
    "GR": "Graubünden / Grigioni / Grischun", "AG": "Aargau", "TG": "Thurgau", "TI": "Ticino", 
    "VD": "Vaud", "VS": "Valais / Wallis", "NE": "Neuchâtel", "GE": "Genève", "JU": "Jura", 
} 

def _get_meta(session: requests.Session) -> dict: 
    r = session.get(ENDPOINT, timeout=(10, 30)) 
    r.raise_for_status() 
    return r.json() 
 
def _value_code(meta: dict, var_code: str, wanted_text: str) -> str: 
    """ 
    Translate a human-readable label (valueText) to the underlying PxWeb value code. 
    """ 
    for var in meta.get("variables", []): 
        if var.get("code") == var_code: 
            texts = var.get("valueTexts", []) 
            values = var.get("values", []) 
            try: 
                idx = texts.index(wanted_text) 
            except ValueError: 
                raise ValueError( 
                    f"'{wanted_text}' not found for variable '{var_code}'. " 
                    f"Example values: {texts[:10]}" 
                ) 
            return values[idx] 
    raise ValueError(f"Variable '{var_code}' not found in table metadata.") 
 
def _jsonstat_to_df(js: dict) -> pd.DataFrame: 
    """ 
    Convert json-stat2 response into a flat DataFrame. 
    """ 
    # The structure seems to be: js is the dataset itself, OR js["dataset"] is the dataset.
    # Based on PxWeb JSON-stat2, the root object *is* the dataset if it has "class": "dataset".
    # Or it might be wrapped.
    
    if "class" in js and js["class"] == "dataset":
        dataset = js
    elif "dataset" in js:
        dataset = js["dataset"]
    else:
        # Debugging aid
        keys = list(js.keys())
        raise KeyError(f"Could not find 'dataset' in response. Keys found: {keys}")

    dim_order = dataset["id"] 
    dims = dataset["dimension"] 
    values = dataset["value"] 
 
    categories = [] 
    for d in dim_order: 
        cat = dims[d]["category"] 
        # json-stat2 can store category index as dict or list 
        idx = cat.get("index") 
        labels = cat.get("label", {}) 
        if isinstance(idx, dict): 
            keys_sorted = sorted(idx, key=idx.get) 
        else: 
            keys_sorted = list(idx) 
        categories.append([labels[k] for k in keys_sorted]) 
 
    mi = pd.MultiIndex.from_product(categories, names=dim_order) 
    df = pd.DataFrame({"value": values}, index=mi).reset_index() 
    return df 
 
def fetch_udemo( 
    observation_text: str = "Unternehmensneugründungen", 
    years: list[int] | None = None, 
    canton_abbrs: list[str] | None = None, 
    legal_form_text: str | None = None, 
    cache_ttl_hours: int = 24, 
) -> pd.DataFrame: 
    """ 
    Fetch UDEMO data from BFS PxWeb and return as DataFrame. 
    - observation_text: e.g. "Unternehmensneugründungen", "Unternehmensschliessungen", ... 
    - years: list of years (ints) 
    - canton_abbrs: e.g. ["ZH","BE"]; None = all cantons 
    - legal_form_text: set to something like "Total" / "Insgesamt" (depending on table labels) 
    """ 
    if years is None or len(years) == 0: 
        raise ValueError("years must be provided (e.g. [2020, 2021, 2022, 2023]).") 
 
    cache_name = f"bfs_udemo_{observation_text}_{min(years)}_{max(years)}" 
    if canton_abbrs: 
        cache_name += "_" + "-".join(sorted(canton_abbrs)) 
    if legal_form_text: 
        cache_name += "_" + legal_form_text 
    cache_file = CACHE_DIR / (cache_name.replace(" ", "_") + ".csv") 
 
    if cache_file.exists(): 
        age = datetime.now() - datetime.fromtimestamp(cache_file.stat().st_mtime) 
        if age < timedelta(hours=cache_ttl_hours): 
            return pd.read_csv(cache_file) 
 
    session = requests.Session() 
    session.headers.update({"User-Agent": "Swiss-SHAB-Dashboard/1.0 (+local)"}) 
 
    meta = _get_meta(session) 
 
    # IMPORTANT: variable codes in this table are typically: 
    # Beobachtungseinheit, Kanton, Rechtsform, Jahr 
    # If your metadata differs, print(meta["variables"]) once and adjust. 
    obs_code = _value_code(meta, "Beobachtungseinheit", observation_text) 
 
    query = [ 
        {"code": "Beobachtungseinheit", "selection": {"filter": "item", "values": [obs_code]}}, 
        {"code": "Jahr", "selection": {"filter": "item", "values": [str(y) for y in years]}}, 
    ] 
 
    if canton_abbrs: 
        canton_labels = [CANTON_ABBR_TO_LABEL[c] for c in canton_abbrs] 
        canton_codes = [_value_code(meta, "Kanton", lab) for lab in canton_labels] 
        query.append({"code": "Kanton", "selection": {"filter": "item", "values": canton_codes}}) 
 
    if legal_form_text: 
        lf_code = _value_code(meta, "Rechtsform", legal_form_text) 
        query.append({"code": "Rechtsform", "selection": {"filter": "item", "values": [lf_code]}}) 
 
    payload = {"query": query, "response": {"format": "json-stat2"}} 
    r = session.post(ENDPOINT, json=payload, timeout=(10, 60)) 
    r.raise_for_status() 
 
    df = _jsonstat_to_df(r.json()) 
 
    # Persist cache 
    df.to_csv(cache_file, index=False) 
    return df 
