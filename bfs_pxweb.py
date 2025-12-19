
import requests
import pandas as pd
import logging
from time import sleep

logger = logging.getLogger(__name__)

# Mapping from internal abbreviation to BFS label (bilingual or specific format)
# This list matches the "Kanton" dimension values in BFS UDEMO datasets.
CANTON_ABBR_TO_LABEL = {
    'ZH': 'Zürich',
    'BE': 'Bern / Berne',
    'LU': 'Luzern',
    'UR': 'Uri',
    'SZ': 'Schwyz',
    'OW': 'Obwalden',
    'NW': 'Nidwalden',
    'GL': 'Glarus',
    'ZG': 'Zug',
    'FR': 'Fribourg / Freiburg',
    'SO': 'Solothurn',
    'BS': 'Basel-Stadt',
    'BL': 'Basel-Landschaft',
    'SH': 'Schaffhausen',
    'AR': 'Appenzell Ausserrhoden',
    'AI': 'Appenzell Innerrhoden',
    'SG': 'St. Gallen',
    'GR': 'Graubünden / Grigioni / Grischun',
    'AG': 'Aargau',
    'TG': 'Thurgau',
    'TI': 'Ticino',
    'VD': 'Vaud',
    'VS': 'Valais / Wallis',
    'NE': 'Neuchâtel',
    'GE': 'Genève',
    'JU': 'Jura'
}

def fetch_udemo(observation_text="Unternehmensneugründungen", years=None, canton_abbrs=None, legal_form_text=None):
    """
    Fetch UDEMO data from BFS PxWeb API.

    Args:
        observation_text: Text filter for 'Beobachtungseinheit' (e.g. 'Unternehmensneugründungen')
        years: List of years to fetch (integers or strings)
        canton_abbrs: List of canton abbreviations (e.g. ['ZH', 'BE']). If None, fetches all.
        legal_form_text: Text filter for 'Rechtsform'. If None, fetches all.

    Returns:
        pd.DataFrame with columns ['Beobachtungseinheit', 'Kanton', 'Rechtsform', 'Jahr', 'value']
    """
    # PXWeb API endpoint for UDEMO (Example endpoint, may need adjustment if dataset ID changes)
    # The dataset ID "px-x-0602010000_102" is a guess based on UDEMO context,
    # but for robustness we will handle failures gracefully.
    # Actually, a more reliable way is to not hardcode too much if we can't search.
    # But since we need to implement it, we will use a common pattern.

    # Using the dataset ID found in online examples or common UDEMO ID.
    # "px-x-0602010000_102" is often "Unternehmensdemografie: Neugründungen"

    api_url = "https://www.pxweb.bfs.admin.ch/api/v1/de/px-x-0602010000_102"

    # If years are provided, format them
    if years:
        year_values = [str(y) for y in years]
    else:
        # Default to last 5 years if not specified, or "0" for all?
        # Better to fetch metadata first, but for simplicity let's try a wildcard if PxWeb supports it (usually top X).
        # We will assume years are passed.
        year_values = ["2020", "2021", "2022", "2023"] # Fallback

    # Map cantons to BFS labels
    canton_values = []
    if canton_abbrs:
        for abbr in canton_abbrs:
            if abbr in CANTON_ABBR_TO_LABEL:
                canton_values.append(CANTON_ABBR_TO_LABEL[abbr])

    # Construct query
    query = []

    # Add Kanton filter if specific cantons requested
    if canton_values:
        query.append({
            "code": "Kanton",
            "selection": {
                "filter": "item",
                "values": canton_values
            }
        })
    else:
        # If no cantons specified, we probably want all of them.
        # But we must exclude "Schweiz" aggregate if we want only cantons?
        # Usually fetching all is fine, we filter later.
        pass

    # Add Year filter
    if years:
        query.append({
            "code": "Jahr",
            "selection": {
                "filter": "item",
                "values": year_values
            }
        })

    # Add Observation unit filter
    if observation_text:
        # We need to know the code for 'Beobachtungseinheit'. Assuming it is "Beobachtungseinheit"
        # And value "Unternehmensneugründungen" might be a text, we need the code?
        # Often codes are simple integers or matching text.
        # Without metadata, this is risky.
        # We will try to fetch metadata first to map text to code.
        pass

    # JSON query payload
    # Note: This is a best-effort implementation.
    # Without dynamic metadata inspection, we might fail if codes change.

    # Let's try to get metadata first to be robust.
    try:
        r_meta = requests.get(api_url, timeout=(10, 30))
        r_meta.raise_for_status()
        metadata = r_meta.json()

        variables = metadata.get('variables', [])

        # Helper to find code by text
        def get_values_for_text(var_code, text_match):
            for var in variables:
                if var['code'] == var_code:
                    values = var['values']
                    value_texts = var['valueTexts']
                    # Find index of text
                    matches = []
                    for i, txt in enumerate(value_texts):
                        if text_match in txt: # Substring match
                            matches.append(values[i])
                    return matches
            return []

        # Refine query based on metadata
        full_query = []

        # 1. Canton
        # If we didn't specify cantons, we want all (excluding total Switzerland if possible)
        # But let's just fetch what is available.
        if not canton_values:
            # Fetch all cantons
            # variable code is usually "Kanton"
            pass # PxWeb defaults to all or none? Usually none if not specified?
                 # Actually, usually we MUST specify values for all variables to get a flat table.
                 # Let's specify all values for Kanton
            for var in variables:
                 if var['code'] == "Kanton":
                     # Exclude "Schweiz" if present?
                     full_query.append({
                         "code": "Kanton",
                         "selection": {
                             "filter": "item",
                             "values": var['values']
                         }
                     })
        else:
            # Map labels to codes
             for var in variables:
                 if var['code'] == "Kanton":
                     # Find codes for our labels
                     codes = []
                     for label in canton_values:
                         if label in var['valueTexts']:
                             idx = var['valueTexts'].index(label)
                             codes.append(var['values'][idx])
                     if codes:
                         full_query.append({
                             "code": "Kanton",
                             "selection": {
                                 "filter": "item",
                                 "values": codes
                             }
                         })

        # 2. Jahr
        # Map years to codes
        if years:
            year_codes = []
            for var in variables:
                if var['code'] == "Jahr":
                    for y in year_values:
                        if y in var['valueTexts']:
                            idx = var['valueTexts'].index(y)
                            year_codes.append(var['values'][idx])

            if year_codes:
                full_query.append({
                    "code": "Jahr",
                    "selection": {
                        "filter": "item",
                        "values": year_codes
                    }
                })

        # 3. Beobachtungseinheit
        if observation_text:
             beob_codes = get_values_for_text("Beobachtungseinheit", observation_text)
             if beob_codes:
                  full_query.append({
                    "code": "Beobachtungseinheit",
                    "selection": {
                        "filter": "item",
                        "values": beob_codes
                    }
                })

        # 4. Rechtsform
        if legal_form_text:
             rf_codes = get_values_for_text("Rechtsform", legal_form_text)
             if rf_codes:
                 full_query.append({
                    "code": "Rechtsform",
                    "selection": {
                        "filter": "item",
                        "values": rf_codes
                    }
                })
        else:
             # Fetch all legal forms
             for var in variables:
                 if var['code'] == "Rechtsform":
                     full_query.append({
                         "code": "Rechtsform",
                         "selection": {
                             "filter": "item",
                             "values": var['values']
                         }
                     })

        payload = {
            "query": full_query,
            "response": {"format": "json"}
        }

        # POST request
        r_data = requests.post(api_url, json=payload, timeout=(10, 30))
        r_data.raise_for_status()

        result = r_data.json()

        # Parse result into DataFrame
        # PxWeb JSON response is list of columns and flattened data
        # "columns": [{"code": "...", "text": "..."}, ...]
        # "data": [{"key": ["...", ...], "values": ["..."]}, ...]

        col_names = [c['code'] for c in result['columns']] # e.g. Kanton, Rechtsform, Jahr...
        # Note: 'value' is separate in "values" list

        data_rows = []
        for item in result['data']:
            row = item['key'] + item['values']
            data_rows.append(row)

        final_cols = col_names + ['value']

        df = pd.DataFrame(data_rows, columns=final_cols)

        # Convert value to numeric
        df['value'] = pd.to_numeric(df['value'], errors='coerce')

        return df

    except Exception as e:
        logger.error(f"BFS Fetch failed: {e}")
        # Return empty dataframe with expected columns to avoid crashing caller
        return pd.DataFrame(columns=['Beobachtungseinheit', 'Kanton', 'Rechtsform', 'Jahr', 'value'])
