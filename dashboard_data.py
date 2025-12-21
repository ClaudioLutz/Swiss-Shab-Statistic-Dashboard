import pandas as pd
import os
import json
import logging

logger = logging.getLogger("dashboard_data")

VALID_CANTONS = {
    "AG", "AI", "AR", "BE", "BL", "BS", "FR", "GE", "GL", "GR",
    "JU", "LU", "NE", "NW", "OW", "SG", "SH", "SO", "SZ", "TG",
    "TI", "UR", "VD", "VS", "ZG", "ZH"
}

def export_dashboard_data(df_shab, udemo_df=None, out_dir="static/data"):
    """
    Generates dashboard-ready JSON files from the SHAB dataframe.
    """
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)

    if df_shab.empty:
        logger.warning("Empty SHAB dataframe provided. Skipping dashboard export.")
        return

    logger.info("Starting dashboard data export...")

    # 1. Prepare base dataframe
    df = df_shab.copy()

    # Ensure date is datetime
    if not pd.api.types.is_datetime64_any_dtype(df["date"]):
         df["date"] = pd.to_datetime(df["date"], errors="coerce")

    df = df.dropna(subset=["date"])

    # Create month column (YYYY-MM-01)
    df["month"] = df["date"].dt.to_period("M").dt.to_timestamp()

    # Normalize Canton
    # Canton codes in SHAB data might be uppercase or have whitespace.
    # We also need to handle multi-value cantons if any (though usually one per row in this dataset).
    df["kanton"] = df["kanton"].astype(str).str.upper().str.strip()
    df = df[df["kanton"].isin(VALID_CANTONS)]

    # Normalize HR type
    # We are interested in HR01 and HR03.
    # The column in df_shab is 'subrubric' based on app.py
    df["hr"] = df["subrubric"].astype(str).str.upper()
    df = df[df["hr"].isin(["HR01", "HR03"])]

    # 2. Aggregations

    # Canton Monthly Counts
    # Group by month, kanton, hr
    canton_monthly = df.groupby(["month", "kanton", "hr"]).size().reset_index(name="count")
    canton_monthly["geo"] = "KT"

    # CH Monthly Counts (Total)
    # Group by month, hr
    ch_monthly = df.groupby(["month", "hr"]).size().reset_index(name="count")
    ch_monthly["geo"] = "CH"
    ch_monthly["kanton"] = None

    # Combine
    combined = pd.concat([canton_monthly, ch_monthly], ignore_index=True)

    # 3. Compute NET (HR01 - HR03)
    # Pivot to get HR01 and HR03 in columns
    pivot_df = combined.pivot(index=["month", "geo", "kanton"], columns="hr", values="count").fillna(0)

    if "HR01" not in pivot_df.columns:
        pivot_df["HR01"] = 0
    if "HR03" not in pivot_df.columns:
        pivot_df["HR03"] = 0

    pivot_df["NET"] = pivot_df["HR01"] - pivot_df["HR03"]

    # Unpivot back to long format for NET
    net_df = pivot_df.reset_index()[["month", "geo", "kanton", "NET"]]
    net_df = net_df.rename(columns={"NET": "count"})
    net_df["hr"] = "NET"

    # Final concat
    final_df = pd.concat([combined, net_df], ignore_index=True)

    # 4. Format for JSON
    final_df["month"] = final_df["month"].dt.strftime("%Y-%m-%d")

    # Sort for tidiness
    final_df = final_df.sort_values(by=["month", "geo", "kanton", "hr"])

    # Export to JSON
    out_file = os.path.join(out_dir, "shab_monthly.json")
    final_df.to_json(out_file, orient="records")
    logger.info(f"Written {len(final_df)} rows to {out_file}")

    # 5. Export Dimensions (Metadata)
    dimensions = {
        "metrics": ["HR01", "HR03", "NET"],
        "measures": ["count"], # Add per_10k later if needed
        "cantons": sorted(list(VALID_CANTONS)),
        "months": sorted(final_df["month"].unique().tolist())
    }

    dim_file = os.path.join(out_dir, "dimensions.json")
    with open(dim_file, "w") as f:
        json.dump(dimensions, f, indent=2)
    logger.info(f"Written dimensions to {dim_file}")
