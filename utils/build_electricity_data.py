#!/usr/bin/env python3

# The following code was created with the assistance of ChatGPT although took
# multiple iterations to adapt and conform to our use case.

"""
Build a CORGIS-style electricity dataset from EIA-861 2024 Excel files.

Inputs (Excel):
  - Utility Data (Schedule 1)         → Utility_Data_2024.xlsx
  - Operational Data (Schedule 4)     → Operational_Data_2024.xlsx
  - Sales to Ultimate Customers (S2)  → Sales_Ult_Cust_2024.xlsx

Output (CSV):
  - electricity_2024.csv with columns:

    Utility.Number, Utility.Name, Utility.State, Utility.Type,
    Demand.Summer Peak, Demand.Winter Peak,
    Sources.Generation, Sources.Purchased, Sources.Other, Sources.Total,
    Uses.Retail, Uses.Resale, Uses.No Charge, Uses.Consumed, Uses.Losses, Uses.Total,
    Revenues.Retail, Revenue.Delivery, Revenue.Resale, Revenue.Adjustments,
    Revenue.Transmission, Revenue.Other, Revenue.Total,
    Retail.Residential.Revenue, Retail.Residential.Sales, Retail.Residential.Customers,
    Retail.Commercial.Revenue,  Retail.Commercial.Sales,  Retail.Commercial.Customers,
    Retail.Industrial.Revenue,  Retail.Industrial.Sales,  Retail.Industrial.Customers,
    Retail.Transportation.Revenue, Retail.Transportation.Sales, Retail.Transportation.Customers,
    Retail.Total.Revenue, Retail.Total.Sales, Retail.Total.Customers
"""

import argparse
from pathlib import Path
import pandas as pd


# ---------------------------------------------------------------------------
# Generic helpers
# ---------------------------------------------------------------------------

def detect_header_row(path, sheet_name=0, schedule="utility", verbose=False):
    """
    Detect the header row by scanning for marker strings that should appear
    in the header row for each schedule.
    """
    raw = pd.read_excel(path, sheet_name=sheet_name, header=None)
    lower = raw.astype(str).applymap(lambda x: x.lower())

    if schedule == "utility":
        markers = ["data year", "utility number", "utility name", "state"]
    elif schedule == "operational":
        markers = ["data year", "utility number", "summer", "winter", "demand"]
    elif schedule == "sales":
        markers = ["data year", "utility number", "state", "residential", "commercial", "industrial"]
    else:
        markers = ["data year", "utility number", "utility name"]

    markers = [m.lower() for m in markers]

    header_row = None
    for i in lower.index:
        row_values = lower.loc[i].tolist()
        score = 0
        for m in markers:
            if any(m in cell for cell in row_values):
                score += 1
        if score >= max(2, len(markers) // 2):
            header_row = i
            if verbose:
                print(f"[{schedule}] Detected header row {header_row} in {path}")
            break

    if header_row is None:
        if verbose:
            print(f"[{schedule}] WARNING: Could not find a good header row in {path}. Using first row.")
        header_row = 0

    return header_row


def read_excel_with_detected_header(path, sheet_name=0, schedule="utility", verbose=False):
    """
    Use detect_header_row to find the header and then read the sheet with that
    row as pandas' header.
    """
    header_row = detect_header_row(path, sheet_name=sheet_name, schedule=schedule, verbose=verbose)
    df = pd.read_excel(path, sheet_name=sheet_name, header=header_row)
    df.columns = [str(c).strip() for c in df.columns]
    return df


def get_utility_id_col(df):
    """
    Try to find the 'Utility Number' / 'Utility ID' / 'Respondent ID' column.

    If we can't find anything, fall back to 'Utility Name', and if that still
    fails, fall back to the first column (with a warning).
    """
    lower_map = {str(c).lower(): c for c in df.columns}

    candidates = [
        "utility number",
        "utility id",
        "eia utility id",
        "utility_identifier",
        "respondent id",
        "respondentid",
        "respondent identification",
        "entity id",
        "eiaid",
    ]

    for key in candidates:
        if key in lower_map:
            return lower_map[key]

    for col in df.columns:
        name = str(col).lower()
        if "utility" in name and "number" in name:
            return col

    for col in df.columns:
        name = str(col).lower()
        if "respondent" in name and "id" in name:
            return col

    for col in df.columns:
        name = str(col).lower()
        if "utility" in name and "name" in name:
            print("WARNING: Using 'Utility Name' as join key because no Utility Number / ID column was found.")
            return col

    print("WARNING: Could not find a Utility ID / Number column. Using the first column as join key.")
    print("Columns present in this DataFrame:")
    for c in df.columns:
        print("  -", c)
    return df.columns[0]


def find_col(df, include, exclude=None, required=True, verbose=False, label=""):
    """
    Find a column whose name contains all strings in `include` and none in
    `exclude`. Case-insensitive. Returns column name as string.
    """
    if isinstance(include, str):
        include = [include]
    exclude = exclude or []

    for col in df.columns:
        name = str(col).lower()
        if all(s.lower() in name for s in include) and not any(
            s.lower() in name for s in exclude
        ):
            return col

    if required:
        print(f"ERROR: No column found matching include={include}, exclude={exclude}")
        print("Available columns:")
        for c in df.columns:
            print("  -", c)
        raise KeyError(f"No column found for {label or include}")
    return None


# ---------------------------------------------------------------------------
# Schedule loaders
# ---------------------------------------------------------------------------

def load_utility_schedule(path, verbose=False):
    """
    Load Utility Data (Schedule 1).

    Returns a DataFrame with:
      Utility.Number, Utility.Name, Utility.State, Utility.Type
    """
    df = read_excel_with_detected_header(path, schedule="utility", verbose=verbose)

    if verbose:
        print("\n[utility] Columns detected:")
        for c in df.columns:
            print("  -", c)

    id_col = get_utility_id_col(df)

    name_col = find_col(df, ["utility name"], required=False)
    if name_col is None:
        name_col = find_col(df, ["entity"], required=True, label="Utility Name")

    state_col = find_col(df, ["state"], label="State")

    type_col = find_col(df, ["ownership"], required=False)
    if type_col is None:
        type_col = find_col(df, ["entity type"], required=False)
    if type_col is None:
        type_col = find_col(df, ["ownership type"], required=False)
    if type_col is None:
        df["__type_dummy__"] = pd.NA
        type_col = "__type_dummy__"

    out = pd.DataFrame()
    out["Utility.Number"] = df[id_col]
    out["Utility.Name"] = df[name_col].astype(str)
    out["Utility.State"] = df[state_col]
    out["Utility.Type"] = df[type_col]

    return out


def load_operational_schedule(path, verbose=False):
    """
    Load Operational Data (Schedule 4).

    Returns DataFrame keyed by Utility.Number with:
      Demand.*, Sources.*, Uses.*, and top-level Revenue.* fields.
    """
    df = read_excel_with_detected_header(path, schedule="operational", verbose=verbose)

    if verbose:
        print("\n[operational] Columns detected:")
        for c in df.columns:
            print("  -", c)

    id_col = get_utility_id_col(df)

    out = pd.DataFrame()
    out["Utility.Number"] = df[id_col]

    # ---------------- Demand ----------------
    out["Demand.Summer Peak"] = df[find_col(df, ["summer", "peak", "demand"], label="Demand.Summer Peak")]
    out["Demand.Winter Peak"] = df[find_col(df, ["winter", "peak", "demand"], label="Demand.Winter Peak")]

    # ---------------- Sources (Energy Sources MWh) ----------------
    out["Sources.Generation"] = df[find_col(df, ["net", "generation"], label="Sources.Generation")]
    out["Sources.Purchased"]  = df[find_col(df, ["wholesale", "power", "purchases"], label="Sources.Purchased")]

    other_src_col = find_col(df, ["other", "source"], required=False)
    if other_src_col is None:
        other_src_col = find_col(df, ["other", "supply"], required=False)
    if other_src_col is None:
        df["__other_sources_dummy__"] = pd.NA
        other_src_col = "__other_sources_dummy__"
    out["Sources.Other"] = df[other_src_col]

    total_sources_col = find_col(df, ["total", "sources"], required=False)
    if total_sources_col is None:
        total_sources_col = find_col(df, ["total", "energy", "sources"], required=False)
    if total_sources_col is None:
        df["__total_sources_dummy__"] = pd.NA
        total_sources_col = "__total_sources_dummy__"
    out["Sources.Total"] = df[total_sources_col]

    # ---------------- Uses / Disposition (MWh) ----------------
    # Uses.Retail: across years this can be:
    #   - "Sales to Ultimate Customers"
    #   - "Retail Sales"
    #   - similar variants
    retail_col = None
    for inc in (
        ["sales", "ultimate", "customers"],  # older wording
        ["sales", "to", "ultimate"],        # variant
        ["retail", "sales"],                # 2021 wording
    ):
        retail_col = find_col(df, inc, required=False)
        if retail_col is not None:
            break

    if retail_col is None:
        # last fallback: just look for any "sales" column that is not clearly "for resale"
        retail_col = find_col(df, ["sales"], exclude=["for resale"], required=False)

    if retail_col is None:
        if verbose:
            print("[operational] WARNING: Could not find Retail Sales / Sales to Ultimate Customers column; filling 0.")
        df["__uses_retail_dummy__"] = pd.NA
        retail_col = "__uses_retail_dummy__"

    out["Uses.Retail"] = df[retail_col]

    # Uses.Resale: "Sales for Resale" (your sample still has this)
    out["Uses.Resale"] = df[find_col(df, ["sales", "for resale"], label="Uses.Resale")]

    # Furnished without charge
    out["Uses.No Charge"] = df[find_col(df, ["furnished", "without", "charge"], label="Uses.No Charge")]

    # Consumed by respondent without charge
    out["Uses.Consumed"] = df[find_col(df, ["consumed", "respondent"], label="Uses.Consumed")]

    # Total Energy Losses
    losses_col = find_col(
        df,
        ["total", "energy", "loss"],
        required=False,
        label="Uses.Losses"
    )
    if losses_col is None:
        losses_col = find_col(df, ["losses"], required=False, label="Uses.Losses")
    if losses_col is None:
        df["__losses_dummy__"] = pd.NA
        losses_col = "__losses_dummy__"
    out["Uses.Losses"] = df[losses_col]

    # Total Disposition / Total Uses
    uses_total_col = find_col(df, ["total", "disposition"], required=False)
    if uses_total_col is None:
        uses_total_col = find_col(df, ["total", "uses"], required=False)
    if uses_total_col is None:
        df["__uses_total_dummy__"] = pd.NA
        uses_total_col = "__uses_total_dummy__"
    out["Uses.Total"] = df[uses_total_col]

    # ---------------- Electric Revenues (Thousands Dollars) ----------------
    out["Revenues.Retail"] = df[find_col(
        df,
        ["from", "retail", "sales"],
        label="Revenues.Retail"
    )]

    out["Revenue.Delivery"] = df[find_col(
        df,
        ["from", "delivery", "customers"],
        label="Revenue.Delivery"
    )]

    out["Revenue.Resale"] = df[find_col(
        df,
        ["from", "sales", "for resale"],
        label="Revenue.Resale"
    )]

    out["Revenue.Adjustments"] = df[find_col(
        df,
        ["from", "credits", "adjustments"],
        label="Revenue.Adjustments"
    )]

    out["Revenue.Transmission"] = df[find_col(
        df,
        ["from", "transmission"],
        label="Revenue.Transmission"
    )]

    out["Revenue.Other"] = df[find_col(
        df,
        ["from", "other"],
        label="Revenue.Other"
    )]

    # Revenue.Total: explicit total if present; otherwise sum parts
    rev_total_col = find_col(
        df,
        ["total"],
        exclude=["sales", "customers", "mwh", "kwh"],
        required=False,
        label="Revenue.Total"
    )

    if rev_total_col is not None:
        out["Revenue.Total"] = df[rev_total_col]
    else:
        out["Revenue.Total"] = (
            out["Revenues.Retail"]
            + out["Revenue.Delivery"]
            + out["Revenue.Resale"]
            + out["Revenue.Adjustments"]
            + out["Revenue.Transmission"]
            + out["Revenue.Other"]
        )

    return out



def load_sales_schedule(path, verbose=False):
    """
    Load Sales to Ultimate Customers (Sales_Ult_Cust_YYYY.xlsx) and extract
    sector-level Retail.*.{Revenue,Sales,Customers} fields.

    Assumes layout like:

      Utility Characteristics ... RESIDENTIAL ... COMMERCIAL ... INDUSTRIAL ... TRANSPORTATION ... TOTAL
      (maybe another row)
      Data Year  Utility Number  Utility Name  Part  Service Type  ...  BA Code
      Thousand Dollars  Megawatthours  Count  Thousand Dollars  Megawatthours  Count  ...

      2024  55  City of Aberdeen - (MS)  A Bundled O MS Municipal TVA
        4,211.0 34,239 2,592  3,550.0 29,391 736  7,185.0 127,260 2  0.0 0 0  14,946.0 190,890 3,330

    We do NOT define Revenues.Retail or Revenue.* totals here; those come from
    the Operational file. This function only produces the Retail.* sector fields.
    """
    # Read raw, no header
    raw = pd.read_excel(path, header=None)
    lower = raw.astype(str).applymap(lambda x: x.lower())

    # ---- 1. Find the "Data Year / Utility Number" header row ----
    header_row = None
    for i in lower.index:
        row_vals = lower.loc[i].tolist()
        has_data_year = any("data year" in cell for cell in row_vals)
        has_util_num = any("utility number" in cell for cell in row_vals)
        if has_data_year and has_util_num:
            header_row = i
            break

    if header_row is None:
        print("ERROR: Could not find 'Data Year / Utility Number' header row in sales file.")
        print("First few rows:")
        print(lower.head(10))
        raise KeyError("Data Year / Utility Number header not found in sales file.")

    if verbose:
        print(f"\n[sales] Detected header_row = {header_row}")
        print("[sales] Header row values:")
        print(raw.iloc[header_row].tolist())

    # ---- 2. Find the Utility Number column index ----
    id_col = None
    for j in range(lower.shape[1]):
        if "utility number" in lower.iloc[header_row, j]:
            id_col = j
            break

    if id_col is None:
        print("ERROR: Could not find Utility Number column in sales file.")
        print("Header row values:", lower.iloc[header_row].tolist())
        raise KeyError("Utility Number column not found in sales file.")

    # ---- 3. Find all revenue columns via 'Thousand Dollars' in header_row ----
    thousand_cols = []
    for j in range(lower.shape[1]):
        if "thousand dollars" in lower.iloc[header_row, j]:
            thousand_cols.append(j)

    # We expect at least 5: Residential, Commercial, Industrial, Transportation, Total
    if len(thousand_cols) < 5:
        print("ERROR: Expected at least 5 'Thousand Dollars' columns (RES, COM, IND, TRANS, TOTAL).")
        print("Header row values:", lower.iloc[header_row].tolist())
        print("Found 'Thousand Dollars' at columns:", thousand_cols)
        raise KeyError("Not enough 'Thousand Dollars' columns to map sectors.")

    if verbose:
        print("\n[sales] 'Thousand Dollars' columns:", thousand_cols)

    # Map them in order:
    #   [0] Residential, [1] Commercial, [2] Industrial, [3] Transportation, [4] Total
    sector_order = ["RESIDENTIAL", "COMMERCIAL", "INDUSTRIAL", "TRANSPORTATION", "TOTAL"]
    sector_labels = {
        "RESIDENTIAL": "Retail.Residential",
        "COMMERCIAL": "Retail.Commercial",
        "INDUSTRIAL": "Retail.Industrial",
        "TRANSPORTATION": "Retail.Transportation",
        "TOTAL": "Retail.Total",
    }

    # Build mapping: sector -> (rev_col, sales_col, cust_col)
    sector_cols = {}
    for idx, sector in enumerate(sector_order):
        if idx >= len(thousand_cols):
            break
        rev_col = thousand_cols[idx]
        sales_col = rev_col + 1
        cust_col = rev_col + 2
        sector_cols[sector] = (rev_col, sales_col, cust_col)

    if verbose:
        print("\n[sales] sector -> (rev_col, sales_col, cust_col):")
        for sec, cols in sector_cols.items():
            print(f"  {sec}: {cols}")

    # ---- 4. Extract data rows (everything after header_row) ----
    data = raw.iloc[header_row + 1 :].copy()
    util_nums = data.iloc[:, id_col]

    # Keep only rows with a valid utility number
    mask_valid_util = util_nums.notna() & (util_nums.astype(str).str.strip() != "")
    data = data[mask_valid_util].copy()
    util_nums = data.iloc[:, id_col]

    # ---- 5. Helper to parse numeric columns safely (handle '.', commas, etc.) ----
    def parse_numeric(series):
        s = series.astype(str)
        # Replace cells that are exactly "." with "0" (EIA suppression)
        s = s.mask(s == ".", "0")
        # Remove commas in numbers
        s = s.str.replace(",", "", regex=False)
        return pd.to_numeric(s, errors="coerce").fillna(0)

    # ---- 6. Build output DataFrame ----
    out = pd.DataFrame()
    # DO NOT force to numeric if you want to be extra-safe on merge types;
    # but numeric is usually fine given Utility.Number is numeric in other schedules.
    out["Utility.Number"] = pd.to_numeric(util_nums, errors="coerce")

    for sector, (rev_col, sales_col, cust_col) in sector_cols.items():
        prefix = sector_labels[sector]
        out[f"{prefix}.Revenue"]   = parse_numeric(data.iloc[:, rev_col])
        out[f"{prefix}.Sales"]     = parse_numeric(data.iloc[:, sales_col])
        out[f"{prefix}.Customers"] = parse_numeric(data.iloc[:, cust_col])

    # We intentionally do NOT define Revenues.Retail or Revenue.Total here.
    # Those come from the Operational schedule.
    return out


import re

def extract_year_from_path(path: Path):
    """
    Extract a 4-digit year from the filename or parent directory.
    Looks for patterns like 2017, 2020, 2024.
    Returns an int or raises ValueError if not found.
    """
    text = str(path)

    match = re.search(r"(20[0-3][0-9])", text)  # matches 2000–2039
    if match:
        return int(match.group(1))

    raise ValueError(f"Could not extract year from path: {path}")



# ---------------------------------------------------------------------------
# Main orchestration
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Build CORGIS-style electricity CSV from EIA-861 Excel files.")
    parser.add_argument("--utility", required=True, help="Path to Utility Data Excel (Schedule 1).")
    parser.add_argument("--operational", required=True, help="Path to Operational Data Excel (Schedule 4).")
    parser.add_argument("--sales", required=True, help="Path to Sales to Ultimate Customers Excel (Schedule 2).")
    parser.add_argument("--out", default="electricity_2024.csv", help="Output CSV path.")
    parser.add_argument("--verbose", action="store_true", help="Print debug info.")
    args = parser.parse_args()

    util_df = load_utility_schedule(Path(args.utility), verbose=args.verbose)
    op_df = load_operational_schedule(Path(args.operational), verbose=args.verbose)
    sales_df = load_sales_schedule(Path(args.sales), verbose=args.verbose)

    # Merge on Utility.Number
    merged = util_df.merge(op_df, on="Utility.Number", how="left") \
                    .merge(sales_df, on="Utility.Number", how="left")

    # Ensure final column order
    columns = [
        "Utility.Number", "Utility.Name", "Utility.State", "Utility.Type",
        "Demand.Summer Peak", "Demand.Winter Peak",
        "Sources.Generation", "Sources.Purchased", "Sources.Other", "Sources.Total",
        "Uses.Retail", "Uses.Resale", "Uses.No Charge", "Uses.Consumed",
        "Uses.Losses", "Uses.Total",
        "Revenues.Retail", "Revenue.Delivery", "Revenue.Resale", "Revenue.Adjustments",
        "Revenue.Transmission", "Revenue.Other", "Revenue.Total",
        "Retail.Residential.Revenue", "Retail.Residential.Sales", "Retail.Residential.Customers",
        "Retail.Commercial.Revenue", "Retail.Commercial.Sales", "Retail.Commercial.Customers",
        "Retail.Industrial.Revenue", "Retail.Industrial.Sales", "Retail.Industrial.Customers",
        "Retail.Transportation.Revenue", "Retail.Transportation.Sales", "Retail.Transportation.Customers",
        "Retail.Total.Revenue", "Retail.Total.Sales", "Retail.Total.Customers",
    ]

    for col in columns:
        if col not in merged.columns:
            merged[col] = pd.NA

    merged = merged[columns]

    # Drop rows that are completely empty numerically (optional)
    num_cols = merged.select_dtypes(include="number").columns
    merged = merged.dropna(how="all", subset=num_cols)

    # Data Cleaning
    merged = merged.drop_duplicates()
    merged = merged.fillna(0)

    # Replace ONLY cells that are exactly "." with 0
    mask = merged == "."
    merged = merged.mask(mask, 0)

    # Remove commas from numbers (e.g., '1,234.56')
    merged = merged.replace(",", "", regex=True)

    year = extract_year_from_path(Path(args.utility))

    merged["Year"] = year

    # Convert numeric-like columns
    for col in merged.columns:
        try:
            merged[col] = pd.to_numeric(merged[col])
        except Exception:
            pass

    out_path = Path(args.out)
    merged.to_csv(out_path, index=False)
    print(f"Wrote {out_path} with shape {merged.shape}")


if __name__ == "__main__":
    main()





""""
/home/jon/Downloads/f8612024/
Utility_Data_2024.xlsx
Operational_Data_2024.xlsx
Sales_Ult_Cust_2024.xlsx 

python3 build_electricity_data.py \
  --utility /home/jon/Documents/grad_school/stat_515/projects/final/website/website/data/power/raw/f8612024/Utility_Data_2024.xlsx \
  --operational /home/jon/Documents/grad_school/stat_515/projects/final/website/website/data/power/raw/f8612024/Operational_Data_2024.xlsx \
  --sales /home/jon/Documents/grad_school/stat_515/projects/final/website/website/data/power/raw/f8612024/Sales_Ult_Cust_2024.xlsx \
  --out electricity_test.csv

  
  python3 build_electricity_data.py \
  --utility /home/jon/Documents/grad_school/stat_515/projects/final/website/website/data/power/raw/f8612021/Utility_Data_2021.xlsx \
  --operational /home/jon/Documents/grad_school/stat_515/projects/final/website/website/data/power/raw/f8612021/Operational_Data_2021.xlsx \
  --sales /home/jon/Documents/grad_school/stat_515/projects/final/website/website/data/power/raw/f8612021/Sales_Ult_Cust_2021.xlsx \
  --out electricity_test.csv


  python3 build_electricity_data.py \
  --utility /home/jon/Documents/grad_school/stat_515/projects/final/website/website/data/power/raw/f8612017/Utility_Data_2017.xlsx \
  --operational /home/jon/Documents/grad_school/stat_515/projects/final/website/website/data/power/raw/f8612017/Operational_Data_2017.xlsx \
  --sales /home/jon/Documents/grad_school/stat_515/projects/final/website/website/data/power/raw/f8612017/Sales_Ult_Cust_2017.xlsx \
  --out electricity_test.csv

"""
