#!/usr/bin/env python3
"""
Batch driver to generate CORGIS-style electricity CSVs for multiple years
using build_electricity_data.py.

Example usage:

Root: for raw data 

  python3 batch_build_electricity_data.py \
    --root /home/jon/Documents/grad_school/stat_515/projects/final/website/website/data/power/raw \
    --start-year 2015 \
    --end-year 2024 \
    --build-script /home/jon/Documents/grad_school/stat_515/projects/final/website/website/utils/build_electricity_data.py \
    --out-dir /home/jon/Documents/grad_school/stat_515/projects/final/website/website/data/power/

This will look for:

  /home/jon/Downloads/f861YYYY/Utility_Data_YYYY.xlsx
  /home/jon/Downloads/f861YYYY/Operational_Data_YYYY.xlsx
  /home/jon/Downloads/f861YYYY/Sales_Ult_Cust_YYYY.xlsx

and produce:

  /home/jon/Documents/.../data/electricity_YYYY.csv
"""

import argparse
import subprocess
from pathlib import Path


def find_child_case_insensitive(parent: Path, name: str) -> Path | None:
    """
    Return the child of `parent` whose name matches `name` ignoring case.
    Used for directories where we know the exact name we expect (f861YYYY).
    """
    target = name.lower()
    if not parent.exists():
        return None
    for child in parent.iterdir():
        if child.name.lower() == target:
            return child
    return None


def find_file_fuzzy(parent: Path, keywords: list[str], year_str: str) -> Path | None:
    """
    Fuzzy search for an Excel file in `parent` whose name:

      - contains all `keywords` (case-insensitive)
      - contains the `year_str` (e.g., "2012")
      - ends with .xls or .xlsx (case-insensitive)

    Returns the first match, or None if not found.
    """
    if not parent.exists():
        return None

    year_str = year_str.lower()

    for child in parent.iterdir():
        if not child.is_file():
            continue

        name = child.name.lower()
        suffix = child.suffix.lower()

        if suffix not in (".xls", ".xlsx"):
            continue

        if year_str not in name:
            continue

        if all(k.lower() in name for k in keywords):
            return child

    return None


def build_for_year(year: int, root: Path, build_script: Path, out_dir: Path, python_cmd: str = "python3"):
    """
    Run build_electricity_data.py for a single year.

    root: directory that contains f861YYYY (any case) subfolders
    """
    year_str = str(year)

    # Find the f861YYYY directory ignoring case
    dir_name = f"f861{year_str}"
    data_dir = find_child_case_insensitive(root, dir_name)

    if data_dir is None:
        print(f"[{year}] Skipping: could not find directory (case-insensitive) named '{dir_name}' under {root}")
        return

    # Fuzzy file resolution inside the year directory
    utility = find_file_fuzzy(data_dir, keywords=["utility", "data"], year_str=year_str)
    operational = find_file_fuzzy(data_dir, keywords=["oper", "data"], year_str=year_str)
    # Sales files often have 'sales' and 'ult' or 'cust' in the name
    sales = find_file_fuzzy(data_dir, keywords=["sales", "ult"], year_str=year_str) \
        or find_file_fuzzy(data_dir, keywords=["sales", "cust"], year_str=year_str) \
        or find_file_fuzzy(data_dir, keywords=["sales"], year_str=year_str)

    # Output file
    out_path = out_dir / f"electricity_{year_str}.csv"

    # Check that the three input files exist
    missing = []
    if utility is None:
        missing.append("Utility (keywords: ['utility', 'data'])")
    if operational is None:
        missing.append("Operational (keywords: ['oper', 'data'])")
    if sales is None:
        missing.append("Sales (keywords: ['sales', 'ult'] or ['sales', 'cust'])")

    if missing:
        print(f"[{year}] Skipping: could not find the following files in {data_dir}:")
        for m in missing:
            print(f"    - {m}")
        print("[debug] Files actually present in that folder:")
        for child in sorted(data_dir.iterdir()):
            print(f"    - {child.name}")
        return

    # Ensure output directory exists
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"[{year}] Building from:")
    print(f"    Utility:     {utility}")
    print(f"    Operational: {operational}")
    print(f"    Sales:       {sales}")
    print(f"    Output:      {out_path}")

    cmd = [
        python_cmd,
        str(build_script),
        "--utility", str(utility),
        "--operational", str(operational),
        "--sales", str(sales),
        "--out", str(out_path),
        "--verbose",
    ]

    result = subprocess.run(cmd)
    if result.returncode != 0:
        print(f"[{year}] ERROR: build_electricity_data.py returned code {result.returncode}")
    else:
        print(f"[{year}] Done.")


def main():
    parser = argparse.ArgumentParser(
        description="Batch-run build_electricity_data.py over a range of years."
    )
    parser.add_argument(
        "--root",
        required=True,
        help="Root directory containing f861YYYY subfolders (any case), e.g. /home/jon/Downloads.",
    )
    parser.add_argument(
        "--build-script",
        required=True,
        help="Path to build_electricity_data.py.",
    )
    parser.add_argument(
        "--start-year",
        type=int,
        required=True,
        help="Start year (inclusive), e.g. 2017.",
    )
    parser.add_argument(
        "--end-year",
        type=int,
        required=True,
        help="End year (inclusive), e.g. 2024.",
    )
    parser.add_argument(
        "--out-dir",
        required=True,
        help="Directory where electricity_YYYY.csv files will be written.",
    )
    parser.add_argument(
        "--python-cmd",
        default="python3",
        help="Python command to use (default: python3).",
    )

    args = parser.parse_args()

    root = Path(args.root).expanduser().resolve()
    build_script = Path(args.build_script).expanduser().resolve()
    out_dir = Path(args.out_dir).expanduser().resolve()

    if not build_script.exists():
        raise FileNotFoundError(f"build_electricity_data.py not found at {build_script}")

    print(f"Root directory:   {root}")
    print(f"Build script:     {build_script}")
    print(f"Output directory: {out_dir}")
    print(f"Years:            {args.start_year}–{args.end_year}")

    for year in range(args.start_year, args.end_year + 1):
        build_for_year(
            year=year,
            root=root,
            build_script=build_script,
            out_dir=out_dir,
            python_cmd=args.python_cmd,
        )


if __name__ == "__main__":
    main()
