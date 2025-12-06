import polars as pl
import glob

def combine_electricity_csvs():
    # Find all matching files
    files = sorted(glob.glob("electricity_*.csv"))

    if not files:
        raise FileNotFoundError("No files found with pattern electricity_*.csv")

    # Read them into a list of DataFrames
    dfs = [pl.read_csv(f) for f in files]

    # Combine them (vertical concat)
    combined = pl.concat(dfs, how="vertical")

    # Sort by Year (assumes column is named 'Year')
    combined = combined.sort("Year")

    # Output filename
    output_name = "electricity_2015_to_2024.csv"

    # Save result
    combined.write_csv(output_name)

    print(f"Created: {output_name}")
    print(f"Rows: {combined.height}, Columns: {combined.width}")

if __name__ == "__main__":
    combine_electricity_csvs()
