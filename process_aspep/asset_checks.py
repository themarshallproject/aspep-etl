from dagster import asset_check, AssetCheckResult
import pandas as pd
import json
from .assets import combine_years  # Link to the asset

# Helper function to load data
def load_combined_data():
    with open("data/out/combined_data.json", "r") as f:
        data = json.load(f)
    return pd.DataFrame(data)

# List of asset checks to generate
CHECKS = [
    ("WI", "corrections", 2017, "total_pay", 42_327_514),
    ("WI", "education - higher education instructional", 2021, "total_pay", 88_769_896),
    ("AR", "judicial and legal", 2022, "ft_pay", 8_001_374),
    ("CA", "hospitals", 2022, "pt_employment", 10_250),
    ("GA", "public welfare", 2020, "pt_pay", 17_900),
    ("IN", "police protection total", 2020, "ft_eq_employment", 1_820),
    ("US", "total - all government employment functions", 2019, "ft_pt_employment", 5_497_394),
    ("HI", "financial administration", 2018, "ft_employment", 692),
]


# Factory function to create asset checks
def create_asset_check(state, gov_function, year, column, expected_value):
    """Generate an asset check function dynamically."""
    
    @asset_check(asset=combine_years, name=f"check_{state.lower().replace(' ', '_')}_{year}_{gov_function.replace(' ', '_').replace('-', '')}_{column}")
    def check_fn():
        df = load_combined_data()
        row = df[
            (df["state code"] == state) &
            (df["gov_function"] == gov_function) &
            (df["year"] == year)
        ]

        if row.empty:
            return AssetCheckResult(
                passed=False,
                metadata={"reason": "Row not found", "expected": expected_value}
            )

        actual_value = int(row[column].values[0])

        return AssetCheckResult(
            passed=actual_value == expected_value,
            metadata={
                "state": state,
                "gov_function": gov_function,
                "year": year,
                "column": column,
                "expected": expected_value,
                "actual": actual_value
            }
        )

    return check_fn

# Dynamically create and register asset checks
asset_checks = [create_asset_check(*params) for params in CHECKS]