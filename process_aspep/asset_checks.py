import pandas as pd
import json
from dagster import asset_check, AssetCheckResult
from .assets import combine_years, derive_stats, derive_extended_stats 
from math import isclose

# Helper function to load data
def load_data(data_path):
    with open(data_path, "r") as f:
        data = json.load(f)
    return pd.DataFrame(data)

# List of asset checks to generate
CHECKS = [
    (combine_years, "WI", "corrections", 2017, "total_pay", 42_327_514),
    (combine_years, "WI", "education - higher education instructional", 2021, "total_pay", 88_769_896),
    (combine_years, "AR", "judicial and legal", 2022, "ft_pay", 8_001_374),
    (combine_years, "CA", "hospitals", 2022, "pt_employment", 10_250),
    (combine_years, "GA", "public welfare", 2020, "pt_pay", 17_900),
    (combine_years, "IN", "police protection total", 2020, "ft_eq_employment", 1_820),
    (combine_years, "US", "total - all government employment functions", 2019, "ft_pt_employment", 5_497_394),
    (combine_years, "HI", "financial administration", 2018, "ft_employment", 692),
    (combine_years, "AZ", "electric power", 2024, "ft_employment", 4),
    (combine_years, "WA", "corrections", 2024, "ft_pay", 71_593_739),
    (derive_stats, "MO", "corrections", 2024, "pay_per_fte", round(38_884_335 / 9_591, 2)),
    (derive_stats, "CA", "hospitals", 2020, "pay_per_ft", round(473_139_785 / 48_767, 2)),
    (derive_extended_stats, "IA", "hospitals", 2024, "ft_eq_employment_5yr_abs", 10_004 - 9_172), 
    (derive_extended_stats, "IA", "hospitals", 2024, "ft_eq_employment_1yr_abs", 10_004 - 9_386), 
    (derive_extended_stats, "NE", "public welfare", 2022, "ft_employment_5yr_abs", 2_167 - 2_426), 
    (derive_extended_stats, "DE", "natural resources", 2008, "ft_employment_5yr_abs", 485 - 420), 
]


# Factory function to create asset checks
def create_asset_check(asset, state, gov_function, year, column, expected_value):
    """Generate an asset check function dynamically."""
    
    @asset_check(asset=asset, name=f"check_{state.lower().replace(' ', '_')}_{year}_{gov_function.replace(' ', '_').replace('-', '')}_{column}")
    def check_fn():
        if asset == combine_years:
            df = load_data('data/out/combined_data.json')
        elif asset == derive_stats:
            df = load_data('data/out/aspep_with_derived_stats.json')
        elif asset == derive_extended_stats:
            df = load_data('data/out/aspep_with_extended_derived_stats.json')

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

        actual_value = row[column].values[0]
        passed = bool(isclose(actual_value, expected_value, rel_tol=1e-3))

        return AssetCheckResult(
            passed=passed,
            metadata={
                "state": state,
                "gov_function": gov_function,
                "year": float(year),
                "column": column,
                "expected": float(expected_value),
                "actual": float(actual_value),
            }
        )

    return check_fn

# Dynamically create and register asset checks
asset_checks = [create_asset_check(*params) for params in CHECKS]