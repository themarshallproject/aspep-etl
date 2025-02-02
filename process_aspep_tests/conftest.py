import pytest
import pandas as pd
import json

@pytest.fixture(scope="module")
def combined_data():
    """Load the combined_data.json file into a pandas DataFrame."""
    with open("data/out/combined_data.json", "r") as f:
        data = json.load(f)
    return pd.DataFrame(data)
