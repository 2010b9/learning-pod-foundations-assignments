"""Test for load_data function"""
from life_expectancy.clean_data.utils.load_save_data import (
    load_data, save_data
)
from . import FIXTURES_DIR
import pandas as pd
from unittest.mock import patch


def test_load_data():
    """Test the data loading"""
    df_input = load_data(FIXTURES_DIR / "eu_life_expectancy_raw.tsv")
    assert df_input.shape == (100, 63)


@patch("pandas.DataFrame.to_csv")
def test_save_data(patched_to_csv):
    """Test the data saving"""
    patched_to_csv.return_value = "Hello!"
    df_to_save = pd.read_csv(FIXTURES_DIR / "pt_life_expectancy_expected.csv")
    a = save_data(df_to_save, "dummy_path.csv")
    assert a == "Hello!"