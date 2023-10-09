"""Test for load_data function"""
from unittest.mock import patch
import pandas as pd
from life_expectancy.clean_data.utils.load_save_data import save_data
from . import FIXTURES_DIR


def test_load_data(eu_life_expectancy_tsv):
    """Test the data loading"""
    assert eu_life_expectancy_tsv.shape == (100, 63)


@patch("pandas.DataFrame.to_csv")
def test_save_data(patched_to_csv):
    """Test the data saving"""
    df_to_save = pd.read_csv(FIXTURES_DIR / "pt_life_expectancy_expected.csv")
    output_path = "dummy_path.csv"
    save_data(df_to_save, output_path)
    patched_to_csv.assert_called_once_with(output_path, index=False)
