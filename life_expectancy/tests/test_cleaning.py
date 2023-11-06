"""Tests for the cleaning module"""
import pandas as pd
from life_expectancy.clean_data.main import main
from life_expectancy.clean_data.utils.countries import Country
from . import FIXTURES_DIR


def test_clean_data():
    """Run the `clean_data` function and compare the output to the expected output"""
    pt_life_expectancy_actual = main(
        Country.PT, FIXTURES_DIR / "eu_life_expectancy_raw.tsv"
    )
    pt_life_expectancy_expected = pd.read_csv(
        FIXTURES_DIR / "pt_life_expectancy_expected.csv"
    )

    pd.testing.assert_frame_equal(
        pt_life_expectancy_actual, pt_life_expectancy_expected
    )
