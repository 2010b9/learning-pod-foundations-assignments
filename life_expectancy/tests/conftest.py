"""Pytest configuration file"""
from typing import List
import pandas as pd
import pytest

from . import FIXTURES_DIR, OUTPUT_DIR


@pytest.fixture(autouse=True)
def run_before_and_after_tests() -> None:
    """Fixture to execute commands before and after a test is run"""
    # Setup: fill with any logic you want

    yield # this is where the testing happens

    # Teardown : fill with any logic you want
    file_path = OUTPUT_DIR / "pt_life_expectancy.csv"
    file_path.unlink(missing_ok=True)


@pytest.fixture(scope="session")
def pt_life_expectancy_expected() -> pd.DataFrame:
    """Fixture to load the expected output of the cleaning script"""
    return pd.read_csv(FIXTURES_DIR / "pt_life_expectancy_expected.csv")

@pytest.fixture(scope="session")
def eu_life_expectancy_tsv() -> pd.DataFrame:
    """Loads the EU life expectancy raw TSV"""
    return pd.read_csv(FIXTURES_DIR / "eu_life_expectancy_raw.tsv", sep="\t")


@pytest.fixture(scope="function")
def all_countries() -> List[str]:
    """Returns a list with all possible countries"""
    return [
        "AL", "AM", "AT", "AZ", "BE", "BG", "BY", "CH", "CY", "CZ", "DE",
        "DK", "EE", "EL", "ES", "FI", "FR", "GE", "HR", "HU", "IE", "IS",
        "IT", "LI", "LT", "LU", "LV", "ME", "MK", "MT", "NL", "NO", "PL",
        "PT", "RO", "RS", "SE", "SI", "SK", "TR", "UA"
    ]
