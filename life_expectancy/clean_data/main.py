import argparse
from pathlib import Path
import pandas as pd
from life_expectancy.clean_data.utils.cleaning import clean_data
from life_expectancy.clean_data.utils.load_save_data import (
    load_data, TSVReadStrategy
)
from life_expectancy.clean_data.utils.countries import Country


CURRENT_DIR = str(Path(__file__).parents[1])
LIFE_EXPECTANCY_DATA_PATH = CURRENT_DIR + "/data/eu_life_expectancy_raw.tsv"


def main(
    region: Country = Country.PT, path: str = LIFE_EXPECTANCY_DATA_PATH
) -> pd.DataFrame:
    """Loads the life expectancy, cleans and saves it as a csv file"""
    life_expectancy_df = load_data(path, TSVReadStrategy())
    life_expectancy_df_cleaned = clean_data(life_expectancy_df, region)
    return life_expectancy_df_cleaned


if __name__ == "__main__": # pragma: no cover
    parser = argparse.ArgumentParser()

    # Solution based in https://stackoverflow.com/questions/43968006/
    # support-for-enum-arguments-in-argparse
    parser.add_argument(
        "--region", type=lambda country: Country[country],
        choices=list(Country), default=Country.PT
    )

    main(parser.parse_args().region)
