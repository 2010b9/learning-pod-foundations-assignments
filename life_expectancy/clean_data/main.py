import argparse
from pathlib import Path
import pandas as pd
from .utils.cleaning import clean_data
from .utils.load_save_data import load_data


CURRENT_DIR = str(Path(__file__).parents[1])
LIFE_EXPECTANCY_DATA_PATH = CURRENT_DIR + "/data/eu_life_expectancy_raw.tsv"


def main(
    region: str = "PT", path: str = LIFE_EXPECTANCY_DATA_PATH
) -> pd.DataFrame:
    """Loads the life expectancy, cleans and saves it as a csv file"""
    life_expectancy_df = load_data(path)
    life_expectancy_df_cleaned = clean_data(life_expectancy_df, region)
    print(life_expectancy_df_cleaned)
    return life_expectancy_df_cleaned


if __name__ == "__main__": # pragma: no cover
    parser = argparse.ArgumentParser()
    parser.add_argument("--region", default="PT")
    region_str = parser.parse_args().region
    main(region_str)
