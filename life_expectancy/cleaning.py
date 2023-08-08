from pathlib import Path
import argparse
import pandas as pd


CURRENT_DIR = str(Path(__file__).parent)
LIFE_EXPECTANCY_DATA_PATH = CURRENT_DIR + "/data/eu_life_expectancy_raw.tsv"
OUTPUT_DATA_PATH = CURRENT_DIR + "/data/pt_life_expectancy.csv"


def main(region: str = "PT") -> None:
    """Loads the life expectancy, cleans and saves it as a csv file"""
    life_expectancy_df = load_data()
    life_expectancy_df_cleaned = clean_data(life_expectancy_df, region)
    save_data(life_expectancy_df_cleaned)


def load_data() -> pd.DataFrame:
    """Loads the life expectancy TSV to a pandas DataFrame"""
    return pd.read_table(LIFE_EXPECTANCY_DATA_PATH)


def clean_data(
    life_expectancy_df: pd.DataFrame, region: str = "PT"
) -> pd.DataFrame:
    """Cleans the data in the life expectancy DataFrame"""
    return (
        life_expectancy_df
        .pipe(_split_column_into_several)
        .pipe(_aggregate_columns)
        .pipe(_process_values)
        .pipe(_cast_columns_to_correct_types)
        .pipe(_filter_dataset_by_region, region=region)
    )


def save_data(life_expectancy_df_cleaned: pd.DataFrame) -> None:
    """Saves the cleaned life expectancy DataFrame to a csv file"""
    life_expectancy_df_cleaned.to_csv(OUTPUT_DATA_PATH, index=False)


def _split_column_into_several(
    life_expectancy_df: pd.DataFrame
) -> pd.DataFrame:
    """Split unit,sex,age,geo\time column into several columns"""
    col_to_split = r"unit,sex,age,geo\time"  # Use raw string due to \t
    df_to_split = life_expectancy_df[col_to_split]
    df_unchanged = life_expectancy_df.drop(columns=[col_to_split])

    return (
        df_to_split.str.split(",", expand=True)
        .rename({0: "unit", 1: "sex", 2: "age", 3: "region"}, axis=1)
        .join(df_unchanged)
    )

def _aggregate_columns(life_expectancy_df: pd.DataFrame) -> pd.DataFrame:
    """Aggregates the years and values in to `year` and `value` columns"""
    id_vars = ["unit", "sex", "age", "region"]
    value_vars = life_expectancy_df.drop(columns=id_vars).columns

    return (
        pd.melt(life_expectancy_df, id_vars, value_vars)
        .rename({"variable": "year"}, axis=1)
    )


def _process_values(life_expectancy_df: pd.DataFrame) -> pd.DataFrame:
    """Arranges the values of the `year` and `value` columns"""
    life_expectancy_df.loc[:, "year"] = life_expectancy_df.year.str.strip()
    life_expectancy_df.loc[:, "value"] = life_expectancy_df.value.str.strip(
        " epb: "
    )
    return life_expectancy_df[life_expectancy_df.value != ""]


def _cast_columns_to_correct_types(
    life_expectancy_df: pd.DataFrame
) -> pd.DataFrame:
    """Cast columns to their proper types"""
    life_expectancy_df = life_expectancy_df.astype(
        {"year": "int", "value": "float"}
    )
    return life_expectancy_df[life_expectancy_df.value != ""]


def _filter_dataset_by_region(
    life_expectancy_df: pd.DataFrame, region: str
) -> pd.DataFrame:
    """Filter the life expectancy DataFrame by region"""
    return life_expectancy_df[life_expectancy_df.region == region]


if __name__ == "__main__": # pragma: no cover
    parser = argparse.ArgumentParser()
    parser.add_argument("--region", default="PT")
    args = parser.parse_args()
    main(args.region)
