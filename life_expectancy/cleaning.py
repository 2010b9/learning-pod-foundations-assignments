from pathlib import Path
import argparse
import pandas as pd


CURRENT_DIR = str(Path(__file__).parent)
LIFE_EXPECTANCY_DATA_PATH = CURRENT_DIR + "/data/eu_life_expectancy_raw.tsv"
OUTPUT_DATA_PATH = CURRENT_DIR + "/data/pt_life_expectancy.csv"


class LifeExpectancyData:
    """Life Expectancy class that stores the life expectancy data"""
    def __init__(self, path) -> None:
        self.life_expectancy_df = self._read_tsv_file_to_pandas_dataframe(path)

    @staticmethod
    def _read_tsv_file_to_pandas_dataframe(path: str) -> pd.DataFrame:
        """Reads tsv file and loads it to a pandas DataFrame"""
        return pd.read_table(path)

    def _set_life_expectancy_df(self, new_df: pd.DataFrame) -> None:
        """Sets the dataset in corresponding attribute"""
        self.life_expectancy_df = new_df

    def get_life_expectancy_df(self) -> pd.DataFrame:
        """Returns the dataset"""
        return self.life_expectancy_df

    def _split_column_into_several(self) -> pd.DataFrame:
        """Split unit,sex,age,geo\time column into several columns"""
        col_to_split = r"unit,sex,age,geo\time"  # Use raw string due to \t
        df_to_split = self.life_expectancy_df[col_to_split]
        df_unchanged = self.life_expectancy_df.drop(columns=[col_to_split])

        self._set_life_expectancy_df(
            df_to_split.str.split(",", expand=True)
            .rename({0: "unit", 1: "sex", 2: "age", 3: "region"}, axis=1)
            .join(df_unchanged)
        )

    def _aggregate_columns(self) -> pd.DataFrame:
        """Aggregates the years and values in to `year` and `value` columns"""
        id_vars = ["unit", "sex", "age", "region"]
        value_vars = self.life_expectancy_df.drop(columns=id_vars).columns

        self._set_life_expectancy_df(
            pd.melt(self.life_expectancy_df, id_vars, value_vars)
            .rename({"variable": "year"}, axis=1)
        )

    def _process_values(self) -> None:
        """Arranges the values of the `year` and `value` columns"""
        df_clean = self.life_expectancy_df
        df_clean.loc[:, "year"] = df_clean.year.str.strip()
        df_clean.loc[:, "value"] = df_clean.value.str.strip(" epb: ")
        self._set_life_expectancy_df(df_clean[df_clean.value != ""])

    def _cast_columns_to_correct_types(self) -> None:
        """Cast columns to their proper types"""
        self._set_life_expectancy_df(
            self.life_expectancy_df
            .astype({"year": "int", "value": "float"})
        )

    def clean_dataset(self) -> None:
        """Cleans the life expectancy dataset"""
        self._split_column_into_several()
        self._aggregate_columns()
        self._process_values()
        self._cast_columns_to_correct_types()

    def filter_dataset_by_region(self, region: str) -> pd.DataFrame:
        """Filter the dataset by region"""
        return self.life_expectancy_df[
            (self.life_expectancy_df.region == region)
            & (self.life_expectancy_df.value != "")
        ]


def clean_data(region: str) -> None:
    """Cleans the life expectancy dataset and saves it to a csv"""
    life_expectancy = LifeExpectancyData(LIFE_EXPECTANCY_DATA_PATH)
    life_expectancy.clean_dataset()
    (
        life_expectancy.filter_dataset_by_region(region)
        .to_csv(OUTPUT_DATA_PATH, index=False)
    )


if __name__ == "__main__": # pragma: no cover
    parser = argparse.ArgumentParser()
    parser.add_argument("--region", default="PT")
    args = parser.parse_args()

    clean_data(args.region)
