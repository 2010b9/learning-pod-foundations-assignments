"""Load and save data function"""
from typing import Protocol
import json
from zipfile import ZipFile
import pandas as pd


class ReadStrategy(Protocol):  # pylint: disable=too-few-public-methods
    """Data reading strategy class"""
    def __call__(self, path: str) -> pd.DataFrame:
        """Apply data reading strategy"""


class TSVReadStrategy():  # pylint: disable=too-few-public-methods
    """TSV Data reading strategy class"""
    def __call__(self, path: str) -> pd.DataFrame:
        return pd.read_table(path)


class ZIPReadStrategy():  # pylint: disable=too-few-public-methods
    """ZIP Data reading strategy class"""
    def __call__(self, path: str) -> pd.DataFrame:
        with ZipFile(path, "r") as zip_file:
            with zip_file.open(zip_file.namelist()[0]) as json_file:
                return pd.DataFrame(json.loads(json_file.read()))


def load_data(path: str, read_strategy: ReadStrategy) -> pd.DataFrame:
    """Loads a TSV file to a pandas DataFrame"""
    return read_strategy(path)


def save_data(df_to_save: pd.DataFrame, output_data_path: str) -> None:
    """Saves the cleaned life expectancy DataFrame to a csv file"""
    df_to_save.to_csv(output_data_path, index=False)
