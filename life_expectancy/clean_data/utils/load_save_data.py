"""Load and save data function"""
import pandas as pd


def load_data(path) -> pd.DataFrame:
    """Loads a TSV file to a pandas DataFrame"""
    return pd.read_table(path)


def save_data(df_to_save: pd.DataFrame, output_data_path: str) -> None:
    """Saves the cleaned life expectancy DataFrame to a csv file"""
    df_to_save.to_csv(output_data_path, index=False)
