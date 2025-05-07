import pandas as pd
from file_utils import get_csv_path_from_folder

def load_table_as_df(table_folder_path):
    csv_path = get_csv_path_from_folder(table_folder_path)
    return pd.read_csv(csv_path, header=None)

