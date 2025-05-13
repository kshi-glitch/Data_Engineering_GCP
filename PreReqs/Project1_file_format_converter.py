#%%
from file_utils import get_all_table_folders
from data_loader import load_table_as_df
from json_exporter import export_df_to_json
import os

data_dir = '/Users/kshitijmac/Documents/Data_Files/retail_db'
output_dir = '/Users/kshitijmac/Documents/Data_Files/retail_db_json'

for table in get_all_table_folders(data_dir):
    table_path = os.path.join(data_dir, table)
    df = load_table_as_df(table_path)
    export_df_to_json(df, table, output_dir)

# %%
