#%%
from file_reader import read_table_as_df
from db_writer import get_engine, upsert_to_postgres

base_dir = '/Users/kshitijmac/Documents/Data_Files/retail_db'
tables = ['departments', 'categories', 'products', 'customers', 'orders', 'order_items']

engine = get_engine()

for table in tables:
    df = read_table_as_df(base_dir, table)
    upsert_to_postgres(df, table, engine, unique_columns=None)
    print("Done")

# %%