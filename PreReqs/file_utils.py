import os

def get_all_table_folders(data_dir):
    return [f for f in os.listdir(data_dir) if os.path.isdir(os.path.join(data_dir, f))]

def get_csv_path_from_folder(table_folder_path):
    files = os.listdir(table_folder_path)
    if not files:
        raise FileNotFoundError(f"No files in {table_folder_path}")
    return os.path.join(table_folder_path, files[0])