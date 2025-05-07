import os

def export_df_to_json(df, table_name, output_dir):
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, f"{table_name}.json")
    df.to_json(output_path, orient='records', lines=True)
    print(f"✅ Exported {table_name} to {output_path}")
