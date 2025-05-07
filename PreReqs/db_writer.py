from sqlalchemy import create_engine
import os
from sqlalchemy import create_engine, text
import pandas as pd
import numpy as np
from sqlalchemy.exc import IntegrityError
import tempfile
import io
import time


def get_engine():
    return create_engine('postgresql://admin:postgres123@localhost:5432/retail')

def upsert_to_postgres(df, table_name, engine, unique_columns=None):
    if unique_columns is None:
        # Try to infer the primary key based on table name
        if table_name == 'departments':
            unique_columns = ['department_id']
        elif table_name == 'categories':
            unique_columns = ['category_id']
        elif table_name == 'products':
            unique_columns = ['product_id']
        elif table_name == 'customers':
            unique_columns = ['customer_id']
        elif table_name == 'orders':
            unique_columns = ['order_id']
        else:
            raise ValueError(f"Please specify unique_columns for table {table_name}")
    
    # Ensure DataFrame column types are compatible with PostgreSQL
    # This helps avoid type conversion issues during the COPY
    df = df.replace({np.nan: None})
    
    # Start timing
    start_time = time.time()
    
    # Create a temporary table with the same structure
    temp_table_name = f"temp_{table_name}_{int(time.time())}"
    
    with engine.connect() as conn:
        # Create temporary table with the same structure as the target table
        conn.execute(text(f"CREATE TEMP TABLE {temp_table_name} (LIKE {table_name} INCLUDING ALL) ON COMMIT DROP"))
        
        # Prepare data for COPY
        buffer = io.StringIO()
        df.to_csv(buffer, sep='\t', header=False, index=False, na_rep='\\N')
        buffer.seek(0)
        
        # Use native PostgreSQL COPY for fastest bulk insert to temp table
        raw_conn = conn.connection
        cursor = raw_conn.cursor()
        cursor.copy_from(buffer, temp_table_name, columns=df.columns.tolist())
        
        # Generate column lists for the SQL command
        all_columns = ', '.join(df.columns)
        update_columns = ', '.join([f"{col} = EXCLUDED.{col}" for col in df.columns if col not in unique_columns])
        
        # Perform the actual upsert operation using ON CONFLICT
        if update_columns:
            upsert_sql = f"""
                INSERT INTO {table_name} ({all_columns})
                SELECT {all_columns} FROM {temp_table_name}
                ON CONFLICT ({', '.join(unique_columns)})
                DO UPDATE SET {update_columns}
            """
        else:
            upsert_sql = f"""
                INSERT INTO {table_name} ({all_columns})
                SELECT {all_columns} FROM {temp_table_name}
                ON CONFLICT ({', '.join(unique_columns)})
                DO NOTHING
            """
        
        # Execute the upsert
        result = conn.execute(text(upsert_sql))
        raw_conn.commit()
        
        # Get stats about the operation
        cursor.execute(f"SELECT COUNT(*) FROM {temp_table_name}")
        total_rows = cursor.fetchone()[0]
        
        # Calculate and report performance metrics
        elapsed = time.time() - start_time
        rows_per_second = total_rows / elapsed if elapsed > 0 else 0
        
        print(f"✅ Processed {total_rows} rows for {table_name} in {elapsed:.2f}s ({rows_per_second:.0f} rows/sec)")
        
        return total_rows


