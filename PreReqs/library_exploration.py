#%%
import pandas as pd
import os
import time
import gzip
import dask.dataframe as dd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType
#%%
# Path to the NYSE data
path = '/Users/kshitijmac/Documents/Data_Files/data/nyse_all/nyse_data'

# Function to measure execution time
def measure_time(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        print(f"{func.__name__} took {end_time - start_time:.2f} seconds to execute")
        return result
    return wrapper

# -------------------- PANDAS --------------------
@measure_time
def process_with_pandas():
    print("\n===== Processing with Pandas =====")
    
    # List all gzipped txt files in the directory
    all_files = [os.path.join(path, f) for f in os.listdir(path) if f.endswith('.txt.gz')]
    
    # Read the first file to explore structure
    print(f"Reading sample from {os.path.basename(all_files[0])}...")
    with gzip.open(all_files[0], 'rt') as f:
        # Read first few lines to determine structure
        lines = [next(f) for _ in range(6)]
        
    # Try to identify delimiter and structure
    sample_line = lines[1]  # Skip header if exists
    print("Sample line:")
    print(sample_line)
    
    # Assuming tab or comma-separated values, read the first file
    sample_df = pd.read_csv(all_files[0], compression='gzip', sep=None, engine='python', nrows=5)
    print("Sample data structure:")
    print(sample_df.head())
    print(f"Columns: {sample_df.columns.tolist()}")
    print(f"Data types: {sample_df.dtypes}")
    
    # Read all files into a single DataFrame
    print(f"Reading {len(all_files)} gzipped text files...")
    df_list = []
    for file in all_files[:3]:  # Limiting to 3 files for demonstration
        print(f"Reading {os.path.basename(file)}...")
        df = pd.read_csv(file, compression='gzip')
        df_list.append(df)
    
    df = pd.concat(df_list, ignore_index=True)
    print(f"Combined DataFrame shape: {df.shape}")
    
    # Basic statistics
    print("\nBasic statistics:")
    if 'close' in df.columns:
        print(f"Average close price: {df['close'].mean():.2f}")
        print(f"Max close price: {df['close'].max():.2f}")
        print(f"Min close price: {df['close'].min():.2f}")
    elif 'Close' in df.columns:  # Check for capitalized column name
        print(f"Average close price: {df['Close'].mean():.2f}")
        print(f"Max close price: {df['Close'].max():.2f}")
        print(f"Min close price: {df['Close'].min():.2f}")
    
    # Group by operation
    symbol_col = None
    price_col = None
    
    if 'symbol' in df.columns and 'close' in df.columns:
        symbol_col, price_col = 'symbol', 'close'
    elif 'Symbol' in df.columns and 'Close' in df.columns:
        symbol_col, price_col = 'Symbol', 'Close'
    
    if symbol_col and price_col:
        symbol_stats = df.groupby(symbol_col)[price_col].agg(['mean', 'min', 'max'])
        print("\nStats by symbol (top 5):")
        print(symbol_stats.head())
    
    return df

# -------------------- DASK --------------------
@measure_time
def process_with_dask():
    print("\n===== Processing with Dask =====")
    
    # Create Dask DataFrame from gzipped text files
    # Use glob pattern to match all gzipped files
    dask_df = dd.read_csv(os.path.join(path, '*.txt.gz'), compression='gzip')
    
    print("Dask DataFrame information:")
    print(f"Columns: {dask_df.columns.tolist()}")
    print(f"Data types: {dask_df.dtypes}")
    
    # Compute basic statistics (lazy evaluation until compute())
    price_col = 'close' if 'close' in dask_df.columns else ('Close' if 'Close' in dask_df.columns else None)
    
    if price_col:
        mean_close = dask_df[price_col].mean().compute()
        max_close = dask_df[price_col].max().compute()
        min_close = dask_df[price_col].min().compute()
        
        print("\nBasic statistics:")
        print(f"Average close price: {mean_close:.2f}")
        print(f"Max close price: {max_close:.2f}")
        print(f"Min close price: {min_close:.2f}")
    
    # Group by operation
    symbol_col = 'symbol' if 'symbol' in dask_df.columns else ('Symbol' if 'Symbol' in dask_df.columns else None)
    
    if symbol_col and price_col:
        symbol_stats = dask_df.groupby(symbol_col)[price_col].agg(['mean', 'min', 'max']).compute()
        print("\nStats by symbol (top 5):")
        print(symbol_stats.head())
    
    return dask_df

# -------------------- PYSPARK --------------------
@measure_time
@measure_time
def process_with_pyspark():
    print("\n===== Processing with PySpark =====")
    
    # Set Java environment variables before initializing Spark
    import os
    os.environ['JAVA_HOME'] = '/Library/Java/JavaVirtualMachines/adoptopenjdk-11.jdk/Contents/Home'
    os.environ['PATH'] = f"{os.environ['JAVA_HOME']}/bin:{os.environ['PATH']}"
    
    # Initialize SparkSession with additional configuration
    spark = SparkSession.builder \
        .appName("NYSE_Data_Analysis") \
        .master("local[*]") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.ui.showConsoleProgress", "true") \
        .getOrCreate()
    
    # Define schema - more robust version that handles different column name cases
    schema = StructType([
        StructField("date", StringType(), True),
        StructField("symbol", StringType(), True),
        StructField("open", DoubleType(), True),
        StructField("high", DoubleType(), True),
        StructField("low", DoubleType(), True),
        StructField("close", DoubleType(), True),
        StructField("volume", DoubleType(), True)
    ])
    
    try:
        # Read files with error handling
        spark_df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .option("delimiter", "\t") \
            .option("nullValue", "null") \
            .csv(os.path.join(path, "*.txt.gz"))
        
        # Standardize column names to lowercase
        for col in spark_df.columns:
            spark_df = spark_df.withColumnRenamed(col, col.lower())
        
        print("Spark DataFrame information:")
        spark_df.printSchema()
        
        # Show record count with error handling
        try:
            print(f"Count: {spark_df.count():,}")
        except Exception as e:
            print(f"Error counting records: {str(e)}")
        
        print("Sample data:")
        spark_df.show(5, truncate=False)
        
        # Handle statistics - more robust column detection
        price_col = 'close' if 'close' in spark_df.columns else None
        symbol_col = 'symbol' if 'symbol' in spark_df.columns else None
        
        if price_col:
            print("\nBasic statistics:")
            spark_df.select(price_col).summary().show()
        
        if symbol_col and price_col:
            print(f"\nStats by {symbol_col} (top 5):")
            from pyspark.sql.functions import mean, min, max
            symbol_stats = spark_df.groupBy(symbol_col) \
                .agg(
                    mean(price_col).alias("mean"),
                    min(price_col).alias("min"),
                    max(price_col).alias("max")
                ) \
                .orderBy("mean", ascending=False)
            symbol_stats.show(5, truncate=False)
        
        return spark_df
        
    except Exception as e:
        print(f"Error during processing: {str(e)}")
        raise
    finally:
        spark.stop()
        print("Spark session stopped")

# Function to explore file structure
def explore_file_structure():
    print("\n===== EXPLORING FILE STRUCTURE =====")
    
    # List all gzipped txt files in the directory
    all_files = [os.path.join(path, f) for f in os.listdir(path) if f.endswith('.txt.gz')]
    if not all_files:
        print("No .txt.gz files found in the specified directory.")
        return
    
    print(f"Found {len(all_files)} .txt.gz files spanning from 1997 to 2017")
    
    # Sample from first and last file to see if format changed over time
    first_file = min(all_files)
    last_file = max(all_files)
    
    print(f"\nExamining first file: {os.path.basename(first_file)}")
    with gzip.open(first_file, 'rt') as f:
        first_lines = [next(f) for _ in range(3) if f]
        print("First 3 lines:")
        for line in first_lines:
            print(f"  {line.strip()}")
    
    print(f"\nExamining last file: {os.path.basename(last_file)}")
    with gzip.open(last_file, 'rt') as f:
        last_lines = [next(f) for _ in range(3) if f]
        print("First 3 lines:")
        for line in last_lines:
            print(f"  {line.strip()}")
    
    # Get file sizes
    sizes = [os.path.getsize(f) for f in all_files]
    total_size_gb = sum(sizes) / (1024**3)
    
    print(f"\nTotal dataset size: {total_size_gb:.2f} GB")
    print(f"Average file size: {sum(sizes)/len(sizes)/1024**2:.2f} MB")
    print(f"Largest file: {max(sizes)/1024**2:.2f} MB")
    print(f"Smallest file: {min(sizes)/1024**2:.2f} MB")

# -------------------- COMPARISON --------------------
def compare_frameworks():
    print("\n===== FRAMEWORK COMPARISON =====")
    
    print("1. Pandas:")
    print("   - In-memory processing (requires all data to fit in RAM)")
    print("   - Easiest to use and most intuitive API")
    print("   - Best for datasets up to a few GB")
    print("   - Single-threaded by default")
    print("   - Good for gzipped files but processes them sequentially")
    
    print("\n2. Dask:")
    print("   - Extends Pandas API for larger-than-memory datasets")
    print("   - Parallel processing on a single machine")
    print("   - Lazy evaluation (operations execute only when results are needed)")
    print("   - Good middle ground for medium-sized datasets (10s-100s of GB)")
    print("   - Can efficiently handle multiple gzipped files in parallel")
    
    print("\n3. PySpark:")
    print("   - Distributed processing across clusters")
    print("   - Most complex setup but most scalable")
    print("   - Best for very large datasets (100s of GB to TB+)")
    print("   - Built-in fault tolerance")
    print("   - Requires more boilerplate code")
    print("   - Automatic handling of compression formats")
    print("   - Best for processing all 21 years of NYSE data efficiently")



#%%
# Run all three approaches and compare
if __name__ == "__main__":
    # Set pandas display options
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 1000)
    
    try:
        # First explore the file structure
        explore_file_structure()
        
        # Run each framework
        print("\nProcessing with each framework (limited to 3 files for demonstration):")
        pandas_df = process_with_pandas()
        dask_df = process_with_dask()
        spark_df = process_with_pyspark()
        
        # Show comparison of approaches
        compare_frameworks()
        
    except Exception as e:
        print(f"Error during processing: {e}")
# %%
