import pandas as pd
import sqlite3
import os

def load_data(df, db_path, table_name):
    """
    Load data into SQLite database
    
    Args:
        df: DataFrame to load
        db_path: Path to SQLite database file
        table_name: Name of the table to create/replace
        
    Returns:
        Boolean: True if successful, False otherwise
    """
    
    print(f"[INFO] Starting data load to database...")
    print(f"[INFO] Database: {db_path}")
    print(f"[INFO] Table: {table_name}")
    
    try:
        # Create connection to SQLite database
        conn = sqlite3.connect(db_path)
        print("[INFO] Connected to database")
        
        # Load DataFrame to database
        df.to_sql(table_name, conn, if_exists='replace', index=False)
        
        # Verify load
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        row_count = cursor.fetchone()[0]
        
        print(f"[SUCCESS] Loaded {row_count} rows to table '{table_name}'")
        
        # Close connection
        conn.close()
        print("[INFO] Database connection closed")
        
        return True
        
    except Exception as e:
        print(f"[ERROR] Failed to load data: {e}")
        return False


def verify_load(db_path, table_name):
    """
    Verify data was loaded correctly
    
    Args:
        db_path: Path to database
        table_name: Table name to verify
    """
    
    print(f"\n[INFO] Verifying data in database...")
    
    try:
        conn = sqlite3.connect(db_path)
        
        # Read data back from database
        query = f"SELECT * FROM {table_name} LIMIT 5"
        df = pd.read_sql(query, conn)
        
        print(f"[SUCCESS] Verification complete")
        print(f"\n[INFO] First 5 rows from database:")
        print(df)
        
        conn.close()
        
    except Exception as e:
        print(f"[ERROR] Verification failed: {e}")


# Test the function
if __name__ == "__main__":
    from extract import extract_data
    from transform import transform_data
    
    # Full ETL pipeline
    print("="*50)
    print("RUNNING FULL ETL PIPELINE")
    print("="*50)
    
    # 1. EXTRACT
    file_path = "data/raw/sales_data.csv"
    df = extract_data(file_path)
    
    if df is not None:
        # 2. TRANSFORM
        df_transformed = transform_data(df)
        
        # 3. LOAD
        db_path = "data/warehouse/sales_data.db"
        table_name = "sales"
        
        # Create warehouse directory if it doesn't exist
        os.makedirs("data/warehouse", exist_ok=True)
        
        success = load_data(df_transformed, db_path, table_name)
        
        if success:
            # 4. VERIFY
            verify_load(db_path, table_name)
            
            print("\n" + "="*50)
            print("ETL PIPELINE COMPLETED SUCCESSFULLY!")
            print("="*50)
        