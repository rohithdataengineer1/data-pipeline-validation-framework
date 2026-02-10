import pandas as pd
import os

def extract_data(file_path):
    """
    Extract data from CSV file
    
    Args:
        file_path: Path to the CSV file
        
    Returns:
        pandas DataFrame with the data
    """
    
    print(f"[INFO] Starting data extraction...")
    print(f"[INFO] Reading file: {file_path}")
    
    # Check if file exists
    if not os.path.exists(file_path):
        print(f"[ERROR] File not found: {file_path}")
        return None
    
    # Read CSV file
    try:
        data = pd.read_csv(file_path)
        print(f"[SUCCESS] Extracted {len(data)} rows")
        print(f"[INFO] Columns: {list(data.columns)}")
        return data
    
    except Exception as e:
        print(f"[ERROR] Failed to read file: {e}")
        return None


# Test the function
if __name__ == "__main__":
    # Path to our CSV file
    file_path = "data/raw/sales_data.csv"
    
    # Extract data
    df = extract_data(file_path)
    
    # Show first few rows
    if df is not None:
        print("\n[INFO] First 5 rows:")
        print(df.head())