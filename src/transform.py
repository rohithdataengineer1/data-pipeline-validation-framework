import pandas as pd

def transform_data(df):
    """
    Transform and clean the data
    
    Args:
        df: Input DataFrame
        
    Returns:
        Cleaned DataFrame
    """
    
    print("[INFO] Starting data transformation...")
    
    # Make a copy to avoid modifying original
    df_clean = df.copy()
    
    # Transformation 1: Clean product names (remove extra spaces, title case)
    print("[INFO] Cleaning product names...")
    df_clean['product_name'] = df_clean['product_name'].str.strip().str.title()
    
    # Transformation 2: Convert price to float (in case it's text)
    print("[INFO] Converting price to numeric...")
    df_clean['price'] = pd.to_numeric(df_clean['price'], errors='coerce')
    
    # Transformation 3: Convert quantity to integer
    print("[INFO] Converting quantity to integer...")
    df_clean['quantity'] = pd.to_numeric(df_clean['quantity'], errors='coerce').astype('Int64')
    
    # Transformation 4: Calculate total amount
    print("[INFO] Calculating total amount...")
    df_clean['total_amount'] = df_clean['quantity'] * df_clean['price']
    
    # Transformation 5: Convert order_date to datetime
    print("[INFO] Converting order_date to datetime...")
    df_clean['order_date'] = pd.to_datetime(df_clean['order_date'])
    
    # Transformation 6: Remove duplicates based on order_id
    print("[INFO] Removing duplicates...")
    before_count = len(df_clean)
    df_clean = df_clean.drop_duplicates(subset='order_id', keep='first')
    after_count = len(df_clean)
    duplicates_removed = before_count - after_count
    print(f"[INFO] Removed {duplicates_removed} duplicate rows")
    
    print(f"[SUCCESS] Transformation complete! {len(df_clean)} rows ready")
    
    return df_clean


# Test the function
if __name__ == "__main__":
    from extract import extract_data
    
    # Extract data first
    file_path = "data/raw/sales_data.csv"
    df = extract_data(file_path)
    
    if df is not None:
        # Transform data
        df_transformed = transform_data(df)
        
        # Show results
        print("\n[INFO] Transformed data (first 5 rows):")
        pd.set_option('display.max_columns', None)  # Show all columns
        pd.set_option('display.width', None)        # No width limit
        print(df_transformed.head())
        
        print("\n[INFO] Data types:")
        print(df_transformed.dtypes)