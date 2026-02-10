"""
Master ETL Pipeline with Validation
Runs the complete data pipeline from raw CSV to validated database
"""

from extract import extract_data
from transform import transform_data
from load import load_data, verify_load
from validate import DataValidator
import os

def run_pipeline():
    """
    Execute complete ETL pipeline with validation
    """
    print("\n" + "="*60)
    print(" DATA PIPELINE WITH VALIDATION FRAMEWORK")
    print("="*60)
    
    # Initialize validator
    validator = DataValidator()
    
    # Configuration
    raw_file = "data/raw/sales_data.csv"
    db_path = "data/warehouse/sales_data.db"
    table_name = "sales"
    
    # ===== STEP 1: EXTRACT =====
    print("\n[STEP 1] EXTRACT")
    print("-" * 60)
    df_raw = extract_data(raw_file)
    
    if df_raw is None:
        print("[ERROR] Extraction failed. Pipeline stopped.")
        return False
    
    # ===== STEP 2: TRANSFORM =====
    print("\n[STEP 2] TRANSFORM")
    print("-" * 60)
    df_transformed = transform_data(df_raw)
    
    # ===== STEP 3: VALIDATE =====
    print("\n[STEP 3] VALIDATE")
    print("-" * 60)
    
    # Schema validation
    expected_columns = ['order_id', 'customer_id', 'product_name', 
                       'quantity', 'price', 'order_date', 'region', 'total_amount']
    validator.schema_validation(df_transformed, expected_columns)
    
    # Row count check
    validator.row_count_check(len(df_raw), len(df_transformed))
    
    # Null check
    critical_columns = ['order_id', 'customer_id', 'product_name', 'quantity', 'price']
    validator.null_check(df_transformed, critical_columns)
    
    # Data type check
    expected_types = {
        'order_id': 'int',
        'price': 'float',
        'quantity': 'int',
        'total_amount': 'float'
    }
    validator.data_type_check(df_transformed, expected_types)
    
    # Duplicate check
    validator.duplicate_check(df_transformed, 'order_id')
    
    # Range checks
    validator.range_check(df_transformed, 'price', 0, 10000)
    validator.range_check(df_transformed, 'quantity', 1, 100)
    
    # Transformation accuracy
    validator.transformation_accuracy_check(df_transformed)
    
    # Generate validation report
    all_passed = validator.generate_report()
    
    if not all_passed:
        print("\n[ERROR] Validation failed. Data will NOT be loaded to database.")
        print("[INFO] Please fix data quality issues and run again.")
        return False
    
    # ===== STEP 4: LOAD =====
    print("\n[STEP 4] LOAD")
    print("-" * 60)
    
    # Create warehouse directory if needed
    os.makedirs("data/warehouse", exist_ok=True)
    
    # Load to database
    success = load_data(df_transformed, db_path, table_name)
    
    if success:
        verify_load(db_path, table_name)
        
        print("\n" + "="*60)
        print("✓ PIPELINE COMPLETED SUCCESSFULLY!")
        print("="*60)
        print(f"✓ Extracted: {len(df_raw)} rows")
        print(f"✓ Transformed: {len(df_transformed)} rows")
        print(f"✓ Validated: 8/8 checks passed")
        print(f"✓ Loaded to: {db_path}")
        print("="*60)
        return True
    else:
        print("\n[ERROR] Load failed. Pipeline stopped.")
        return False


if __name__ == "__main__":
    success = run_pipeline()
    
    if success:
        print("\n✅ You can now use the data in: data/warehouse/sales_data.db")
    else:
        print("\n❌ Pipeline failed. Check errors above.")