import pandas as pd
import sqlite3
from datetime import datetime

class DataValidator:
    """
    Validates data quality at each step of ETL pipeline
    """
    
    def __init__(self):
        self.validation_results = []
        
    def add_result(self, check_name, status, message):
        """Add a validation result"""
        self.validation_results.append({
            'check': check_name,
            'status': status,
            'message': message,
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        })
        
    def schema_validation(self, df, expected_columns):
        """
        Check if DataFrame has expected columns
        """
        print("\n[VALIDATION] Schema Check")
        print("-" * 50)
        
        actual_columns = list(df.columns)
        missing = set(expected_columns) - set(actual_columns)
        extra = set(actual_columns) - set(expected_columns)
        
        if len(missing) == 0 and len(extra) == 0:
            print("✓ PASSED: All expected columns present")
            self.add_result("Schema Validation", "PASSED", 
                          f"All {len(expected_columns)} columns present")
            return True
        else:
            msg = f"Missing: {missing}, Extra: {extra}"
            print(f"✗ FAILED: {msg}")
            self.add_result("Schema Validation", "FAILED", msg)
            return False
    
    def row_count_check(self, source_count, target_count):
        """
        Check if row counts match between source and target
        """
        print("\n[VALIDATION] Row Count Check")
        print("-" * 50)
        
        if source_count == target_count:
            print(f"✓ PASSED: Row count matches ({source_count} rows)")
            self.add_result("Row Count Check", "PASSED", 
                          f"Source: {source_count}, Target: {target_count}")
            return True
        else:
            msg = f"Mismatch! Source: {source_count}, Target: {target_count}"
            print(f"✗ FAILED: {msg}")
            self.add_result("Row Count Check", "FAILED", msg)
            return False
    
    def null_check(self, df, columns_to_check):
        """
        Check for null values in specified columns
        """
        print("\n[VALIDATION] Null Value Check")
        print("-" * 50)
        
        null_counts = df[columns_to_check].isnull().sum()
        has_nulls = null_counts.sum() > 0
        
        if not has_nulls:
            print("✓ PASSED: No null values found")
            self.add_result("Null Check", "PASSED", 
                          f"No nulls in {len(columns_to_check)} columns")
            return True
        else:
            msg = f"Found nulls: {dict(null_counts[null_counts > 0])}"
            print(f"✗ FAILED: {msg}")
            self.add_result("Null Check", "FAILED", msg)
            return False
    
    def data_type_check(self, df, expected_types):
        """
        Check if columns have expected data types
        """
        print("\n[VALIDATION] Data Type Check")
        print("-" * 50)
        
        failures = []
        for column, expected_type in expected_types.items():
            actual_type = str(df[column].dtype).lower()  # Convert to lowercase
            expected_lower = expected_type.lower()        # Convert to lowercase
            
            # Check if expected type is in actual type (flexible matching)
            if expected_lower not in actual_type:
                failures.append(f"{column}: expected {expected_type}, got {df[column].dtype}")
        
        if len(failures) == 0:
            print("✓ PASSED: All data types correct")
            self.add_result("Data Type Check", "PASSED", 
                          f"All {len(expected_types)} columns have correct types")
            return True
        else:
            msg = "; ".join(failures)
            print(f"✗ FAILED: {msg}")
            self.add_result("Data Type Check", "FAILED", msg)
            return False
    
    def duplicate_check(self, df, key_column):
        """
        Check for duplicate values in key column
        """
        print("\n[VALIDATION] Duplicate Check")
        print("-" * 50)
        
        duplicate_count = df[key_column].duplicated().sum()
        
        if duplicate_count == 0:
            print("✓ PASSED: No duplicates found")
            self.add_result("Duplicate Check", "PASSED", 
                          f"No duplicates in {key_column}")
            return True
        else:
            msg = f"Found {duplicate_count} duplicates in {key_column}"
            print(f"✗ FAILED: {msg}")
            self.add_result("Duplicate Check", "FAILED", msg)
            return False
    
    def range_check(self, df, column, min_val, max_val):
        """
        Check if values are within expected range
        """
        print(f"\n[VALIDATION] Range Check ({column})")
        print("-" * 50)
        
        out_of_range = df[(df[column] < min_val) | (df[column] > max_val)]
        
        if len(out_of_range) == 0:
            print(f"✓ PASSED: All values in range [{min_val}, {max_val}]")
            self.add_result(f"Range Check ({column})", "PASSED", 
                          f"All values between {min_val} and {max_val}")
            return True
        else:
            msg = f"Found {len(out_of_range)} values out of range"
            print(f"✗ FAILED: {msg}")
            self.add_result(f"Range Check ({column})", "FAILED", msg)
            return False
    
    def transformation_accuracy_check(self, df):
        """
        Verify transformation calculations are correct
        """
        print("\n[VALIDATION] Transformation Accuracy Check")
        print("-" * 50)
        
        # Check if total_amount = quantity * price
        df['calculated_total'] = df['quantity'] * df['price']
        mismatch = df[df['total_amount'] != df['calculated_total']]
        
        if len(mismatch) == 0:
            print("✓ PASSED: total_amount calculations correct")
            self.add_result("Transformation Accuracy", "PASSED", 
                          "All total_amount = quantity × price")
            return True
        else:
            msg = f"Found {len(mismatch)} rows with incorrect calculations"
            print(f"✗ FAILED: {msg}")
            self.add_result("Transformation Accuracy", "FAILED", msg)
            return False
    
    def generate_report(self):
        """
        Generate validation report
        """
        print("\n" + "="*50)
        print("VALIDATION REPORT")
        print("="*50)
        
        passed = sum(1 for r in self.validation_results if r['status'] == 'PASSED')
        failed = sum(1 for r in self.validation_results if r['status'] == 'FAILED')
        total = len(self.validation_results)
        
        for result in self.validation_results:
            status_symbol = "✓" if result['status'] == "PASSED" else "✗"
            print(f"{status_symbol} {result['check']}: {result['status']}")
            print(f"   {result['message']}")
        
        print("\n" + "-"*50)
        print(f"Total Checks: {total}")
        print(f"Passed: {passed}")
        print(f"Failed: {failed}")
        
        if failed == 0:
            print("\n🎉 ALL VALIDATIONS PASSED! 🎉")
            print("="*50)
            return True
        else:
            print(f"\n⚠️  {failed} VALIDATION(S) FAILED ⚠️")
            print("="*50)
            return False


# Test the validator
if __name__ == "__main__":
    from extract import extract_data
    from transform import transform_data
    
    print("="*50)
    print("RUNNING DATA VALIDATION")
    print("="*50)
    
    # Create validator
    validator = DataValidator()
    
    # 1. Extract data
    file_path = "data/raw/sales_data.csv"
    df_raw = extract_data(file_path)
    
    if df_raw is not None:
        # 2. Transform data
        df_transformed = transform_data(df_raw)
        
        # 3. Run validations
        
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
        
        # Generate final report
        validator.generate_report()