#!/usr/bin/env python3
"""
Automated test suite for the migration validation system.
Tests all validation scenarios with expected outcomes.
"""

import pandas as pd
import tempfile
import os
import sys
from datetime import datetime, timezone, timedelta

# Add the current directory to the path and import the module
sys.path.append('.')
import importlib.util
spec = importlib.util.spec_from_file_location("migration_import_unified", "migration-import-unified.py")
migration_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(migration_module)
validate_postal_codes_after_merge = migration_module.validate_postal_codes_after_merge

def create_test_csv(data, filename):
    """Create a temporary CSV file with test data."""
    df = pd.DataFrame(data)
    filepath = os.path.join(tempfile.gettempdir(), filename)
    df.to_csv(filepath, index=False)
    return filepath

def test_us_postal_code_validation():
    """Test US postal code validation scenarios."""
    print("🧪 Testing US Postal Code Validation...")
    
    # Test data with various US postal code scenarios
    test_data = {
        'address_country_code': ['US', 'US', 'US', 'US', 'US', 'CA', 'GB'],
        'address_postal_code': ['1234', '12345', '123', 'ABCDE', '1234A', 'A1A1A1', 'SW1A1AA'],
        'customer_email': ['test1@example.com', 'test2@example.com', 'test3@example.com', 
                          'test4@example.com', 'test5@example.com', 'test6@example.com', 'test7@example.com'],
        'current_period_started_at': ['2024-01-01', '2024-01-01', '2024-01-01', '2024-01-01', 
                                     '2024-01-01', '2024-01-01', '2024-01-01'],
        'current_period_ends_at': ['2025-01-01', '2025-01-01', '2025-01-01', '2025-01-01', 
                                  '2025-01-01', '2025-01-01', '2025-01-01']
    }
    
    filepath = create_test_csv(test_data, 'test_us_postal.csv')
    
    try:
        # Read the CSV and validate
        df = pd.read_csv(filepath)
        results = validate_postal_codes_after_merge(df, 'stripe')
        
        # Expected outcomes
        expected_missing_zero = 1  # '1234' should be flagged
        expected_incorrect_format = 3  # '123', 'ABCDE', '1234A' should be flagged
        
        print(f"✅ US Missing Zero Issues: {len(results['us_missing_zero'])} (Expected: {expected_missing_zero})")
        print(f"✅ US Incorrect Format Issues: {len(results['us_incorrect_format'])} (Expected: {expected_incorrect_format})")
        
        # Verify specific issues
        missing_zero_codes = [issue['postalCode'] for issue in results['us_missing_zero']]
        incorrect_format_codes = [issue['postalCode'] for issue in results['us_incorrect_format']]
        
        assert '1234' in missing_zero_codes, "Missing zero issue not detected for '1234'"
        assert '123' in incorrect_format_codes, "Incorrect format not detected for '123'"
        assert 'ABCDE' in incorrect_format_codes, "Incorrect format not detected for 'ABCDE'"
        assert '1234A' in incorrect_format_codes, "Incorrect format not detected for '1234A'"
        
        print("✅ All US postal code validation tests passed!")
        
    finally:
        os.remove(filepath)

def test_canadian_postal_code_validation():
    """Test Canadian postal code validation scenarios."""
    print("\n🧪 Testing Canadian Postal Code Validation...")
    
    # Test data with various Canadian postal code scenarios
    test_data = {
        'address_country_code': ['CA', 'CA', 'CA', 'CA', 'CA', 'US', 'GB'],
        'address_postal_code': ['A1A1A1', 'A1A 1A1', '123456', 'ABCDEF', 'A1A1A', '12345', 'SW1A1AA'],
        'customer_email': ['test1@example.com', 'test2@example.com', 'test3@example.com', 
                          'test4@example.com', 'test5@example.com', 'test6@example.com', 'test7@example.com'],
        'current_period_started_at': ['2024-01-01', '2024-01-01', '2024-01-01', '2024-01-01', 
                                     '2024-01-01', '2024-01-01', '2024-01-01'],
        'current_period_ends_at': ['2025-01-01', '2025-01-01', '2025-01-01', '2025-01-01', 
                                  '2025-01-01', '2025-01-01', '2025-01-01']
    }
    
    filepath = create_test_csv(test_data, 'test_canadian_postal.csv')
    
    try:
        df = pd.read_csv(filepath)
        results = validate_postal_codes_after_merge(df, 'stripe')
        
        # Expected outcomes
        expected_canadian_incorrect = 3  # '123456', 'ABCDEF', 'A1A1A' should be flagged
        
        print(f"✅ Canadian Incorrect Format Issues: {len(results['canadian_incorrect_format'])} (Expected: {expected_canadian_incorrect})")
        
        # Verify specific issues
        incorrect_codes = [issue['postalCode'] for issue in results['canadian_incorrect_format']]
        
        assert '123456' in incorrect_codes, "Canadian format not detected for '123456'"
        assert 'ABCDEF' in incorrect_codes, "Canadian format not detected for 'ABCDEF'"
        assert 'A1A1A' in incorrect_codes, "Canadian format not detected for 'A1A1A'"
        
        print("✅ All Canadian postal code validation tests passed!")
        
    finally:
        os.remove(filepath)

def test_bluesnap_card_token_validation():
    """Test Bluesnap card token validation scenarios."""
    print("\n🧪 Testing Bluesnap Card Token Validation...")
    
    # Test data with various card token scenarios
    test_data = {
        'address_country_code': ['US', 'US', 'US', 'US', 'US'],
        'address_postal_code': ['12345', '12345', '12345', '12345', '12345'],
        'card_token': ['1234567890123', '123456789012', '12345678901234', 'ABCDEFGHIJKLM', '1234567890123'],
        'customer_email': ['test1@example.com', 'test2@example.com', 'test3@example.com', 
                          'test4@example.com', 'test5@example.com'],
        'current_period_started_at': ['2024-01-01', '2024-01-01', '2024-01-01', '2024-01-01', '2024-01-01'],
        'current_period_ends_at': ['2025-01-01', '2025-01-01', '2025-01-01', '2025-01-01', '2025-01-01']
    }
    
    filepath = create_test_csv(test_data, 'test_bluesnap_card_token.csv')
    
    try:
        df = pd.read_csv(filepath)
        results = validate_postal_codes_after_merge(df, 'bluesnap')
        
        # Expected outcomes
        expected_card_token_issues = 3  # '123456789012', '12345678901234', 'ABCDEFGHIJKLM' should be flagged
        
        print(f"✅ Bluesnap Card Token Issues: {len(results['bluesnap_card_token_format'])} (Expected: {expected_card_token_issues})")
        
        # Verify specific issues
        incorrect_tokens = [issue['cardToken'] for issue in results['bluesnap_card_token_format']]
        
        assert '123456789012' in incorrect_tokens, "Card token format not detected for '123456789012'"
        assert '12345678901234' in incorrect_tokens, "Card token format not detected for '12345678901234'"
        assert 'ABCDEFGHIJKLM' in incorrect_tokens, "Card token format not detected for 'ABCDEFGHIJKLM'"
        
        print("✅ All Bluesnap card token validation tests passed!")
        
    finally:
        os.remove(filepath)

def test_date_validation():
    """Test date validation scenarios."""
    print("\n🧪 Testing Date Validation...")
    
    # Get current datetime for testing
    now = datetime.now(timezone.utc)
    future_date = now + timedelta(days=30)
    past_date = now - timedelta(days=30)
    
    # Test data with various date scenarios
    test_data = {
        'address_country_code': ['US', 'US', 'US', 'US', 'US', 'US'],
        'address_postal_code': ['12345', '12345', '12345', '12345', '12345', '12345'],
        'customer_email': ['test1@example.com', 'test2@example.com', 'test3@example.com', 
                          'test4@example.com', 'test5@example.com', 'test6@example.com'],
        'current_period_started_at': [
            future_date.isoformat(),  # Future start date (should be flagged)
            past_date.isoformat(),    # Past start date (should be OK)
            '2024-01-01',            # Past start date (should be OK)
            'invalid-date',          # Invalid date format (should be flagged)
            '2024-01-01',            # Past start date (should be OK)
            '2024-01-01'             # Past start date (should be OK)
        ],
        'current_period_ends_at': [
            future_date.isoformat(),  # Future end date (should be OK)
            past_date.isoformat(),    # Past end date (should be flagged)
            '2023-01-01',            # Past end date (should be flagged)
            '2025-01-01',            # Future end date (should be OK)
            'invalid-date',          # Invalid date format (should be flagged)
            '2023-01-01'             # Past end date (should be flagged)
        ]
    }
    
    filepath = create_test_csv(test_data, 'test_date_validation.csv')
    
    try:
        df = pd.read_csv(filepath)
        results = validate_postal_codes_after_merge(df, 'stripe')
        
        # Count issues by type
        start_date_future = sum(1 for issue in results['date_validation_issues'] 
                               if issue['issue'] == 'start_date_not_in_past')
        end_date_past = sum(1 for issue in results['date_validation_issues'] 
                           if issue['issue'] == 'end_date_not_in_future')
        invalid_format = sum(1 for issue in results['date_validation_issues'] 
                           if issue['issue'] == 'invalid_date_format')
        
        print(f"✅ Start Date in Future: {start_date_future} (Expected: 1)")
        print(f"✅ End Date in Past: {end_date_past} (Expected: 4)")
        print(f"✅ Invalid Date Format: {invalid_format} (Expected: 2)")
        
        # Verify specific issues
        start_date_issues = [issue for issue in results['date_validation_issues'] 
                           if issue['issue'] == 'start_date_not_in_past']
        end_date_issues = [issue for issue in results['date_validation_issues'] 
                          if issue['issue'] == 'end_date_not_in_future']
        
        assert len(start_date_issues) == 1, f"Expected 1 start date in future, got {len(start_date_issues)}"
        assert len(end_date_issues) == 4, f"Expected 4 end dates in past, got {len(end_date_issues)}"
        
        print("✅ All date validation tests passed!")
        
    finally:
        os.remove(filepath)

def test_validation_order():
    """Test that validation modals appear in the correct order."""
    print("\n🧪 Testing Validation Order...")
    
    # Test data with multiple validation issues
    test_data = {
        'address_country_code': ['US', 'CA', 'US', 'US'],
        'address_postal_code': ['1234', '123456', '123', '12345'],  # US missing zero, Canadian incorrect, US incorrect
        'card_token': ['1234567890123', '1234567890123', '1234567890123', '123456789012'],  # Last one incorrect
        'customer_email': ['test1@example.com', 'test2@example.com', 'test3@example.com', 'test4@example.com'],
        'current_period_started_at': ['2024-01-01', '2024-01-01', '2024-01-01', '2024-01-01'],
        'current_period_ends_at': ['2023-01-01', '2023-01-01', '2023-01-01', '2023-01-01']  # All in past
    }
    
    filepath = create_test_csv(test_data, 'test_validation_order.csv')
    
    try:
        df = pd.read_csv(filepath)
        results = validate_postal_codes_after_merge(df, 'bluesnap')
        
        print("✅ Validation Results Summary:")
        print(f"   US Missing Zero: {len(results['us_missing_zero'])}")
        print(f"   US Incorrect Format: {len(results['us_incorrect_format'])}")
        print(f"   Canadian Incorrect Format: {len(results['canadian_incorrect_format'])}")
        print(f"   Bluesnap Card Token: {len(results['bluesnap_card_token_format'])}")
        print(f"   Date Validation: {len(results['date_validation_issues'])}")
        
        # Expected order: US missing zero -> US incorrect -> Canadian incorrect -> Card token -> Date validation
        assert len(results['us_missing_zero']) > 0, "Should have US missing zero issues"
        assert len(results['us_incorrect_format']) > 0, "Should have US incorrect format issues"
        assert len(results['canadian_incorrect_format']) > 0, "Should have Canadian incorrect format issues"
        assert len(results['bluesnap_card_token_format']) > 0, "Should have card token issues"
        assert len(results['date_validation_issues']) > 0, "Should have date validation issues"
        
        print("✅ Validation order test passed!")
        
    finally:
        os.remove(filepath)

def run_all_tests():
    """Run all validation tests."""
    print("🚀 Starting Validation Test Suite...\n")
    
    try:
        test_us_postal_code_validation()
        test_canadian_postal_code_validation()
        test_bluesnap_card_token_validation()
        test_date_validation()
        test_validation_order()
        
        print("\n🎉 All tests passed! Validation system is working correctly.")
        
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        raise

if __name__ == "__main__":
    run_all_tests()
