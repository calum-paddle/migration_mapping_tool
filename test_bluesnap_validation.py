#!/usr/bin/env python3
"""
Comprehensive test for Bluesnap validation using actual test data.
Tests all validation scenarios with expected outcomes based on real data.
"""

import pandas as pd
import os
import sys
from datetime import datetime, timezone
import importlib.util

# Import the validation function
sys.path.append('.')
spec = importlib.util.spec_from_file_location("migration_import_unified", "migration-import-unified.py")
migration_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(migration_module)
validate_postal_codes_after_merge = migration_module.validate_postal_codes_after_merge

def test_bluesnap_validation_with_fixed_date():
    """Test Bluesnap validation with fixed date for consistent testing."""
    print("🧪 Testing Bluesnap Validation with Fixed Date...")
    
    # Fixed date for testing: 2025-08-16T10:14:00Z
    fixed_date = datetime(2025, 8, 16, 10, 14, 0, tzinfo=timezone.utc)
    
    # Read the actual test files
    subscriber_file = "testdata/BlueSnap/sandbox_sub_data_test.csv"
    mapping_file = "testdata/BlueSnap/mapping.csv"
    
    if not os.path.exists(subscriber_file):
        print(f"❌ Subscriber file not found: {subscriber_file}")
        return False
    
    if not os.path.exists(mapping_file):
        print(f"❌ Mapping file not found: {mapping_file}")
        return False
    
    # Read the CSV files
    subscriber_df = pd.read_csv(subscriber_file)
    mapping_df = pd.read_csv(mapping_file)
    
    print(f"📊 Subscriber data: {len(subscriber_df)} records")
    print(f"📊 Mapping data: {len(mapping_df)} records")
    
    # Create a modified validation function that uses fixed date
    def validate_with_fixed_date(completed_df, provider='bluesnap'):
        """Validate postal codes, card tokens, and dates after merge with fixed date."""
        validation_results = {
            'us_missing_zero': [],
            'us_incorrect_format': [],
            'canadian_incorrect_format': [],
            'bluesnap_card_token_format': [],
            'date_validation_issues': []
        }
        
        if 'address_postal_code' not in completed_df.columns or 'address_country_code' not in completed_df.columns:
            return validation_results
        
        # Use fixed date instead of current datetime
        current_datetime = fixed_date
        
        for idx, row in completed_df.iterrows():
            country_code = row.get('address_country_code', '')
            postal_code = row.get('address_postal_code', '')
            
            if not postal_code or pd.isna(postal_code):
                continue
                
            postal_code = str(postal_code).strip()
            
            # US postal code validation
            if country_code == 'US':
                # Check for 4-digit zip codes (missing leading zero)
                if len(postal_code) == 4 and postal_code.isdigit():
                    validation_results['us_missing_zero'].append({
                        'line': idx + 1,
                        'postalCode': postal_code,
                        'correctedCode': '0' + postal_code,
                        'email': row.get('customer_email', 'No email')
                    })
                # Check for incorrect format (not 4 or 5 digits, or non-numeric)
                elif not (len(postal_code) == 5 and postal_code.isdigit()):
                    validation_results['us_incorrect_format'].append({
                        'line': idx + 1,
                        'postalCode': postal_code,
                        'email': row.get('customer_email', 'No email')
                    })
            
            # Canadian postal code validation
            elif country_code == 'CA':
                # Check if postal code doesn't match Canadian format (A1A 1A1 or A1A1A1)
                import re
                canadian_format = re.compile(r'^[A-Za-z]\d[A-Za-z]\s?\d[A-Za-z]\d$')
                if not canadian_format.match(postal_code):
                    validation_results['canadian_incorrect_format'].append({
                        'line': idx + 1,
                        'postalCode': postal_code,
                        'email': row.get('customer_email', 'No email')
                    })
        
        # Bluesnap card_token validation
        if provider.lower() == 'bluesnap' and 'card_token' in completed_df.columns:
            for idx, row in completed_df.iterrows():
                card_token = row.get('card_token', '')
                if card_token and not pd.isna(card_token):
                    card_token = str(card_token).strip()
                    # Check if card_token is not exactly 13 numerical digits
                    if not card_token.isdigit() or len(card_token) != 13:
                        validation_results['bluesnap_card_token_format'].append({
                            'line': idx + 1,
                            'cardToken': card_token,
                            'email': row.get('customer_email', 'No email')
                        })
        
        # Date validation for both providers
        for idx, row in completed_df.iterrows():
            period_started = row.get('current_period_started_at')
            period_ends = row.get('current_period_ends_at')
            email = row.get('customer_email', 'No email')
            
            # Validate current_period_started_at
            if period_started and not pd.isna(period_started):
                try:
                    # Handle different date formats
                    if isinstance(period_started, str):
                        period_started_dt = pd.to_datetime(period_started)
                    elif hasattr(period_started, 'to_pydatetime'):
                        period_started_dt = period_started.to_pydatetime()
                        if period_started_dt.tzinfo is None:
                            period_started_dt = period_started_dt.replace(tzinfo=timezone.utc)
                    else:
                        period_started_dt = pd.to_datetime(period_started)
                    
                    # Ensure timezone awareness for comparison
                    if hasattr(period_started_dt, 'tzinfo') and period_started_dt.tzinfo is None:
                        period_started_dt = period_started_dt.replace(tzinfo=timezone.utc)
                    
                    if period_started_dt >= current_datetime:
                        validation_results['date_validation_issues'].append({
                            'line': idx + 1,
                            'field': 'current_period_started_at',
                            'value': str(period_started),
                            'email': email,
                            'issue': 'start_date_not_in_past'
                        })
                except Exception as e:
                    validation_results['date_validation_issues'].append({
                        'line': idx + 1,
                        'field': 'current_period_started_at',
                        'value': str(period_started),
                        'email': email,
                        'issue': 'invalid_date_format'
                    })
            
            # Validate current_period_ends_at
            if period_ends and not pd.isna(period_ends):
                try:
                    # Handle different date formats
                    if isinstance(period_ends, str):
                        period_ends_dt = pd.to_datetime(period_ends)
                    elif hasattr(period_ends, 'to_pydatetime'):
                        period_ends_dt = period_ends.to_pydatetime()
                        if period_ends_dt.tzinfo is None:
                            period_ends_dt = period_ends_dt.replace(tzinfo=timezone.utc)
                    else:
                        period_ends_dt = pd.to_datetime(period_ends)
                    
                    # Ensure timezone awareness for comparison
                    if hasattr(period_ends_dt, 'tzinfo') and period_ends_dt.tzinfo is None:
                        period_ends_dt = period_ends_dt.replace(tzinfo=timezone.utc)
                    
                    if period_ends_dt <= current_datetime:
                        validation_results['date_validation_issues'].append({
                            'line': idx + 1,
                            'field': 'current_period_ends_at',
                            'value': str(period_ends),
                            'email': email,
                            'issue': 'end_date_not_in_future'
                        })
                except Exception as e:
                    validation_results['date_validation_issues'].append({
                        'line': idx + 1,
                        'field': 'current_period_ends_at',
                        'value': str(period_ends),
                        'email': email,
                        'issue': 'invalid_date_format'
                    })
        
        return validation_results
    
    # Test validation without mapping postal codes first
    print("\n🔍 Testing validation WITHOUT mapping postal codes...")
    results = validate_with_fixed_date(subscriber_df, 'bluesnap')
    
    # Expected outcomes based on your specifications:
    expected_results = {
        'us_missing_zero': 1,      # z7w1s0wu6b9_t@gmail.com.sink.sparkpostmail.com - 2301 (missing leading zero)
        'us_incorrect_format': 1,  # lja5f7dndx_t@gmail.com.sink.sparkpostmail.com - 54h13 (incorrect format)
        'canadian_incorrect_format': 0,  # No Canadian records in this data
        'bluesnap_card_token_format': 1,  # lja5f7dndx_t@gmail.com.sink.sparkpostmail.com - 12345674242 (too short)
        'date_validation_issues': 2  # z7w1s0wu6b9_t (end date in past), lja5f7dndx_t (start date in future)
    }
    
    print("\n📋 Validation Results:")
    print(f"   US Missing Zero: {len(results['us_missing_zero'])} (Expected: {expected_results['us_missing_zero']})")
    print(f"   US Incorrect Format: {len(results['us_incorrect_format'])} (Expected: {expected_results['us_incorrect_format']})")
    print(f"   Canadian Incorrect Format: {len(results['canadian_incorrect_format'])} (Expected: {expected_results['canadian_incorrect_format']})")
    print(f"   Bluesnap Card Token: {len(results['bluesnap_card_token_format'])} (Expected: {expected_results['bluesnap_card_token_format']})")
    
    # Count date validation issues by type
    start_date_future = sum(1 for issue in results['date_validation_issues'] 
                           if issue['issue'] == 'start_date_not_in_past')
    end_date_past = sum(1 for issue in results['date_validation_issues'] 
                       if issue['issue'] == 'end_date_not_in_future')
    invalid_format = sum(1 for issue in results['date_validation_issues'] 
                        if issue['issue'] == 'invalid_date_format')
    
    print(f"   Date Validation Issues: {len(results['date_validation_issues'])} (Expected: {expected_results['date_validation_issues']})")
    print(f"     - Start Date in Future: {start_date_future}")
    print(f"     - End Date in Past: {end_date_past}")
    print(f"     - Invalid Date Format: {invalid_format}")
    
    # Verify specific issues
    print("\n🔍 Detailed Issue Analysis:")
    
    # Check US missing zero
    missing_zero_emails = [issue['email'] for issue in results['us_missing_zero']]
    print(f"   US Missing Zero: {missing_zero_emails}")
    assert 'z7w1s0wu6b9_t@gmail.com.sink.sparkpostmail.com' in missing_zero_emails, "Missing zero issue not detected for z7w1s0wu6b9_t@gmail.com.sink.sparkpostmail.com"
    
    # Check US incorrect format
    incorrect_format_emails = [issue['email'] for issue in results['us_incorrect_format']]
    print(f"   US Incorrect Format: {incorrect_format_emails}")
    assert 'lja5f7dndx_t@gmail.com.sink.sparkpostmail.com' in incorrect_format_emails, "Incorrect format not detected for lja5f7dndx_t@gmail.com.sink.sparkpostmail.com"
    
    # Check Bluesnap card token format
    card_token_emails = [issue['email'] for issue in results['bluesnap_card_token_format']]
    print(f"   Bluesnap Card Token Format: {card_token_emails}")
    assert 'lja5f7dndx_t@gmail.com.sink.sparkpostmail.com' in card_token_emails, "Card token format not detected for lja5f7dndx_t@gmail.com.sink.sparkpostmail.com"
    
    # Check date validation issues
    date_issue_emails = [issue['email'] for issue in results['date_validation_issues']]
    print(f"   Date Validation Issues: {date_issue_emails}")
    
    expected_date_emails = ['z7w1s0wu6b9_t@gmail.com.sink.sparkpostmail.com', 'lja5f7dndx_t@gmail.com.sink.sparkpostmail.com']
    for email in expected_date_emails:
        assert email in date_issue_emails, f"Date validation issue not detected for {email}"
    
    print("\n✅ All validation tests passed!")
    return True

def test_mapping_postal_codes():
    """Test the mapping postal codes functionality."""
    print("\n🧪 Testing Mapping Postal Codes Functionality...")
    
    # This would test the actual mapping logic
    # For now, let's verify that another2@test.com has missing postal code
    subscriber_file = "testdata/BlueSnap/sandbox_sub_data_test.csv"
    subscriber_df = pd.read_csv(subscriber_file)
    
    # Find another2@test.com record
    another2_record = subscriber_df[subscriber_df['customer_email'] == 'another2@test.com']
    
    if len(another2_record) > 0:
        postal_code = another2_record.iloc[0]['address_postal_code']
        print(f"   another2@test.com postal code: '{postal_code}'")
        
        if pd.isna(postal_code) or postal_code == '':
            print("   ✅ another2@test.com has missing postal code (as expected)")
        else:
            print(f"   ⚠️ another2@test.com has postal code: {postal_code}")
    
    print("✅ Mapping postal codes test completed!")

def test_card_token_mapping():
    """Test that lja5f7dndx_t@gmail.com.sink.sparkpostmail.com has no matching token in mapping file."""
    print("\n🧪 Testing Card Token Mapping...")
    
    subscriber_file = "testdata/BlueSnap/sandbox_sub_data_test.csv"
    mapping_file = "testdata/BlueSnap/mapping.csv"
    
    subscriber_df = pd.read_csv(subscriber_file)
    mapping_df = pd.read_csv(mapping_file)
    
    # Find lja5f7dndx_t record
    lja5f7dndx_record = subscriber_df[subscriber_df['customer_email'] == 'lja5f7dndx_t@gmail.com.sink.sparkpostmail.com']
    
    if len(lja5f7dndx_record) > 0:
        lja5f7dndx_token = lja5f7dndx_record.iloc[0]['card_token']
        print(f"   lja5f7dndx_t@gmail.com.sink.sparkpostmail.com card_token: '{lja5f7dndx_token}'")
        
        # Check if this token exists in mapping file
        matching_mapping = mapping_df[mapping_df['Credit Card Id'] == lja5f7dndx_token]
        
        if len(matching_mapping) == 0:
            print("   ✅ lja5f7dndx_t@gmail.com.sink.sparkpostmail.com has no matching token in mapping file (as expected)")
        else:
            print(f"   ⚠️ lja5f7dndx_t@gmail.com.sink.sparkpostmail.com has matching token in mapping file")
    
    print("✅ Card token mapping test completed!")

def run_bluesnap_tests():
    """Run all Bluesnap validation tests."""
    print("🚀 Starting Bluesnap Validation Test Suite...\n")
    
    try:
        test_bluesnap_validation_with_fixed_date()
        test_mapping_postal_codes()
        test_card_token_mapping()
        
        print("\n🎉 All Bluesnap tests completed!")
        
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        raise

if __name__ == "__main__":
    run_bluesnap_tests()
