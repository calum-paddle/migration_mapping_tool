from datetime import datetime, timedelta
import pandas as pd
import random
import string
import os
import time
import zipfile

def generate_random_email():
    """Generate a random email for sandbox data anonymization"""
    random_string = ''.join(random.choices(string.ascii_lowercase + string.digits, k=5))
    return f"blackhole+{random_string}@paddle.com"

def validate_subscriber_columns(columns):
    """
    Validate that the subscriber file has all required columns
    
    Args:
        columns: List of column names from the subscriber file
    
    Returns:
        dict: Validation results with status and missing columns
    """
    # Required columns (exact names)
    required_columns = [
        'customer_email',
        'customer_full_name', 
        'customer_external_id',
        'business_tax_identifier',
        'business_name',
        'business_company_number',
        'business_external_id',
        'address_country_code',
        'address_street_line1',
        'address_street_line2',
        'address_city',
        'address_region',
        'address_postal_code',
        'address_external_id',
        'status',
        'currency_code',
        'started_at',
        'paused_at',
        'collection_mode',
        'enable_checkout',
        'purchase_order_number',
        'additional_information',
        'payment_terms_frequency',
        'payment_terms_interval',
        'current_period_started_at',
        'current_period_ends_at',
        'trial_period_frequency',
        'trial_period_interval',
        'subscription_external_id',
        'card_token',
        'discount_id',
        'discount_remaining_cycles',
        'subscription_custom_data_key_1',
        'subscription_custom_data_value_1',
        'subscription_custom_data_key_2',
        'subscription_custom_data_value_2',
        'price_id_1',
        'quantity_1',
        'price_id_2',
        'quantity_2'
    ]
    
    # Convert columns to list if it's a pandas Index
    if hasattr(columns, 'tolist'):
        columns = columns.tolist()
    
    # Check for missing required columns
    missing_columns = [col for col in required_columns if col not in columns]
    
    # Check for optional custom data pairs and line items (should not cause validation to fail)
    optional_patterns = [
        r'subscription_custom_data_key_\d+',
        r'subscription_custom_data_value_\d+',
        r'price_id_\d+',
        r'quantity_\d+'
    ]
    
    import re
    optional_columns = []
    for pattern in optional_patterns:
        for col in columns:
            if re.match(pattern, col) and col not in required_columns:
                optional_columns.append(col)
    
    return {
        'valid': len(missing_columns) == 0,
        'missing_columns': missing_columns,
        'optional_columns': optional_columns,
        'total_columns': len(columns),
        'required_columns_count': len(required_columns)
    }

def validate_bluesnap_card_tokens(subscriber_data, seller_name='', is_sandbox=False):
    """
    Validate that Bluesnap card tokens are exactly 13 numerical characters
    
    Args:
        subscriber_data: DataFrame containing subscriber data
        seller_name: Name of the seller for file naming
        is_sandbox: Boolean indicating if this is sandbox mode
    
    Returns:
        dict: Validation results with status and incorrect records
    """
    import re
    
    try:
        # Check if card_token column exists
        if 'card_token' not in subscriber_data.columns:
            return {
                'valid': False,
                'error': 'card_token column not found',
                'incorrect_count': 0,
                'incorrect_records': None
            }
        
        # Filter out rows where card_token is null/empty
        valid_data = subscriber_data[subscriber_data['card_token'].notna() & (subscriber_data['card_token'] != '')]
        
        # Check each card_token for exactly 13 numerical characters
        pattern = r'^\d{13}$'
        incorrect_mask = ~valid_data['card_token'].astype(str).str.match(pattern)
        incorrect_records = valid_data[incorrect_mask]
        
        return {
            'valid': len(incorrect_records) == 0,
            'incorrect_count': len(incorrect_records),
            'incorrect_records': incorrect_records,
            'total_records': len(valid_data)
        }
    except Exception as e:
        print(f"Error in card token validation: {e}")
        return {
            'valid': False,
            'error': f'Validation error: {str(e)}',
            'incorrect_count': 0,
            'total_records': 0,
            'download_file': None
        }

def validate_date_format(subscriber_data, seller_name='', is_sandbox=False):
    """
    Validate that current_period_started_at and current_period_ends_at dates are in the correct format
    - Format must be: YYYY-MM-DDTHH:MM:SSZ (e.g., 2025-07-06T00:00:00Z)
    
    Args:
        subscriber_data: DataFrame containing subscriber data
        seller_name: Name of the seller for file naming
        is_sandbox: Boolean indicating if this is sandbox mode
    
    Returns:
        dict: Validation results with status and incorrect records
    """
    import re
    
    try:
        # Check if required columns exist
        required_columns = ['current_period_started_at', 'current_period_ends_at']
        missing_columns = [col for col in required_columns if col not in subscriber_data.columns]
        
        if missing_columns:
            return {
                'valid': False,
                'error': f'Missing required columns: {missing_columns}',
                'incorrect_count': 0,
                'total_records': 0,
                'incorrect_records': None
            }
        
        # Create a copy of the data for validation (don't modify original)
        validation_data = subscriber_data.copy()
        
        # Expected format: YYYY-MM-DDTHH:MM:SSZ (e.g., 2025-07-06T00:00:00Z)
        date_format_pattern = r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$'
        
        # Check format for both date columns
        # Convert to string, handling NaN values
        # Note: If dates were already parsed as datetime objects by pandas, 
        # we check their string representation. The validation ensures the original
        # CSV format matches the required pattern.
        def check_date_format(value):
            if pd.isna(value):
                return False
            # Convert to string
            value_str = str(value)
            # Check for common NaN representations
            if value_str.lower() in ['nan', 'none', 'nat', '']:
                return False
            # Check if it matches the exact required format
            return bool(re.match(date_format_pattern, value_str))
        
        started_valid = validation_data['current_period_started_at'].apply(check_date_format)
        ended_valid = validation_data['current_period_ends_at'].apply(check_date_format)
        
        # Find records where either date has incorrect format
        incorrect_format_mask = ~(started_valid & ended_valid)
        incorrect_records = validation_data[incorrect_format_mask].copy()
        
        # Convert all columns to strings to prevent float conversion in CSV
        if not incorrect_records.empty:
            for col in incorrect_records.columns:
                # Handle NaN values and ensure all data is string
                incorrect_records[col] = incorrect_records[col].fillna('').astype(str).replace('nan', '')
                # Remove decimal points from numeric strings (e.g., '8830.0' -> '8830')
                incorrect_records[col] = incorrect_records[col].str.replace(r'\.0$', '', regex=True)
        
        return {
            'valid': len(incorrect_records) == 0,
            'incorrect_count': len(incorrect_records),
            'incorrect_records': incorrect_records,
            'total_records': len(validation_data)
        }
        
    except Exception as e:
        print(f"Error in date format validation: {e}")
        import traceback
        traceback.print_exc()
        return {
            'valid': False,
            'error': f'Validation error: {str(e)}',
            'incorrect_count': 0,
            'total_records': 0,
            'incorrect_records': None
        }

def validate_date_periods(subscriber_data, seller_name='', is_sandbox=False):
    """
    Validate that current_period_started_at and current_period_ends_at dates are logical
    - current_period_started_at should not be after current date/time
    - current_period_ends_at should not be before current date/time
    
    Args:
        subscriber_data: DataFrame containing subscriber data
        seller_name: Name of the seller for file naming
        is_sandbox: Boolean indicating if this is sandbox mode
    """
    try:
        # Check if required columns exist
        required_columns = ['current_period_started_at', 'current_period_ends_at']
        missing_columns = [col for col in required_columns if col not in subscriber_data.columns]
        
        if missing_columns:
            return {
                'valid': False,
                'error': f'Missing required columns: {missing_columns}',
                'incorrect_count': 0,
                'total_records': 0,
                'incorrect_records': None
            }
        
        # Get current date/time (timezone-naive)
        current_datetime = datetime.now()
        
        # Create a copy of the data for validation (don't modify original)
        validation_data = subscriber_data.copy()
        
        # Parse dates ONLY for this validation (force timezone-naive)
        try:
            # Parse dates and convert to timezone-naive
            started_parsed = pd.to_datetime(
                validation_data['current_period_started_at'], 
                errors='coerce'
            )
            ended_parsed = pd.to_datetime(
                validation_data['current_period_ends_at'], 
                errors='coerce'
            )
            
            # Convert to timezone-naive if they have timezone info
            if started_parsed.dt.tz is not None:
                started_parsed = started_parsed.dt.tz_convert(None)
            if ended_parsed.dt.tz is not None:
                ended_parsed = ended_parsed.dt.tz_convert(None)
                
            validation_data['current_period_started_at_parsed'] = started_parsed
            validation_data['current_period_ends_at_parsed'] = ended_parsed
            
        except Exception as e:
            return {
                'valid': False,
                'error': f'Error parsing dates: {str(e)}',
                'incorrect_count': 0,
                'total_records': 0,
                'incorrect_records': None
            }
        
        # Filter out records with valid dates
        valid_data = validation_data[
            validation_data['current_period_started_at_parsed'].notna() & 
            validation_data['current_period_ends_at_parsed'].notna()
        ]
        
        if len(valid_data) == 0:
            return {
                'valid': False,
                'error': 'No valid date records found',
                'incorrect_count': 0,
                'total_records': 0,
                'incorrect_records': None
            }
        
        # Check for invalid date periods
        invalid_started = valid_data['current_period_started_at_parsed'] > current_datetime
        invalid_ended = valid_data['current_period_ends_at_parsed'] < current_datetime
        
        # Get records with invalid date periods
        incorrect_records = valid_data[invalid_started | invalid_ended].copy()
        
        # Remove the parsed columns before returning (keep original format)
        if 'current_period_started_at_parsed' in incorrect_records.columns:
            incorrect_records = incorrect_records.drop('current_period_started_at_parsed', axis=1)
        if 'current_period_ends_at_parsed' in incorrect_records.columns:
            incorrect_records = incorrect_records.drop('current_period_ends_at_parsed', axis=1)
        
        # Ensure all datetime columns are converted to strings for JSON serialization
        for col in incorrect_records.columns:
            if incorrect_records[col].dtype == 'datetime64[ns]' or incorrect_records[col].dtype == 'datetime64[ns, UTC]':
                incorrect_records[col] = incorrect_records[col].astype(str)
        
        return {
            'valid': len(incorrect_records) == 0,
            'incorrect_count': len(incorrect_records),
            'incorrect_records': incorrect_records,
            'total_records': len(valid_data)
        }
        
    except Exception as e:
        print(f"Error in date period validation: {e}")
        return {
            'valid': False,
            'error': f'Validation error: {str(e)}',
            'incorrect_count': 0,
            'total_records': 0,
            'incorrect_records': None
        }

def validate_missing_postal_codes(data, provider, seller_name='', is_sandbox=False):
    """
    Validate missing postal codes for specific countries and check if they can be filled from mapping file
    """
    try:
        # Countries that require postal codes
        required_countries = ['AU', 'CA', 'FR', 'DE', 'IN', 'IT', 'NL', 'ES', 'GB', 'US']
        
        # Filter for records from required countries
        required_records = data[data['address_country_code'].isin(required_countries)].copy()
        
        if len(required_records) == 0:
            return {
                'valid': True,
                'missing_count': 0,
                'total_records': 0,
                'available_from_mapping': 0,
                'missing_records': None
            }
        
        # Find records with missing postal codes
        missing_postal_codes = required_records[
            required_records['address_postal_code'].isna() | 
            (required_records['address_postal_code'].astype(str).str.strip() == '')
        ].copy()
        
        # Calculate missing_count early so we can preserve it even if an exception occurs later
        missing_count = len(missing_postal_codes)
        total_records_count = len(required_records)
        
        if missing_count == 0:
            return {
                'valid': True,
                'missing_count': 0,
                'total_records': total_records_count,
                'available_from_mapping': 0,
                'missing_records': None
            }
        
        # Check mapping file column name based on provider
        mapping_column = 'card.address_zip' if provider.lower() == 'stripe' else 'Zip Code'
        
        # Count records that have postal codes available in mapping file (already merged)
        # Check if mapping column exists before trying to use it
        available_count = 0
        if mapping_column in missing_postal_codes.columns:
            try:
                available_from_mapping = missing_postal_codes[
                    missing_postal_codes[mapping_column].notna() & 
                    (missing_postal_codes[mapping_column].astype(str).str.strip() != '')
                ]
                available_count = len(available_from_mapping)
            except Exception as e:
                print(f"Warning: Error counting available postal codes from mapping: {e}")
                available_count = 0
        
        # Convert all columns to strings to prevent float conversion in CSV
        # Wrap this in try/except to preserve missing_count even if conversion fails
        try:
            if not missing_postal_codes.empty:
                for col in missing_postal_codes.columns:
                    missing_postal_codes[col] = missing_postal_codes[col].fillna('').astype(str).replace('nan', '')
                    missing_postal_codes[col] = missing_postal_codes[col].str.replace(r'\.0$', '', regex=True)
        except Exception as e:
            print(f"Warning: Error converting columns to strings: {e}")
            # Continue with unconverted data - missing_count is still valid
        
        return {
            'valid': False,
            'missing_count': missing_count,
            'total_records': total_records_count,
            'available_from_mapping': available_count,
            'missing_records': missing_postal_codes
        }
        
    except Exception as e:
        print(f"Error in missing postal code validation: {e}")
        import traceback
        traceback.print_exc()
        # Try to preserve any counts we might have calculated
        return {
            'valid': False,
            'error': f'Validation error: {str(e)}',
            'missing_count': 0,  # Can't preserve count if exception occurs before calculation
            'total_records': 0,
            'available_from_mapping': 0,
            'missing_records': None
        }

def validate_ca_postal_codes(data, seller_name='', is_sandbox=False):
    """
    Validate Canadian postal codes for records with address_country_code = 'CA'
    
    Args:
        data: DataFrame containing the merged data
        seller_name: Name of the seller for file naming
        is_sandbox: Boolean indicating if this is sandbox mode
    
    Returns:
        dict: Validation results with status and incorrect records
    """
    try:
        import re
        
        # Filter for Canadian records
        ca_records = data[data['address_country_code'] == 'CA'].copy()
        
        if len(ca_records) == 0:
            return {
                'valid': True,
                'incorrect_count': 0,
                'total_records': 0,
                'incorrect_records': None
            }
        
        # Canadian postal code regex pattern
        # Format: Letter-Number-Letter Number-Letter-Number (e.g., A1A 1A1)
        # Letters exclude D, F, I, O, Q, U, W, Z
        ca_postal_pattern = r'^[A-CEGHJ-NPR-TV-Z]\d[A-CEGHJ-NPR-TV-Z] ?\d[A-CEGHJ-NPR-TV-Z]\d$'
        
        # Check postal codes
        invalid_postal_codes = ca_records[
            ca_records['address_postal_code'].notna() & 
            ~ca_records['address_postal_code'].astype(str).str.match(ca_postal_pattern, case=False)
        ].copy()
        
        # Convert all columns to strings to prevent float conversion in CSV
        if not invalid_postal_codes.empty:
            print(f"DEBUG: CA validation - Found {len(invalid_postal_codes)} invalid records")
            print(f"DEBUG: CA validation - DataFrame shape: {invalid_postal_codes.shape}")
            print(f"DEBUG: CA validation - Column dtypes before conversion:")
            for col in invalid_postal_codes.columns:
                print(f"  {col}: {invalid_postal_codes[col].dtype}")
                print(f"  Sample values: {invalid_postal_codes[col].head(3).tolist()}")
            
            for col in invalid_postal_codes.columns:
                # Handle NaN values and ensure all data is string
                invalid_postal_codes[col] = invalid_postal_codes[col].fillna('').astype(str).replace('nan', '')
                # Remove decimal points from numeric strings (e.g., '8830.0' -> '8830')
                invalid_postal_codes[col] = invalid_postal_codes[col].str.replace(r'\.0$', '', regex=True)
            
            print(f"DEBUG: CA validation - Column dtypes after conversion:")
            for col in invalid_postal_codes.columns:
                print(f"  {col}: {invalid_postal_codes[col].dtype}")
                print(f"  Sample values: {invalid_postal_codes[col].head(3).tolist()}")
        
        return {
            'valid': len(invalid_postal_codes) == 0,
            'incorrect_count': len(invalid_postal_codes),
            'incorrect_records': invalid_postal_codes,
            'total_records': len(ca_records)
        }
        
    except Exception as e:
        print(f"Error in CA postal code validation: {e}")
        return {
            'valid': False,
            'error': f'Validation error: {str(e)}',
            'incorrect_count': 0,
            'total_records': 0,
            'incorrect_records': None
        }

def validate_us_postal_codes(data, seller_name='', is_sandbox=False):
    """
    Validate US postal codes for records with address_country_code = 'US'
    
    Args:
        data: DataFrame containing the merged data
        seller_name: Name of the seller for file naming
        is_sandbox: Boolean indicating if this is sandbox mode
    
    Returns:
        dict: Validation results with status and incorrect records
    """
    try:
        import re
        
        # Filter for US records
        us_records = data[data['address_country_code'] == 'US'].copy()
        
        if len(us_records) == 0:
            return {
                'valid': True,
                'incorrect_count': 0,
                'total_records': 0,
                'incorrect_records': None,
                'autocorrectable_count': 0
            }
        
        # US postal code regex pattern - only 5 digits
        us_postal_pattern = r'^\d{5}$'
        
        # Check postal codes
        invalid_postal_codes = us_records[
            us_records['address_postal_code'].notna() & 
            ~us_records['address_postal_code'].astype(str).str.match(us_postal_pattern)
        ].copy()
        
        # Count 4-digit codes that can be autocorrected
        four_digit_codes = us_records[
            us_records['address_postal_code'].notna() & 
            us_records['address_postal_code'].astype(str).str.match(r'^\d{4}$')
        ]
        autocorrectable_count = len(four_digit_codes)
        
        # Convert all columns to strings to prevent float conversion in CSV
        if not invalid_postal_codes.empty:
            for col in invalid_postal_codes.columns:
                # Handle NaN values and ensure all data is string
                invalid_postal_codes[col] = invalid_postal_codes[col].fillna('').astype(str).replace('nan', '')
                # Remove decimal points from numeric strings (e.g., '8830.0' -> '8830')
                invalid_postal_codes[col] = invalid_postal_codes[col].str.replace(r'\.0$', '', regex=True)
        
        return {
            'valid': len(invalid_postal_codes) == 0,
            'incorrect_count': len(invalid_postal_codes),
            'incorrect_records': invalid_postal_codes,
            'total_records': len(us_records),
            'autocorrectable_count': autocorrectable_count
        }
        
    except Exception as e:
        print(f"Error in US postal code validation: {e}")
        return {
            'valid': False,
            'error': f'Validation error: {str(e)}',
            'incorrect_count': 0,
            'total_records': 0,
            'incorrect_records': None,
            'autocorrectable_count': 0
        }

def process_migration(subscriber_file, mapping_file, vault_provider, is_sandbox=False, provider='stripe', seller_name='', autocorrect_us_postal=False, use_mapping_postal_codes=False, proceed_without_missing_records=False):
    """
    Process migration from payment providers to Paddle Billing
    
    Args:
        subscriber_file: File object or path to subscriber CSV
        mapping_file: File object or path to mapping CSV
        vault_provider: Name of the vault provider
        is_sandbox: Boolean indicating if this is sandbox mode
        provider: String indicating the payment provider ('stripe' or 'bluesnap')
        seller_name: Name of the seller for file naming
    
    Returns:
        dict: Processing results and file information
    """
    start_time = time.time()
    
    # Welcome message based on environment
    if is_sandbox:
        welcome = '''
Paddle Billing SANDBOX Script

This script assumes you have a test mapping file to map against. 

You will need to update the references in your 'card_token' column to match 'card_1J0yEyH65PkfON7EQ0Owsy3Q' in the mapping file, 
and the card token that works in sandbox for tokenex is: 42424205H9gc4242

PLEASE ENSURE ALL COLUMNS HEADERS HAVE NO HIDDEN WHITE SPACES

'''
    else:
        welcome = '''
Paddle Billing PRODUCTION Script

This script assumes you have a CSV mapping file from TokenEx to map against. 

PLEASE ENSURE ALL COLUMNS HEADERS HAVE NO HIDDEN WHITE SPACES

'''
    
    print(welcome)
    
    # Handle file inputs (could be File objects from React or file paths)
    if hasattr(subscriber_file, 'read'):
        # File object from React
        subscribedata = pd.read_csv(subscriber_file,
                                  dtype={'postal_code': object},
                                  keep_default_na=False, na_values=['_'])
        subscriber_filename = subscriber_file.name
    else:
        # File path
        subscribedata = pd.read_csv(subscriber_file,
                                  dtype={'postal_code': object},
                                  keep_default_na=False, na_values=['_'])
        subscriber_filename = os.path.basename(subscriber_file)
    
    if hasattr(mapping_file, 'read'):
        # File object from React
        mappingdata = pd.read_csv(mapping_file, encoding='latin-1')
    else:
        # File path
        mappingdata = pd.read_csv(mapping_file, encoding='latin-1')
    
    print(subscribedata)
    
    # Validate subscriber file columns
    print("Validating subscriber file columns...")
    validation_result = validate_subscriber_columns(subscribedata.columns)
    
    if not validation_result['valid']:
        print(f"Validation failed. Missing columns: {validation_result['missing_columns']}")
        return {
            'error': 'Column validation failed',
            'validation_result': validation_result,
            'step': 'column_validation',
            'validation_results': []  # No previous validations to show
        }
    
    print(f"Column validation passed. Found {validation_result['total_columns']} columns including {len(validation_result['optional_columns'])} optional columns.")
    
    # Add successful column validation to results
    validation_results = [{
        'valid': True,
        'step': 'column_validation',
        'total_columns': validation_result['total_columns'],
        'optional_columns': validation_result['optional_columns']
    }]
    
    # Bluesnap card token validation (only for Bluesnap provider)
    # COMMENTED OUT: Skipping card token length validation
    # if provider.lower() == 'bluesnap':
    #     print("Validating Bluesnap card tokens...")
    #     try:
    #         card_token_validation = validate_bluesnap_card_tokens(subscribedata, seller_name, is_sandbox)
    #     except Exception as e:
    #         print(f"Error during card token validation: {e}")
    #         return {
    #             'error': 'Card token validation error',
    #             'validation_result': {
    #                 'valid': False,
    #                 'error': f'Validation error: {str(e)}',
    #                 'incorrect_count': 0,
    #                 'total_records': 0,
    #                 'download_file': None
    #             },
    #             'step': 'card_token_validation',
    #             'validation_results': validation_results  # Include previous successful validations
    #         }
    #     
    #     if not card_token_validation['valid']:
    #         print(f"Card token validation failed. Found {card_token_validation['incorrect_count']} incorrect formats.")
    #         
    #         # Save incorrect records to a file for download
    #         if card_token_validation['incorrect_records'] is not None:
    #             try:
    #                 # Use the same output directory as the server
    #                 output_dir = 'outputs'
    #                 os.makedirs(output_dir, exist_ok=True)
    #                 
    #                 # Create filename with seller name and environment
    #                 clean_seller_name = "".join(c for c in seller_name if c.isalnum() or c in (' ', '-', '_')).rstrip()
    #                 clean_seller_name = clean_seller_name.replace(' ', '_')
    #                 env_suffix = "_sandbox" if is_sandbox else "_production"
    #                 incorrect_filename = f"{clean_seller_name}_incorrect_card_tokens{env_suffix}_{int(time.time())}.csv"
    #                 incorrect_path = os.path.join(output_dir, incorrect_filename)
    #                 card_token_validation['incorrect_records'].to_csv(incorrect_path, index=False)
    #                 card_token_validation['download_file'] = incorrect_filename
    #                 print(f"Saved incorrect records to: {incorrect_path}")
    #                 print(f"File exists after save: {os.path.exists(incorrect_path)}")
    #             except Exception as e:
    #                 print(f"Error saving incorrect records file: {e}")
    #                 # Continue without download file if saving fails
    #                 card_token_validation['download_file'] = None
    #         
    #         # Convert DataFrame to list of dictionaries for JSON serialization
    #         validation_result_for_json = {
    #             'valid': card_token_validation['valid'],
    #             'incorrect_count': card_token_validation['incorrect_count'],
    #             'total_records': card_token_validation['total_records'],
    #             'download_file': card_token_validation.get('download_file')
    #         }
    #         
    #         return {
    #             'error': 'Card token validation failed',
    #             'validation_result': validation_result_for_json,
    #             'step': 'card_token_validation',
    #             'validation_results': validation_results  # Include previous successful validations
    #         }
    #     
    #     print(f"Card token validation passed. All {card_token_validation['total_records']} card tokens are correctly formatted.")
    #     
    #     # Add successful card token validation to results
    #     validation_results.append({
    #         'valid': True,
    #         'step': 'card_token_validation',
    #         'total_records': card_token_validation['total_records']
    #     })
    
    # Date format validation (for all providers) - must be before date period validation
    print("Validating date formats...")
    try:
        date_format_validation = validate_date_format(subscribedata, seller_name, is_sandbox)
    except Exception as e:
        print(f"Error during date format validation: {e}")
        return {
            'error': 'Date format validation error',
            'validation_result': {
                'valid': False,
                'error': f'Validation error: {str(e)}',
                'incorrect_count': 0,
                'total_records': 0,
                'download_file': None
            },
            'step': 'date_format_validation',
            'validation_results': validation_results
        }
    
    if not date_format_validation['valid']:
        print(f"Date format validation failed. Found {date_format_validation['incorrect_count']} records with incorrect date formats.")
        
        # Save incorrect records to a file for download
        if date_format_validation['incorrect_records'] is not None:
            try:
                output_dir = 'outputs'
                os.makedirs(output_dir, exist_ok=True)
                
                # Create filename with seller name and environment
                clean_seller_name = "".join(c for c in seller_name if c.isalnum() or c in (' ', '-', '_')).rstrip()
                clean_seller_name = clean_seller_name.replace(' ', '_')
                env_suffix = "_sandbox" if is_sandbox else "_production"
                incorrect_filename = f"{clean_seller_name}_invalid_date_formats{env_suffix}_{int(time.time())}.csv"
                incorrect_path = os.path.join(output_dir, incorrect_filename)
                date_format_validation['incorrect_records'].to_csv(incorrect_path, index=False)
                date_format_validation['download_file'] = incorrect_filename
                print(f"Saved incorrect records to: {incorrect_path}")
                print(f"File exists after save: {os.path.exists(incorrect_path)}")
            except Exception as e:
                print(f"Error saving incorrect records file: {e}")
                date_format_validation['download_file'] = None
        
        # Convert DataFrame to list of dictionaries for JSON serialization
        validation_result_for_json = {
            'valid': date_format_validation['valid'],
            'incorrect_count': date_format_validation['incorrect_count'],
            'total_records': date_format_validation['total_records'],
            'download_file': date_format_validation.get('download_file')
        }
        
        print(f"Returning date format validation failure with {len(validation_results)} previous validations")
        
        # Ensure validation_results is JSON serializable
        clean_validation_results = []
        for validation in validation_results:
            clean_validation = {
                'valid': validation['valid'],
                'step': validation['step']
            }
            # Add other fields if they exist and are JSON serializable
            if 'total_columns' in validation:
                clean_validation['total_columns'] = validation['total_columns']
            if 'optional_columns' in validation:
                clean_validation['optional_columns'] = validation['optional_columns']
            if 'total_records' in validation:
                clean_validation['total_records'] = validation['total_records']
            clean_validation_results.append(clean_validation)
        
        return {
            'error': 'Date format validation failed',
            'validation_result': validation_result_for_json,
            'step': 'date_format_validation',
            'validation_results': clean_validation_results
        }
    
    print(f"Date format validation passed. All {date_format_validation['total_records']} date formats are valid.")
    
    # Add successful date format validation to results
    validation_results.append({
        'valid': True,
        'step': 'date_format_validation',
        'total_records': date_format_validation['total_records']
    })
    
    # Date period validation (for all providers)
    print("Validating date periods...")
    try:
        date_validation = validate_date_periods(subscribedata, seller_name, is_sandbox)
    except Exception as e:
        print(f"Error during date validation: {e}")
        return {
            'error': 'Date validation error',
            'validation_result': {
                'valid': False,
                'error': f'Validation error: {str(e)}',
                'incorrect_count': 0,
                'total_records': 0,
                'download_file': None
            },
            'step': 'date_validation',
            'validation_results': validation_results
        }
    
    if not date_validation['valid']:
        print(f"Date validation failed. Found {date_validation['incorrect_count']} records with invalid date periods.")
        
        # Save incorrect records to a file for download
        if date_validation['incorrect_records'] is not None:
            try:
                output_dir = 'outputs'
                os.makedirs(output_dir, exist_ok=True)
                
                # Create filename with seller name and environment
                clean_seller_name = "".join(c for c in seller_name if c.isalnum() or c in (' ', '-', '_')).rstrip()
                clean_seller_name = clean_seller_name.replace(' ', '_')
                env_suffix = "_sandbox" if is_sandbox else "_production"
                incorrect_filename = f"{clean_seller_name}_invalid_date_periods{env_suffix}_{int(time.time())}.csv"
                incorrect_path = os.path.join(output_dir, incorrect_filename)
                date_validation['incorrect_records'].to_csv(incorrect_path, index=False)
                date_validation['download_file'] = incorrect_filename
                print(f"Saved incorrect records to: {incorrect_path}")
                print(f"File exists after save: {os.path.exists(incorrect_path)}")
            except Exception as e:
                print(f"Error saving incorrect records file: {e}")
                date_validation['download_file'] = None
        
        # Convert DataFrame to list of dictionaries for JSON serialization
        validation_result_for_json = {
            'valid': date_validation['valid'],
            'incorrect_count': date_validation['incorrect_count'],
            'total_records': date_validation['total_records'],
            'download_file': date_validation.get('download_file')
        }
        
        print(f"Returning date validation failure with {len(validation_results)} previous validations")
        
        # Ensure validation_results is JSON serializable
        clean_validation_results = []
        for validation in validation_results:
            clean_validation = {
                'valid': validation['valid'],
                'step': validation['step']
            }
            # Add other fields if they exist and are JSON serializable
            if 'total_columns' in validation:
                clean_validation['total_columns'] = validation['total_columns']
            if 'optional_columns' in validation:
                clean_validation['optional_columns'] = validation['optional_columns']
            if 'total_records' in validation:
                clean_validation['total_records'] = validation['total_records']
            clean_validation_results.append(clean_validation)
        
        return {
            'error': 'Date validation failed',
            'validation_result': validation_result_for_json,
            'step': 'date_validation',
            'validation_results': clean_validation_results
        }
    
    print(f"Date validation passed. All {date_validation['total_records']} date periods are valid.")
    
    # Add successful date validation to results
    validation_results.append({
        'valid': True,
        'step': 'date_validation',
        'total_records': date_validation['total_records']
    })
    
    # Provider-specific data processing
    if provider.lower() == 'bluesnap':
        print("Processing Bluesnap data format...")
        
        # Create `card_token` in mapping file (BlueSnap Account Id + last 4 digits of credit card)
        mappingdata['card_token'] = (
            mappingdata['BlueSnap Account Id'].astype(str) +
            mappingdata['Credit Card Number'].astype(str).str[-4:]
        )
        
        # Map columns to match the required format
        mappingdata['card_holder_name'] = (
            mappingdata['First Name'].str.strip() + " " + mappingdata['Last Name'].str.strip()
        )
        
        # Keep both the original 'Credit Card Number' and the created 'card_token'
        mappingdata['original_credit_card_number'] = mappingdata['Credit Card Number']
        
        # Rename columns to match the expected output format
        mappingdata = mappingdata.rename(columns={
            'Expiration Month': 'card_expiry_month',
            'Expiration Year': 'card_expiry_year',
            'Network Transaction Id': 'network_transaction_id'
        })
        
        # Select necessary columns for the merge
        columns_to_keep = [
            'card_token',  # This is used for the merge
            'original_credit_card_number',  # Preserve the original credit card number
            'card_holder_name',
            'card_expiry_month',
            'card_expiry_year',
            'network_transaction_id'
        ]
        filtered_mappingdata = mappingdata[columns_to_keep]
        
        # Ensure `card_token` columns in both DataFrames are of the same type (string)
        filtered_mappingdata['card_token'] = filtered_mappingdata['card_token'].astype(str)
        subscribedata['card_token'] = subscribedata['card_token'].astype(str)
        
        # Merge the filtered mapping file with subscriber data on `card_token`
        finaljoin = pd.merge(
            filtered_mappingdata,
            subscribedata,
            on='card_token',  # Match on `card_token`
            how='outer'
        )
        
        # Keep only rows where `card_token` is not null
        finaljoin = finaljoin[finaljoin['card_token'].notna()]
        
        # Check for duplicate card_tokens BEFORE replacing with full card number
        # This identifies duplicates based on the original merge key (Account ID + last 4)
        duplicate_token_mask = finaljoin.duplicated(subset='card_token', keep=False)
        finaljoin['is_duplicate_token'] = duplicate_token_mask
        
        # Replace `card_token` in the final DataFrame with the original `Credit Card Number` from the mapping data
        finaljoin.loc[finaljoin['original_credit_card_number'].notna(), 'card_token'] = \
            finaljoin.loc[finaljoin['original_credit_card_number'].notna(), 'original_credit_card_number']
        
        # Drop the 'original_credit_card_number' column, as we no longer need it in the final output
        finaljoin = finaljoin.drop(columns=['original_credit_card_number'])
        
        completed = finaljoin
    
    else:
        # Stripe processing (using the working logic from original files)
        print("Processing Stripe data format...")
        
        subscribedata = subscribedata.rename(columns={'card_token': 'card_id'})
        
        mappingdata = mappingdata.rename(columns={'card.id': 'card_id'})
        mappingdata = mappingdata.rename(columns={'card.transaction_ids': 'network_transaction_id'})
        
        # Merge the two datasets (simple merge like original)
        finaljoin = pd.merge(mappingdata,
                            subscribedata,
                            left_on='card_id', 
                            right_on='card_id', 
                            how='outer')
        
        # Filter null card_ids after merge (like original)
        finaljoin = finaljoin[finaljoin['card_id'].notna()]
        
        # Check for duplicate card_ids BEFORE renaming card.number to card_token
        # This identifies duplicates based on the original merge key (card_id)
        duplicate_token_mask = finaljoin.duplicated(subset='card_id', keep=False)
        finaljoin['is_duplicate_token'] = duplicate_token_mask
        
        # Rename columns as required (like original)
        completed = finaljoin.rename(columns={
            'card.number': 'card_token',
            'card.name': 'card_holder_name',
            'card.exp_month': 'card_expiry_month',
            'card.exp_year': 'card_expiry_year',
        })
        
        completed['card_holder_name'] = completed['card_holder_name'].fillna(completed['customer_full_name'])
    
    # Missing Postal Code Validation (after merge, before column removal)
    print("Validating missing postal codes...")
    try:
        missing_postal_validation = validate_missing_postal_codes(completed, provider, seller_name, is_sandbox)
    except Exception as e:
        print(f"Error during missing postal code validation: {e}")
        return {
            'error': 'Missing postal code validation error',
            'validation_result': {
                'valid': False,
                'error': f'Validation error: {str(e)}',
                'missing_count': 0,
                'total_records': 0,
                'available_from_mapping': 0,
                'download_file': None
            },
            'step': 'missing_postal_code_validation',
            'validation_results': validation_results
        }
    
    if not missing_postal_validation['valid']:
        print(f"Missing postal code validation failed. Found {missing_postal_validation['missing_count']} missing postal codes.")
        print(f"Of these, {missing_postal_validation['available_from_mapping']} can be pulled from mapping file.")
        
        # Handle user choices for missing postal codes
        if use_mapping_postal_codes and missing_postal_validation['available_from_mapping'] > 0:
            print("User chose to use mapping postal codes. Pulling postal codes from mapping file...")
            
            # Get the missing records that can be fixed
            missing_records = missing_postal_validation['missing_records']
            print(f"Missing records DataFrame columns: {missing_records.columns.tolist() if missing_records is not None else 'None'}")
            
            if missing_records is not None and len(missing_records) > 0:
                # Determine the mapping column name based on provider
                mapping_column = 'card.address_zip' if provider.lower() == 'stripe' else 'Zip Code'
                print(f"Using mapping column: {mapping_column}")
                
                # Check if the mapping column exists in the merged data
                if mapping_column not in completed.columns:
                    print(f"Error: '{mapping_column}' column not found in merged data. Available columns: {completed.columns.tolist()}")
                    return {
                        'error': f'Mapping column {mapping_column} not found',
                        'validation_result': {
                            'valid': False,
                            'error': f'Mapping column {mapping_column} not found in merged data',
                            'missing_count': missing_postal_validation['missing_count'],
                            'total_records': missing_postal_validation['total_records'],
                            'available_from_mapping': missing_postal_validation['available_from_mapping'],
                            'download_file': missing_postal_validation.get('download_file')
                        },
                        'step': 'missing_postal_code_validation',
                        'validation_results': validation_results
                    }
                
                # Update the main dataset with postal codes from mapping
                updated_count = 0
                print(f"Missing records card_tokens: {missing_records['card_token'].tolist()}")
                
                # For each missing record, copy the postal code from the mapping column to address_postal_code
                for idx, row in missing_records.iterrows():
                    card_token = row['card_token']
                    mapping_postal_code = row[mapping_column]
                    print(f"Processing card_token: {card_token}, mapping postal code: {mapping_postal_code}")
                    
                    if pd.notna(mapping_postal_code) and str(mapping_postal_code).strip() != '':
                        # Find the corresponding row in the main dataset and update it
                        main_idx = completed[completed['card_token'] == card_token].index
                        if len(main_idx) > 0:
                            completed.loc[main_idx, 'address_postal_code'] = mapping_postal_code
                            updated_count += 1
                            print(f"Updated record {main_idx[0]} with postal code {mapping_postal_code}")
                        else:
                            print(f"No matching record found in main dataset for card_token: {card_token}")
                    else:
                        print(f"No valid postal code found in mapping for card_token: {card_token}")
                
                print(f"Updated {updated_count} records with postal codes from mapping file.")
                
                # Re-run the missing postal code validation
                try:
                    missing_postal_validation = validate_missing_postal_codes(completed, provider, seller_name, is_sandbox)
                except Exception as e:
                    print(f"Error during missing postal code validation after update: {e}")
                    return {
                        'error': 'Missing postal code validation error after update',
                        'validation_result': {
                            'valid': False,
                            'error': f'Validation error: {str(e)}',
                            'missing_count': 0,
                            'total_records': 0,
                            'available_from_mapping': 0,
                            'download_file': None
                        },
                        'step': 'missing_postal_code_validation',
                        'validation_results': validation_results
                    }
                
                if missing_postal_validation['valid']:
                    print(f"Missing postal code validation passed after using mapping postal codes. All {missing_postal_validation['total_records']} records have postal codes.")
                    # Add successful missing postal code validation to results
                    validation_results.append({
                        'valid': True,
                        'step': 'missing_postal_code_validation',
                        'total_records': missing_postal_validation['total_records']
                    })
                else:
                    # Still have missing postal codes after update
                    print(f"Still have {missing_postal_validation['missing_count']} missing postal codes after using mapping postal codes.")
                    # Continue with the existing error handling logic below
            else:
                print("No missing records found to update.")
                # Continue with the existing error handling logic below
        
        elif proceed_without_missing_records:
            print("User chose to proceed without missing records. Removing records with missing postal codes...")
            
            # Remove records with missing postal codes for the specified countries
            target_countries = ['AU', 'CA', 'FR', 'DE', 'IN', 'IT', 'NL', 'ES', 'GB', 'US']
            missing_mask = (completed['address_country_code'].isin(target_countries)) & (completed['address_postal_code'].isna() | (completed['address_postal_code'] == ''))
            
            records_to_remove = completed[missing_mask]
            completed = completed[~missing_mask]
            
            print(f"Removed {len(records_to_remove)} records with missing postal codes.")
            print(f"Remaining records: {len(completed)}")
            
            # Re-run the missing postal code validation
            try:
                missing_postal_validation = validate_missing_postal_codes(completed, provider, seller_name, is_sandbox)
            except Exception as e:
                print(f"Error during missing postal code validation after removal: {e}")
                return {
                    'error': 'Missing postal code validation error after removal',
                    'validation_result': {
                        'valid': False,
                        'error': f'Validation error: {str(e)}',
                        'missing_count': 0,
                        'total_records': 0,
                        'available_from_mapping': 0,
                        'download_file': None
                    },
                    'step': 'missing_postal_code_validation',
                    'validation_results': validation_results
                }
            
            if missing_postal_validation['valid']:
                print(f"Missing postal code validation passed after removing missing records. All {missing_postal_validation['total_records']} records have postal codes.")
                # Add successful missing postal code validation to results
                validation_results.append({
                    'valid': True,
                    'step': 'missing_postal_code_validation',
                    'total_records': missing_postal_validation['total_records']
                })
            else:
                # Still have missing postal codes after removal
                print(f"Still have {missing_postal_validation['missing_count']} missing postal codes after removal.")
                # Continue with the existing error handling logic below
        
        else:
            # User hasn't made a choice yet, return the validation error
            # Save missing records to a file for download
            if missing_postal_validation['missing_records'] is not None:
                try:
                    output_dir = 'outputs'
                    os.makedirs(output_dir, exist_ok=True)
                    
                    clean_seller_name = "".join(c for c in seller_name if c.isalnum() or c in (' ', '-', '_')).rstrip()
                    clean_seller_name = clean_seller_name.replace(' ', '_')
                    env_suffix = "_sandbox" if is_sandbox else "_production"
                    missing_filename = f"{clean_seller_name}_missing_postal_codes{env_suffix}_{int(time.time())}.csv"
                    missing_path = os.path.join(output_dir, missing_filename)
                    missing_postal_validation['missing_records'].to_csv(missing_path, index=False)
                    missing_postal_validation['download_file'] = missing_filename
                    print(f"Saved missing records to: {missing_path}")
                except Exception as e:
                    print(f"Error saving missing records file: {e}")
                    missing_postal_validation['download_file'] = None
            
            validation_result_for_json = {
                'valid': missing_postal_validation['valid'],
                'missing_count': missing_postal_validation['missing_count'],
                'total_records': missing_postal_validation['total_records'],
                'download_file': missing_postal_validation.get('download_file'),
                'available_from_mapping': missing_postal_validation['available_from_mapping']
            }
            
            return {
                'error': 'Missing postal code validation failed',
                'validation_result': validation_result_for_json,
                'step': 'missing_postal_code_validation',
                'validation_results': validation_results
            }
    
    print(f"Missing postal code validation passed. All {missing_postal_validation['total_records']} records have postal codes.")
    
    # Add successful missing postal code validation to results
    validation_results.append({
        'valid': True,
        'step': 'missing_postal_code_validation',
        'total_records': missing_postal_validation['total_records']
    })
    
    # Provider-specific column removal and ordering
    if provider.lower() == 'stripe':
        # For Stripe: Keep card address columns, remove other unnecessary columns
        columns_to_remove = [
            'default_source',
            'email',
            'id'
        ]
        
        # Ensure proper column ordering for Stripe
        stripe_column_order = [
            'description',
            'name',
            'card.address_city',
            'card.address_country',
            'card.address_line1',
            'card.address_line2',
            'card.address_state',
            'card.address_zip',
            'card_expiry_month',
            'card_expiry_year',
            'card_id',
            'card_holder_name',
            'card_token',
            'network_transaction_id',
            'customer_email',
            'customer_full_name',
            'customer_external_id',
            'business_tax_identifier',
            'business_name',
            'business_company_number',
            'business_external_id',
            'address_country_code',
            'address_street_line1',
            'address_street_line2',
            'address_city',
            'address_region',
            'address_postal_code',
            'address_external_id',
            'status',
            'currency_code',
            'started_at',
            'paused_at',
            'collection_mode',
            'enable_checkout',
            'purchase_order_number',
            'additional_information',
            'payment_terms_frequency',
            'payment_terms_interval',
            'current_period_started_at',
            'current_period_ends_at',
            'trial_period_frequency',
            'trial_period_interval',
            'subscription_external_id',
            'discount_id',
            'discount_remaining_cycles',
            'subscription_custom_data_key_1',
            'subscription_custom_data_value_1',
            'subscription_custom_data_key_2',
            'subscription_custom_data_value_2',
            'price_id_1',
            'quantity_1',
            'price_id_2',
            'quantity_2',
            'vault_provider'
        ]
        
    else:  # Bluesnap
        # For Bluesnap: Remove card address columns and card_id
        columns_to_remove = [
            'card_address_line1',
            'card_address_line2', 
            'card_address_city',
            'card_address_state',
            'card_address_zip',
            'card_address_country',
            'default_source',
            'email',
            'id',
            'card.address_zip', 
            'card.address_state', 
            'card.address_line2', 
            'card.address_line1', 
            'card.address_country', 
            'card.address_city', 
            'name', 
            'description',
            'card_id'  # Bluesnap doesn't have card_id
        ]
        
        # Ensure proper column ordering for Bluesnap
        bluesnap_column_order = [
            'card_token',
            'card_holder_name',
            'card_expiry_month',
            'card_expiry_year',
            'network_transaction_id',
            'customer_email',
            'customer_full_name',
            'customer_external_id',
            'business_tax_identifier',
            'business_name',
            'business_company_number',
            'business_external_id',
            'address_country_code',
            'address_street_line1',
            'address_street_line2',
            'address_city',
            'address_region',
            'address_postal_code',
            'address_external_id',
            'status',
            'currency_code',
            'started_at',
            'paused_at',
            'collection_mode',
            'enable_checkout',
            'purchase_order_number',
            'additional_information',
            'payment_terms_frequency',
            'payment_terms_interval',
            'current_period_started_at',
            'current_period_ends_at',
            'trial_period_frequency',
            'trial_period_interval',
            'subscription_external_id',
            'discount_id',
            'discount_remaining_cycles',
            'subscription_custom_data_key_1',
            'subscription_custom_data_value_1',
            'subscription_custom_data_key_2',
            'subscription_custom_data_value_2',
            'price_id_1',
            'quantity_1',
            'price_id_2',
            'quantity_2',
            'vault_provider'
        ]
    
    # Remove columns that exist in the dataframe
    columns_to_remove = [col for col in columns_to_remove if col in completed.columns]
    print(f"Removing columns: {columns_to_remove}")
    completed = completed.drop(columns=columns_to_remove)
    print(f"Shape after removing columns: {completed.shape}")
    
    # Reorder columns according to provider specification
    if provider.lower() == 'stripe':
        # Add any missing columns that should be in Stripe output
        for col in stripe_column_order:
            if col not in completed.columns:
                completed[col] = None
        
        # Reorder columns to match Stripe specification
        existing_columns = [col for col in stripe_column_order if col in completed.columns]
        # Preserve is_duplicate_token flag if it exists (needed for duplicate detection)
        if 'is_duplicate_token' in completed.columns and 'is_duplicate_token' not in existing_columns:
            existing_columns.append('is_duplicate_token')
        completed = completed[existing_columns]
        
    else:  # Bluesnap
        # Add any missing columns that should be in Bluesnap output
        for col in bluesnap_column_order:
            if col not in completed.columns:
                completed[col] = None
        
        # Reorder columns to match Bluesnap specification
        existing_columns = [col for col in bluesnap_column_order if col in completed.columns]
        # Preserve is_duplicate_token flag if it exists (needed for duplicate detection)
        if 'is_duplicate_token' in completed.columns and 'is_duplicate_token' not in existing_columns:
            existing_columns.append('is_duplicate_token')
        completed = completed[existing_columns]
    
    print("Filtering rows with customer_email...")
    print(f"Rows before email filtering: {len(completed)}")
    completed = completed[completed['customer_email'].notna()]
    print(f"Rows after email filtering: {len(completed)}")
    
    # Sandbox-specific data anonymization
    if is_sandbox:
        # Generate random emails to anonymize data (only emails, keep real names)
        completed['customer_email'] = completed['customer_email'].apply(lambda x: generate_random_email())
        print("Email addresses anonymized for sandbox")
    
    print("Processing date formatting...")
    # Keep original date format - no parsing or reformatting needed
    print("Date columns left in original format")
    
    print("Adding vault_provider column...")
    # Add vault_provider column
    # For Bluesnap sandbox with tokenex, ensure it's lowercase
    if provider.lower() == 'bluesnap' and is_sandbox and vault_provider.lower() == 'tokenex':
        completed['vault_provider'] = 'tokenex'
    else:
        completed['vault_provider'] = vault_provider
    
    print("Processing enable_checkout column...")
    # Check if 'enable_checkout' exists in the dataframe and convert to upper case if it does
    if 'enable_checkout' in completed.columns:
        completed['enable_checkout'] = completed['enable_checkout'].apply(lambda x: str(x).upper() if pd.notnull(x) else x)
        print("enable_checkout processing completed")
    else:
        print("enable_checkout column not found")
    
    # CA Postal Code Validation
    print("Validating Canadian postal codes...")
    try:
        ca_postal_validation = validate_ca_postal_codes(completed, seller_name, is_sandbox)
    except Exception as e:
        print(f"Error during CA postal code validation: {e}")
        return {
            'error': 'CA postal code validation error',
            'validation_result': {
                'valid': False,
                'error': f'Validation error: {str(e)}',
                'incorrect_count': 0,
                'total_records': 0,
                'download_file': None
            },
            'step': 'ca_postal_code_validation',
            'validation_results': validation_results
        }
    
    if not ca_postal_validation['valid']:
        print(f"CA postal code validation failed. Found {ca_postal_validation['incorrect_count']} incorrect formats.")
        
        # Save incorrect records to a file for download
        if ca_postal_validation['incorrect_records'] is not None:
            try:
                output_dir = 'outputs'
                os.makedirs(output_dir, exist_ok=True)
                
                clean_seller_name = "".join(c for c in seller_name if c.isalnum() or c in (' ', '-', '_')).rstrip()
                clean_seller_name = clean_seller_name.replace(' ', '_')
                env_suffix = "_sandbox" if is_sandbox else "_production"
                incorrect_filename = f"{clean_seller_name}_invalid_ca_postal_codes{env_suffix}_{int(time.time())}.csv"
                incorrect_path = os.path.join(output_dir, incorrect_filename)
                print(f"DEBUG: CA validation - About to save CSV to: {incorrect_path}")
                print(f"DEBUG: CA validation - DataFrame dtypes before saving:")
                for col in ca_postal_validation['incorrect_records'].columns:
                    print(f"  {col}: {ca_postal_validation['incorrect_records'][col].dtype}")
                ca_postal_validation['incorrect_records'].to_csv(incorrect_path, index=False)
                ca_postal_validation['download_file'] = incorrect_filename
                print(f"Saved incorrect records to: {incorrect_path}")
            except Exception as e:
                print(f"Error saving incorrect records file: {e}")
                ca_postal_validation['download_file'] = None
        
        validation_result_for_json = {
            'valid': ca_postal_validation['valid'],
            'incorrect_count': ca_postal_validation['incorrect_count'],
            'total_records': ca_postal_validation['total_records'],
            'download_file': ca_postal_validation.get('download_file')
        }
        
        return {
            'error': 'CA postal code validation failed',
            'validation_result': validation_result_for_json,
            'step': 'ca_postal_code_validation',
            'validation_results': validation_results
        }
    
    print(f"CA postal code validation passed. All {ca_postal_validation['total_records']} Canadian postal codes are correctly formatted.")
    
    # Add successful CA postal code validation to results
    validation_results.append({
        'valid': True,
        'step': 'ca_postal_code_validation',
        'total_records': ca_postal_validation['total_records']
    })
    
    # US Postal Code Validation
    print("Validating US postal codes...")
    try:
        us_postal_validation = validate_us_postal_codes(completed, seller_name, is_sandbox)
    except Exception as e:
        print(f"Error during US postal code validation: {e}")
        return {
            'error': 'US postal code validation error',
            'validation_result': {
                'valid': False,
                'error': f'Validation error: {str(e)}',
                'incorrect_count': 0,
                'total_records': 0,
                'download_file': None,
                'autocorrectable_count': 0
            },
            'step': 'us_postal_code_validation',
            'validation_results': validation_results
        }
    
    if not us_postal_validation['valid']:
        print(f"US postal code validation failed. Found {us_postal_validation['incorrect_count']} incorrect formats.")
        print(f"Of these, {us_postal_validation['autocorrectable_count']} can be autocorrected with leading zeros.")
        
        # Check if autocorrect is requested
        if autocorrect_us_postal and us_postal_validation['autocorrectable_count'] > 0:
            print("Autocorrecting 4-digit US postal codes with leading zeros...")
            
            # Find US records with 4-digit postal codes and add leading zero
            us_records_mask = completed['address_country_code'] == 'US'
            four_digit_mask = completed['address_postal_code'].astype(str).str.match(r'^\d{4}$')
            
            # Apply autocorrect
            completed.loc[us_records_mask & four_digit_mask, 'address_postal_code'] = \
                completed.loc[us_records_mask & four_digit_mask, 'address_postal_code'].astype(str).str.zfill(5)
            
            print(f"Autocorrected {us_postal_validation['autocorrectable_count']} US postal codes.")
            
            # Re-run US validation to check if all issues are resolved
            us_postal_validation = validate_us_postal_codes(completed, seller_name, is_sandbox)
            
            if us_postal_validation['valid']:
                print("US postal code validation passed after autocorrection.")
                validation_results.append({
                    'valid': True,
                    'step': 'us_postal_code_validation',
                    'total_records': us_postal_validation['total_records'],
                    'autocorrected': True,
                    'autocorrected_count': us_postal_validation['autocorrectable_count']
                })
            else:
                # Still have invalid codes after autocorrection
                print(f"US postal code validation still failed after autocorrection. Found {us_postal_validation['incorrect_count']} remaining incorrect formats.")
                
                # Save incorrect records to a file for download
                if us_postal_validation['incorrect_records'] is not None:
                    try:
                        output_dir = 'outputs'
                        os.makedirs(output_dir, exist_ok=True)
                        
                        clean_seller_name = "".join(c for c in seller_name if c.isalnum() or c in (' ', '-', '_')).rstrip()
                        clean_seller_name = clean_seller_name.replace(' ', '_')
                        env_suffix = "_sandbox" if is_sandbox else "_production"
                        incorrect_filename = f"{clean_seller_name}_invalid_us_postal_codes_after_autocorrect{env_suffix}_{int(time.time())}.csv"
                        incorrect_path = os.path.join(output_dir, incorrect_filename)
                        us_postal_validation['incorrect_records'].to_csv(incorrect_path, index=False)
                        us_postal_validation['download_file'] = incorrect_filename
                        print(f"Saved incorrect records to: {incorrect_path}")
                    except Exception as e:
                        print(f"Error saving incorrect records file: {e}")
                        us_postal_validation['download_file'] = None
                
                validation_result_for_json = {
                    'valid': us_postal_validation['valid'],
                    'incorrect_count': us_postal_validation['incorrect_count'],
                    'total_records': us_postal_validation['total_records'],
                    'download_file': us_postal_validation.get('download_file'),
                    'autocorrectable_count': us_postal_validation['autocorrectable_count']
                }
                
                return {
                    'error': 'US postal code validation failed after autocorrection',
                    'validation_result': validation_result_for_json,
                    'step': 'us_postal_code_validation',
                    'validation_results': validation_results
                }
        else:
            # No autocorrect requested or no autocorrectable codes
            # Save incorrect records to a file for download
            if us_postal_validation['incorrect_records'] is not None:
                try:
                    output_dir = 'outputs'
                    os.makedirs(output_dir, exist_ok=True)
                    
                    clean_seller_name = "".join(c for c in seller_name if c.isalnum() or c in (' ', '-', '_')).rstrip()
                    clean_seller_name = clean_seller_name.replace(' ', '_')
                    env_suffix = "_sandbox" if is_sandbox else "_production"
                    incorrect_filename = f"{clean_seller_name}_invalid_us_postal_codes{env_suffix}_{int(time.time())}.csv"
                    incorrect_path = os.path.join(output_dir, incorrect_filename)
                    us_postal_validation['incorrect_records'].to_csv(incorrect_path, index=False)
                    us_postal_validation['download_file'] = incorrect_filename
                    print(f"Saved incorrect records to: {incorrect_path}")
                except Exception as e:
                    print(f"Error saving incorrect records file: {e}")
                    us_postal_validation['download_file'] = None
            
            validation_result_for_json = {
                'valid': us_postal_validation['valid'],
                'incorrect_count': us_postal_validation['incorrect_count'],
                'total_records': us_postal_validation['total_records'],
                'download_file': us_postal_validation.get('download_file'),
                'autocorrectable_count': us_postal_validation['autocorrectable_count']
            }
            
            return {
                'error': 'US postal code validation failed',
                'validation_result': validation_result_for_json,
                'step': 'us_postal_code_validation',
                'validation_results': validation_results
            }
    
    print(f"US postal code validation passed. All {us_postal_validation['total_records']} US postal codes are correctly formatted.")
    
    # Add successful US postal code validation to results
    validation_results.append({
        'valid': True,
        'step': 'us_postal_code_validation',
        'total_records': us_postal_validation['total_records']
    })
    
    print("Starting duplicate detection...")
    
    # Find all rows where card_token appears more than once
    # For both Bluesnap and Stripe: use the flag set before card_token was replaced/renamed
    # This checks duplicates based on the original merge key, not the final card_token value
    if 'is_duplicate_token' in completed.columns:
        duplicate_tokens = completed[completed['is_duplicate_token'] == True]
        # Drop the flag column from both completed and duplicate_tokens as it's only used for duplicate detection
        if 'is_duplicate_token' in duplicate_tokens.columns:
            duplicate_tokens = duplicate_tokens.drop(columns=['is_duplicate_token'])
        completed = completed.drop(columns=['is_duplicate_token'])
    else:
        # Fallback: check duplicates in card_token (shouldn't happen with current logic)
        duplicate_tokens = completed[completed['card_token'].notna() & completed.duplicated(subset='card_token', keep=False)]
    print(f"Duplicate tokens records: {len(duplicate_tokens)}")
    
    # Duplicate detection (same for both environments)
    success = completed[completed['card_token'].notna()]
    print(f"Success records: {len(success)}")
    
    no_tokens = completed[completed['card_token'].isnull()]
    print(f"No tokens records: {len(no_tokens)}")
    
    # Find all rows where card_id appears more than once (only for Stripe)
    duplicate_card_ids = pd.DataFrame()
    if provider.lower() == 'stripe' and 'card_id' in completed.columns:
        duplicate_card_ids = completed[completed['card_id'].notna() & completed.duplicated(subset='card_id', keep=False)]
        print(f"Duplicate card IDs records: {len(duplicate_card_ids)}")
    
    # Find all rows where subscription_external_id appears more than once
    duplicate_external_subscription_ids = completed[completed.duplicated(subset='subscription_external_id', keep=False)]
    print(f"Duplicate external subscription IDs records: {len(duplicate_external_subscription_ids)}")
    
    # Find all rows where customer_email appears more than once
    duplicate_emails = completed[completed.duplicated(subset='customer_email', keep=False)]
    print(f"Duplicate emails records: {len(duplicate_emails)}")
    
    # Generate output filenames
    if seller_name:
        # Use seller name as prefix, clean it for filename
        clean_seller_name = "".join(c for c in seller_name if c.isalnum() or c in (' ', '-', '_')).rstrip()
        clean_seller_name = clean_seller_name.replace(' ', '_')
        base_filename = f"{clean_seller_name}_{provider.lower()}"
    else:
        base_filename = os.path.splitext(subscriber_filename)[0]
        base_filename += f"_{provider.lower()}"
    
    if is_sandbox:
        base_filename += "_sandbox"
    
    output_files = []
    
    # Create outputs directory if it doesn't exist
    output_dir = 'outputs'
    os.makedirs(output_dir, exist_ok=True)
    
    # Save files and collect information
    files_to_save = [
        (success, f'{base_filename}_final_import.csv'),
        (no_tokens, f'{base_filename}_no_token_found.csv'),
        (duplicate_tokens, f'{base_filename}_duplicate_tokens.csv'),
        (duplicate_external_subscription_ids, f'{base_filename}_duplicate_external_subscription_ids.csv'),
        (duplicate_emails, f'{base_filename}_duplicate_emails.csv')
    ]
    
    # Add duplicate card IDs file only for Stripe
    if provider.lower() == 'stripe' and not duplicate_card_ids.empty:
        files_to_save.append((duplicate_card_ids, f'{base_filename}_duplicate_card_ids.csv'))
    
    for df, filename in files_to_save:
        if not df.empty:
            file_path = os.path.join(output_dir, filename)
            print(f"Saving file: {file_path}")
            
            # Convert all columns to strings to prevent float conversion
            df_string = df.copy()
            for col in df_string.columns:
                # Handle NaN values and ensure all data is string
                df_string[col] = df_string[col].fillna('').astype(str).replace('nan', '')
                # Remove decimal points from numeric strings (e.g., '8830.0' -> '8830')
                df_string[col] = df_string[col].str.replace(r'\.0$', '', regex=True)
            
            # Save with string formatting
            df_string.to_csv(file_path, index=False)
            
            file_size = os.path.getsize(file_path)
            print(f"File saved successfully. Size: {file_size} bytes")
            output_files.append({
                'name': filename,
                'size': file_size,
                'url': f'file://{os.path.abspath(file_path)}'
            })
        else:
            print(f"Skipping empty dataframe for: {filename}")
    
    # Create zip file with all reports
    if output_files:  # Only create zip if there are files to include
        zip_filename = f'{base_filename}_all_reports.zip'
        zip_path = os.path.join(output_dir, zip_filename)
        
        try:
            with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                for file_info in output_files:
                    file_path = os.path.join(output_dir, file_info['name'])
                    if os.path.exists(file_path):
                        zipf.write(file_path, file_info['name'])
                        print(f"Added {file_info['name']} to zip file")
            
            zip_size = os.path.getsize(zip_path)
            print(f"Zip file created successfully: {zip_path} (Size: {zip_size} bytes)")
            
            # Add zip file to output files list
            output_files.append({
                'name': zip_filename,
                'size': zip_size,
                'url': f'file://{os.path.abspath(zip_path)}',
                'is_zip': True
            })
        except Exception as e:
            print(f"Error creating zip file: {e}")
            # Continue without zip file if creation fails
    
    processing_time = time.time() - start_time
    
    # Prepare results
    results = {
        'success_count': len(success),
        'no_tokens_count': len(no_tokens),
        'duplicate_tokens_count': len(duplicate_tokens),
        'duplicate_external_subscription_ids_count': len(duplicate_external_subscription_ids),
        'duplicate_emails_count': len(duplicate_emails),
        'total_processed': len(completed),
        'processing_time': f"{processing_time:.2f} seconds",
        'output_files': output_files,
        'environment': 'Sandbox' if is_sandbox else 'Production',
        'validation_results': validation_results
    }
    
    # Add duplicate card IDs count only for Stripe
    if provider.lower() == 'stripe':
        results['duplicate_card_ids_count'] = len(duplicate_card_ids)
    
    print('Success')
    return results

# For direct script execution (backward compatibility)
if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 4:
        print("Usage: python migration-import-unified.py <subscriber_file> <mapping_file> <vault_provider> [--sandbox]")
        sys.exit(1)
    
    subscriber_file = sys.argv[1]
    mapping_file = sys.argv[2]
    vault_provider = sys.argv[3]
    is_sandbox = '--sandbox' in sys.argv
    
    results = process_migration(subscriber_file, mapping_file, vault_provider, is_sandbox)
    print(f"Processing complete. Results: {results}") 