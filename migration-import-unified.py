from datetime import datetime, timedelta
import pandas as pd
import random
import string
import os
import time

def generate_random_email():
    """Generate a random email for sandbox data anonymization"""
    random_string = ''.join(random.choices(string.ascii_lowercase + string.digits, k=5))
    return f"blackhole+{random_string}@paddle.com"

def validate_and_fix_zipcode(postal_code, country_code):
    """Validate and fix postal codes from mapping file data"""
    if not postal_code or pd.isna(postal_code):
        return postal_code
    
    postal_code = str(postal_code).strip()
    
    # Basic validation for all countries - ensure it's not completely invalid
    if len(postal_code) < 2 or len(postal_code) > 10:
        print(f"Warning: Postal code '{postal_code}' for {country_code} is too short or too long - skipping")
        return None
    
    # Country-specific validation
    if country_code == 'US':
        # US zip codes: 5 digits or 4 digits (missing leading zero)
        if len(postal_code) == 4 and postal_code.isdigit():
            return '0' + postal_code
        elif len(postal_code) == 5 and postal_code.isdigit():
            return postal_code
        else:
            print(f"Warning: Invalid US postal code format '{postal_code}' from mapping file - skipping")
            return None
    
    elif country_code == 'CA':
        # Canadian postal codes: A1A 1A1 format (letters and numbers)
        if len(postal_code) == 6 and postal_code[0].isalpha() and postal_code[1].isdigit() and postal_code[2].isalpha() and postal_code[3].isdigit() and postal_code[4].isalpha() and postal_code[5].isdigit():
            return postal_code
        elif len(postal_code) == 7 and postal_code[0].isalpha() and postal_code[1].isdigit() and postal_code[2].isalpha() and postal_code[3] == ' ' and postal_code[4].isdigit() and postal_code[5].isalpha() and postal_code[6].isdigit():
            return postal_code
        else:
            print(f"Warning: Invalid Canadian postal code format '{postal_code}' from mapping file - skipping")
            return None
    
    elif country_code == 'GB':
        # UK postal codes: Various formats but should contain letters and numbers
        if any(c.isalpha() for c in postal_code) and any(c.isdigit() for c in postal_code):
            return postal_code
        else:
            print(f"Warning: Invalid UK postal code format '{postal_code}' from mapping file - skipping")
            return None
    
    elif country_code == 'AU':
        # Australian postal codes: 4 digits
        if len(postal_code) == 4 and postal_code.isdigit():
            return postal_code
        else:
            print(f"Warning: Invalid Australian postal code format '{postal_code}' from mapping file - skipping")
            return None
    
    else:
        # For other countries, do basic validation
        # Must contain at least one letter or digit, and reasonable length
        if any(c.isalnum() for c in postal_code):
            return postal_code
        else:
            print(f"Warning: Invalid postal code format '{postal_code}' for {country_code} from mapping file - skipping")
            return None

def process_migration(subscriber_file, mapping_file, vault_provider, is_sandbox=False, provider='stripe', seller_name='', use_mapping_zipcodes=False):
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
                                  parse_dates=['current_period_started_at'],
                                  dtype={'postal_code': object},
                                  keep_default_na=False, na_values=['_'])
        subscriber_filename = subscriber_file.name
    else:
        # File path
        subscribedata = pd.read_csv(subscriber_file,
                                  parse_dates=['current_period_started_at'],
                                  dtype={'postal_code': object},
                                  keep_default_na=False, na_values=['_'])
        subscriber_filename = os.path.basename(subscriber_file)
    
    if hasattr(mapping_file, 'read'):
        # File object from React
        mappingdata = pd.read_csv(mapping_file, encoding='latin-1')
    else:
        # File path
        mappingdata = pd.read_csv(mapping_file, encoding='latin-1')
    
    print("=== MAPPING FILE DEBUG ===")
    print(f"Mapping file columns (raw): {mappingdata.columns.tolist()}")
    print(f"Mapping file shape: {mappingdata.shape}")
    print("=== END MAPPING FILE DEBUG ===")
    
    print(subscribedata)
    
    # Debug: Print available columns in subscriber file
    print("Available columns in subscriber file:")
    print(subscribedata.columns.tolist())
    
    # Check for required columns in subscriber file
    required_subscriber_columns = ['card_token', 'customer_email', 'customer_full_name']
    missing_subscriber_columns = [col for col in required_subscriber_columns if col not in subscribedata.columns]
    
    if missing_subscriber_columns:
        print(f"Warning: Missing required subscriber columns: {missing_subscriber_columns}")
        print("Available columns:", subscribedata.columns.tolist())
    
    # Provider-specific data processing
    if provider.lower() == 'bluesnap':
        # Bluesnap-specific column mapping and card_token creation
        print("Processing Bluesnap data format...")
        
        # Debug: Print available columns
        print("Available columns in mapping file:")
        print(mappingdata.columns.tolist())
        
        # Check for required Bluesnap columns
        required_columns = ['BlueSnap Account Id', 'Credit Card Number', 'First Name', 'Last Name']
        missing_columns = [col for col in required_columns if col not in mappingdata.columns]
        
        if missing_columns:
            print(f"Error: Missing required Bluesnap columns: {missing_columns}")
            print("Available columns:", mappingdata.columns.tolist())
            raise ValueError(f"Missing required Bluesnap columns: {missing_columns}")
        
        # Create card_token from Bluesnap data
        mappingdata['card_token'] = (
            mappingdata['BlueSnap Account Id'].astype(str) +
            mappingdata['Credit Card Number'].astype(str).str[-4:]
        )
        
        # Combine first and last names
        mappingdata['card_holder_name'] = (
            mappingdata['First Name'].str.strip() + " " + mappingdata['Last Name'].str.strip()
        )
        
        # Keep original credit card number
        mappingdata['original_credit_card_number'] = mappingdata['Credit Card Number']
        
        # Rename Bluesnap columns to match expected format
        rename_columns = {}
        if 'Expiration Month' in mappingdata.columns:
            rename_columns['Expiration Month'] = 'card_expiry_month'
        if 'Expiration Year' in mappingdata.columns:
            rename_columns['Expiration Year'] = 'card_expiry_year'
        if 'Network Transaction Id' in mappingdata.columns:
            rename_columns['Network Transaction Id'] = 'network_transaction_id'
        
        if rename_columns:
            mappingdata = mappingdata.rename(columns=rename_columns)
        
        # Select necessary columns for merge
        columns_to_keep = [
            'card_token',
            'original_credit_card_number',
            'card_holder_name'
        ]
        
        # Add optional columns if they exist
        if 'card_expiry_month' in mappingdata.columns:
            columns_to_keep.append('card_expiry_month')
        if 'card_expiry_year' in mappingdata.columns:
            columns_to_keep.append('card_expiry_year')
        if 'network_transaction_id' in mappingdata.columns:
            columns_to_keep.append('network_transaction_id')
        
        # Add postal code columns for mapping feature
        if 'Zip Code' in mappingdata.columns:
            columns_to_keep.append('Zip Code')
        if 'Shipping Zip Code' in mappingdata.columns:
            columns_to_keep.append('Shipping Zip Code')
        if 'Billing Zip Code' in mappingdata.columns:
            columns_to_keep.append('Billing Zip Code')
        
        print(f"Columns to keep: {columns_to_keep}")
        mappingdata = mappingdata[columns_to_keep]
        
        # Ensure card_token columns are strings
        mappingdata['card_token'] = mappingdata['card_token'].astype(str)
        subscribedata['card_token'] = subscribedata['card_token'].astype(str)
        
        print(f"Mapping data shape: {mappingdata.shape}")
        print(f"Subscriber data shape: {subscribedata.shape}")
        print(f"Sample card_tokens from mapping: {mappingdata['card_token'].head().tolist()}")
        print(f"Sample card_tokens from subscriber: {subscribedata['card_token'].head().tolist()}")
        
        # Merge on card_token
        try:
            finaljoin = pd.merge(
                mappingdata,
                subscribedata,
                on='card_token',
                how='outer'
            )
            print(f"Merge successful. Final shape: {finaljoin.shape}")
        except Exception as e:
            print(f"Merge failed with error: {e}")
            raise
        
        # Keep only rows where card_token is not null
        print(f"Rows before filtering null card_tokens: {len(finaljoin)}")
        finaljoin = finaljoin[finaljoin['card_token'].notna()]
        print(f"Rows after filtering null card_tokens: {len(finaljoin)}")
        
        # Replace card_token with original credit card number
        finaljoin['card_token'] = finaljoin['original_credit_card_number']
        print("Replaced card_token with original credit card number")
        
        # Drop the original_credit_card_number column
        finaljoin = finaljoin.drop(columns=['original_credit_card_number'])
        print("Dropped original_credit_card_number column")
        
        completed = finaljoin
        print(f"Completed Bluesnap processing. Final shape: {completed.shape}")
        
    else:
        # Stripe processing (original logic)
        print("Processing Stripe data format...")
        
        subscribedata = subscribedata.rename(columns={'card_token': 'card_id'})
        
        mappingdata = mappingdata.rename(columns={'card.id': 'card_id'})
        mappingdata = mappingdata.rename(columns={'card.transaction_ids': 'network_transaction_id'})
        
        # Merge the two datasets
        finaljoin = pd.merge(mappingdata,
                            subscribedata,
                            left_on='card_id', 
                            right_on='card_id', 
                            how='outer')
        
        finaljoin = finaljoin[finaljoin['card_id'].notna()]
        
        # Rename columns as required
        completed = finaljoin.rename(columns={
            'card.number': 'card_token',
            'card.name': 'card_holder_name',
            'card.exp_month': 'card_expiry_month',
            'card.exp_year': 'card_expiry_year',
        })
        
        completed['card_holder_name'] = completed['card_holder_name'].fillna(completed['customer_full_name'])
    
    # Ensure card_holder_name is filled for both providers
    if 'customer_full_name' in completed.columns:
        completed['card_holder_name'] = completed['card_holder_name'].fillna(completed['customer_full_name'])
    
    # Fill missing postal codes from mapping file FIRST (if toggle enabled)
    if use_mapping_zipcodes and 'address_postal_code' in completed.columns:
        print("Filling missing postal codes from mapping file...")
        print(f"Provider: {provider}")
        print(f"Mapping file columns: {mappingdata.columns.tolist()}")
        
        # Create a mapping of card tokens to postal codes from mapping file
        mapping_postal_codes = {}
        
        if provider.lower() == 'bluesnap':
            # For Bluesnap, use the original credit card number as the key
            if 'original_credit_card_number' in mappingdata.columns:
                print("Processing Bluesnap mapping data...")
                print(f"'Zip Code' column exists: {'Zip Code' in mappingdata.columns}")
                if 'Zip Code' in mappingdata.columns:
                    print(f"Sample 'Zip Code' values: {mappingdata['Zip Code'].head().tolist()}")
                
                for idx, row in mappingdata.iterrows():
                    if pd.notna(row.get('original_credit_card_number')):
                        # Look for postal code in mapping file - only check specific column
                        postal_code = None
                        
                        # Check for the specific Bluesnap column name
                        if 'Zip Code' in mappingdata.columns:
                            raw_value = row['Zip Code']
                            print(f"Row {idx}: Raw 'Zip Code' value: '{raw_value}' (type: {type(raw_value)})")
                            
                            if pd.notna(raw_value) and str(raw_value).strip() != '' and str(raw_value).strip() != '0':
                                postal_code = str(raw_value).strip()
                                print(f"Found postal code '{postal_code}' in column 'Zip Code' for card {row['original_credit_card_number']}")
                            else:
                                print(f"Row {idx}: Skipping empty/invalid postal code: '{raw_value}'")
                        
                        if postal_code:
                            # Validate and fix the postal code from mapping file
                            validated_postal_code = validate_and_fix_zipcode(postal_code, row.get('address_country_code', ''))
                            print(f"Row {idx}: Original: '{postal_code}' -> Validated: '{validated_postal_code}'")
                            if validated_postal_code is not None:
                                mapping_postal_codes[str(row['original_credit_card_number'])] = validated_postal_code
                                print(f"Added mapping: {row['original_credit_card_number']} -> {validated_postal_code}")
        else:
            # For Stripe, use card_id as the key
            if 'card_id' in mappingdata.columns:
                print("Processing Stripe mapping data...")
                print(f"'card.address_zip' column exists: {'card.address_zip' in mappingdata.columns}")
                if 'card.address_zip' in mappingdata.columns:
                    print(f"Sample 'card.address_zip' values: {mappingdata['card.address_zip'].head().tolist()}")
                
                for idx, row in mappingdata.iterrows():
                    if pd.notna(row.get('card_id')):
                        # Look for postal code in mapping file - only check specific column
                        postal_code = None
                        
                        # Check for the specific Stripe column name
                        if 'card.address_zip' in mappingdata.columns:
                            raw_value = row['card.address_zip']
                            print(f"Row {idx}: Raw 'card.address_zip' value: '{raw_value}' (type: {type(raw_value)})")
                            
                            if pd.notna(raw_value) and str(raw_value).strip() != '' and str(raw_value).strip() != '0':
                                postal_code = str(raw_value).strip()
                                print(f"Found postal code '{postal_code}' in column 'card.address_zip' for card {row['card_id']}")
                            else:
                                print(f"Row {idx}: Skipping empty/invalid postal code: '{raw_value}'")
                        
                        if postal_code:
                            # Validate and fix the postal code from mapping file
                            validated_postal_code = validate_and_fix_zipcode(postal_code, row.get('address_country_code', ''))
                            print(f"Row {idx}: Original: '{postal_code}' -> Validated: '{validated_postal_code}'")
                            if validated_postal_code is not None:
                                mapping_postal_codes[str(row['card_id'])] = validated_postal_code
                                print(f"Added mapping: {row['card_id']} -> {validated_postal_code}")
        
        print(f"Created {len(mapping_postal_codes)} postal code mappings")
        print(f"Mapping keys: {list(mapping_postal_codes.keys())[:5]}...")  # Show first 5 keys
        
        # Fill missing postal codes
        filled_count = 0
        missing_count = 0
        supported_countries = ['AU', 'CA', 'FR', 'DE', 'IN', 'IT', 'NL', 'ES', 'GB', 'US']
        
        for idx, row in completed.iterrows():
            if pd.isna(row['address_postal_code']) or row['address_postal_code'] == '':
                # Only fill postal codes for supported countries
                country_code = row.get('address_country_code', '')
                if country_code not in supported_countries:
                    missing_count += 1
                    if missing_count <= 5:  # Only show first 5 missing for debugging
                        print(f"Skipping unsupported country: {country_code}")
                    continue
                
                # Find the corresponding mapping key
                mapping_key = None
                if provider.lower() == 'bluesnap':
                    mapping_key = str(row.get('card_token', ''))
                else:
                    mapping_key = str(row.get('card_id', ''))
                
                if mapping_key in mapping_postal_codes:
                    completed.at[idx, 'address_postal_code'] = mapping_postal_codes[mapping_key]
                    filled_count += 1
                    print(f"Filled postal code for {mapping_key} ({country_code}): {mapping_postal_codes[mapping_key]}")
                else:
                    missing_count += 1
                    if missing_count <= 5:  # Only show first 5 missing for debugging
                        print(f"No mapping found for key: {mapping_key} (country: {country_code})")
        
        print(f"Filled {filled_count} missing postal codes from mapping file")
        print(f"Could not find mapping for {missing_count} records")
        print(f"Supported countries for postal code mapping: {supported_countries}")
    else:
        # When toggle is not enabled, ensure postal codes remain blank/empty
        if 'address_postal_code' in completed.columns:
            print("Missing zip codes toggle is disabled - postal codes will remain as original")
            # Ensure any '0' values in postal codes are converted back to empty strings
            completed['address_postal_code'] = completed['address_postal_code'].replace('0', '')
            # Also handle any other zero-like values
            completed['address_postal_code'] = completed['address_postal_code'].replace(['0', '0.0', 0, 0.0], '')
            print("Ensured postal codes remain blank when toggle is disabled")
    
    print("Starting common processing...")
    
    # Remove unnecessary columns (provider-specific)
    if provider.lower() == 'stripe':
        # For Stripe, keep card.address_zip but remove other unwanted columns
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
            'card.address_state', 
            'card.address_line2', 
            'card.address_line1', 
            'card.address_country', 
            'card.address_city', 
            'name', 
            'description'
            # Note: card.address_zip is KEPT for Stripe output
        ]
    else:
        # For Bluesnap, remove card.address_zip and other unwanted columns
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
            'card.address_zip',  # Remove for Bluesnap
            'card.address_state', 
            'card.address_line2', 
            'card.address_line1', 
            'card.address_country', 
            'card.address_city', 
            'name', 
            'description',
            # Remove postal code columns from mapping file (they were only used for filling missing values)
            'Zip Code',
            'Shipping Zip Code',
            'Billing Zip Code'
        ]
    
    columns_to_remove = [col for col in columns_to_remove if col in completed.columns]
    print(f"Removing columns: {columns_to_remove}")
    completed = completed.drop(columns=columns_to_remove)
    print(f"Shape after removing columns: {completed.shape}")
    
    print("Filtering rows with customer_email...")
    print(f"Rows before email filtering: {len(completed)}")
    completed = completed[completed['customer_email'].notna()]
    print(f"Rows after email filtering: {len(completed)}")
    
    # Sandbox-specific data anonymization
    if is_sandbox:
        # Generate random emails to anonymize data (only emails, keep real names)
        completed['customer_email'] = completed['customer_email'].apply(lambda x: generate_random_email())
        
        # Keep the original customer_full_name from subscriber data (real customer name)
        # No anonymization needed for names - use customer data, not card holder data
    
    print("Processing date formatting...")
    # Reformat 'current_period_started_at' to desired format
    if 'current_period_started_at' in completed.columns:
        completed['current_period_started_at'] = completed['current_period_started_at'].dt.strftime('%Y-%m-%dT%H:%M:%SZ')
        print("Date formatting completed")
    else:
        print("Warning: current_period_started_at column not found")
    
    print("Adding vault_provider column...")
    # Add vault_provider column
    completed['vault_provider'] = vault_provider
    
    print("Processing enable_checkout column...")
    # Check if 'enable_checkout' exists in the dataframe and convert to upper case if it does
    if 'enable_checkout' in completed.columns:
        completed['enable_checkout'] = completed['enable_checkout'].apply(lambda x: str(x).upper() if pd.notnull(x) else x)
        print("enable_checkout processing completed")
    else:
        print("enable_checkout column not found")
    
    print("Starting duplicate detection...")
    
    # Duplicate detection (same for both environments)
    success = completed[completed['card_token'].notna()]
    print(f"Success records: {len(success)}")
    
    no_tokens = completed[completed['card_token'].isnull()]
    print(f"No tokens records: {len(no_tokens)}")
    
    # Find all rows where card_token (encrypted card number) appears more than once
    duplicate_tokens = completed[completed['card_token'].notna() & completed.duplicated(subset='card_token', keep=False)]
    print(f"Duplicate tokens: {len(duplicate_tokens)}")
    
    # Find all rows where card_id (pm_*) appears more than once
    if 'card_id' in completed.columns:
        duplicate_card_ids = completed[completed['card_id'].notna() & completed.duplicated(subset='card_id', keep=False)]
        print(f"Duplicate card IDs: {len(duplicate_card_ids)}")
    else:
        duplicate_card_ids = pd.DataFrame()
        print("card_id column not found, skipping duplicate card ID detection")
    
    # Find all rows where external_subscription_id appears more than once
    if 'subscription_external_id' in completed.columns:
        duplicate_external_subscription_ids = completed[completed.duplicated(subset='subscription_external_id', keep=False)]
        print(f"Duplicate subscription IDs: {len(duplicate_external_subscription_ids)}")
    else:
        duplicate_external_subscription_ids = pd.DataFrame()
        print("subscription_external_id column not found, skipping duplicate subscription ID detection")
    
    # Find all rows where emails appears more than once
    duplicate_emails = completed[completed.duplicated(subset='customer_email', keep=False)]
    print(f"Duplicate emails: {len(duplicate_emails)}")
    
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
        (duplicate_card_ids, f'{base_filename}_duplicate_card_ids.csv'),
        (duplicate_external_subscription_ids, f'{base_filename}_duplicate_external_subscription_ids.csv'),
        (duplicate_emails, f'{base_filename}_duplicate_emails.csv')
    ]
    
    for df, filename in files_to_save:
        if not df.empty:
            file_path = os.path.join(output_dir, filename)
            print(f"Saving file: {file_path}")
            
            # Ensure zip codes are formatted as text to preserve leading zeros
            df_to_save = df.copy()
            if 'address_postal_code' in df_to_save.columns:
                # Convert postal codes to strings to ensure proper CSV formatting
                df_to_save['address_postal_code'] = df_to_save['address_postal_code'].astype(str)
                # Convert empty strings back to empty (not "nan")
                df_to_save['address_postal_code'] = df_to_save['address_postal_code'].replace(['nan', 'None'], '')
            
            # Save with proper formatting - use quoting=1 to force quotes around all fields
            # This ensures Excel treats all fields as text, preserving leading zeros
            df_to_save.to_csv(file_path, index=False, quoting=1)
            
            file_size = os.path.getsize(file_path)
            print(f"File saved successfully. Size: {file_size} bytes")
            output_files.append({
                'name': filename,
                'size': file_size,
                'url': f'file://{os.path.abspath(file_path)}'
            })
        else:
            print(f"Skipping empty dataframe for: {filename}")
    
    processing_time = time.time() - start_time
    
    # Prepare results
    results = {
        'success_count': len(success),
        'no_tokens_count': len(no_tokens),
        'duplicate_tokens_count': len(duplicate_tokens),
        'duplicate_card_ids_count': len(duplicate_card_ids),
        'duplicate_external_subscription_ids_count': len(duplicate_external_subscription_ids),
        'duplicate_emails_count': len(duplicate_emails),
        'total_processed': len(completed),
        'processing_time': f"{processing_time:.2f} seconds",
        'output_files': output_files,
        'environment': 'Sandbox' if is_sandbox else 'Production'
    }
    
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