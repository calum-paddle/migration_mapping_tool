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

def process_migration(subscriber_file, mapping_file, vault_provider, is_sandbox=False, provider='stripe', seller_name=''):
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
    
    print(subscribedata)
    
    # Check for required columns in subscriber file
    required_subscriber_columns = ['card_token', 'customer_email', 'customer_full_name']
    missing_subscriber_columns = [col for col in required_subscriber_columns if col not in subscribedata.columns]
    
    if missing_subscriber_columns:
        print(f"Warning: Missing required subscriber columns: {missing_subscriber_columns}")
        print("Available columns:", subscribedata.columns.tolist())
    
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
        
        # Replace `card_token` in the final DataFrame with the original `Credit Card Number` from the mapping data
        finaljoin['card_token'] = finaljoin['original_credit_card_number']
        
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
        
        # Rename columns as required (like original)
        completed = finaljoin.rename(columns={
            'card.number': 'card_token',
            'card.name': 'card_holder_name',
            'card.exp_month': 'card_expiry_month',
            'card.exp_year': 'card_expiry_year',
        })
        
        completed['card_holder_name'] = completed['card_holder_name'].fillna(completed['customer_full_name'])
    
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
        completed = completed[existing_columns]
        
    else:  # Bluesnap
        # Add any missing columns that should be in Bluesnap output
        for col in bluesnap_column_order:
            if col not in completed.columns:
                completed[col] = None
        
        # Reorder columns to match Bluesnap specification
        existing_columns = [col for col in bluesnap_column_order if col in completed.columns]
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
        (no_tokens, f'{base_filename}_no_token_found.csv')
    ]
    
    for df, filename in files_to_save:
        if not df.empty:
            file_path = os.path.join(output_dir, filename)
            print(f"Saving file: {file_path}")
            
            # Save with proper formatting
            df.to_csv(file_path, index=False, float_format='%.0f')
            
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