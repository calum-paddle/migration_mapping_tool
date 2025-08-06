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
    
    # Provider-specific data processing
    if provider.lower() == 'bluesnap':
        # Bluesnap-specific column mapping and card_token creation
        print("Processing Bluesnap data format...")
        
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
        mappingdata = mappingdata.rename(columns={
            'Expiration Month': 'card_expiry_month',
            'Expiration Year': 'card_expiry_year',
            'Network Transaction Id': 'network_transaction_id'
        })
        
        # Select necessary columns for merge
        columns_to_keep = [
            'card_token',
            'original_credit_card_number',
            'card_holder_name',
            'card_expiry_month',
            'card_expiry_year',
            'network_transaction_id'
        ]
        mappingdata = mappingdata[columns_to_keep]
        
        # Ensure card_token columns are strings
        mappingdata['card_token'] = mappingdata['card_token'].astype(str)
        subscribedata['card_token'] = subscribedata['card_token'].astype(str)
        
        # Merge on card_token
        finaljoin = pd.merge(
            mappingdata,
            subscribedata,
            on='card_token',
            how='outer'
        )
        
        # Keep only rows where card_token is not null
        finaljoin = finaljoin[finaljoin['card_token'].notna()]
        
        # Replace card_token with original credit card number
        finaljoin['card_token'] = finaljoin['original_credit_card_number']
        
        # Drop the original_credit_card_number column
        finaljoin = finaljoin.drop(columns=['original_credit_card_number'])
        
        completed = finaljoin
        
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
    
    # Remove unnecessary columns (same for both environments)
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
        'description'
    ]
    
    columns_to_remove = [col for col in columns_to_remove if col in completed.columns]
    completed = completed.drop(columns=columns_to_remove)
    
    completed = completed[completed['customer_email'].notna()]
    
    # Sandbox-specific data anonymization
    if is_sandbox:
        # Generate random emails to anonymize data
        completed['customer_email'] = completed['customer_email'].apply(lambda x: generate_random_email())
        
        # Anonymize customer name using customer external id
        completed['customer_full_name'] = completed['customer_external_id']
    
    # Reformat 'current_period_started_at' to desired format
    completed['current_period_started_at'] = completed['current_period_started_at'].dt.strftime('%Y-%m-%dT%H:%M:%SZ')
    
    # Add vault_provider column
    completed['vault_provider'] = vault_provider
    
    # Check if 'enable_checkout' exists in the dataframe and convert to upper case if it does
    if 'enable_checkout' in completed.columns:
        completed['enable_checkout'] = completed['enable_checkout'].apply(lambda x: str(x).upper() if pd.notnull(x) else x)
    
    # Duplicate detection (same for both environments)
    success = completed[completed['card_token'].notna()]
    no_tokens = completed[completed['card_token'].isnull()]
    
    # Find all rows where card_token (encrypted card number) appears more than once
    duplicate_tokens = completed[completed['card_token'].notna() & completed.duplicated(subset='card_token', keep=False)]
    
    # Find all rows where card_id (pm_*) appears more than once
    duplicate_card_ids = completed[completed['card_id'].notna() & completed.duplicated(subset='card_id', keep=False)]
    
    # Find all rows where external_subscription_id appears more than once
    duplicate_external_subscription_ids = completed[completed.duplicated(subset='subscription_external_id', keep=False)]
    
    # Find all rows where emails appears more than once
    duplicate_emails = completed[completed.duplicated(subset='customer_email', keep=False)]
    
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
            df.to_csv(file_path, index=False, float_format='%.0f')
            file_size = os.path.getsize(file_path)
            output_files.append({
                'name': filename,
                'size': file_size,
                'url': f'file://{os.path.abspath(file_path)}'
            })
    
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