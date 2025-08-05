from datetime import datetime, timedelta
import pandas as pd

welcome = '''
Paddle Billing PRODUCTION Script (Updated for New Mapping Format)

This script assumes you have a new CSV mapping file from TokenEx to map against. 

PLEASE ENSURE ALL COLUMN HEADERS HAVE NO HIDDEN WHITE SPACES

'''

print(welcome)

# Input vault provider name
vault_provider = input("Enter the name of your vault provider: ")

# Load subscriber data file
subscribeinput = str(input("Enter the name of your Subscriber export file: "))
if not ".csv" in subscribeinput:
    subscribeinput += ".csv"

subscribedata = pd.read_csv(subscribeinput,
                            dtype={'postal_code': object},
                            keep_default_na=False, na_values=['_'])

print(subscribedata)

# Load mapping file
mappinginput = str(input("Enter the name of your TokenEx mapping file: "))
if not ".csv" in mappinginput:
    mappinginput += ".csv"

mappingdata = pd.read_csv(mappinginput, encoding='latin-1')

# Create `card_token` in mapping file
mappingdata['card_token'] = (
    mappingdata['BlueSnap Account Id'].astype(str) +
    mappingdata['Credit Card Number'].astype(str).str[-4:]
)

# Map columns to match the required format
mappingdata['card_holder_name'] = (
    mappingdata['First Name'].str.strip() + " " + mappingdata['Last Name'].str.strip()
)

# Keep both the original 'Credit Card Number' and the renamed 'card_token'
mappingdata['original_credit_card_number'] = mappingdata['Credit Card Number']

# Rename columns to match the expected output format, excluding 'Credit Card Number'
mappingdata = mappingdata.rename(columns={
    'Credit_Card_Number': 'card_token',  # This is the renamed version for the merge
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

# Add vault_provider column
finaljoin['vault_provider'] = vault_provider

# Convert `enable_checkout` to uppercase if it exists
if 'enable_checkout' in finaljoin.columns:
    finaljoin['enable_checkout'] = finaljoin['enable_checkout'].apply(
        lambda x: str(x).upper() if pd.notnull(x) else x
    )

# Split data into success and no_tokens
success = finaljoin[finaljoin['card_token'].notna()]
no_tokens = finaljoin[finaljoin['card_token'].isnull()]

# Write to CSV
success.to_csv('final_import.csv', index=False, float_format='%.0f')
no_tokens.to_csv('no_token_found.csv', index=False, float_format='%.0f')

print('Success')
