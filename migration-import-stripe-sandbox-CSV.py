from datetime import datetime, timedelta
import pandas as pd
import random
import string

welcome = '''
Paddle Billing Sandbox Script

This script assumes you have a test mapping file to map against. 

You will need to update the references in your 'card_token' column to match 'card_1J0yEyH65PkfON7EQ0Owsy3Q' in the mapping file, 
and the card token that works in sandbox for tokenex is: 42424205H9gc4242

PLEASE ENSURE ALL COLUMNS HEADERS HAVE NO HIDDEN WHITE SPACES

'''

print(welcome)

# input vault provider name
vault_provider = input("Enter the name of your vault provider: ")

# grab subscriber data file
subscribeinput = str(input("Enter the name of your Subscriber export file: "))
if not ".csv" in subscribeinput:
  subscribeinput += ".csv"

subscribedata = pd.read_csv(subscribeinput,
                   parse_dates=['current_period_started_at'],
                   dtype={'postal_code':object},
                   keep_default_na=False, na_values=['_'] # keep country code NA for Namibia
)

subscribedata = subscribedata.rename(columns={'card_token': 'card_id'})

print(subscribedata)


# grab mapping file
mappinginput = str(input("Enter the name of your TokenEx mapping file: "))
if not ".csv" in mappinginput:
  mappinginput += ".csv"

mappingdata = pd.read_csv(mappinginput, encoding='latin-1')

mappingdata = mappingdata.rename(columns={'card.id': 'card_id'})
mappingdata = mappingdata.rename(columns={'card.transaction_ids': 'network_transaction_id'})

# merge the two datasets
finaljoin = pd.merge(mappingdata,
                    subscribedata,
                    left_on='card_id', 
                    right_on='card_id', 
                    how='outer')

# merge the two datasets
finaljoin = pd.merge(mappingdata,
                    subscribedata,
                    left_on='card_id', 
                    right_on='card_id', 
                    how='outer')

finaljoin = finaljoin[finaljoin['card_id'].notna()]

# rename columns as required
completed = finaljoin.rename(columns={
    'card.number': 'card_token',
    'card.name': 'card_holder_name',
    'card.exp_month': 'card_expiry_month',
    'card.exp_year': 'card_expiry_year',
})

completed['card_holder_name'] = completed['card_holder_name'].fillna(completed['customer_full_name'])

# remove unnecessary columns
columns_to_remove = [
    'card_address_line1',
    'card_address_line2',
    'card_address_city',
    'card_address_state',
    'card_address_zip',
    'card_address_country',
    'default_source',
    'email',
    'id'
]

columns_to_remove = [col for col in columns_to_remove if col in completed.columns]

completed = completed.drop(columns=columns_to_remove)

completed = completed[completed['customer_email'].notna()]

# generate random emails to anonymize data
def generate_random_email():
    random_string = ''.join(random.choices(string.ascii_lowercase + string.digits, k=5))
    return f"blackhole+{random_string}@paddle.com"

completed['customer_email'] = completed['customer_email'].apply(lambda x: generate_random_email())

# anonymize customer name using customer external id (not strictly necessary)
completed['customer_full_name'] = completed['customer_external_id']

# Add vault_provider column
completed['vault_provider'] = vault_provider

# Check if 'enable_checkout' exists in the dataframe and convert to upper case if it does
if 'enable_checkout' in completed.columns:
    completed['enable_checkout'] = completed['enable_checkout'].apply(lambda x: str(x).upper() if pd.notnull(x) else x)

success = completed[completed['card_token'].notna()]
no_tokens = completed[completed['card_token'].isnull()]

success.to_csv('final_import.csv', index=False, float_format='%.0f')
no_tokens.to_csv('no_token_found.csv', index=False, float_format='%.0f')

print('Success')
