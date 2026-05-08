from datetime import datetime, timedelta
import pandas as pd
import random
import string
import os
import time
import zipfile
import re
import unicodedata


def _normalized_country_codes(series):
    """
    Normalize address_country_code for comparisons: strip, uppercase.
    Empty / nan-like string tokens become NA so they do not match ISO lists.
    """
    if series is None:
        return series
    s = series.astype(str).str.strip().str.upper()
    s = s.mask(s.isin(['', 'NAN', 'NONE', 'NAT']))
    return s


def _apply_us_zip_leading_zero_autocorrect(df):
    """
    In-place: for rows whose normalized country is US and postal normalizes to exactly 4 digits,
    left-pad address_postal_code to 5 digits. Returns number of cells updated.
    """
    if df is None or getattr(df, 'empty', True):
        return 0
    if 'address_country_code' not in df.columns or 'address_postal_code' not in df.columns:
        return 0
    us_mask = _normalized_country_codes(df['address_country_code']) == 'US'
    if not us_mask.any():
        return 0

    def normalize_zip_for_autocorrect(zip_val):
        if pd.isna(zip_val):
            return ''
        if isinstance(zip_val, float) and zip_val.is_integer():
            return str(int(zip_val))
        return str(zip_val).strip()

    us_records_subset = df.loc[us_mask, 'address_postal_code'].copy()
    normalized_zips = us_records_subset.apply(normalize_zip_for_autocorrect)
    four_digit_mask = normalized_zips.str.match(r'^\d{4}$')
    autocorrected_count = int(four_digit_mask.sum())
    if autocorrected_count > 0:
        indices_to_correct = us_records_subset[four_digit_mask].index
        df.loc[indices_to_correct, 'address_postal_code'] = (
            normalized_zips.loc[indices_to_correct].str.zfill(5)
        )
    return autocorrected_count


def clean_dataframe_for_csv(df):
    """
    Helper function to clean DataFrame columns for CSV export.
    Converts all columns to strings, handles NaN values, and removes .0 suffixes.
    
    Args:
        df: DataFrame to clean
        
    Returns:
        DataFrame with cleaned string columns
    """
    df_cleaned = df.copy()
    for col in df_cleaned.columns:
        # Handle NaN values and ensure all data is string
        df_cleaned[col] = df_cleaned[col].fillna('').astype(str).replace('nan', '')
        # Remove decimal points from numeric strings (e.g., '8830.0' -> '8830')
        df_cleaned[col] = df_cleaned[col].str.replace(r'\.0$', '', regex=True)
    return df_cleaned

# Set per process_migration run: mapping-origin column names as they appear in `completed` (after renames).
_validation_report_exclude_columns = frozenset()

# Subscriber column names from the subscription CSV only (_temp_row_id is internal and excluded). Used to filter error-report CSVs.
_subscriber_report_columns = frozenset()
_subscriber_report_column_order = []


def compute_columns_originating_from_mapping_only(
    mapping_columns,
    subscriber_columns,
    merge_keys,
    merged_column_names,
):
    """
    Names of merged columns that come only from the left (mapping) dataframe, plus
    left halves of duplicate non-key columns (pandas suffix _x).

    merge_keys: columns used for the merge (appear once in the merged frame, not duplicated).
    """
    mk = set(merge_keys)
    mc = set(mapping_columns)
    sc = set(subscriber_columns)
    merged = set(merged_column_names)
    names = set()
    for c in mc - sc - mk:
        if c in merged:
            names.add(c)
    for c in (mc & sc) - mk:
        suffixed = f'{c}_x'
        if suffixed in merged:
            names.add(suffixed)
    return names


def map_column_names_through_rename(names, rename_dict):
    """Apply the same renames used on the merged dataframe (Stripe completed step)."""
    out = set()
    for c in names:
        out.add(rename_dict.get(c, c))
    return out


def strip_mapping_metadata_from_validation_report_df(df):
    """
    Remove columns that originated from the mapping file merge so validation error CSVs
    only show subscriber-side fields (merge keys and subscriber-only columns remain).

    Uses _validation_report_exclude_columns populated after merge; empty before merge (subscriber-only phases).
    Always drops pandas merge indicator `_merge` if present.
    """
    if df is None or getattr(df, 'empty', True):
        return df
    exclude = set(_validation_report_exclude_columns) | {'_merge'}
    drop = []
    for col in df.columns:
        if col in exclude or str(col) in exclude:
            drop.append(col)
    if not drop:
        return df
    return df.drop(columns=drop, errors='ignore')


def restrict_validation_report_to_subscriber_columns(df):
    """
    Keep only columns that existed on the subscription CSV (plus card_token as the join field).
    Drops Stripe template padding (description, name, card.address_*, …), vault_provider, and other
    columns introduced after merge.
    """
    if df is None or getattr(df, 'empty', True):
        return df
    if not _subscriber_report_columns:
        return df
    allowed = set(_subscriber_report_columns) | {'card_token'}
    keep = [c for c in df.columns if c in allowed]
    if len(keep) == len(df.columns):
        return df
    if not keep:
        return df
    order = [c for c in _subscriber_report_column_order if c in keep]
    seen = set(order)
    rest = [c for c in keep if c not in seen]
    return df[order + rest].copy()


def normalize_validation_report_columns(df):
    """
    After mapping-only columns are stripped, align names with the subscriber export:

    - Stripe merge uses `card_id` internally; subscriber CSV calls the join field `card_token`.
      After strip, mapping's `card.number` (renamed to `card_token`) is removed — rename `card_id`
      to `card_token` when no `card_token` column remains.
    - Merge duplicates from overlapping column names use `_y` / `_x` suffixes; keep subscriber
      side (`_y`) under the original header and drop stray `_x` columns.
    - Drop internal tracking columns not present in the subscriber template.
    """
    if df is None or getattr(df, 'empty', True):
        return df
    out = df.copy()
    # Subscriber-side duplicate merge columns: foo_y -> foo when foo is not already present
    renames_y = {}
    for col in list(out.columns):
        if isinstance(col, str) and col.endswith('_y'):
            base = col[:-2]
            if base and base not in out.columns:
                renames_y[col] = base
    if renames_y:
        out = out.rename(columns=renames_y)
    drop_x = [c for c in out.columns if isinstance(c, str) and c.endswith('_x')]
    if drop_x:
        out = out.drop(columns=drop_x, errors='ignore')
    # Stripe: join key stored as card_id after rename from subscriber card_token
    if 'card_id' in out.columns and 'card_token' not in out.columns:
        out = out.rename(columns={'card_id': 'card_token'})
    if 'is_duplicate_token' in out.columns:
        out = out.drop(columns=['is_duplicate_token'], errors='ignore')
    return out


def extract_failed_temp_row_ids_from_df(df):
    """Collect numeric _temp_row_id values before export cleaning drops that column."""
    if df is None or getattr(df, 'empty', True) or '_temp_row_id' not in df.columns:
        return []
    temp_ids = df['_temp_row_id'].replace('', pd.NA).dropna()
    failed_ids = []
    for x in temp_ids:
        try:
            if str(x).strip() == '':
                continue
            failed_ids.append(int(float(x)))
        except (TypeError, ValueError):
            continue
    return failed_ids


def merge_failed_temp_row_ids_into_set(failed_row_ids_set, validation_dict):
    """Prefer failed_temp_row_ids from validators; else parse incorrect_records if _temp_row_id still present."""
    if not validation_dict:
        return
    ft = validation_dict.get('failed_temp_row_ids')
    if ft is not None:
        failed_row_ids_set.update(ft)
        return
    rec = validation_dict.get('incorrect_records')
    if rec is not None and '_temp_row_id' in rec.columns:
        failed_row_ids_set.update(extract_failed_temp_row_ids_from_df(rec))


def clean_dataframe_for_validation_report_csv(df):
    """Use for validation / warning download CSVs; omits mapping-only columns, then string-cleans."""
    df = strip_mapping_metadata_from_validation_report_df(df)
    df = normalize_validation_report_columns(df)
    df = restrict_validation_report_to_subscriber_columns(df)
    df = df.drop(columns=['_temp_row_id'], errors='ignore')
    return clean_dataframe_for_csv(df)

def generate_random_email():
    """Generate a random email for sandbox data anonymization"""
    random_string = ''.join(random.choices(string.ascii_lowercase + string.digits, k=5))
    return f"blackhole+{random_string}@paddle.com"

_ISO_TIMESTAMP_FRACTIONAL_Z = re.compile(
    r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+Z$'
)
_SUBSCRIBER_DATE_COLS_STRIP_FRACTIONAL_Z = (
    'started_at',
    'paused_at',
    'current_period_started_at',
    'current_period_ends_at',
)


def strip_iso_fractional_seconds_z_suffixes(subscriber_df, enabled):
    """
    When enabled, rewrite values like ...T..:..:..<digits>Z to ...T..:..:..Z
    only when the full string matches ISO-style UTC with fractional seconds (any length).
    Values without a fractional part are unchanged. Skips empty cells.
    Mutates subscriber_df in place.
    """
    if not enabled:
        return

    def fix_cell(value):
        if pd.isna(value):
            return value
        s = str(value).strip()
        if s.lower() in ('nan', 'none', 'nat', ''):
            return value
        if _ISO_TIMESTAMP_FRACTIONAL_Z.match(s):
            return re.sub(r'\.\d+Z$', 'Z', s, count=1)
        return value

    for col in _SUBSCRIBER_DATE_COLS_STRIP_FRACTIONAL_Z:
        if col not in subscriber_df.columns:
            continue
        subscriber_df[col] = subscriber_df[col].apply(fix_cell)

_SUB_CUSTOM_KEY_RE = re.compile(r'^subscription_custom_data_key_(\d+)$')
_SUB_CUSTOM_VALUE_RE = re.compile(r'^subscription_custom_data_value_(\d+)$')

# Single source of truth for Paddle subscription CSV required headers (canonical snake_case).
SUBSCRIBER_REQUIRED_COLUMNS = (
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
    'price_id_1',
    'quantity_1',
)

_SUBSCRIBER_OPTIONAL_HEADER_RULES = (
    (re.compile(r'^subscription_custom_data_key_(\d+)$', re.I),
     lambda m: f'subscription_custom_data_key_{int(m.group(1))}'),
    (re.compile(r'^subscription_custom_data_value_(\d+)$', re.I),
     lambda m: f'subscription_custom_data_value_{int(m.group(1))}'),
    (re.compile(r'^price_id_(\d+)$', re.I),
     lambda m: f'price_id_{int(m.group(1))}'),
    (re.compile(r'^quantity_(\d+)$', re.I),
     lambda m: f'quantity_{int(m.group(1))}'),
)

# Mapping CSV headers referenced by merge / rename / validation — normalized with trim + case-insensitive match.
_STRIPE_MAPPING_HEADER_CANONICAL = (
    'card.id',
    'card.transaction_ids',
    'card.number',
    'card.name',
    'card.exp_month',
    'card.exp_year',
    'card.address_zip',
    'card.address_city',
    'card.address_country',
    'card.address_line1',
    'card.address_line2',
    'card.address_state',
    'description',
    'name',
    'default_source',
    'email',
    'id',
)

_BLUESNAP_MAPPING_HEADER_CANONICAL = (
    'BlueSnap Account Id',
    'Credit Card Number',
    'First Name',
    'Last Name',
    'Expiration Month',
    'Expiration Year',
    'Network Transaction Id',
    'Zip Code',
)


def _canonicalize_subscriber_headers(subscriber_df):
    """
    Rename subscription CSV headers to canonical snake_case when they differ only by case,
    including numbered optional columns (price_id_N, quantity_N, custom data pairs).
    """
    if subscriber_df is None or getattr(subscriber_df, 'empty', True):
        return subscriber_df
    fixed_lower = {name.lower(): name for name in SUBSCRIBER_REQUIRED_COLUMNS}
    renames = {}
    for col in list(subscriber_df.columns):
        if col == '_temp_row_id':
            continue
        s = str(col).strip()
        sl = s.lower()
        if sl in fixed_lower:
            canon = fixed_lower[sl]
            if s != canon:
                renames[col] = canon
            continue
        matched = False
        for rx, fmt in _SUBSCRIBER_OPTIONAL_HEADER_RULES:
            m = rx.match(s)
            if m:
                canon = fmt(m)
                if s != canon:
                    renames[col] = canon
                matched = True
                break
        if matched:
            continue
    if not renames:
        return subscriber_df
    final_names = []
    seen_final = set()
    collisions = []
    for col in subscriber_df.columns:
        new_name = renames.get(col, col)
        if new_name in seen_final:
            collisions.append(new_name)
        seen_final.add(new_name)
        final_names.append(new_name)
    if collisions:
        raise ValueError(
            'Ambiguous subscriber headers after case normalization (duplicate logical columns): '
            + ', '.join(sorted(set(str(x) for x in collisions)))
        )
    return subscriber_df.rename(columns=renames)


def ordered_subscription_custom_data_columns(columns):
    """
    Return subscription_custom_data_key_N / value_N columns in ascending N,
    each pair as key then value (only columns that exist).
    """
    col_set = {str(c) for c in columns}
    indices = set()
    for c in col_set:
        m = _SUB_CUSTOM_KEY_RE.match(c)
        if m:
            indices.add(int(m.group(1)))
        m = _SUB_CUSTOM_VALUE_RE.match(c)
        if m:
            indices.add(int(m.group(1)))
    ordered = []
    for i in sorted(indices):
        k = f'subscription_custom_data_key_{i}'
        v = f'subscription_custom_data_value_{i}'
        if k in col_set:
            ordered.append(k)
        if v in col_set:
            ordered.append(v)
    return ordered

def validate_subscriber_columns(columns):
    """
    Validate that the subscriber file has all required columns
    
    Args:
        columns: List of column names from the subscriber file
    
    Returns:
        dict: Validation results with status and missing columns
    """
    required_columns = list(SUBSCRIBER_REQUIRED_COLUMNS)
    
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
    
    optional_columns = []
    for pattern in optional_patterns:
        for col in columns:
            if re.match(pattern, col, re.IGNORECASE) and col not in required_columns:
                optional_columns.append(col)
    
    return {
        'valid': len(missing_columns) == 0,
        'missing_columns': missing_columns,
        'optional_columns': optional_columns,
        'total_columns': len(columns),
        'required_columns_count': len(required_columns)
    }

def validate_unsupported_countries(subscriber_data, seller_name='', is_sandbox=False):
    """
    Validate that address_country_code does not contain unsupported countries.
    
    Args:
        subscriber_data: DataFrame containing subscriber data
        seller_name: Name of the seller for file naming
        is_sandbox: Boolean indicating if this is sandbox mode
    
    Returns:
        dict: Validation results with status and incorrect records
    """
    try:
        # Dictionary of unsupported country codes with their flag emojis
        unsupported_countries_dict = {
            'AF': '🇦🇫', 'AQ': '🇦🇶', 'BY': '🇧🇾', 'MM': '🇲🇲', 'CF': '🇨🇫', 'CU': '🇨🇺', 
            'CD': '🇨🇩', 'HT': '🇭🇹', 'IR': '🇮🇷', 'IQ': '🇮🇶', 'LY': '🇱🇾', 'ML': '🇲🇱', 'AN': '🇦🇳', 
            'NI': '🇳🇮', 'KP': '🇰🇵', 'RU': '🇷🇺', 'SO': '🇸🇴', 'SS': '🇸🇸', 'SD': '🇸🇩', 
            'SY': '🇸🇾', 'VE': '🇻🇪', 'YE': '🇾🇪', 'ZW': '🇿🇼'
        }
        # List of unsupported country codes (for validation logic)
        unsupported_countries = list(unsupported_countries_dict.keys())
        
        # Create a copy to avoid modifying original
        validation_data = subscriber_data.copy()
        
        # Ensure _temp_row_id exists
        if '_temp_row_id' not in validation_data.columns:
            validation_data['_temp_row_id'] = range(len(validation_data))
        
        # Check if address_country_code column exists
        if 'address_country_code' not in validation_data.columns:
            return {
                'valid': True,  # If column doesn't exist, consider it valid (will be caught by column validation)
                'incorrect_count': 0,
                'total_records': len(validation_data),
                'incorrect_records': None
            }
        
        # Find records with unsupported country codes (case-insensitive ISO codes)
        unsupported_mask = _normalized_country_codes(validation_data['address_country_code']).isin(
            unsupported_countries
        )
        incorrect_records = validation_data[unsupported_mask].copy()
        failed_temp_row_ids = extract_failed_temp_row_ids_from_df(incorrect_records)
        
        # Convert all columns to strings to prevent float conversion in CSV
        if not incorrect_records.empty:
            incorrect_records = clean_dataframe_for_validation_report_csv(incorrect_records)
        
        incorrect_count = len(incorrect_records)
        total_records = len(validation_data)
        
        if incorrect_count > 0:
            return {
                'valid': False,
                'incorrect_count': incorrect_count,
                'total_records': total_records,
                'incorrect_records': incorrect_records,
                'failed_temp_row_ids': failed_temp_row_ids,
                'unsupported_countries': unsupported_countries,
                'unsupported_countries_dict': unsupported_countries_dict
            }
        else:
            return {
                'valid': True,
                'incorrect_count': 0,
                'total_records': total_records,
                'incorrect_records': None,
                'unsupported_countries': unsupported_countries,
                'unsupported_countries_dict': unsupported_countries_dict
            }
    except Exception as e:
            # Fallback dictionary if error occurs
            fallback_dict = {
                'AF': '🇦🇫', 'AQ': '🇦🇶', 'BY': '🇧🇾', 'MM': '🇲🇲', 'CF': '🇨🇫', 'CU': '🇨🇺', 
                'CD': '🇨🇩', 'HT': '🇭🇹', 'IR': '🇮🇷', 'IQ': '🇮🇶', 'LY': '🇱🇾', 'ML': '🇲🇱', 'AN': '🇦🇳', 
                'NI': '🇳🇮', 'KP': '🇰🇵', 'RU': '🇷🇺', 'SO': '🇸🇴', 'SS': '🇸🇸', 'SD': '🇸🇩', 
                'SY': '🇸🇾', 'VE': '🇻🇪', 'YE': '🇾🇪', 'ZW': '🇿🇼'
            }
            return {
                'valid': False,
                'incorrect_count': 0,
                'total_records': 0,
                'incorrect_records': None,
                'error': f'Validation error: {str(e)}',
                'unsupported_countries': list(fallback_dict.keys()),
                'unsupported_countries_dict': fallback_dict
            }

def validate_address_country_code_format(subscriber_data, seller_name='', is_sandbox=False):
    """
    Validate that address_country_code is present on every row and is exactly
    two alphabetic characters (ISO 3166-1 alpha-2 style; letters only, any case).

    If the column is missing, returns valid (column validation reports missing headers).
    """
    try:
        validation_data = subscriber_data.copy()
        if '_temp_row_id' not in validation_data.columns:
            validation_data['_temp_row_id'] = range(len(validation_data))

        if 'address_country_code' not in validation_data.columns:
            return {
                'valid': True,
                'incorrect_count': 0,
                'total_records': len(validation_data),
                'incorrect_records': None
            }

        def is_valid_alpha2(value):
            if pd.isna(value):
                return False
            s = str(value).strip()
            if s.lower() in ('nan', 'none', 'nat', ''):
                return False
            if len(s) != 2:
                return False
            return s.isalpha()

        col = validation_data['address_country_code']
        valid_mask = col.apply(is_valid_alpha2)
        incorrect_records = validation_data[~valid_mask].copy()
        failed_temp_row_ids = extract_failed_temp_row_ids_from_df(incorrect_records)

        if not incorrect_records.empty:
            incorrect_records = clean_dataframe_for_validation_report_csv(incorrect_records)

        return {
            'valid': len(incorrect_records) == 0,
            'incorrect_count': len(incorrect_records),
            'incorrect_records': incorrect_records,
            'failed_temp_row_ids': failed_temp_row_ids,
            'total_records': len(validation_data)
        }
    except Exception as e:
        print(f"Error in address country code validation: {e}")
        import traceback
        traceback.print_exc()
        return {
            'valid': False,
            'error': f'Validation error: {str(e)}',
            'incorrect_count': 0,
            'total_records': 0,
            'incorrect_records': None
        }

_PRICE_ID_COLUMN_RE = re.compile(r'^price_id_\d+$')
_PRICE_ID_LINE_INDEX_RE = re.compile(r'^price_id_(\d+)$')
_QUANTITY_LINE_INDEX_RE = re.compile(r'^quantity_(\d+)$')


def ordered_price_id_quantity_columns(columns):
    """
    Return price_id_N then quantity_N for each index N present in columns (sorted by N).
    """
    col_set = {str(c) for c in columns}
    indices = set()
    for c in col_set:
        m = _PRICE_ID_LINE_INDEX_RE.match(c)
        if m:
            indices.add(int(m.group(1)))
        m = _QUANTITY_LINE_INDEX_RE.match(c)
        if m:
            indices.add(int(m.group(1)))
    ordered = []
    for i in sorted(indices):
        p = f'price_id_{i}'
        q = f'quantity_{i}'
        if p in col_set:
            ordered.append(p)
        if q in col_set:
            ordered.append(q)
    return ordered


def _normalize_subscription_quantity_cell(value):
    """
    If the cell is a whole non-negative number (e.g. float 2.0 or string '2.0'), return digit string '2'.
    Otherwise return the value unchanged (trimmed string for plain strings) so validation can reject.
    Empty / NaN left unchanged.
    """
    if pd.isna(value):
        return value
    s = str(value).strip()
    if s.lower() in ('nan', 'none', 'nat', ''):
        return value
    if re.fullmatch(r'\d+', s):
        return s
    try:
        f = float(s)
    except ValueError:
        return value
    if f != f or abs(f) == float('inf'):
        return value
    if f < 0:
        return value
    i = int(f)
    if f != float(i):
        return value
    return str(i)


def validate_price_id_prefix(subscriber_data, seller_name='', is_sandbox=False):
    """
    - price_id_1: required on every row (non-empty after strip); value must start with 'pri_'.
    - Other columns matching price_id_<digits>: optional; if a cell has a value it must start with 'pri_'.
    - quantity_* columns: whole non-negative numbers like 2.0 are rewritten to 2 (in place on subscriber_data).
    - For every index N: if price_id_N is non-empty on a row, quantity_N must be one or more ASCII digits only
      (no sign, no decimal point; e.g. 1, 20, 0).
    If price_id_1 is missing from the dataframe, returns valid (column validation reports missing headers).
    """
    try:
        if 'price_id_1' not in subscriber_data.columns:
            return {
                'valid': True,
                'incorrect_count': 0,
                'total_records': len(subscriber_data),
                'incorrect_records': None
            }

        for col in list(subscriber_data.columns):
            if _QUANTITY_LINE_INDEX_RE.match(str(col)):
                subscriber_data[col] = subscriber_data[col].apply(_normalize_subscription_quantity_cell)

        validation_data = subscriber_data.copy()
        if '_temp_row_id' not in validation_data.columns:
            validation_data['_temp_row_id'] = range(len(validation_data))

        def normalize_cell(value):
            if pd.isna(value):
                return ''
            s = str(value).strip()
            if s.lower() in ('nan', 'none', 'nat', ''):
                return ''
            return s

        def price_id_1_ok(value):
            s = normalize_cell(value)
            return s != '' and s.startswith('pri_')

        def optional_price_id_ok(value):
            s = normalize_cell(value)
            return s == '' or s.startswith('pri_')

        def is_non_negative_integer_quantity_string(value):
            s = normalize_cell(value)
            if s == '':
                return False
            return bool(re.fullmatch(r'\d+', s))

        invalid_mask = ~validation_data['price_id_1'].apply(price_id_1_ok)

        for col in validation_data.columns:
            if col == 'price_id_1' or not _PRICE_ID_COLUMN_RE.match(str(col)):
                continue
            invalid_mask = invalid_mask | ~validation_data[col].apply(optional_price_id_ok)

        line_indices = set()
        for c in validation_data.columns:
            m = _PRICE_ID_LINE_INDEX_RE.match(str(c))
            if m:
                line_indices.add(int(m.group(1)))
            m = _QUANTITY_LINE_INDEX_RE.match(str(c))
            if m:
                line_indices.add(int(m.group(1)))

        for i in sorted(line_indices):
            pcol = f'price_id_{i}'
            qcol = f'quantity_{i}'
            if pcol not in validation_data.columns:
                continue
            price_nonempty = validation_data[pcol].apply(lambda v: normalize_cell(v) != '')
            if qcol not in validation_data.columns:
                invalid_mask = invalid_mask | price_nonempty
            else:
                qty_ok = validation_data[qcol].apply(is_non_negative_integer_quantity_string)
                invalid_mask = invalid_mask | (price_nonempty & ~qty_ok)

        incorrect_records = validation_data[invalid_mask].copy()
        failed_temp_row_ids = extract_failed_temp_row_ids_from_df(incorrect_records)
        if not incorrect_records.empty:
            incorrect_records = clean_dataframe_for_validation_report_csv(incorrect_records)

        return {
            'valid': len(incorrect_records) == 0,
            'incorrect_count': len(incorrect_records),
            'incorrect_records': incorrect_records,
            'failed_temp_row_ids': failed_temp_row_ids,
            'total_records': len(validation_data)
        }
    except Exception as e:
        print(f"Error in price ID validation: {e}")
        import traceback
        traceback.print_exc()
        return {
            'valid': False,
            'error': f'Validation error: {str(e)}',
            'incorrect_count': 0,
            'total_records': 0,
            'incorrect_records': None
        }


def _subscriber_presence_cell_empty(value):
    """True if a subscription CSV cell has no usable non-empty value (presence checks)."""
    if pd.isna(value):
        return True
    s = str(value).strip()
    if s == '':
        return True
    if s.lower() in ('nan', 'none', 'nat'):
        return True
    return False


def _resolve_subscriber_column_case_insensitive(subscriber_data, logical_name):
    """Match template columns when headers differ only by case or outer whitespace (e.g. Status vs status)."""
    want = logical_name.strip().lower()
    for c in subscriber_data.columns:
        if str(c).strip().lower() == want:
            return c
    return None


def _mapping_has_column(mapping_df, logical_name):
    """Match mapping CSV headers with trim + case-insensitive compare (e.g. card.id)."""
    want = logical_name.strip().lower()
    for c in mapping_df.columns:
        if str(c).strip().lower() == want:
            return True
    return False


def _canonicalize_mapping_column(mapping_df, canonical_name):
    """If canonical header missing but a case-insensitive match exists, rename that column to canonical_name."""
    if canonical_name in mapping_df.columns:
        return mapping_df
    want = canonical_name.strip().lower()
    for c in list(mapping_df.columns):
        if str(c).strip().lower() == want:
            return mapping_df.rename(columns={c: canonical_name})
    return mapping_df


def _normalize_bluesnap_mapping_headers(mapping_df):
    """Ensure BlueSnap mapping columns match expected spelling for downstream indexing and rename()."""
    out = mapping_df
    for canonical in _BLUESNAP_MAPPING_HEADER_CANONICAL:
        out = _canonicalize_mapping_column(out, canonical)
    return out


def _normalize_stripe_mapping_headers(mapping_df):
    """Stripe exports use dotted headers; normalize casing so rename/dict access uses exact keys."""
    out = mapping_df
    for canonical in _STRIPE_MAPPING_HEADER_CANONICAL:
        out = _canonicalize_mapping_column(out, canonical)
    return out


def validate_merge_key_columns_present(subscribedata, mappingdata, provider):
    """
    Required merge keys before any other validation:
    - Subscriber: card_token (same header for Stripe and BlueSnap; case-insensitive match allowed).
    - Stripe mapping: card.id (Stripe export column for card identifier — renamed to card_id before merge).
    - BlueSnap mapping: BlueSnap Account Id and Credit Card Number (used to build card_token for merge);
      headers matched case-insensitively (same as card.id on Stripe).
    """
    missing_subscriber = []
    missing_mapping = []
    if _resolve_subscriber_column_case_insensitive(subscribedata, 'card_token') is None:
        missing_subscriber.append('card_token')

    prov = (provider or '').strip().lower()
    if prov == 'bluesnap':
        if not _mapping_has_column(mappingdata, 'BlueSnap Account Id'):
            missing_mapping.append('BlueSnap Account Id')
        if not _mapping_has_column(mappingdata, 'Credit Card Number'):
            missing_mapping.append('Credit Card Number')
    else:
        # Stripe (default): mapping export uses card.id
        if not _mapping_has_column(mappingdata, 'card.id'):
            missing_mapping.append('card.id')

    if missing_subscriber or missing_mapping:
        parts = []
        if missing_subscriber:
            parts.append(
                'Subscriber export is missing required column(s): '
                + ', '.join(missing_subscriber) + '.'
            )
        if missing_mapping:
            parts.append(
                'Token / mapping file is missing required column(s): '
                + ', '.join(missing_mapping) + '.'
            )
        msg = (
            ' '.join(parts)
            + ' These columns are required to match subscriber rows to the token file. '
            'Fix the CSV headers and run again.'
        )
        return {'valid': False, 'message': msg}

    return {'valid': True}


def _clean_validation_results_for_response(validation_results):
    """Match JSON-safe fields to the end-of-run failure payload (subset of each validation dict)."""
    clean_validation_results = []
    extra_keys = (
        'missing_columns', 'total_columns', 'optional_columns', 'incorrect_count',
        'total_records', 'download_file', 'error', 'missing_count', 'available_from_mapping',
        'pulled_from_mapping_count', 'autocorrectable_count', 'autocorrected', 'autocorrected_count',
        'type', 'count', 'message', 'required_countries', 'required_countries_dict',
        'unsupported_countries', 'unsupported_countries_dict',
    )
    for validation in validation_results:
        clean_validation = {
            'valid': validation.get('valid', True),
            'step': validation.get('step', 'unknown'),
        }
        for key in extra_keys:
            if key in validation:
                clean_validation[key] = validation[key]
        clean_validation_results.append(clean_validation)
    return clean_validation_results


def _create_all_reports_zip_from_validation_downloads(
    validation_results,
    seller_name,
    subscriber_filename,
    provider,
    is_sandbox,
):
    """
    Build outputs/<base>_all_reports.zip from unique validation download_file CSV paths.
    Uses the same base filename convention as the full migration export (seller + provider + sandbox).
    Returns a single output_files-style dict with is_zip True, or None if nothing to zip.
    """
    validation_files_to_zip = []
    for validation in validation_results:
        fn = validation.get('download_file')
        if fn:
            validation_files_to_zip.append(fn)
    seen = set()
    unique_files = []
    for fn in validation_files_to_zip:
        if fn not in seen:
            seen.add(fn)
            unique_files.append(fn)
    if not unique_files:
        return None

    output_dir = 'outputs'
    os.makedirs(output_dir, exist_ok=True)

    if seller_name:
        clean_seller_name = "".join(c for c in seller_name if c.isalnum() or c in (' ', '-', '_')).rstrip()
        clean_seller_name = clean_seller_name.replace(' ', '_')
        prov = (provider or 'stripe').strip().lower()
        base_filename = f"{clean_seller_name}_{prov}"
    else:
        base = os.path.splitext(os.path.basename(str(subscriber_filename)))[0]
        prov = (provider or 'stripe').strip().lower()
        base_filename = f"{base}_{prov}"
    if is_sandbox:
        base_filename += "_sandbox"

    zip_filename = f'{base_filename}_all_reports.zip'
    zip_path = os.path.join(output_dir, zip_filename)

    try:
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for filename in unique_files:
                file_path = os.path.join(output_dir, filename)
                if os.path.exists(file_path):
                    zipf.write(file_path, filename)
                    print(f"Added {filename} to subscriber check-only zip file")
        zip_size = os.path.getsize(zip_path)
        print(f"Subscriber check-only zip created: {zip_path} (Size: {zip_size} bytes)")
        return {
            'name': zip_filename,
            'size': zip_size,
            'url': f'file://{os.path.abspath(zip_path)}',
            'is_zip': True,
        }
    except Exception as e:
        print(f"Error creating subscriber check-only zip file: {e}")
        return None


def _return_subscriber_csv_check_only_result(validation_results, start_time):
    """After subscriber-only validations (through date_validation); no merge or mapping."""
    failed_validations = [v for v in validation_results if not v.get('valid', True)]
    clean_validation_results = _clean_validation_results_for_response(validation_results)
    processing_time = time.time() - start_time
    out = {
        'subscriber_check_only': True,
        'validation_results': clean_validation_results,
        'processing_time': f'{processing_time:.2f} seconds',
        'zip_file': None,
        'output_files': [],
    }
    if failed_validations:
        out['error'] = 'Validation failures detected'
        out['failed_count'] = len(failed_validations)
    return out


def _run_zip_validations_subscriber_check_only(
    subscribedata,
    validation_results,
    failed_row_ids,
    seller_name,
    is_sandbox,
    provider,
    autocorrect_us_zip,
):
    """
    Missing postal + CA + US zip validations on subscriber export only (no token/mapping fill).
    Mutates subscribedata when US leading-zero autocorrect runs.
    """
    print('Validating missing zip codes (subscriber check-only)...')
    missing_zip_validation = None
    try:
        missing_zip_validation = validate_missing_zip_codes(subscribedata, provider, seller_name, is_sandbox)
    except Exception as e:
        print(f'Error during missing zip code validation: {e}')
        validation_results.append({
            'valid': False,
            'step': 'missing_zip_code_validation',
            'error': f'Validation error: {str(e)}',
            'missing_count': 0,
            'total_records': 0,
            'available_from_mapping': 0,
            'download_file': None,
            'required_countries': ['AU', 'CA', 'FR', 'DE', 'IN', 'IT', 'NL', 'ES', 'GB', 'US'],
            'required_countries_dict': {
                'AU': '🇦🇺', 'CA': '🇨🇦', 'FR': '🇫🇷', 'DE': '🇩🇪', 'IN': '🇮🇳',
                'IT': '🇮🇹', 'NL': '🇳🇱', 'ES': '🇪🇸', 'GB': '🇬🇧', 'US': '🇺🇸'
            },
        })

    if missing_zip_validation:
        if not missing_zip_validation['valid']:
            download_file = None
            if missing_zip_validation['missing_records'] is not None:
                try:
                    output_dir = 'outputs'
                    os.makedirs(output_dir, exist_ok=True)
                    clean_seller_name = "".join(
                        c for c in seller_name if c.isalnum() or c in (' ', '-', '_')
                    ).rstrip()
                    clean_seller_name = clean_seller_name.replace(' ', '_')
                    env_suffix = '_sandbox' if is_sandbox else '_production'
                    missing_filename = (
                        f'{clean_seller_name}_missing_postal_codes{env_suffix}_{int(time.time())}.csv'
                    )
                    missing_path = os.path.join(output_dir, missing_filename)
                    missing_zip_validation['missing_records'].to_csv(missing_path, index=False)
                    download_file = missing_filename
                    print(f'Saved missing records to: {missing_path}')
                except Exception as save_err:
                    print(f'Error saving missing records file: {save_err}')
            merge_failed_temp_row_ids_into_set(failed_row_ids, missing_zip_validation)
            validation_results.append({
                'valid': False,
                'step': 'missing_zip_code_validation',
                'missing_count': missing_zip_validation['missing_count'],
                'total_records': missing_zip_validation['total_records'],
                'available_from_mapping': missing_zip_validation['available_from_mapping'],
                'pulled_from_mapping_count': 0,
                'download_file': download_file,
                'required_countries': missing_zip_validation.get('required_countries', []),
                'required_countries_dict': missing_zip_validation.get('required_countries_dict', {}),
            })
        else:
            validation_results.append({
                'valid': True,
                'step': 'missing_zip_code_validation',
                'total_records': missing_zip_validation['total_records'],
                'pulled_from_mapping_count': 0,
                'required_countries': missing_zip_validation.get('required_countries', []),
                'required_countries_dict': missing_zip_validation.get('required_countries_dict', {}),
            })

    print('Validating Canadian zip codes (subscriber check-only)...')
    ca_zip_validation = None
    try:
        ca_zip_validation = validate_ca_zip_codes(subscribedata, seller_name, is_sandbox)
    except Exception as e:
        print(f'Error during CA zip code validation: {e}')
        validation_results.append({
            'valid': False,
            'step': 'ca_zip_code_validation',
            'error': f'Validation error: {str(e)}',
            'incorrect_count': 0,
            'total_records': 0,
            'download_file': None,
        })

    if ca_zip_validation:
        if not ca_zip_validation['valid']:
            print(
                f"CA zip code validation failed. Found {ca_zip_validation['incorrect_count']} incorrect formats."
            )
            download_file = None
            if ca_zip_validation['incorrect_records'] is not None:
                try:
                    output_dir = 'outputs'
                    os.makedirs(output_dir, exist_ok=True)
                    clean_seller_name = "".join(
                        c for c in seller_name if c.isalnum() or c in (' ', '-', '_')
                    ).rstrip()
                    clean_seller_name = clean_seller_name.replace(' ', '_')
                    env_suffix = '_sandbox' if is_sandbox else '_production'
                    incorrect_filename = (
                        f'{clean_seller_name}_invalid_ca_zip_codes{env_suffix}_{int(time.time())}.csv'
                    )
                    incorrect_path = os.path.join(output_dir, incorrect_filename)
                    ca_zip_validation['incorrect_records'].to_csv(incorrect_path, index=False)
                    download_file = incorrect_filename
                    print(f'Saved incorrect records to: {incorrect_path}')
                except Exception as save_err:
                    print(f'Error saving incorrect records file: {save_err}')
                merge_failed_temp_row_ids_into_set(failed_row_ids, ca_zip_validation)
            validation_results.append({
                'valid': False,
                'step': 'ca_zip_code_validation',
                'incorrect_count': ca_zip_validation['incorrect_count'],
                'total_records': ca_zip_validation['total_records'],
                'download_file': download_file,
            })
        else:
            print(
                f"CA zip code validation passed. All {ca_zip_validation['total_records']} Canadian zip codes are correctly formatted."
            )
            validation_results.append({
                'valid': True,
                'step': 'ca_zip_code_validation',
                'total_records': ca_zip_validation['total_records'],
            })

    print('Validating US zip codes (subscriber check-only)...')
    us_zip_validation = None
    try:
        us_zip_validation = validate_us_zip_codes(subscribedata, seller_name, is_sandbox)
    except Exception as e:
        print(f'Error during US zip code validation: {e}')
        validation_results.append({
            'valid': False,
            'step': 'us_zip_code_validation',
            'error': f'Validation error: {str(e)}',
            'incorrect_count': 0,
            'total_records': 0,
            'download_file': None,
            'autocorrectable_count': 0,
        })

    if us_zip_validation:
        if not us_zip_validation['valid']:
            print(
                f"US zip code validation failed. Found {us_zip_validation['incorrect_count']} incorrect formats."
            )
            print(
                f"Of these, {us_zip_validation['autocorrectable_count']} can be autocorrected with leading zeros."
            )
            autocorrected_count = 0
            if autocorrect_us_zip and us_zip_validation['autocorrectable_count'] > 0:
                print('Autocorrecting 4-digit US zip codes with leading zeros...')
                autocorrected_count = _apply_us_zip_leading_zero_autocorrect(subscribedata)
                print(f'Autocorrected {autocorrected_count} US zip codes.')
                us_zip_validation = validate_us_zip_codes(subscribedata, seller_name, is_sandbox)

            download_file = None
            if us_zip_validation and not us_zip_validation['valid'] and us_zip_validation['incorrect_records'] is not None:
                try:
                    output_dir = 'outputs'
                    os.makedirs(output_dir, exist_ok=True)
                    clean_seller_name = "".join(
                        c for c in seller_name if c.isalnum() or c in (' ', '-', '_')
                    ).rstrip()
                    clean_seller_name = clean_seller_name.replace(' ', '_')
                    env_suffix = '_sandbox' if is_sandbox else '_production'
                    filename_suffix = '_after_autocorrect' if autocorrected_count > 0 else ''
                    incorrect_filename = (
                        f'{clean_seller_name}_invalid_us_zip_codes{filename_suffix}{env_suffix}_{int(time.time())}.csv'
                    )
                    incorrect_path = os.path.join(output_dir, incorrect_filename)
                    us_zip_validation['incorrect_records'].to_csv(incorrect_path, index=False)
                    download_file = incorrect_filename
                    print(f'Saved incorrect records to: {incorrect_path}')
                except Exception as save_err:
                    print(f'Error saving incorrect records file: {save_err}')

            if us_zip_validation and us_zip_validation['valid']:
                print('US zip code validation passed after autocorrection.')
                validation_results.append({
                    'valid': True,
                    'step': 'us_zip_code_validation',
                    'total_records': us_zip_validation.get('total_records', 0),
                    'autocorrected_count': int(autocorrected_count),
                })
            else:
                if us_zip_validation:
                    print(
                        f"US zip code validation failed. Found {us_zip_validation['incorrect_count']} incorrect formats."
                    )
                    merge_failed_temp_row_ids_into_set(failed_row_ids, us_zip_validation)
                validation_results.append({
                    'valid': False,
                    'step': 'us_zip_code_validation',
                    'incorrect_count': us_zip_validation.get('incorrect_count', 0) if us_zip_validation else 0,
                    'total_records': us_zip_validation.get('total_records', 0) if us_zip_validation else 0,
                    'download_file': download_file,
                    'autocorrectable_count': (
                        us_zip_validation.get('autocorrectable_count', 0) if us_zip_validation else 0
                    ),
                    'autocorrected_count': int(autocorrected_count),
                })
        else:
            print(
                f"US zip code validation passed. All {us_zip_validation['total_records']} US zip codes are correctly formatted."
            )
            validation_results.append({
                'valid': True,
                'step': 'us_zip_code_validation',
                'autocorrected_count': 0,
                'total_records': us_zip_validation['total_records'],
            })


def _validate_subscriber_column_presence(subscriber_data, column_name):
    """
    Every row must have a non-empty value in column_name.
    If the column is missing, returns invalid with a clear error message.
    """
    try:
        if column_name not in subscriber_data.columns:
            return {
                'valid': False,
                'error': f"Unable to validate as column is missing: {column_name}",
                'incorrect_count': 0,
                'total_records': len(subscriber_data),
                'incorrect_records': None,
                'failed_temp_row_ids': [],
            }

        validation_data = subscriber_data.copy()
        if '_temp_row_id' not in validation_data.columns:
            validation_data['_temp_row_id'] = range(len(validation_data))

        empty_mask = validation_data[column_name].apply(_subscriber_presence_cell_empty)
        incorrect_records = validation_data[empty_mask].copy()
        failed_temp_row_ids = extract_failed_temp_row_ids_from_df(incorrect_records)
        if not incorrect_records.empty:
            incorrect_records = clean_dataframe_for_validation_report_csv(incorrect_records)

        return {
            'valid': len(incorrect_records) == 0,
            'incorrect_count': len(incorrect_records),
            'incorrect_records': incorrect_records,
            'failed_temp_row_ids': failed_temp_row_ids,
            'total_records': len(validation_data),
        }
    except Exception as e:
        print(f"Error in {column_name} presence validation: {e}")
        import traceback
        traceback.print_exc()
        return {
            'valid': False,
            'error': f'Validation error: {str(e)}',
            'incorrect_count': 0,
            'total_records': 0,
            'incorrect_records': None,
            'failed_temp_row_ids': [],
        }


def validate_card_token_presence(subscriber_data, seller_name='', is_sandbox=False):
    """Every row must have a non-empty card_token before merge (Stripe / BlueSnap join key)."""
    return _validate_subscriber_column_presence(subscriber_data, 'card_token')


_ALLOWED_SUBSCRIPTION_STATUSES = frozenset({'active', 'trialing', 'paused'})


def _normalized_subscription_status(value):
    """Lowercase stripped status token, or '' if empty/missing."""
    if _subscriber_presence_cell_empty(value):
        return ''
    s = str(value).strip()
    s = s.lstrip('\ufeff')
    s = unicodedata.normalize('NFKC', s).strip().lower()
    s = re.sub(r'\s+', ' ', s).strip()
    return s


def validate_subscription_status(subscriber_data, seller_name='', is_sandbox=False):
    """
    Each row's status must be non-empty and one of: active, trialing, paused (case-insensitive).
    Paused rows are valid for processing; callers may report them separately as an alert (not excluded).
    If the status column is missing, returns invalid with a clear error message.
    """
    try:
        status_col = _resolve_subscriber_column_case_insensitive(subscriber_data, 'status')
        if status_col is None:
            return {
                'valid': False,
                'error': 'Unable to validate as column is missing: status',
                'incorrect_count': 0,
                'total_records': len(subscriber_data),
                'incorrect_records': None,
                'failed_temp_row_ids': [],
                'paused_records': None,
                'paused_count': 0,
            }

        validation_data = subscriber_data.copy()
        if '_temp_row_id' not in validation_data.columns:
            validation_data['_temp_row_id'] = range(len(validation_data))

        norm_series = validation_data[status_col].apply(_normalized_subscription_status)
        invalid_mask = ~norm_series.isin(_ALLOWED_SUBSCRIPTION_STATUSES)
        incorrect_records = validation_data[invalid_mask].copy()
        failed_temp_row_ids = extract_failed_temp_row_ids_from_df(incorrect_records)

        paused_mask = norm_series == 'paused'
        paused_records = validation_data[paused_mask].copy()
        paused_count = len(paused_records)

        incorrect_count = len(incorrect_records)
        if not incorrect_records.empty:
            incorrect_records = clean_dataframe_for_validation_report_csv(incorrect_records)
        else:
            incorrect_records = None

        if not paused_records.empty:
            paused_records = clean_dataframe_for_validation_report_csv(paused_records)
        else:
            paused_records = None

        return {
            'valid': incorrect_count == 0,
            'incorrect_count': incorrect_count,
            'incorrect_records': incorrect_records,
            'failed_temp_row_ids': failed_temp_row_ids,
            'total_records': len(validation_data),
            'paused_records': paused_records,
            'paused_count': paused_count,
        }
    except Exception as e:
        print(f"Error in status validation: {e}")
        import traceback
        traceback.print_exc()
        return {
            'valid': False,
            'error': f'Validation error: {str(e)}',
            'incorrect_count': 0,
            'total_records': 0,
            'incorrect_records': None,
            'failed_temp_row_ids': [],
            'paused_records': None,
            'paused_count': 0,
        }


# (column_name, validation_results step key, filename slug before env_suffix)
_SUBSCRIBER_FIELD_PRESENCE_CHECKS = (
    ('customer_email', 'customer_email_presence_validation', 'missing_customer_email_values'),
    ('status', 'status_presence_validation', 'invalid_status_values'),
    ('currency_code', 'currency_code_presence_validation', 'missing_currency_code_values'),
    ('collection_mode', 'collection_mode_presence_validation', 'missing_collection_mode_values'),
    ('subscription_external_id', 'subscription_external_id_presence_validation', 'missing_subscription_external_id_values'),
)


def validate_date_format(subscriber_data, seller_name='', is_sandbox=False):
    """
    Validate date formats for subscription period and lifecycle fields.
    - current_period_started_at and current_period_ends_at: required; must match
      YYYY-MM-DDTHH:MM:SSZ (e.g., 2025-07-06T00:00:00Z); empty is invalid.
    - started_at and paused_at: when present, must match the same pattern; empty
      or missing values are treated as valid.
    
    Args:
        subscriber_data: DataFrame containing subscriber data
        seller_name: Name of the seller for file naming
        is_sandbox: Boolean indicating if this is sandbox mode
    
    Returns:
        dict: Validation results with status and incorrect records
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
        
        # Create a copy of the data for validation (don't modify original)
        validation_data = subscriber_data.copy()
        
        # Expected format: YYYY-MM-DDTHH:MM:SSZ (e.g., 2025-07-06T00:00:00Z)
        date_format_pattern = r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$'
        
        # Convert to string, handling NaN values
        # Note: If dates were already parsed as datetime objects by pandas,
        # we check their string representation. The validation ensures the original
        # CSV format matches the required pattern.
        def check_date_format_required(value):
            if pd.isna(value):
                return False
            value_str = str(value)
            if value_str.lower() in ['nan', 'none', 'nat', '']:
                return False
            return bool(re.match(date_format_pattern, value_str))

        def check_date_format_optional(value):
            """Same pattern when non-empty; blank/NaN is allowed."""
            if pd.isna(value):
                return True
            value_str = str(value).strip()
            if value_str.lower() in ['nan', 'none', 'nat', '']:
                return True
            return bool(re.match(date_format_pattern, value_str))

        period_started_valid = validation_data['current_period_started_at'].apply(
            check_date_format_required
        )
        period_ended_valid = validation_data['current_period_ends_at'].apply(
            check_date_format_required
        )

        incorrect_format_mask = ~(period_started_valid & period_ended_valid)

        for col in ('started_at', 'paused_at'):
            if col in validation_data.columns:
                col_valid = validation_data[col].apply(check_date_format_optional)
                incorrect_format_mask = incorrect_format_mask | ~col_valid

        incorrect_records = validation_data[incorrect_format_mask].copy()
        failed_temp_row_ids = extract_failed_temp_row_ids_from_df(incorrect_records)

        # Convert all columns to strings to prevent float conversion in CSV
        if not incorrect_records.empty:
            incorrect_records = clean_dataframe_for_validation_report_csv(incorrect_records)
        
        return {
            'valid': len(incorrect_records) == 0,
            'incorrect_count': len(incorrect_records),
            'incorrect_records': incorrect_records,
            'failed_temp_row_ids': failed_temp_row_ids,
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

        failed_temp_row_ids = extract_failed_temp_row_ids_from_df(incorrect_records)
        if not incorrect_records.empty:
            incorrect_records = clean_dataframe_for_validation_report_csv(incorrect_records)

        return {
            'valid': len(incorrect_records) == 0,
            'incorrect_count': len(incorrect_records),
            'incorrect_records': incorrect_records,
            'failed_temp_row_ids': failed_temp_row_ids,
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

def validate_missing_zip_codes(data, provider, seller_name='', is_sandbox=False):
    """
    Validate missing zip codes for specific countries and check if they can be filled from mapping file
    """
    try:
        # Dictionary of required country codes with their flag emojis
        required_countries_dict = {
            'AU': '🇦🇺', 'CA': '🇨🇦', 'FR': '🇫🇷', 'DE': '🇩🇪', 'IN': '🇮🇳', 
            'IT': '🇮🇹', 'NL': '🇳🇱', 'ES': '🇪🇸', 'GB': '🇬🇧', 'US': '🇺🇸'
        }
        # List of required country codes (for validation logic)
        required_countries = list(required_countries_dict.keys())
        
        # Filter for records from required countries (case-insensitive ISO codes)
        cc_norm = _normalized_country_codes(data['address_country_code'])
        required_records = data[cc_norm.isin(required_countries)].copy()
        
        if len(required_records) == 0:
            return {
                'valid': True,
                'missing_count': 0,
                'total_records': 0,
                'available_from_mapping': 0,
                'missing_records': None,
                'required_countries': required_countries,
                'required_countries_dict': required_countries_dict
            }
        
        # Find records with missing zip codes
        missing_zip_codes = required_records[
            required_records['address_postal_code'].isna() | 
            (required_records['address_postal_code'].astype(str).str.strip() == '')
        ].copy()
        
        # Calculate missing_count early so we can preserve it even if an exception occurs later
        missing_count = len(missing_zip_codes)
        total_records_count = len(required_records)
        
        if missing_count == 0:
            return {
                'valid': True,
                'missing_count': 0,
                'total_records': total_records_count,
                'available_from_mapping': 0,
                'missing_records': None,
                'required_countries': required_countries,
                'required_countries_dict': required_countries_dict
            }
        
        # Check mapping file column name based on provider
        mapping_column = 'card.address_zip' if provider.lower() == 'stripe' else 'Zip Code'
        
        # Count records that have zip codes available in mapping file (already merged)
        # Check if mapping column exists before trying to use it
        available_count = 0
        if mapping_column in missing_zip_codes.columns:
            try:
                available_from_mapping = missing_zip_codes[
                    missing_zip_codes[mapping_column].notna() & 
                    (missing_zip_codes[mapping_column].astype(str).str.strip() != '')
                ]
                available_count = len(available_from_mapping)
            except Exception as e:
                print(f"Warning: Error counting available zip codes from mapping: {e}")
                available_count = 0
        
        failed_temp_row_ids = extract_failed_temp_row_ids_from_df(missing_zip_codes)
        # Convert all columns to strings to prevent float conversion in CSV
        # Wrap this in try/except to preserve missing_count even if conversion fails
        try:
            if not missing_zip_codes.empty:
                missing_zip_codes = clean_dataframe_for_validation_report_csv(missing_zip_codes)
        except Exception as e:
            print(f"Warning: Error converting columns to strings: {e}")
            # Continue with unconverted data - missing_count is still valid
        
        return {
            'valid': False,
            'missing_count': missing_count,
            'total_records': total_records_count,
            'available_from_mapping': available_count,
            'missing_records': missing_zip_codes,
            'failed_temp_row_ids': failed_temp_row_ids,
            'required_countries': required_countries,
            'required_countries_dict': required_countries_dict
        }
        
    except Exception as e:
        print(f"Error in missing zip code validation: {e}")
        import traceback
        traceback.print_exc()
        # Try to preserve any counts we might have calculated
        return {
            'valid': False,
            'error': f'Validation error: {str(e)}',
            'missing_count': 0,  # Can't preserve count if exception occurs before calculation
            'total_records': 0,
            'available_from_mapping': 0,
            'missing_records': None,
            'required_countries': ['AU', 'CA', 'FR', 'DE', 'IN', 'IT', 'NL', 'ES', 'GB', 'US'],  # Fallback if error occurs
            'required_countries_dict': {
                'AU': '🇦🇺', 'CA': '🇨🇦', 'FR': '🇫🇷', 'DE': '🇩🇪', 'IN': '🇮🇳', 
                'IT': '🇮🇹', 'NL': '🇳🇱', 'ES': '🇪🇸', 'GB': '🇬🇧', 'US': '🇺🇸'
            }  # Fallback if error occurs
        }

# Canada Post: letters D, F, I, O, Q, U never used; W and Z not used as first letter of FSA.
_CA_POSTAL_LETTER_FIRST = 'ABCEGHJKLMNPRSTVXY'
_CA_POSTAL_LETTER_OTHER = 'ABCEGHJKLMNPRSTVWXYZ'
_CA_ZIP_PATTERN = (
    rf'^[{_CA_POSTAL_LETTER_FIRST}]\d[{_CA_POSTAL_LETTER_OTHER}]'
    rf' ?\d[{_CA_POSTAL_LETTER_OTHER}]\d$'
)


def _normalize_ca_postal_code_string(value):
    """Uppercase, trim, collapse internal whitespace (common CSV issues)."""
    if pd.isna(value):
        return ''
    s = str(value).strip().upper()
    s = re.sub(r'\s+', ' ', s)
    return s


def validate_ca_zip_codes(data, seller_name='', is_sandbox=False):
    """
    Validate Canadian zip codes for records with address_country_code = 'CA'
    
    Args:
        data: DataFrame containing the merged data
        seller_name: Name of the seller for file naming
        is_sandbox: Boolean indicating if this is sandbox mode
    
    Returns:
        dict: Validation results with status and incorrect records
    """
    try:
        # Filter for Canadian records (case-insensitive country code)
        ca_mask = _normalized_country_codes(data['address_country_code']) == 'CA'
        ca_records = data[ca_mask].copy()
        
        if len(ca_records) == 0:
            return {
                'valid': True,
                'incorrect_count': 0,
                'total_records': 0,
                'incorrect_records': None
            }
        
        # Filter out missing/empty zip codes (those are handled by missing zip code validation)
        # Only validate format for records that have zip codes
        ca_records_with_zip = ca_records[
            ca_records['address_postal_code'].notna() & 
            (ca_records['address_postal_code'].astype(str).str.strip() != '')
        ].copy()
        
        if len(ca_records_with_zip) == 0:
            return {
                'valid': True,
                'incorrect_count': 0,
                'total_records': len(ca_records),
                'incorrect_records': None
            }
        
        normalized_zip = ca_records_with_zip['address_postal_code'].apply(_normalize_ca_postal_code_string)
        # Check zip codes format (only for records that have zip codes)
        invalid_zip_codes = ca_records_with_zip[
            ~normalized_zip.str.match(_CA_ZIP_PATTERN, case=False)
        ].copy()
        
        failed_temp_row_ids = extract_failed_temp_row_ids_from_df(invalid_zip_codes)
        # Convert all columns to strings to prevent float conversion in CSV
        if not invalid_zip_codes.empty:
            invalid_zip_codes = clean_dataframe_for_validation_report_csv(invalid_zip_codes)
        
        return {
            'valid': len(invalid_zip_codes) == 0,
            'incorrect_count': len(invalid_zip_codes),
            'incorrect_records': invalid_zip_codes,
            'failed_temp_row_ids': failed_temp_row_ids,
            'total_records': len(ca_records_with_zip)
        }
        
    except Exception as e:
        print(f"Error in CA zip code validation: {e}")
        return {
            'valid': False,
            'error': f'Validation error: {str(e)}',
            'incorrect_count': 0,
            'total_records': 0,
            'incorrect_records': None
        }

def validate_us_zip_codes(data, seller_name='', is_sandbox=False):
    """
    Validate US zip codes for records with address_country_code = 'US'
    
    Args:
        data: DataFrame containing the merged data
        seller_name: Name of the seller for file naming
        is_sandbox: Boolean indicating if this is sandbox mode
    
    Returns:
        dict: Validation results with status and incorrect records
    """
    try:
        # Filter for US records (case-insensitive country code)
        us_records = data[_normalized_country_codes(data['address_country_code']) == 'US'].copy()
        
        if len(us_records) == 0:
            return {
                'valid': True,
                'incorrect_count': 0,
                'total_records': 0,
                'incorrect_records': None,
                'autocorrectable_count': 0
            }
        
        # US zip code regex pattern - only 5 digits
        us_zip_pattern = r'^\d{5}$'
        
        # Helper function to convert zip codes properly (handle floats like 90031.0 -> '90031')
        def normalize_zip_code(zip_val):
            """Convert zip code to string, handling both float and string values properly.
            
            Handles:
            - Floats like 90031.0 -> '90031' (converts to int first)
            - Strings like '90031' -> '90031' (strips whitespace)
            - Strings with spaces like ' 90031 ' -> '90031' (strips whitespace)
            """
            if pd.isna(zip_val):
                return ''
            # If it's a float that represents an integer (e.g., 90031.0), convert to int first
            if isinstance(zip_val, float):
                # Check if it's a whole number
                if zip_val.is_integer():
                    return str(int(zip_val))
                else:
                    return str(zip_val)
            # For strings or other types, convert to string and strip whitespace
            return str(zip_val).strip()
        
        # Filter out missing/empty zip codes (those are handled by missing zip code validation)
        # Only validate format for records that have zip codes
        us_records_with_zip = us_records[
            us_records['address_postal_code'].notna() & 
            (us_records['address_postal_code'].apply(normalize_zip_code) != '')
        ].copy()
        
        if len(us_records_with_zip) == 0:
            return {
                'valid': True,
                'incorrect_count': 0,
                'total_records': len(us_records),
                'incorrect_records': None,
                'autocorrectable_count': 0
            }
        
        # Normalize zip codes (convert floats to proper strings)
        us_records_with_zip = us_records_with_zip.copy()
        us_records_with_zip['_normalized_zip'] = us_records_with_zip['address_postal_code'].apply(normalize_zip_code)
        
        # Check zip codes format using normalized values
        matches = us_records_with_zip['_normalized_zip'].str.match(us_zip_pattern)
        invalid_zip_codes = us_records_with_zip[~matches].copy()
        
        # Count 4-digit codes that can be autocorrected
        four_digit_codes = us_records_with_zip[
            us_records_with_zip['_normalized_zip'].str.match(r'^\d{4}$')
        ]
        autocorrectable_count = len(four_digit_codes)
        
        # Drop the temporary normalized column before returning
        invalid_zip_codes = invalid_zip_codes.drop(columns=['_normalized_zip'], errors='ignore')
        
        failed_temp_row_ids = extract_failed_temp_row_ids_from_df(invalid_zip_codes)
        # Convert all columns to strings to prevent float conversion in CSV
        if not invalid_zip_codes.empty:
            invalid_zip_codes = clean_dataframe_for_validation_report_csv(invalid_zip_codes)
        
        return {
            'valid': len(invalid_zip_codes) == 0,
            'incorrect_count': len(invalid_zip_codes),
            'incorrect_records': invalid_zip_codes,
            'failed_temp_row_ids': failed_temp_row_ids,
            'total_records': len(us_records_with_zip),
            'autocorrectable_count': autocorrectable_count
        }
        
    except Exception as e:
        print(f"Error in US zip code validation: {e}")
        return {
            'valid': False,
            'error': f'Validation error: {str(e)}',
            'incorrect_count': 0,
            'total_records': 0,
            'incorrect_records': None,
            'autocorrectable_count': 0
        }

def process_migration(subscriber_file, mapping_file, vault_provider, is_sandbox=False, provider='stripe', seller_name='', autocorrect_us_zip=False, use_mapping_zip_codes=False, anonymise_email=False, strip_iso_date_fractional_suffix=False, subscriber_csv_check_only=False):
    """
    Process migration from payment providers to Paddle Billing
    
    Args:
        subscriber_file: File object or path to subscriber CSV
        mapping_file: File object or path to mapping CSV (ignored when subscriber_csv_check_only is True)
        vault_provider: Name of the vault provider
        is_sandbox: Boolean indicating if this is sandbox mode
        provider: String indicating the payment provider ('stripe' or 'bluesnap')
        seller_name: Name of the seller for file naming
        anonymise_email: Boolean; when True and is_sandbox, customer emails are anonymised (blackhole addresses)
        strip_iso_date_fractional_suffix: When True, strip fractional seconds (...T..:..:..<ms>Z to ...T..:..:..Z) on subscriber date columns
        subscriber_csv_check_only: When True, run subscriber validations only (through date_validation); no mapping file or merge.
    
    Returns:
        dict: Processing results and file information
    """
    global _validation_report_exclude_columns, _subscriber_report_columns, _subscriber_report_column_order
    _validation_report_exclude_columns = frozenset()
    _subscriber_report_columns = frozenset()
    _subscriber_report_column_order = []

    # Only anonymise when in sandbox and the option is enabled
    anonymise_emails = is_sandbox and anonymise_email
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
    
    # Add temporary unique row ID to track records through merge and validations
    subscribedata['_temp_row_id'] = range(len(subscribedata))
    try:
        subscribedata = _canonicalize_subscriber_headers(subscribedata)
    except ValueError as e:
        processing_time = time.time() - start_time
        err_ret = {
            'error': 'Validation failures detected',
            'validation_results': [{
                'valid': False,
                'step': 'subscriber_header_normalization',
                'type': 'super_failure',
                'message': str(e),
            }],
            'failed_count': 1,
            'zip_file': None,
            'output_files': [],
            'processing_time': f'{processing_time:.2f} seconds',
        }
        if subscriber_csv_check_only:
            err_ret['subscriber_check_only'] = True
        return err_ret
    _subscriber_report_columns = frozenset(
        c for c in subscribedata.columns if c != '_temp_row_id'
    )
    _subscriber_report_column_order = [
        c for c in subscribedata.columns if c != '_temp_row_id'
    ]

    mappingdata = None
    if not subscriber_csv_check_only:
        if hasattr(mapping_file, 'read'):
            mappingdata = pd.read_csv(mapping_file, encoding='latin-1')
        else:
            mappingdata = pd.read_csv(mapping_file, encoding='latin-1')

        prov_lc = (provider or '').strip().lower()
        if prov_lc == 'bluesnap':
            mappingdata = _normalize_bluesnap_mapping_headers(mappingdata)
        else:
            mappingdata = _normalize_stripe_mapping_headers(mappingdata)

        merge_key_check = validate_merge_key_columns_present(subscribedata, mappingdata, provider)
        if not merge_key_check['valid']:
            print(f"Merge key column validation failed: {merge_key_check['message']}")
            processing_time = time.time() - start_time
            return {
                'error': 'Validation failures detected',
                'validation_results': [{
                    'valid': False,
                    'step': 'merge_key_columns_validation',
                    'type': 'super_failure',
                    'message': merge_key_check['message'],
                }],
                'failed_count': 1,
                'zip_file': None,
                'output_files': [],
                'processing_time': f'{processing_time:.2f} seconds',
            }

    print(subscribedata)
    
    # Validate subscriber file columns
    print("Validating subscriber file columns...")
    validation_result = validate_subscriber_columns(subscribedata.columns)
    
    # Initialize validation results list
    validation_results = []
    
    # Initialize set to collect all failed _temp_row_id values
    failed_row_ids = set()
    
    if not validation_result['valid']:
        print(f"Column validation failed. Missing columns: {validation_result['missing_columns']}")
        # Add failed validation to results but continue processing
        validation_results.append({
            'valid': False,
            'step': 'column_validation',
            'missing_columns': validation_result['missing_columns'],
            'total_columns': validation_result.get('total_columns', 0),
            'optional_columns': validation_result.get('optional_columns', [])
        })
    else:
        print(f"Column validation passed. Found {validation_result['total_columns']} columns including {len(validation_result['optional_columns'])} optional columns.")
        # Add successful column validation to results
        validation_results.append({
        'valid': True,
        'step': 'column_validation',
        'total_columns': validation_result['total_columns'],
        'optional_columns': validation_result['optional_columns']
        })

    if strip_iso_date_fractional_suffix:
        print(
            "Normalizing subscriber date columns: stripping fractional seconds (.digitsZ) where the full value matches that pattern..."
        )
        strip_iso_fractional_seconds_z_suffixes(subscribedata, True)

    # Address country code format (alpha-2, required on every row)
    print("Validating address country codes...")
    address_country_code_validation = None
    try:
        address_country_code_validation = validate_address_country_code_format(
            subscribedata, seller_name, is_sandbox
        )
    except Exception as e:
        print(f"Error during address country code validation: {e}")
        validation_results.append({
            'valid': False,
            'step': 'address_country_code_validation',
            'error': f'Validation error: {str(e)}',
            'incorrect_count': 0,
            'total_records': 0,
            'download_file': None
        })

    if address_country_code_validation:
        if not address_country_code_validation['valid']:
            err = address_country_code_validation.get('error')
            if err:
                print(f"Address country code validation failed: {err}")
            else:
                print(
                    f"Address country code validation failed. Found "
                    f"{address_country_code_validation['incorrect_count']} records with invalid or missing codes."
                )
            download_file = None
            if address_country_code_validation.get('incorrect_records') is not None:
                try:
                    output_dir = 'outputs'
                    os.makedirs(output_dir, exist_ok=True)
                    clean_seller_name = "".join(
                        c for c in seller_name if c.isalnum() or c in (' ', '-', '_')
                    ).rstrip()
                    clean_seller_name = clean_seller_name.replace(' ', '_')
                    env_suffix = "_sandbox" if is_sandbox else "_production"
                    incorrect_filename = (
                        f"{clean_seller_name}_invalid_address_country_codes{env_suffix}_{int(time.time())}.csv"
                    )
                    incorrect_path = os.path.join(output_dir, incorrect_filename)
                    address_country_code_validation['incorrect_records'].to_csv(
                        incorrect_path, index=False
                    )
                    download_file = incorrect_filename
                    print(f"Saved incorrect records to: {incorrect_path}")
                except Exception as save_err:
                    print(f"Error saving incorrect records file: {save_err}")

            merge_failed_temp_row_ids_into_set(failed_row_ids, address_country_code_validation)

            validation_results.append({
                'valid': False,
                'step': 'address_country_code_validation',
                'incorrect_count': address_country_code_validation['incorrect_count'],
                'total_records': address_country_code_validation['total_records'],
                'download_file': download_file,
                **({'error': address_country_code_validation['error']} if err else {})
            })
        else:
            print(
                f"Address country code validation passed. All "
                f"{address_country_code_validation['total_records']} country codes are valid."
            )
            validation_results.append({
                'valid': True,
                'step': 'address_country_code_validation',
                'total_records': address_country_code_validation['total_records']
            })

    # Price ID format (pri_ prefix; price_id_1 required)
    print("Validating price IDs...")
    price_id_validation = None
    try:
        price_id_validation = validate_price_id_prefix(subscribedata, seller_name, is_sandbox)
    except Exception as e:
        print(f"Error during price ID validation: {e}")
        validation_results.append({
            'valid': False,
            'step': 'price_id_validation',
            'error': f'Validation error: {str(e)}',
            'incorrect_count': 0,
            'total_records': 0,
            'download_file': None
        })

    if price_id_validation:
        if not price_id_validation['valid']:
            err = price_id_validation.get('error')
            if err:
                print(f"Price ID validation failed: {err}")
            else:
                print(
                    f"Price ID validation failed. Found "
                    f"{price_id_validation['incorrect_count']} records with invalid price IDs."
                )
            download_file = None
            if price_id_validation.get('incorrect_records') is not None:
                try:
                    output_dir = 'outputs'
                    os.makedirs(output_dir, exist_ok=True)
                    clean_seller_name = "".join(
                        c for c in seller_name if c.isalnum() or c in (' ', '-', '_')
                    ).rstrip()
                    clean_seller_name = clean_seller_name.replace(' ', '_')
                    env_suffix = "_sandbox" if is_sandbox else "_production"
                    incorrect_filename = (
                        f"{clean_seller_name}_invalid_price_ids{env_suffix}_{int(time.time())}.csv"
                    )
                    incorrect_path = os.path.join(output_dir, incorrect_filename)
                    price_id_validation['incorrect_records'].to_csv(
                        incorrect_path, index=False
                    )
                    download_file = incorrect_filename
                    print(f"Saved incorrect records to: {incorrect_path}")
                except Exception as save_err:
                    print(f"Error saving incorrect records file: {save_err}")

            merge_failed_temp_row_ids_into_set(failed_row_ids, price_id_validation)

            validation_results.append({
                'valid': False,
                'step': 'price_id_validation',
                'incorrect_count': price_id_validation['incorrect_count'],
                'total_records': price_id_validation['total_records'],
                'download_file': download_file,
                **({'error': price_id_validation['error']} if err else {})
            })
        else:
            print(
                f"Price ID validation passed for all "
                f"{price_id_validation['total_records']} records."
            )
            validation_results.append({
                'valid': True,
                'step': 'price_id_validation',
                'total_records': price_id_validation['total_records']
            })

    # Card token presence (non-empty value per row before merge)
    print("Validating card token values...")
    card_token_presence_validation = None
    try:
        card_token_presence_validation = validate_card_token_presence(
            subscribedata, seller_name, is_sandbox
        )
    except Exception as e:
        print(f"Error during card token presence validation: {e}")
        validation_results.append({
            'valid': False,
            'step': 'card_token_presence_validation',
            'error': f'Validation error: {str(e)}',
            'incorrect_count': 0,
            'total_records': 0,
            'download_file': None
        })

    if card_token_presence_validation:
        if not card_token_presence_validation['valid']:
            err = card_token_presence_validation.get('error')
            if err:
                print(f"Card token presence validation failed: {err}")
            else:
                print(
                    f"Card token presence validation failed. Found "
                    f"{card_token_presence_validation['incorrect_count']} records with missing card_token."
                )
            download_file = None
            if card_token_presence_validation.get('incorrect_records') is not None:
                try:
                    output_dir = 'outputs'
                    os.makedirs(output_dir, exist_ok=True)
                    clean_seller_name = "".join(
                        c for c in seller_name if c.isalnum() or c in (' ', '-', '_')
                    ).rstrip()
                    clean_seller_name = clean_seller_name.replace(' ', '_')
                    env_suffix = "_sandbox" if is_sandbox else "_production"
                    incorrect_filename = (
                        f"{clean_seller_name}_missing_card_token_values{env_suffix}_{int(time.time())}.csv"
                    )
                    incorrect_path = os.path.join(output_dir, incorrect_filename)
                    card_token_presence_validation['incorrect_records'].to_csv(
                        incorrect_path, index=False
                    )
                    download_file = incorrect_filename
                    print(f"Saved incorrect records to: {incorrect_path}")
                except Exception as save_err:
                    print(f"Error saving incorrect records file: {save_err}")

            merge_failed_temp_row_ids_into_set(failed_row_ids, card_token_presence_validation)

            validation_results.append({
                'valid': False,
                'step': 'card_token_presence_validation',
                'incorrect_count': card_token_presence_validation['incorrect_count'],
                'total_records': card_token_presence_validation['total_records'],
                'download_file': download_file,
                **({'error': card_token_presence_validation['error']} if err else {})
            })
        else:
            print(
                f"Card token presence validation passed for all "
                f"{card_token_presence_validation['total_records']} records."
            )
            validation_results.append({
                'valid': True,
                'step': 'card_token_presence_validation',
                'total_records': card_token_presence_validation['total_records']
            })

    # Individual subscriber column presence checks (separate reports / UI steps)
    for column_name, step_key, file_slug in _SUBSCRIBER_FIELD_PRESENCE_CHECKS:
        if column_name == 'status':
            print("Validating status (must be active, trialing, or paused)...")
        else:
            print(f"Validating {column_name} (non-empty values)...")
        col_validation = None
        try:
            if column_name == 'status':
                col_validation = validate_subscription_status(subscribedata, seller_name, is_sandbox)
            else:
                col_validation = _validate_subscriber_column_presence(subscribedata, column_name)
        except Exception as e:
            print(f"Error during {column_name} presence validation: {e}")
            validation_results.append({
                'valid': False,
                'step': step_key,
                'error': f'Validation error: {str(e)}',
                'incorrect_count': 0,
                'total_records': 0,
                'download_file': None
            })
            continue

        if not col_validation['valid']:
            err = col_validation.get('error')
            if err:
                print(f"{column_name} validation failed: {err}")
            elif column_name == 'status':
                print(
                    f"Status validation failed. Found "
                    f"{col_validation['incorrect_count']} records with invalid or missing status."
                )
            else:
                print(
                    f"{column_name} presence validation failed. Found "
                    f"{col_validation['incorrect_count']} records with missing values."
                )
            download_file = None
            if col_validation.get('incorrect_records') is not None:
                try:
                    output_dir = 'outputs'
                    os.makedirs(output_dir, exist_ok=True)
                    clean_seller_name = "".join(
                        c for c in seller_name if c.isalnum() or c in (' ', '-', '_')
                    ).rstrip()
                    clean_seller_name = clean_seller_name.replace(' ', '_')
                    env_suffix = "_sandbox" if is_sandbox else "_production"
                    incorrect_filename = (
                        f"{clean_seller_name}_{file_slug}{env_suffix}_{int(time.time())}.csv"
                    )
                    incorrect_path = os.path.join(output_dir, incorrect_filename)
                    col_validation['incorrect_records'].to_csv(incorrect_path, index=False)
                    download_file = incorrect_filename
                    print(f"Saved incorrect records to: {incorrect_path}")
                except Exception as save_err:
                    print(f"Error saving incorrect records file: {save_err}")

            merge_failed_temp_row_ids_into_set(failed_row_ids, col_validation)

            validation_results.append({
                'valid': False,
                'step': step_key,
                'incorrect_count': col_validation['incorrect_count'],
                'total_records': col_validation['total_records'],
                'download_file': download_file,
                **({'error': col_validation['error']} if err else {})
            })
        else:
            if column_name == 'status':
                print(
                    f"Status validation passed for all "
                    f"{col_validation['total_records']} records."
                )
            else:
                print(
                    f"{column_name} presence validation passed for all "
                    f"{col_validation['total_records']} records."
                )
            validation_results.append({
                'valid': True,
                'step': step_key,
                'total_records': col_validation['total_records']
            })

        # Paused subscriptions: warn whenever any row is paused, even if status validation failed
        # on other rows (invalid / empty statuses still produce the red failure box above).
        if column_name == 'status':
            paused_count = col_validation.get('paused_count') or 0
            paused_df = col_validation.get('paused_records')
            if paused_count > 0:
                download_paused = None
                if paused_df is not None and not paused_df.empty:
                    try:
                        output_dir = 'outputs'
                        os.makedirs(output_dir, exist_ok=True)
                        clean_seller_name = "".join(
                            c for c in seller_name if c.isalnum() or c in (' ', '-', '_')
                        ).rstrip()
                        clean_seller_name = clean_seller_name.replace(' ', '_')
                        env_suffix = "_sandbox" if is_sandbox else "_production"
                        paused_filename = (
                            f"{clean_seller_name}_paused_status_records{env_suffix}_{int(time.time())}.csv"
                        )
                        paused_path = os.path.join(output_dir, paused_filename)
                        paused_df.to_csv(paused_path, index=False)
                        download_paused = paused_filename
                        print(f"Saved paused status report to: {paused_path}")
                    except Exception as save_err:
                        print(f"Error saving paused status report: {save_err}")
                validation_results.append({
                    'valid': True,
                    'step': 'status_paused_warning',
                    'type': 'warning',
                    'count': paused_count,
                    'download_file': download_paused,
                    'message': (
                        f'Found {paused_count} subscription row(s) with status paused. '
                        'This is an informational alert only, subscriptions will still be included.'
                    ),
                })

    # Unsupported Countries Validation
    print("Validating unsupported countries...")
    # Ensure _temp_row_id exists for tracking (it should already be added at line 710)
    unsupported_countries_validation = None
    try:
        unsupported_countries_validation = validate_unsupported_countries(subscribedata, seller_name, is_sandbox)
    except Exception as e:
        print(f"Error during unsupported countries validation: {e}")
        # Fallback dictionary if error occurs
        fallback_dict = {
            'AF': '🇦🇫', 'AQ': '🇦🇶', 'BY': '🇧🇾', 'MM': '🇲🇲', 'CF': '🇨🇫', 'CU': '🇨🇺', 
            'CD': '🇨🇩', 'HT': '🇭🇹', 'IR': '🇮🇷', 'IQ': '🇮🇶', 'LY': '🇱🇾', 'ML': '🇲🇱', 'AN': '🇦🇳', 
            'NI': '🇳🇮', 'KP': '🇰🇵', 'RU': '🇷🇺', 'SO': '🇸🇴', 'SS': '🇸🇸', 'SD': '🇸🇩', 
            'SY': '🇸🇾', 'VE': '🇻🇪', 'YE': '🇾🇪', 'ZW': '🇿🇼'
        }
        validation_results.append({
            'valid': False,
            'step': 'unsupported_countries_validation',
            'error': f'Validation error: {str(e)}',
            'incorrect_count': 0,
            'total_records': 0,
            'download_file': None,
            'unsupported_countries': list(fallback_dict.keys()),
            'unsupported_countries_dict': fallback_dict
        })
    
    if unsupported_countries_validation:
        if not unsupported_countries_validation['valid']:
            print(f"Unsupported countries validation failed. Found {unsupported_countries_validation['incorrect_count']} records with unsupported country codes.")
            
            # Save incorrect records to a file for download
            download_file = None
            if unsupported_countries_validation['incorrect_records'] is not None:
                try:
                    output_dir = 'outputs'
                    os.makedirs(output_dir, exist_ok=True)
                    
                    # Create filename with seller name and environment
                    clean_seller_name = "".join(c for c in seller_name if c.isalnum() or c in (' ', '-', '_')).rstrip()
                    clean_seller_name = clean_seller_name.replace(' ', '_')
                    env_suffix = "_sandbox" if is_sandbox else "_production"
                    incorrect_filename = f"{clean_seller_name}_unsupported_countries{env_suffix}_{int(time.time())}.csv"
                    incorrect_path = os.path.join(output_dir, incorrect_filename)
                    unsupported_countries_validation['incorrect_records'].to_csv(incorrect_path, index=False)
                    download_file = incorrect_filename
                    print(f"Saved incorrect records to: {incorrect_path}")
                except Exception as e:
                    print(f"Error saving incorrect records file: {e}")
            
            merge_failed_temp_row_ids_into_set(failed_row_ids, unsupported_countries_validation)
            
            # Add failed validation to results but continue processing
            validation_results.append({
                'valid': False,
                'step': 'unsupported_countries_validation',
                'incorrect_count': unsupported_countries_validation['incorrect_count'],
                'total_records': unsupported_countries_validation['total_records'],
                'download_file': download_file,
                'unsupported_countries': unsupported_countries_validation.get('unsupported_countries', []),
                'unsupported_countries_dict': unsupported_countries_validation.get('unsupported_countries_dict', {})
            })
        else:
            print(f"Unsupported countries validation passed. All {unsupported_countries_validation['total_records']} records have supported country codes.")
            # Add successful unsupported countries validation to results
            validation_results.append({
                'valid': True,
                'step': 'unsupported_countries_validation',
                'total_records': unsupported_countries_validation['total_records'],
                'unsupported_countries': unsupported_countries_validation.get('unsupported_countries', []),
                'unsupported_countries_dict': unsupported_countries_validation.get('unsupported_countries_dict', {})
            })
    
    # Date format validation (for all providers) - must be before date period validation
    print("Validating date formats...")
    date_format_validation = None
    try:
        date_format_validation = validate_date_format(subscribedata, seller_name, is_sandbox)
    except Exception as e:
        print(f"Error during date format validation: {e}")
        validation_results.append({
            'valid': False,
            'step': 'date_format_validation',
            'error': f'Validation error: {str(e)}',
            'incorrect_count': 0,
            'total_records': 0,
            'download_file': None
        })
    
    if date_format_validation:
        if not date_format_validation['valid']:
            print(f"Date format validation failed. Found {date_format_validation['incorrect_count']} records with incorrect date formats.")
            
            # Save incorrect records to a file for download
            download_file = None
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
                    download_file = incorrect_filename
                    print(f"Saved incorrect records to: {incorrect_path}")
                except Exception as e:
                    print(f"Error saving incorrect records file: {e}")
            
            merge_failed_temp_row_ids_into_set(failed_row_ids, date_format_validation)
            
            # Add failed validation to results but continue processing
            validation_results.append({
                'valid': False,
                'step': 'date_format_validation',
                'incorrect_count': date_format_validation['incorrect_count'],
                'total_records': date_format_validation['total_records'],
                'download_file': download_file
            })
        else:
            print(f"Date format validation passed. All {date_format_validation['total_records']} date formats are valid.")
            # Add successful date format validation to results
            validation_results.append({
                'valid': True,
                'step': 'date_format_validation',
                'total_records': date_format_validation['total_records']
            })
    
    # Date period validation (for all providers)
    print("Validating date periods...")
    date_validation = None
    try:
        date_validation = validate_date_periods(subscribedata, seller_name, is_sandbox)
    except Exception as e:
        print(f"Error during date validation: {e}")
        validation_results.append({
                'valid': False,
            'step': 'date_validation',
                'error': f'Validation error: {str(e)}',
                'incorrect_count': 0,
                'total_records': 0,
                'download_file': None
        })
    
    if date_validation:
        if not date_validation['valid']:
            print(f"Date validation failed. Found {date_validation['incorrect_count']} records with invalid date periods.")
            
            # Save incorrect records to a file for download
            download_file = None
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
                    download_file = incorrect_filename
                    print(f"Saved incorrect records to: {incorrect_path}")
                except Exception as e:
                    print(f"Error saving incorrect records file: {e}")
            
            merge_failed_temp_row_ids_into_set(failed_row_ids, date_validation)
            
            # Add failed validation to results but continue processing
            validation_results.append({
                'valid': False,
                'step': 'date_validation',
                'incorrect_count': date_validation['incorrect_count'],
                'total_records': date_validation['total_records'],
                'download_file': download_file
            })
        else:
            print(f"Date validation passed. All {date_validation['total_records']} date periods are valid.")
            # Add successful date validation to results
            validation_results.append({
                'valid': True,
                'step': 'date_validation',
                'total_records': date_validation['total_records']
            })

    if subscriber_csv_check_only:
        _run_zip_validations_subscriber_check_only(
            subscribedata,
            validation_results,
            failed_row_ids,
            seller_name,
            is_sandbox,
            provider,
            autocorrect_us_zip,
        )
        print('Subscriber CSV check-only mode: skipping merge and mapping-dependent steps.')
        result = _return_subscriber_csv_check_only_result(validation_results, start_time)
        zip_bundle = _create_all_reports_zip_from_validation_downloads(
            validation_results,
            seller_name,
            subscriber_filename,
            provider,
            is_sandbox,
        )
        if zip_bundle:
            result['zip_file'] = zip_bundle
            result['output_files'] = [zip_bundle]
        return result

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
        # Include 'Zip Code' column if it exists in mapping data (needed for zip code validation)
        columns_to_keep = [
            'card_token',  # This is used for the merge
            'original_credit_card_number',  # Preserve the original credit card number
            'card_holder_name',
            'card_expiry_month',
            'card_expiry_year',
            'network_transaction_id'
        ]
        # Add 'Zip Code' if it exists in the mapping data
        if 'Zip Code' in mappingdata.columns:
            columns_to_keep.append('Zip Code')
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
        
        # Keep only rows where `card_token` is not null (from either side)
        finaljoin = finaljoin[finaljoin['card_token'].notna()]
        
        # Check for duplicate card_tokens BEFORE replacing with full card number
        # This identifies duplicates based on the original merge key (Account ID + last 4)
        duplicate_token_mask = finaljoin.duplicated(subset='card_token', keep=False)
        finaljoin['is_duplicate_token'] = duplicate_token_mask
        
        # Identify records without a match BEFORE replacing card_token
        # A record has no token if it doesn't have original_credit_card_number from mapping file
        # Store this info before we replace card_token
        no_token_mask = finaljoin['original_credit_card_number'].isna()
        
        # Replace `card_token` in the final DataFrame with the original `Credit Card Number` from the mapping data
        # Only replace for records that have a match (original_credit_card_number is not null)
        finaljoin.loc[finaljoin['original_credit_card_number'].notna(), 'card_token'] = \
            finaljoin.loc[finaljoin['original_credit_card_number'].notna(), 'original_credit_card_number']
        
        # For records without a match, set card_token to null so they can be identified later
        finaljoin.loc[no_token_mask, 'card_token'] = None
        
        # Drop the 'original_credit_card_number' column, as we no longer need it in the final output
        finaljoin = finaljoin.drop(columns=['original_credit_card_number'])

        drops_raw = compute_columns_originating_from_mapping_only(
            set(filtered_mappingdata.columns),
            set(subscribedata.columns),
            ('card_token',),
            set(finaljoin.columns),
        )
        completed = finaljoin
        _validation_report_exclude_columns = frozenset(drops_raw & set(completed.columns))
    
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

        drops_raw = compute_columns_originating_from_mapping_only(
            set(mappingdata.columns),
            set(subscribedata.columns),
            ('card_id',),
            set(finaljoin.columns),
        )
        stripe_mapping_columns_rename = {
            'card.number': 'card_token',
            'card.name': 'card_holder_name',
            'card.exp_month': 'card_expiry_month',
            'card.exp_year': 'card_expiry_year',
        }
        drops_after_rename = map_column_names_through_rename(drops_raw, stripe_mapping_columns_rename)
        
        # Filter null card_ids after merge (like original)
        finaljoin = finaljoin[finaljoin['card_id'].notna()]
        
        # Check for duplicate card_ids BEFORE renaming card.number to card_token
        # This identifies duplicates based on the original merge key (card_id)
        duplicate_token_mask = finaljoin.duplicated(subset='card_id', keep=False)
        finaljoin['is_duplicate_token'] = duplicate_token_mask
        
        # Rename columns as required (like original)
        completed = finaljoin.rename(columns=stripe_mapping_columns_rename)
        _validation_report_exclude_columns = frozenset(
            drops_after_rename & set(completed.columns)
        )
        
        completed['card_holder_name'] = completed['card_holder_name'].fillna(completed['customer_full_name'])
    
    # Missing Zip Code Validation (after merge, before column removal)
    print("Validating missing zip codes...")
    try:
        missing_zip_validation = validate_missing_zip_codes(completed, provider, seller_name, is_sandbox)
    except Exception as e:
        print(f"Error during missing zip code validation: {e}")
        return {
            'error': 'Missing zip code validation error',
            'validation_result': {
                'valid': False,
                'error': f'Validation error: {str(e)}',
                'missing_count': 0,
                'total_records': 0,
                'available_from_mapping': 0,
                'download_file': None
            },
            'step': 'missing_zip_code_validation',
            'validation_results': validation_results
        }
    
    if not missing_zip_validation['valid']:
        print(f"Missing zip code validation failed. Found {missing_zip_validation['missing_count']} missing zip codes.")
        print(f"Of these, {missing_zip_validation['available_from_mapping']} can be pulled from mapping file.")
        
        # Handle user choices for missing zip codes
        # Initialize updated_count to track how many records were pulled from mapping file
        updated_count = 0
        if use_mapping_zip_codes and missing_zip_validation['available_from_mapping'] > 0:
            print("User chose to use mapping zip codes. Pulling zip codes from mapping file...")
            
            # Get the missing records that can be fixed
            missing_records = missing_zip_validation['missing_records']
            
            if missing_records is not None and len(missing_records) > 0:
                # Determine the mapping column name based on provider
                mapping_column = 'card.address_zip' if provider.lower() == 'stripe' else 'Zip Code'
                
                # Check if the mapping column exists in the merged data
                if mapping_column not in completed.columns:
                    # Add error to validation results but continue processing
                    validation_results.append({
                            'valid': False,
                        'step': 'missing_zip_code_validation',
                            'error': f'Mapping column {mapping_column} not found in merged data',
                        'missing_count': missing_zip_validation['missing_count'],
                        'total_records': missing_zip_validation['total_records'],
                        'available_from_mapping': missing_zip_validation['available_from_mapping'],
                        'download_file': None
                    })
                else:
                    # Update the main dataset with zip codes from mapping
                    updated_count = 0
                    
                    # For each missing record, copy the zip code from the mapping column to address_postal_code
                    # Use the row index from missing_records (which corresponds to the row in completed)
                    for idx, row in missing_records.iterrows():
                        card_token = row['card_token']
                        mapping_zip_code = row[mapping_column]
                        
                        if pd.notna(mapping_zip_code) and str(mapping_zip_code).strip() != '':
                            # Basic cleaning: convert to string and strip whitespace
                            cleaned_zip_code = str(mapping_zip_code).strip()
                            # Remove .0 suffix if present (from float conversion) - safe for all zip codes
                            if cleaned_zip_code.endswith('.0'):
                                cleaned_zip_code = cleaned_zip_code.rstrip('.0')
                            
                            # Use the row index directly from missing_records (which corresponds to completed)
                            # This ensures each row gets its specific zip code from the mapping file
                            if idx in completed.index:
                                # Check if this is a US record for additional US-specific cleaning
                                if 'address_country_code' in completed.columns:
                                    raw_cc = completed.loc[idx, 'address_country_code']
                                    is_us_record = pd.notna(raw_cc) and str(raw_cc).strip().upper() == 'US'
                                else:
                                    is_us_record = False
                                
                                if is_us_record:
                                    # For US records only: handle ZIP+4 format and extract digits
                                    # Handle ZIP+4 format (e.g., "12345-6789" -> "12345")
                                    if '-' in cleaned_zip_code:
                                        cleaned_zip_code = cleaned_zip_code.split('-')[0]
                                    # For US, extract digits only (US zip codes are numeric)
                                    import re
                                    digits_only = re.sub(r'\D', '', cleaned_zip_code)
                                    if digits_only:
                                        cleaned_zip_code = digits_only
                                
                                # For non-US records, keep the zip code as-is (may contain letters, spaces, etc.)
                                completed.loc[idx, 'address_postal_code'] = cleaned_zip_code
                            updated_count += 1
                
                    print(f"Updated {updated_count} records with zip codes from mapping file.")
                
                    # Re-run the missing zip code validation
                    try:
                        missing_zip_validation = validate_missing_zip_codes(completed, provider, seller_name, is_sandbox)
                    except Exception as e:
                        print(f"Error during missing zip code validation after update: {e}")
                        validation_results.append({
                            'valid': False,
                            'step': 'missing_zip_code_validation',
                            'error': f'Validation error after update: {str(e)}',
                            'missing_count': 0,
                            'total_records': 0,
                            'available_from_mapping': 0,
                            'download_file': None,
                            'required_countries': ['AU', 'CA', 'FR', 'DE', 'IN', 'IT', 'NL', 'ES', 'GB', 'US']  # Fallback
                        })
                        missing_zip_validation = None
                    
                    if missing_zip_validation:
                        if missing_zip_validation['valid']:
                            print(f"Missing zip code validation passed after using mapping zip codes. All {missing_zip_validation['total_records']} records have zip codes.")
                            # Add successful missing zip code validation to results
                            validation_results.append({
                                'valid': True,
                                'step': 'missing_zip_code_validation',
                                'total_records': missing_zip_validation['total_records'],
                                'pulled_from_mapping_count': updated_count,
                                'required_countries': missing_zip_validation.get('required_countries', []),
                                'required_countries_dict': missing_zip_validation.get('required_countries_dict', {})
                            })
                        else:
                            # Still have missing zip codes after update - continue processing
                            print(f"Still have {missing_zip_validation['missing_count']} missing zip codes after using mapping zip codes.")
                            # Save error but continue - will be handled at end
                            download_file = None
                            if missing_zip_validation['missing_records'] is not None:
                                try:
                                    output_dir = 'outputs'
                                    os.makedirs(output_dir, exist_ok=True)
                                    clean_seller_name = "".join(c for c in seller_name if c.isalnum() or c in (' ', '-', '_')).rstrip()
                                    clean_seller_name = clean_seller_name.replace(' ', '_')
                                    env_suffix = "_sandbox" if is_sandbox else "_production"
                                    missing_filename = f"{clean_seller_name}_missing_zip_codes{env_suffix}_{int(time.time())}.csv"
                                    missing_path = os.path.join(output_dir, missing_filename)
                                    missing_zip_validation['missing_records'].to_csv(missing_path, index=False)
                                    download_file = missing_filename
                                except Exception as e:
                                    print(f"Error saving missing records file: {e}")
                            
                            merge_failed_temp_row_ids_into_set(failed_row_ids, missing_zip_validation)
                            
                            validation_results.append({
                                'valid': False,
                                'step': 'missing_zip_code_validation',
                                'missing_count': missing_zip_validation['missing_count'],
                                'total_records': missing_zip_validation['total_records'],
                                'available_from_mapping': missing_zip_validation.get('available_from_mapping', 0),
                                'pulled_from_mapping_count': updated_count,
                                'download_file': download_file,
                                'required_countries': missing_zip_validation.get('required_countries', []),
                                'required_countries_dict': missing_zip_validation.get('required_countries_dict', {})
                            })
            else:
                print("No missing records found to update.")
                # Continue with the existing error handling logic below
        
        else:
            # User hasn't made a choice yet - save error but continue processing
            # Save missing records to a file for download
            download_file = None
            if missing_zip_validation['missing_records'] is not None:
                try:
                    output_dir = 'outputs'
                    os.makedirs(output_dir, exist_ok=True)
                    
                    clean_seller_name = "".join(c for c in seller_name if c.isalnum() or c in (' ', '-', '_')).rstrip()
                    clean_seller_name = clean_seller_name.replace(' ', '_')
                    env_suffix = "_sandbox" if is_sandbox else "_production"
                    missing_filename = f"{clean_seller_name}_missing_postal_codes{env_suffix}_{int(time.time())}.csv"
                    missing_path = os.path.join(output_dir, missing_filename)
                    missing_zip_validation['missing_records'].to_csv(missing_path, index=False)
                    download_file = missing_filename
                    print(f"Saved missing records to: {missing_path}")
                except Exception as e:
                    print(f"Error saving missing records file: {e}")
            
            before_missing_zip_merge = len(failed_row_ids)
            merge_failed_temp_row_ids_into_set(failed_row_ids, missing_zip_validation)
            added_missing = len(failed_row_ids) - before_missing_zip_merge
            if added_missing:
                print(f"Collected {added_missing} failed row IDs from missing zip code validation")
            
            # Add failed validation to results but continue processing
            validation_results.append({
                'valid': False,
                'step': 'missing_zip_code_validation',
                'missing_count': missing_zip_validation['missing_count'],
                'total_records': missing_zip_validation['total_records'],
                'available_from_mapping': missing_zip_validation['available_from_mapping'],
                'pulled_from_mapping_count': 0,  # No records pulled since checkbox not checked
                'download_file': download_file,
                'required_countries': missing_zip_validation.get('required_countries', []),
                'required_countries_dict': missing_zip_validation.get('required_countries_dict', {})
            })
    else:
        print(f"Missing zip code validation passed. All {missing_zip_validation['total_records']} records have zip codes.")

        # Add successful missing zip code validation to results
        validation_results.append({
            'valid': True,
            'step': 'missing_zip_code_validation',
            'total_records': missing_zip_validation['total_records'],
            'pulled_from_mapping_count': 0,  # No records pulled since validation passed without action
            'required_countries': missing_zip_validation.get('required_countries', []),
            'required_countries_dict': missing_zip_validation.get('required_countries_dict', {})
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
        ] + ordered_subscription_custom_data_columns(completed.columns) + ordered_price_id_quantity_columns(completed.columns) + [
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
        ] + ordered_subscription_custom_data_columns(completed.columns) + ordered_price_id_quantity_columns(completed.columns) + [
            'vault_provider'
        ]
    
    # Remove columns that exist in the dataframe
    columns_to_remove = [col for col in columns_to_remove if col in completed.columns]
    completed = completed.drop(columns=columns_to_remove)
    
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
        # Preserve _temp_row_id if it exists (needed for tracking failed records)
        if '_temp_row_id' in completed.columns and '_temp_row_id' not in existing_columns:
            existing_columns.append('_temp_row_id')
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
        # Preserve _temp_row_id if it exists (needed for tracking failed records)
        if '_temp_row_id' in completed.columns and '_temp_row_id' not in existing_columns:
            existing_columns.append('_temp_row_id')
        completed = completed[existing_columns]
    
    completed = completed[completed['customer_email'].notna()]
    
    # Detect duplicate emails BEFORE anonymization (so we can catch real duplicates)
    # Store this for later use - we'll use this directly for reporting
    # Skip duplicate email detection when we will anonymise (emails become unique)
    if anonymise_emails:
        duplicate_emails_before_anonymization = pd.DataFrame()
    else:
        duplicate_emails_before_anonymization = completed[completed.duplicated(subset='customer_email', keep=False)].copy()
    
    # Optional email anonymization (sandbox only, when toggle enabled)
    if anonymise_emails:
        # Generate random emails to anonymize data (only emails, keep real names)
        completed['customer_email'] = completed['customer_email'].apply(lambda x: generate_random_email())
        print("Email addresses anonymized for sandbox")
    
    print("Processing date formatting...")
    # Keep original date format - no parsing or reformatting needed
    print("Date columns left in original format")
    
    print("Adding vault_provider column...")
    # Add vault_provider column
    # For TokenEx, ensure it's always lowercase
    if vault_provider.lower() == 'tokenex':
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
    
    # CA Zip Code Validation
    print("Validating Canadian zip codes...")
    ca_zip_validation = None
    try:
        ca_zip_validation = validate_ca_zip_codes(completed, seller_name, is_sandbox)
    except Exception as e:
        print(f"Error during CA zip code validation: {e}")
        validation_results.append({
                'valid': False,
            'step': 'ca_zip_code_validation',
                'error': f'Validation error: {str(e)}',
                'incorrect_count': 0,
                'total_records': 0,
                'download_file': None
        })
    
    if ca_zip_validation:
        if not ca_zip_validation['valid']:
            print(f"CA zip code validation failed. Found {ca_zip_validation['incorrect_count']} incorrect formats.")
            
            # Save incorrect records to a file for download
            download_file = None
            if ca_zip_validation['incorrect_records'] is not None:
                try:
                    output_dir = 'outputs'
                    os.makedirs(output_dir, exist_ok=True)
                    
                    clean_seller_name = "".join(c for c in seller_name if c.isalnum() or c in (' ', '-', '_')).rstrip()
                    clean_seller_name = clean_seller_name.replace(' ', '_')
                    env_suffix = "_sandbox" if is_sandbox else "_production"
                    incorrect_filename = f"{clean_seller_name}_invalid_ca_zip_codes{env_suffix}_{int(time.time())}.csv"
                    incorrect_path = os.path.join(output_dir, incorrect_filename)
                    ca_zip_validation['incorrect_records'].to_csv(incorrect_path, index=False)
                    download_file = incorrect_filename
                    print(f"Saved incorrect records to: {incorrect_path}")
                except Exception as e:
                    print(f"Error saving incorrect records file: {e}")
                
                merge_failed_temp_row_ids_into_set(failed_row_ids, ca_zip_validation)
            
            # Add failed validation to results but continue processing
            validation_results.append({
                'valid': False,
                'step': 'ca_zip_code_validation',
                'incorrect_count': ca_zip_validation['incorrect_count'],
                'total_records': ca_zip_validation['total_records'],
                'download_file': download_file
            })
        else:
            print(f"CA zip code validation passed. All {ca_zip_validation['total_records']} Canadian zip codes are correctly formatted.")
            
            # Add successful CA zip code validation to results
            validation_results.append({
                'valid': True,
                'step': 'ca_zip_code_validation',
                'total_records': ca_zip_validation['total_records']
            })
    
    # US Zip Code Validation
    print("Validating US zip codes...")
    us_zip_validation = None
    try:
        us_zip_validation = validate_us_zip_codes(completed, seller_name, is_sandbox)
    except Exception as e:
        print(f"Error during US zip code validation: {e}")
        validation_results.append({
                'valid': False,
            'step': 'us_zip_code_validation',
                'error': f'Validation error: {str(e)}',
                'incorrect_count': 0,
                'total_records': 0,
                'download_file': None,
                'autocorrectable_count': 0
        })
    
    if us_zip_validation:
        if not us_zip_validation['valid']:
            print(f"US zip code validation failed. Found {us_zip_validation['incorrect_count']} incorrect formats.")
            print(f"Of these, {us_zip_validation['autocorrectable_count']} can be autocorrected with leading zeros.")
        
            autocorrected_count = 0
            if autocorrect_us_zip and us_zip_validation['autocorrectable_count'] > 0:
                print("Autocorrecting 4-digit US zip codes with leading zeros...")
                autocorrected_count = _apply_us_zip_leading_zero_autocorrect(completed)
                print(f"Autocorrected {autocorrected_count} US zip codes.")
                us_zip_validation = validate_us_zip_codes(completed, seller_name, is_sandbox)

            # Save incorrect records to a file for download (whether autocorrected or not)
            download_file = None
            if us_zip_validation and not us_zip_validation['valid'] and us_zip_validation['incorrect_records'] is not None:
                try:
                    output_dir = 'outputs'
                    os.makedirs(output_dir, exist_ok=True)
                    
                    clean_seller_name = "".join(c for c in seller_name if c.isalnum() or c in (' ', '-', '_')).rstrip()
                    clean_seller_name = clean_seller_name.replace(' ', '_')
                    env_suffix = "_sandbox" if is_sandbox else "_production"
                    filename_suffix = "_after_autocorrect" if autocorrected_count > 0 else ""
                    incorrect_filename = f"{clean_seller_name}_invalid_us_zip_codes{filename_suffix}{env_suffix}_{int(time.time())}.csv"
                    incorrect_path = os.path.join(output_dir, incorrect_filename)
                    us_zip_validation['incorrect_records'].to_csv(incorrect_path, index=False)
                    download_file = incorrect_filename
                    print(f"Saved incorrect records to: {incorrect_path}")
                except Exception as e:
                        print(f"Error saving incorrect records file: {e}")
            
            # Add validation result (failed or passed after autocorrect)
            if us_zip_validation and us_zip_validation['valid']:
                print("US zip code validation passed after autocorrection.")
                validation_results.append({
                    'valid': True,
                    'step': 'us_zip_code_validation',
                    'total_records': us_zip_validation.get('total_records', 0),
                    'autocorrected_count': int(autocorrected_count)
                })
            else:
                # Still have invalid codes (either no autocorrect or autocorrect didn't fix everything)
                if us_zip_validation:
                    print(f"US zip code validation failed. Found {us_zip_validation['incorrect_count']} incorrect formats.")
                    merge_failed_temp_row_ids_into_set(failed_row_ids, us_zip_validation)
                
                # Add failed validation to results but continue processing
                validation_results.append({
                    'valid': False,
                    'step': 'us_zip_code_validation',
                    'incorrect_count': us_zip_validation.get('incorrect_count', 0) if us_zip_validation else 0,
                    'total_records': us_zip_validation.get('total_records', 0) if us_zip_validation else 0,
                    'download_file': download_file,
                    'autocorrectable_count': us_zip_validation.get('autocorrectable_count', 0) if us_zip_validation else 0,
                    'autocorrected_count': int(autocorrected_count)
                })
        else:
            print(f"US zip code validation passed. All {us_zip_validation['total_records']} US zip codes are correctly formatted.")
            
            # Add successful US zip code validation to results
            validation_results.append({
                'valid': True,
                'step': 'us_zip_code_validation',
                'autocorrected_count': 0,  # No records autocorrected since validation passed without action
                'total_records': us_zip_validation['total_records']
            })
    
    print("Starting duplicate detection...")
    
    # Detect ALL duplicates BEFORE removing failed records (so we catch all duplicates even if some are removed)
    
    # Find all rows where card_token appears more than once
    # For both Bluesnap and Stripe: use the flag set before card_token was replaced/renamed
    # This checks duplicates based on the original merge key, not the final card_token value
    if 'is_duplicate_token' in completed.columns:
        duplicate_tokens_before_removal = completed[completed['is_duplicate_token'] == True].copy()
        # Drop the flag column from duplicate_tokens but keep it in completed for now
        if 'is_duplicate_token' in duplicate_tokens_before_removal.columns:
            duplicate_tokens_before_removal = duplicate_tokens_before_removal.drop(columns=['is_duplicate_token'])
    else:
        # Fallback: check duplicates in card_token (shouldn't happen with current logic)
        duplicate_tokens_before_removal = completed[completed['card_token'].notna() & completed.duplicated(subset='card_token', keep=False)].copy()
    print(f"Duplicate tokens records (before removal): {len(duplicate_tokens_before_removal)}")
    
    # Find all rows where card_id appears more than once (only for Stripe) - BEFORE removal
    duplicate_card_ids_before_removal = pd.DataFrame()
    if provider.lower() == 'stripe' and 'card_id' in completed.columns:
        duplicate_card_ids_before_removal = completed[completed['card_id'].notna() & completed.duplicated(subset='card_id', keep=False)].copy()
        print(f"Duplicate card IDs records (before removal): {len(duplicate_card_ids_before_removal)}")
    
    # Find all rows where subscription_external_id appears more than once - BEFORE removal
    duplicate_external_subscription_ids_before_removal = completed[completed.duplicated(subset='subscription_external_id', keep=False)].copy()
    print(f"Duplicate external subscription IDs records (before removal): {len(duplicate_external_subscription_ids_before_removal)}")
    
    # Identify no_tokens before removal (for reporting)
    no_tokens = completed[completed['card_token'].isnull()]
    print(f"No tokens records: {len(no_tokens)}")
    
    # Drop is_duplicate_token flag from completed now that we've saved duplicates
    if 'is_duplicate_token' in completed.columns:
        completed = completed.drop(columns=['is_duplicate_token'])
    
    # Collect failed _temp_row_id values from no_tokens
    if len(no_tokens) > 0 and '_temp_row_id' in no_tokens.columns:
        # Ensure _temp_row_id is numeric
        temp_ids = pd.to_numeric(no_tokens['_temp_row_id'], errors='coerce').dropna()
        failed_ids = [int(x) for x in temp_ids if pd.notna(x)]
        failed_row_ids.update(failed_ids)
    
    # Remove all failed records from completed (records that failed any validation or have no token)
    if len(failed_row_ids) > 0:
        print(f"Removing {len(failed_row_ids)} records that failed validation or have no token...")
        print(f"Failed row IDs to remove: {sorted(list(failed_row_ids))[:20]}...")  # Show first 20
        if '_temp_row_id' in completed.columns:
            print(f"Total records in completed before removal: {len(completed)}")
            print(f"_temp_row_id column type: {completed['_temp_row_id'].dtype}")
            print(f"Sample _temp_row_id values: {completed['_temp_row_id'].head(10).tolist()}")
            # Ensure _temp_row_id is numeric for comparison
            if completed['_temp_row_id'].dtype == 'object':
                # Convert from string if needed
                completed['_temp_row_id'] = pd.to_numeric(completed['_temp_row_id'], errors='coerce')
            completed = completed[~completed['_temp_row_id'].isin(failed_row_ids)]
            print(f"Remaining records after removal: {len(completed)}")
        else:
            print("ERROR: _temp_row_id column not found in completed DataFrame, cannot remove failed records")
            print(f"Available columns: {completed.columns.tolist()}")
    
    # Recalculate success after removing failed records
    # Successfully mapped records are those that remain in completed and have a card_token
    success = completed[completed['card_token'].notna()].copy()
    print(f"Successfully mapped records: {len(success)}")
    
    # Remove _temp_row_id from success before saving (it's only for tracking)
    if '_temp_row_id' in success.columns:
        success = success.drop(columns=['_temp_row_id'])
    
    # Use the duplicate detections from before removal for reporting
    # This ensures we show all duplicates even if some records were removed due to validation failures
    duplicate_tokens = duplicate_tokens_before_removal
    duplicate_card_ids = duplicate_card_ids_before_removal
    duplicate_external_subscription_ids = duplicate_external_subscription_ids_before_removal
    print(f"Using duplicate detections from before removal for reporting")
    
    # Duplicate email detection - skip when emails were anonymized (they become unique)
    if anonymise_emails:
        duplicate_emails = pd.DataFrame()
        duplicate_emails_for_report = pd.DataFrame()
    else:
        # In production, detect duplicate emails
        # We use the pre-anonymization detection because we want to show duplicates even if some records were removed due to validation failures
        # Map the duplicate_emails_before_anonymization to current records using _temp_row_id
        if len(duplicate_emails_before_anonymization) > 0 and '_temp_row_id' in duplicate_emails_before_anonymization.columns and '_temp_row_id' in completed.columns:
            # Find records in completed that match the _temp_row_id from duplicate_emails_before_anonymization
            # This gives us the duplicate records that are still in completed (not removed by validation)
            duplicate_emails = completed[completed['_temp_row_id'].isin(duplicate_emails_before_anonymization['_temp_row_id'])]
            print(f"Duplicate emails records (mapped to current records): {len(duplicate_emails)}")
        else:
            # Fallback: try to detect again
            duplicate_emails = completed[completed.duplicated(subset='customer_email', keep=False)]
            print(f"Duplicate emails records (detected after validation): {len(duplicate_emails)}")
        
        # For reporting purposes, we want to show ALL duplicates that were detected before anonymization
        # even if some were removed due to validation failures
        # So we'll use duplicate_emails_before_anonymization for the report file
        duplicate_emails_for_report = duplicate_emails_before_anonymization.copy()
    
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
    # Use all duplicate detections from BEFORE removal/anonymization for reporting
    # This ensures we show all duplicates even if some records were removed due to validation failures
    files_to_save = [
        (success, f'{base_filename}_final_import.csv'),
        (no_tokens, f'{base_filename}_no_token_found.csv'),
        (duplicate_tokens_before_removal, f'{base_filename}_duplicate_tokens.csv'),
        (duplicate_external_subscription_ids_before_removal, f'{base_filename}_duplicate_external_subscription_ids.csv'),
        (duplicate_emails_for_report, f'{base_filename}_duplicate_emails.csv')
    ]
    
    # Add duplicate card IDs file only for Stripe
    if provider.lower() == 'stripe' and not duplicate_card_ids.empty:
        files_to_save.append((duplicate_card_ids, f'{base_filename}_duplicate_card_ids.csv'))
    
    for df, filename in files_to_save:
        if not df.empty:
            file_path = os.path.join(output_dir, filename)
            print(f"Saving file: {file_path}")
            
            # Final import keeps full merged columns for migration; other CSVs match subscriber headers + card_token
            if filename.endswith('_final_import.csv'):
                df_string = clean_dataframe_for_csv(df)
            else:
                df_string = clean_dataframe_for_validation_report_csv(df)
            
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
    
    # Collect all files from validation_results to include in zip
    validation_files_to_zip = []
    for validation in validation_results:
        if 'download_file' in validation and validation['download_file']:
            validation_files_to_zip.append(validation['download_file'])
    
    # Create zip file with all reports (always create if there are any files)
    all_files_to_zip = output_files + [{'name': f} for f in validation_files_to_zip if f not in [of['name'] for of in output_files]]
    
    if all_files_to_zip:  # Create zip if there are any files to include
        zip_filename = f'{base_filename}_all_reports.zip'
        zip_path = os.path.join(output_dir, zip_filename)
        
        try:
            with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                # Add files from output_files
                for file_info in output_files:
                    file_path = os.path.join(output_dir, file_info['name'])
                    if os.path.exists(file_path):
                        zipf.write(file_path, file_info['name'])
                        print(f"Added {file_info['name']} to zip file")
                
                # Add files from validation_results
                for filename in validation_files_to_zip:
                    file_path = os.path.join(output_dir, filename)
                    if os.path.exists(file_path) and filename not in [of['name'] for of in output_files]:
                        zipf.write(file_path, filename)
                        print(f"Added {filename} to zip file")
            
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
    
    # Add duplicate detection results to validation_results (as warnings)
    # These should be shown even if validation errors occur
    # Use the before_removal versions to show all duplicates detected
    if len(duplicate_tokens_before_removal) > 0:
        duplicate_tokens_filename = f'{base_filename}_duplicate_tokens.csv'
        validation_results.append({
            'valid': True,  # Not a failure, just a warning
            'step': 'duplicate_tokens',
            'type': 'warning',
            'count': len(duplicate_tokens_before_removal),
            'download_file': duplicate_tokens_filename,
            'message': f'Found {len(duplicate_tokens_before_removal)} records with duplicate card tokens.'
        })
    
    if len(duplicate_external_subscription_ids_before_removal) > 0:
        duplicate_external_ids_filename = f'{base_filename}_duplicate_external_subscription_ids.csv'
        validation_results.append({
            'valid': True,
            'step': 'duplicate_external_subscription_ids',
            'type': 'warning',
            'count': len(duplicate_external_subscription_ids_before_removal),
            'download_file': duplicate_external_ids_filename,
            'message': f'Found {len(duplicate_external_subscription_ids_before_removal)} records with duplicate external subscription IDs.'
        })
    
    # Use duplicate_emails_for_report count for the validation result (shows all duplicates detected)
    if len(duplicate_emails_for_report) > 0:
        duplicate_emails_filename = f'{base_filename}_duplicate_emails.csv'
        validation_results.append({
            'valid': True,
            'step': 'duplicate_emails',
            'type': 'warning',
            'count': len(duplicate_emails_for_report),
            'download_file': duplicate_emails_filename,
            'message': f'Found {len(duplicate_emails_for_report)} records with duplicate customer emails.'
        })
    
    if provider.lower() == 'stripe' and len(duplicate_card_ids_before_removal) > 0:
        duplicate_card_ids_filename = f'{base_filename}_duplicate_card_ids.csv'
        validation_results.append({
            'valid': True,
            'step': 'duplicate_card_ids',
            'type': 'warning',
            'count': len(duplicate_card_ids_before_removal),
            'download_file': duplicate_card_ids_filename,
            'message': f'Found {len(duplicate_card_ids_before_removal)} records with duplicate card IDs.'
        })
    
    # Add no_tokens as a validation box (always show, even if count is 0)
    no_tokens_filename = f'{base_filename}_no_token_found.csv'
    validation_results.append({
        'valid': len(no_tokens) == 0,  # Valid if no records have missing tokens
        'step': 'no_token_found',
        'type': 'error' if len(no_tokens) > 0 else 'success',
        'count': len(no_tokens),
        'download_file': no_tokens_filename if len(no_tokens) > 0 else None,
        'message': f'Found {len(no_tokens)} records with no matching token in mapping file.' if len(no_tokens) > 0 else 'All records have matching tokens in mapping file.'
    })
    
    # Add successfully mapped records as a validation box (always show, including zero mapped)
    if len(success) > 0:
        success_filename = f'{base_filename}_final_import.csv'
        validation_results.append({
            'valid': True,
            'step': 'successfully_mapped_records',
            'type': 'success',
            'count': len(success),
            'download_file': success_filename,
            'message': f'Successfully mapped {len(success)} records ready for import.'
        })
    else:
        validation_results.append({
            'valid': False,
            'step': 'successfully_mapped_records',
            'type': 'error',
            'count': 0,
            'download_file': None,
            'message': 'Unable to successfully map any records.',
        })
    
    processing_time = time.time() - start_time
    
    # Check if any validations failed - if so, stop and return all errors
    failed_validations = [v for v in validation_results if not v.get('valid', True)]
    if failed_validations:
        print(f"Processing stopped due to {len(failed_validations)} validation failure(s).")
        clean_validation_results = _clean_validation_results_for_response(validation_results)
        
        # Find zip file in output_files if it exists
        zip_file_info = None
        for file_info in output_files:
            if file_info.get('is_zip', False):
                zip_file_info = file_info
                break
        
        return {
            'error': 'Validation failures detected',
            'validation_results': clean_validation_results,
            'failed_count': len(failed_validations),
            'zip_file': zip_file_info,  # Include zip file info even when validations fail
            'output_files': output_files  # Also include all output files for consistency
        }
    
    # Prepare results
    results = {
        'success_count': len(success),
        'no_tokens_count': len(no_tokens),
        'duplicate_tokens_count': len(duplicate_tokens_before_removal),
        'duplicate_external_subscription_ids_count': len(duplicate_external_subscription_ids_before_removal),
        'duplicate_emails_count': len(duplicate_emails_for_report),
        'total_processed': len(completed),
        'processing_time': f"{processing_time:.2f} seconds",
        'output_files': output_files,
        'environment': 'Sandbox' if is_sandbox else 'Production',
        'validation_results': validation_results
    }
    
    # Add duplicate card IDs count only for Stripe
    if provider.lower() == 'stripe':
        results['duplicate_card_ids_count'] = len(duplicate_card_ids_before_removal)
    
    print('Success')
    return results

# For direct script execution (backward compatibility)
if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 4:
        print("Usage: python migration-import-unified.py <subscriber_file> <mapping_file> <vault_provider> [--sandbox] [--anonymise-email]")
        sys.exit(1)
    
    subscriber_file = sys.argv[1]
    mapping_file = sys.argv[2]
    vault_provider = sys.argv[3]
    is_sandbox = '--sandbox' in sys.argv
    anonymise_email = '--anonymise-email' in sys.argv
    
    results = process_migration(subscriber_file, mapping_file, vault_provider, is_sandbox, anonymise_email=anonymise_email)
    print(f"Processing complete. Results: {results}") 