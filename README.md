# Paddle Migration Tool

A React-based frontend application for migrating data from Stripe or Bluesnap to Paddle Billing, with support for both production and sandbox environments.

## 🚀 Quick Start

**Get up and running in 3 steps:**

1. **Clone the repository** (if you haven't already):
   ```bash
   git clone https://github.com/calum-paddle/migration_mapping_tool.git
   cd migration_mapping_tool
   ```

2. **Run the setup script**:
   ```bash
   python3 setup.py
   ```
   This will create a virtual environment and install all dependencies.

3. **Start the application**:
   ```bash
   python3 start.py
   ```

That's it! The application will be available at:
- **Frontend**: http://localhost:3000 (opens automatically)
- **Backend API**: http://localhost:5001

### Prerequisites

Before starting, make sure you have:
- **Python 3.7+** (with pip)
- **Node.js 14+** (with npm)

The setup script will check these for you automatically.

## Features

- **Modern React Interface**: Clean, responsive UI inspired by Paddle Billing design
- **Dual Environment Support**: Toggle between production and sandbox modes
- **File Upload**: Drag-and-drop CSV file upload functionality
- **Data Processing**: Unified Python backend for data migration
- **Duplicate Detection**: Comprehensive duplicate detection across multiple fields (tokens, card IDs, subscription IDs, emails)
- **Data Anonymization**: Automatic email anonymization for sandbox mode
- **Comprehensive Validation**: Column, unsupported countries, date format, date period, and zip code validation with downloadable error reports
- **Zip Code Handling**: Options to use mapping file zip codes and autocorrect US zip codes with leading zeros via checkboxes
- **Collapsible UI**: Validation boxes can be collapsed/expanded for better organization
- **Results Summary**: Detailed processing results and statistics displayed on a single page
- **File Downloads**: Easy download of individual reports or all reports in a zip file
- **Large File Support**: Supports files up to 2GB (approximately 1 million records)

## Project Structure

```
├── src/
│   ├── components/
│   │   ├── FileUpload.js          # File upload interface
│   │   ├── ProcessingStatus.js    # Processing progress indicator
│   │   ├── ResultsSummary.js      # Results display
│   │   └── DownloadLinks.js       # Download links
│   ├── App.js                     # Main application component
│   └── index.js                   # React entry point
├── migration-import-unified.py    # Unified Python migration script
├── package.json                   # React dependencies
└── README.md                      # This file
```

## Installation

### Automatic Setup (Recommended)

The setup script handles everything automatically:

```bash
python3 setup.py
```

This will:
- ✅ Check Python and Node.js versions
- ✅ Create a Python virtual environment (`venv/`)
- ✅ Install all Python dependencies in the virtual environment
- ✅ Install all Node.js dependencies

**Note:** The virtual environment keeps dependencies isolated from your system Python, following Python best practices.

### Manual Installation

If you prefer to set up manually:

1. **Create and activate virtual environment**:

   ```bash
   python3 -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

2. **Install Python dependencies**:

   ```bash
   pip install -r requirements.txt
   ```

3. **Install Node.js dependencies**:

   ```bash
   npm install
   ```

### Troubleshooting

#### "pip: command not found" Error

If you get this error, try these solutions:

**macOS:**

```bash
# Using Homebrew
brew install python

# Or download from python.org
```

**Ubuntu/Debian:**

```bash
sudo apt-get update
sudo apt-get install python3-pip
```

**Windows:**

- Download Python from [python.org](https://www.python.org/downloads/)
- Ensure "Add Python to PATH" is checked during installation

**Alternative pip commands:**

```bash
# Try these if 'pip' doesn't work:
pip3 install -r requirements.txt
python -m pip install -r requirements.txt
python3 -m pip install -r requirements.txt
```

#### Virtual Environment Issues

If you encounter issues with the virtual environment:

**Recreate the virtual environment:**

```bash
rm -rf venv  # On Windows: rmdir /s venv
python3 setup.py
```

**Check if venv is being used:**

The `start.py` script automatically detects and uses the venv if it exists. You can verify by checking if `venv/` directory exists in the project root.

## Usage

### Starting the Application

**Recommended: Use the startup script**

```bash
python3 start.py
```

This automatically:
- Uses the virtual environment Python interpreter (if venv exists)
- Starts the Flask backend server at `http://localhost:5001`
- Waits for the backend to be ready
- Starts the React frontend at `http://localhost:3000`
- Opens the frontend in your browser

**Manual startup** (if needed):

1. **Activate virtual environment** (if using manual setup):

   ```bash
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

2. **Start the Flask backend** (in one terminal):

   ```bash
   python3 server.py
   ```

   The backend will run at `http://localhost:5001`

3. **Start the React frontend** (in another terminal):
   ```bash
   npm start
   ```
   The frontend will open at `http://localhost:3000`

### Using the Migration Tool

1. **Select Provider**: Choose between Stripe or Bluesnap
2. **Select Environment**: Toggle between Production and Sandbox modes
3. **Enter Vault Provider**: Provide the name of your vault provider
4. **Enter Seller Name**: Provide the seller name for file naming
5. **Configure Options** (optional):
   - **Use Mapping ZIP Codes**: If checked, missing zip codes will be pulled from the mapping file if available
   - **Autocorrect US ZIP codes leading zeros**: If checked, 4-digit US zip codes will have a leading zero added
6. **Upload Files**:
   - **Subscriber Export File**: CSV file containing subscriber data from Stripe or Bluesnap
   - **Mapping File**: CSV file containing mapping data (TokenEx for Stripe, or Bluesnap mapping file)
7. **Process Migration**: Click "Process Migration" to start the data processing
8. **Review Results**: All validation results, duplicate warnings, and successfully mapped records are displayed on one page with downloadable reports

### Environment Modes

#### Production Mode

- Processes real customer data
- Uses actual payment tokens
- Generates production-ready output files
- Comprehensive duplicate detection

#### Sandbox Mode

- Anonymizes customer email addresses (names are preserved)
- Uses test tokens for processing
- Adds "\_sandbox" suffix to output filenames
- Duplicate email detection is skipped (since emails are anonymized and become unique)
- All other duplicate detection (tokens, card IDs, subscription IDs) works the same as production

## Validation Checks

The migration process performs comprehensive validation checks in the following order:

1. **Column Validation**: Ensures all required columns are present
2. **Unsupported Countries Validation**: Checks that `address_country_code` does not contain unsupported countries (AF, AQ, BY, MM, CF, CU, CD, HT, IR, LY, ML, AN, NI, KP, RU, SO, SS, SD, SY, VE, YE, ZW)
3. **Date Format Validation**: Validates that `current_period_started_at` and `current_period_ends_at` are in ISO 8601 format (YYYY-MM-DDTHH:MM:SSZ)
4. **Date Period Validation**: Ensures date periods are logical (`current_period_started_at` dates should not be in the future, `current_period_ends_at` dates should not be in the past)
5. **Missing Zip Code Validation**: Checks for missing zip codes in required countries (AU, CA, FR, DE, IN, IT, NL, ES, GB, US). Can optionally pull missing zip codes from the mapping file if the checkbox is enabled
6. **Canadian Zip Code Validation**: Validates Canadian zip code format (Letter-Number-Letter Number-Letter-Number)
7. **US Zip Code Validation**: Validates US zip code format (exactly 5 digits, with optional autocorrection for 4-digit codes if the checkbox is enabled)
8. **No Token Found**: Identifies records with no matching token in the mapping file
9. **Successfully Mapped Records**: Shows the final count of records ready for import

All validation results are displayed on a single page with downloadable error reports. Validation boxes are collapsible (except when they pass without additional content). A "Download All Reports" button provides a zip file containing all validation failures, duplicate warnings, no token found records, and successfully mapped records.

**Note**: If any validation check fails, those records are removed from the final successful export. All validation checks run on the complete dataset before any records are removed, so records with multiple validation failures will be identified correctly.

## Duplicate Detection

In addition to validation checks, the migration process also performs duplicate detection. These are displayed as **warnings** (yellow boxes) on the same page as validation results and do **not** cause records to be excluded from the final export:

- **Duplicate Tokens**: Records with duplicate `card_token` values
- **Duplicate Card IDs**: Records with duplicate card IDs (Stripe only)
- **Duplicate External Subscription IDs**: Records with duplicate `subscription_external_id` values
- **Duplicate Emails**: Records with duplicate `customer_email` values (Production only, skipped in Sandbox mode since emails are anonymized)

Duplicate detection runs even if validation checks fail, allowing you to see all potential issues at once. Downloadable reports are available for each duplicate type.

## Configuration

### Modifying Unsupported Countries

To change the list of unsupported countries that will cause validation to fail, edit the `unsupported_countries_dict` dictionary in the `validate_unsupported_countries()` function in `migration-import-unified.py`.

**Location**: `migration-import-unified.py`, line ~160

**Example**:
```python
unsupported_countries_dict = {
    'AF': '🇦🇫', 'AQ': '🇦🇶', 'BY': '🇧🇾', 'MM': '🇲🇲', 'CF': '🇨🇫', 'CU': '🇨🇺', 
    'CD': '🇨🇩', 'HT': '🇭🇹', 'IR': '🇮🇷', 'LY': '🇱🇾', 'ML': '🇲🇱', 'AN': '🇦🇳', 
    'NI': '🇳🇮', 'KP': '🇰🇵', 'RU': '🇷🇺', 'SO': '🇸🇴', 'SS': '🇸🇸', 'SD': '🇸🇩', 
    'SY': '🇸🇾', 'VE': '🇻🇪', 'YE': '🇾🇪', 'ZW': '🇿🇼'
}
```

**To add a new unsupported country**:
1. Add a new key-value pair: `'XX': '🇽🇽'` (where `XX` is the ISO country code and `🇽🇽` is the flag emoji)
2. The country code will automatically be used for validation
3. The flag will automatically appear in the frontend validation display

**To remove a country**:
1. Simply delete the key-value pair from the dictionary

### Modifying Required Zip Code Countries

To change the list of countries that require zip codes, edit the `required_countries_dict` dictionary in the `validate_missing_zip_codes()` function in `migration-import-unified.py`.

**Location**: `migration-import-unified.py`, line ~438

**Example**:
```python
required_countries_dict = {
    'AU': '🇦🇺', 'CA': '🇨🇦', 'FR': '🇫🇷', 'DE': '🇩🇪', 'IN': '🇮🇳', 
    'IT': '🇮🇹', 'NL': '🇳🇱', 'ES': '🇪🇸', 'GB': '🇬🇧', 'US': '🇺🇸'
}
```

**To add a new required zip code country**:
1. Add a new key-value pair: `'XX': '🇽🇽'` (where `XX` is the ISO country code and `🇽🇽` is the flag emoji)
2. Records from this country will be validated for missing zip codes
3. The flag will automatically appear in the frontend validation display

**To remove a country**:
1. Simply delete the key-value pair from the dictionary

**Note**: Both dictionaries use the format `'COUNTRY_CODE': 'FLAG_EMOJI'`. The country codes are automatically extracted from the dictionary keys for validation logic, and the flags are sent to the frontend for display. No additional changes are needed in the frontend code.

## Output Files

The migration process generates several CSV files:

- **`*_final_import.csv`**: Successfully processed data ready for import (excludes all records that failed any validation check)
- **`*_no_token_found.csv`**: Records with missing payment tokens
- **`*_duplicate_tokens.csv`**: Records with duplicate payment tokens (warning, not a validation failure)
- **`*_duplicate_card_ids.csv`**: Records with duplicate card IDs (Stripe only, warning, not a validation failure)
- **`*_duplicate_external_subscription_ids.csv`**: Records with duplicate subscription IDs (warning, not a validation failure)
- **`*_duplicate_emails.csv`**: Records with duplicate email addresses (Production only, skipped in Sandbox, warning, not a validation failure)
- **`*_unsupported_countries.csv`**: Records with unsupported country codes
- **`*_invalid_date_formats.csv`**: Records with incorrect date formats
- **`*_invalid_date_periods.csv`**: Records with invalid date periods
- **`*_missing_zip_codes.csv`**: Records with missing zip codes
- **`*_invalid_ca_zip_codes.csv`**: Records with invalid Canadian zip code formats
- **`*_invalid_us_zip_codes.csv`**: Records with invalid US zip code formats

**Note**: All files are prefixed with the seller name and suffixed with `_sandbox` if running in sandbox mode. The "Download All Reports" button creates a zip file containing all available reports.

## Python Script Usage

The unified Python script can also be used directly:

```bash
python migration-import-unified.py subscriber_file.csv mapping_file.csv vault_provider_name [--sandbox]
```

### Script Features

- **Function-based API**: Can be imported and called from other Python code
- **File Object Support**: Accepts both file paths and file objects
- **Environment Toggle**: Sandbox mode with data anonymization
- **Comprehensive Logging**: Detailed processing information
- **Error Handling**: Robust error handling and validation

## Data Requirements

### Subscriber Export File (CSV)

Required columns:

- `card_token`: Payment token (Stripe: card ID like `pm_xxx`, Bluesnap: Account ID + last 4 digits)
- `current_period_started_at`: Subscription start date (ISO 8601 format: YYYY-MM-DDTHH:MM:SSZ)
- `current_period_ends_at`: Subscription end date (ISO 8601 format: YYYY-MM-DDTHH:MM:SSZ)
- `customer_email`: Customer email address
- `customer_full_name`: Customer full name
- `customer_external_id`: External customer ID
- `subscription_external_id`: External subscription ID
- `address_postal_code`: Customer postal/zip code
- `address_country_code`: Customer country code (required for zip code validation)

### Mapping File (CSV)

**For Stripe:**
- `card.id`: Card ID for mapping (e.g., `pm_xxx`)
- `card.number`: Card number/token
- `card.name`: Card holder name
- `card.exp_month`: Card expiry month
- `card.exp_year`: Card expiry year
- `card.transaction_ids`: Network transaction IDs
- `card.address_zip`: Zip code (optional, used if missing in subscriber file)

**For Bluesnap:**
- `BlueSnap Account Id`: Account ID
- `Credit Card Number`: Full credit card number
- `First Name`: Card holder first name
- `Last Name`: Card holder last name
- `Expiration Month`: Card expiry month
- `Expiration Year`: Card expiry year
- `Network Transaction Id`: Network transaction ID
- `Zip Code`: Zip code (optional, used if missing in subscriber file)

## Technical Details

### React Frontend

- Built with React 18
- Modern CSS with flexbox and grid layouts
- Responsive design for all screen sizes
- File upload handling with validation
- Real-time processing status updates
- HTTP API communication with backend

### Flask Backend

- RESTful API endpoints for file processing
- File upload handling and validation
- Large file support (up to 2GB, approximately 1 million records)
- Secure file processing on server-side
- CORS support for frontend communication
- File download endpoints
- Error handling and logging

### Python Backend

- Pandas for data manipulation
- Comprehensive data validation
- Duplicate detection algorithms
- File I/O with error handling
- Performance timing and logging

### Data Processing

- CSV parsing with proper encoding handling
- Date format standardization
- Column mapping and renaming
- Data type validation and conversion
- Null value handling

## Error Handling

The application includes comprehensive error handling:

- **File Validation**: Checks for valid CSV files
- **Data Validation**: Validates required columns and data types
- **Processing Errors**: Handles processing failures gracefully
- **User Feedback**: Clear error messages and status updates

## Security Considerations

- **Sandbox Mode**: Automatically anonymizes sensitive data
- **File Validation**: Validates file types and content
- **Error Logging**: Secure error handling without exposing sensitive data
- **Local Processing**: All processing happens locally, no data sent to external services

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test thoroughly
5. Submit a pull request

## License

This project is licensed under the MIT License.
