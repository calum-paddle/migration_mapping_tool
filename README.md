# Paddle Migration Tool

A React-based frontend application for migrating data from Stripe to Paddle Billing, with support for both production and sandbox environments.

## Features

- **Modern React Interface**: Clean, responsive UI inspired by Paddle Billing design
- **Dual Environment Support**: Toggle between production and sandbox modes
- **File Upload**: Drag-and-drop CSV file upload functionality
- **Data Processing**: Unified Python backend for data migration
- **Duplicate Detection**: Comprehensive duplicate detection across multiple fields
- **Data Anonymization**: Automatic data anonymization for sandbox mode
- **Results Summary**: Detailed processing results and statistics
- **File Downloads**: Easy download of processed output files

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

1. **Run the setup script** (recommended):

   ```bash
   python setup.py
   ```

   This will install all dependencies automatically.

2. **Manual installation**:

   **Install Node.js dependencies**:

   ```bash
   npm install
   ```

   **Install Python dependencies**:

   ```bash
   pip install -r requirements.txt
   ```

## Usage

### Starting the Application

**Option 1: Use the startup script (recommended)**:

```bash
python start.py
```

This will start both the backend and frontend automatically.

**Option 2: Start manually**:

1. **Start the Flask backend**:

   ```bash
   python server.py
   ```

   The backend will run at `http://localhost:5001`

2. **Start the React frontend** (in a new terminal):
   ```bash
   npm start
   ```
   The frontend will open at `http://localhost:3000`

### Using the Migration Tool

1. **Select Environment**: Toggle between Production and Sandbox modes
2. **Enter Vault Provider**: Provide the name of your vault provider
3. **Upload Files**:
   - **Subscriber Export File**: CSV file containing subscriber data from Stripe
   - **Mapping File**: CSV file containing TokenEx mapping data (or test mapping for sandbox)
4. **Process Migration**: Click "Process Migration" to start the data processing
5. **Review Results**: View processing statistics and download output files

### Environment Modes

#### Production Mode

- Processes real customer data
- Uses actual payment tokens
- Generates production-ready output files
- Comprehensive duplicate detection

#### Sandbox Mode

- Anonymizes customer data (emails and names)
- Uses test tokens for processing
- Adds "\_sandbox" suffix to output filenames
- Same duplicate detection as production

## Output Files

The migration process generates several CSV files:

- **`*_final_import.csv`**: Successfully processed data ready for import
- **`*_no_token_found.csv`**: Records with missing payment tokens
- **`*_duplicate_tokens.csv`**: Records with duplicate payment tokens
- **`*_duplicate_card_ids.csv`**: Records with duplicate card IDs
- **`*_duplicate_external_subscription_ids.csv`**: Records with duplicate subscription IDs
- **`*_duplicate_emails.csv`**: Records with duplicate email addresses

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

- `card_token`: Payment token from Stripe
- `current_period_started_at`: Subscription start date
- `customer_email`: Customer email address
- `customer_full_name`: Customer full name
- `customer_external_id`: External customer ID
- `subscription_external_id`: External subscription ID
- `postal_code`: Customer postal code

### Mapping File (CSV)

Required columns:

- `card.id`: Card ID for mapping
- `card.number`: Card number/token
- `card.name`: Card holder name
- `card.exp_month`: Card expiry month
- `card.exp_year`: Card expiry year
- `card.transaction_ids`: Network transaction IDs

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
