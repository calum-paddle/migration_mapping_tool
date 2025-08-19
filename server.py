from flask import Flask, request, jsonify, send_file
from flask_cors import CORS
from werkzeug.utils import secure_filename
import os
import tempfile
import importlib.util
import sys
import pandas as pd
import json

# Load the migration script dynamically
spec = importlib.util.spec_from_file_location("migration_import_unified", "migration-import-unified.py")
migration_module = importlib.util.module_from_spec(spec)
sys.modules["migration_import_unified"] = migration_module
spec.loader.exec_module(migration_module)

process_migration = migration_module.process_migration

app = Flask(__name__)
CORS(app)  # Enable CORS for React frontend

# Configuration
UPLOAD_FOLDER = 'uploads'
OUTPUT_FOLDER = 'outputs'
ALLOWED_EXTENSIONS = {'csv'}

# Create directories if they don't exist
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['OUTPUT_FOLDER'] = OUTPUT_FOLDER
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB max file size

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@app.route('/api/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({'status': 'healthy', 'message': 'Paddle Migration API is running'})

@app.route('/api/process-migration', methods=['POST'])
def process_migration_api():
    """Process migration endpoint"""
    try:
        # Check if files are present
        if 'subscriber_file' not in request.files:
            return jsonify({'error': 'Subscriber file is required'}), 400
        
        if 'mapping_file' not in request.files:
            return jsonify({'error': 'Mapping file is required'}), 400
        
        subscriber_file = request.files['subscriber_file']
        mapping_file = request.files['mapping_file']
        seller_name = request.form.get('seller_name', '')
        vault_provider = request.form.get('vault_provider', '')
        is_sandbox = request.form.get('is_sandbox', 'false').lower() == 'true'
        provider = request.form.get('provider', 'stripe')
        
        # Validate files
        if subscriber_file.filename == '':
            return jsonify({'error': 'No subscriber file selected'}), 400
        
        if mapping_file.filename == '':
            return jsonify({'error': 'No mapping file selected'}), 400
        
        if not allowed_file(subscriber_file.filename):
            return jsonify({'error': 'Subscriber file must be a CSV'}), 400
        
        if not allowed_file(mapping_file.filename):
            return jsonify({'error': 'Mapping file must be a CSV'}), 400
        
        if not seller_name:
            return jsonify({'error': 'Seller name is required'}), 400
        
        if not vault_provider:
            return jsonify({'error': 'Vault provider name is required'}), 400
        
        # Save uploaded files temporarily
        subscriber_filename = secure_filename(subscriber_file.filename)
        mapping_filename = secure_filename(mapping_file.filename)
        
        subscriber_path = os.path.join(app.config['UPLOAD_FOLDER'], subscriber_filename)
        mapping_path = os.path.join(app.config['UPLOAD_FOLDER'], mapping_filename)
        
        subscriber_file.save(subscriber_path)
        mapping_file.save(mapping_path)
        
        # Call the migration processing function
        result = process_migration(
            subscriber_path, 
            mapping_path, 
            vault_provider, 
            is_sandbox, 
            provider, 
            seller_name
        )
        
        # Check if validation failed
        if 'error' in result and result.get('step') in ['column_validation', 'card_token_validation', 'date_validation']:
            # Clean up uploaded files
            os.remove(subscriber_path)
            os.remove(mapping_path)
            return jsonify(result)
        
        # Update file URLs to be downloadable
        for file_info in result['output_files']:
            file_info['url'] = f'http://localhost:5001/api/download/{file_info["name"]}'
        
        # Clean up uploaded files
        os.remove(subscriber_path)
        os.remove(mapping_path)
        
        return jsonify(result)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/download/<filename>', methods=['GET'])
def download_file(filename):
    """Download processed file"""
    try:
        file_path = os.path.join(app.config['OUTPUT_FOLDER'], filename)
        print(f"Download request for: {filename}")
        print(f"Looking for file: {file_path}")
        print(f"File exists: {os.path.exists(file_path)}")
        print(f"Absolute path: {os.path.abspath(file_path)}")
        print(f"Current working directory: {os.getcwd()}")
        print(f"Output folder: {app.config['OUTPUT_FOLDER']}")
        
        if os.path.exists(file_path):
            print(f"File size: {os.path.getsize(file_path)} bytes")
            print(f"File permissions: {oct(os.stat(file_path).st_mode)[-3:]}")
            return send_file(file_path, as_attachment=True)
        else:
            # List all files in output directory for debugging
            output_dir = app.config['OUTPUT_FOLDER']
            print(f"Output directory: {output_dir}")
            print(f"Output directory exists: {os.path.exists(output_dir)}")
            if os.path.exists(output_dir):
                files = os.listdir(output_dir)
                print(f"Files in output directory: {files}")
                print(f"Requested filename: {filename}")
                print(f"Available files: {[f for f in files if f.endswith('.csv')]}")
            else:
                print(f"Output directory does not exist: {output_dir}")
            return jsonify({'error': 'File not found'}), 404
    except Exception as e:
        print(f"Download error: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/files', methods=['GET'])
def list_output_files():
    """List all available output files"""
    try:
        files = []
        output_dir = app.config['OUTPUT_FOLDER']
        
        if os.path.exists(output_dir):
            for filename in os.listdir(output_dir):
                if filename.endswith('.csv'):
                    file_path = os.path.join(output_dir, filename)
                    file_size = os.path.getsize(file_path)
                    files.append({
                        'name': filename,
                        'size': file_size,
                        'url': f'/api/download/{filename}'
                    })
        
        return jsonify({'files': files})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/continue-processing', methods=['POST'])
def continue_processing():
    """Continue processing after user input"""
    try:
        data = request.get_json()
        user_choice = data.get('user_choice')
        step = data.get('step')
        
        print(f"User chose: {user_choice} for step: {step}")
        
        # Handle different user choices
        if user_choice == 'stop_processing':
            return jsonify({
                'status': 'stopped_by_user',
                'message': 'Processing stopped by user request'
            })
        elif user_choice == 'skip_duplicates':
            # Continue processing, skipping duplicates
            # This would call the migration function again with skip_duplicates=True
            return jsonify({
                'status': 'continuing',
                'message': 'Continuing with duplicates skipped'
            })
        elif user_choice == 'continue_with_duplicates':
            # Continue processing with duplicates
            return jsonify({
                'status': 'continuing',
                'message': 'Continuing with duplicates included'
            })
        else:
            return jsonify({'error': 'Invalid user choice'}), 400
            
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/cleanup', methods=['POST'])
def cleanup_files():
    """Clean up output files"""
    try:
        output_dir = app.config['OUTPUT_FOLDER']
        if os.path.exists(output_dir):
            for filename in os.listdir(output_dir):
                if filename.endswith('.csv'):
                    os.remove(os.path.join(output_dir, filename))
        
        return jsonify({'message': 'Files cleaned up successfully'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/validate-zipcodes', methods=['POST'])
def validate_zipcodes():
    """Validate US zip codes in processed data"""
    try:
        if 'file' not in request.files:
            return jsonify({'error': 'No file provided'}), 400
        
        file = request.files['file']
        if file.filename == '':
            return jsonify({'error': 'No file selected'}), 400
        
        # Read the CSV file
        df = pd.read_csv(file)
        
        # Check if required columns exist
        if 'address_country_code' not in df.columns or 'address_postal_code' not in df.columns:
            return jsonify({'error': 'Required columns not found'}), 400
        
        issues = []
        
        # Check each US postal code
        for idx, row in df.iterrows():
            country_code = row.get('address_country_code', '')
            postal_code = row.get('address_postal_code', '')
            
            if country_code == 'US' and postal_code:
                postal_code = str(postal_code).strip()
                
                # Check for 4-digit zip codes (missing leading zero)
                if len(postal_code) == 4 and postal_code.isdigit():
                    issues.append({
                        'line': idx + 1,
                        'postalCode': postal_code,
                        'correctedCode': '0' + postal_code,
                        'type': 'missing_zero'
                    })
                # Check for incorrect format (not 4 or 5 digits, or non-numeric)
                elif not (len(postal_code) == 5 and postal_code.isdigit()):
                    issues.append({
                        'line': idx + 1,
                        'postalCode': postal_code,
                        'correctedCode': None,
                        'type': 'incorrect_format'
                    })
        
        return jsonify({
            'hasIssues': len(issues) > 0,
            'issues': issues
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5001) 