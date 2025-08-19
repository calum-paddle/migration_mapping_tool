import React, { useState } from 'react';

const FileUpload = ({ onProcessingComplete }) => {
  const [subscriberFile, setSubscriberFile] = useState(null);
  const [mappingFile, setMappingFile] = useState(null);
  const [sellerName, setSellerName] = useState('');
  const [vaultProvider, setVaultProvider] = useState('');
  const [isSandbox, setIsSandbox] = useState(false);
  const [provider, setProvider] = useState('stripe');
  const [isProcessing, setIsProcessing] = useState(false);
  const [processingStatus, setProcessingStatus] = useState('');
  const [error, setError] = useState(null);
  const [subscriberRecordCount, setSubscriberRecordCount] = useState(0);
  const [mappingRecordCount, setMappingRecordCount] = useState(0);
  const [validationResults, setValidationResults] = useState([]);
  const [currentValidationStep, setCurrentValidationStep] = useState('');

  const handleFileChange = (e, fileType) => {
    const file = e.target.files[0];
    if (!file) return;

    // Check if it's a CSV file
    const isCSV = file.name.toLowerCase().endsWith('.csv') ||
                  (file.type === 'text/plain' && file.name.toLowerCase().endsWith('.txt'));

    if (!isCSV) {
      alert('Please select a valid CSV file.');
      return;
    }

    // Count records in the file
    const reader = new FileReader();
    reader.onload = (event) => {
      const content = event.target.result;
      const lines = content.split('\n').filter(line => line.trim() !== '');
      const recordCount = Math.max(0, lines.length - 1); // Subtract 1 for header row
      
      if (fileType === 'subscriber') {
        setSubscriberFile(file);
        setSubscriberRecordCount(recordCount);
      } else if (fileType === 'mapping') {
        setMappingFile(file);
        setMappingRecordCount(recordCount);
      }
    };
    reader.readAsText(file);
  };

  const processFiles = async (subFile, mapFile, seller, vault, sandbox, prov) => {
    const formData = new FormData();
    formData.append('subscriber_file', subFile);
    formData.append('mapping_file', mapFile);
    formData.append('seller_name', seller);
    formData.append('vault_provider', vault);
    formData.append('is_sandbox', sandbox);
    formData.append('provider', prov);

    try {
      setProcessingStatus('Uploading files...');
      setCurrentValidationStep('column_validation');
      const response = await fetch('/api/process-migration', {
        method: 'POST',
        body: formData,
      });

      if (!response.ok) {
        let errorMessage = 'Processing failed';
        
        try {
          const errorData = await response.json();
          errorMessage = errorData.error || errorMessage;
        } catch (parseError) {
          const responseText = await response.text();
          if (responseText.includes('<!DOCTYPE')) {
            errorMessage = 'Server error - please check if the backend server is running on port 5001';
          } else {
            errorMessage = `Server error: ${response.status} ${response.statusText}`;
          }
        }
        
        throw new Error(errorMessage);
      }

      const result = await response.json();
      
      // Check if validation failed
      if (result.error && (result.step === 'column_validation' || result.step === 'card_token_validation')) {
        // Add any previous successful validations first
        if (result.validation_results) {
          const previousValidations = result.validation_results.map(validation => ({
            ...validation,
            timestamp: Date.now()
          }));
          setValidationResults(prev => [...prev, ...previousValidations]);
        }
        
        // Then add the failed validation
        const newValidation = {
          ...result.validation_result,
          step: result.step,
          timestamp: Date.now()
        };
        setValidationResults(prev => [...prev, newValidation]);
        setIsProcessing(false);
        setProcessingStatus('');
        setCurrentValidationStep('');
        return;
      }
      
      // Handle successful validation results
      if (result.validation_results) {
        const newValidations = result.validation_results.map(validation => ({
          ...validation,
          timestamp: Date.now()
        }));
        setValidationResults(prev => [...prev, ...newValidations]);
      }
      
      onProcessingComplete(result);
      setProcessingStatus('Processing completed successfully!');
    } catch (err) {
      setError('Error processing migration: ' + err.message);
      setIsProcessing(false);
      setProcessingStatus('');
    }
  };

  const resetValidationState = () => {
    setValidationResults([]);
    setError(null);
    setProcessingStatus('');
    setCurrentValidationStep('');
  };

  const handleSubmit = async (e) => {
    e.preventDefault();
    
    if (!subscriberFile || !mappingFile || !sellerName || !vaultProvider) {
      setError('Please fill in all required fields and upload both files.');
      return;
    }

    // Reset all validation state when starting a new process
    resetValidationState();
    setIsProcessing(true);
    setProcessingStatus('Processing migration...');

    try {
      // Check if server is running
      const healthResponse = await fetch('/api/health');
      if (!healthResponse.ok) {
        throw new Error('Backend server is not responding. Please ensure the Python server is running on port 5001.');
      }
      
      // Process the migration with basic parameters only
      await processFiles(subscriberFile, mappingFile, sellerName, vaultProvider, isSandbox, provider);
      
    } catch (err) {
      setError('Error processing migration: ' + err.message);
      setIsProcessing(false);
      setProcessingStatus('');
    }
  };

  return (
    <div className="file-upload-container">
      <h2>Paddle Billing Migration Tool</h2>
      
      <form onSubmit={handleSubmit}>
        <div className="form-group">
          <label htmlFor="sellerName">Seller Name:</label>
          <input
            type="text"
            id="sellerName"
            value={sellerName}
            onChange={(e) => setSellerName(e.target.value)}
            required
          />
        </div>

        <div className="form-group">
          <label>Vault Provider:</label>
          <div className="vault-provider-selection">
            <button
              type="button"
              className={`provider-btn ${vaultProvider === 'TokenEx' ? 'selected' : ''}`}
              onClick={() => setVaultProvider('TokenEx')}
            >
              TokenEx
            </button>
            <button
              type="button"
              className={`provider-btn ${vaultProvider === 'Other' ? 'selected' : ''}`}
              onClick={() => setVaultProvider('Other')}
            >
              Other
            </button>
          </div>
          {vaultProvider === 'Other' && (
            <input
              type="text"
              placeholder="Enter vault provider name"
              value={vaultProvider === 'Other' ? '' : vaultProvider}
              onChange={(e) => setVaultProvider(e.target.value)}
              className="other-vault-input"
              required
            />
          )}
        </div>

        <div className="form-group">
          <label>Payment Service Provider:</label>
          <div className="psp-selection">
            <button
              type="button"
              className={`provider-btn ${provider === 'stripe' ? 'selected' : ''}`}
              onClick={() => setProvider('stripe')}
            >
              Stripe
            </button>
            <button
              type="button"
              className={`provider-btn ${provider === 'bluesnap' ? 'selected' : ''}`}
              onClick={() => setProvider('bluesnap')}
            >
              Bluesnap
            </button>
          </div>
        </div>

        <div className="form-group">
          <label>Environment:</label>
          <div className="environment-selection">
            <button
              type="button"
              className={`environment-btn ${!isSandbox ? 'selected' : ''}`}
              onClick={() => setIsSandbox(false)}
            >
              Production
            </button>
            <button
              type="button"
              className={`environment-btn ${isSandbox ? 'selected' : ''}`}
              onClick={() => setIsSandbox(true)}
            >
              Sandbox
            </button>
          </div>
          {isSandbox && (
            <div className="sandbox-message">
              Anonymize email addresses
            </div>
          )}
        </div>

        <div className="form-group">
          <label htmlFor="subscriberFile">Subscriber Export File:</label>
          <div className="file-input-wrapper">
            <input
              type="file"
              id="subscriberFile"
              accept=".csv,.txt"
              onChange={(e) => handleFileChange(e, 'subscriber')}
              required
              className="hidden-file-input"
            />
            <span className="custom-file-button">Choose file</span>
            <span className="custom-file-name">
              {subscriberFile ? subscriberFile.name : 'No file chosen'}
            </span>
            {subscriberFile && (
              <div className="record-count-box">
                <span className="record-icon">📊</span>
                <span className="record-count">{subscriberRecordCount} records</span>
              </div>
            )}
          </div>
        </div>

        <div className="form-group">
          <label htmlFor="mappingFile">Mapping File:</label>
          <div className="file-input-wrapper">
            <input
              type="file"
              id="mappingFile"
              accept=".csv,.txt"
              onChange={(e) => handleFileChange(e, 'mapping')}
              required
              className="hidden-file-input"
            />
            <span className="custom-file-button">Choose file</span>
            <span className="custom-file-name">
              {mappingFile ? mappingFile.name : 'No file chosen'}
            </span>
            {mappingFile && (
              <div className="record-count-box">
                <span className="record-icon">📊</span>
                <span className="record-count">{mappingRecordCount} records</span>
              </div>
            )}
          </div>
        </div>

        <button type="submit" disabled={isProcessing} className="submit-btn">
          {isProcessing ? (
            <div className="loading-spinner">
              <div className="spinner"></div>
              <span>Processing...</span>
            </div>
          ) : (
            'Process Migration'
          )}
        </button>
      </form>

      {processingStatus && (
        <div className="processing-status">
          {processingStatus}
          {currentValidationStep && (
            <div className="validation-progress">
              {currentValidationStep === 'column_validation' && 'Column validation in progress...'}
              {currentValidationStep === 'card_token_validation' && 'Bluesnap card token validation in progress...'}
            </div>
          )}
        </div>
      )}

      {validationResults.map((validation, index) => (
        <div key={validation.timestamp || index} className={`validation-result ${validation.valid ? 'valid' : 'invalid'}`}>
          <div className="validation-header">
            <span className="validation-icon">
              {validation.valid ? '✓' : '✗'}
            </span>
            <span className="validation-title">
              {validation.step === 'column_validation' 
                ? (validation.valid ? 'Column validation passed' : 'Column validation failed')
                : (validation.valid ? 'Card token validation passed' : 'Card token validation failed')
              }
            </span>
            {!validation.valid && validation.download_file && (
              <button 
                className="download-report-btn"
                onClick={() => {
                  const link = document.createElement('a');
                  link.href = `http://localhost:5001/api/download/${validation.download_file}`;
                  link.download = validation.download_file;
                  document.body.appendChild(link);
                  link.click();
                  document.body.removeChild(link);
                }}
                title="Download incorrect records report"
              >
                📥
              </button>
            )}
          </div>
          <div className="validation-details">
            {validation.step === 'column_validation' ? (
              <>
                {validation.valid ? (
                  <>
                    {validation.optional_columns && validation.optional_columns.length > 0 && (
                      <p>Including {validation.optional_columns.length} optional columns</p>
                    )}
                  </>
                ) : (
                  <>
                    <p>Please include all columns from the template file even if they are empty.</p>
                    {validation.missing_columns && (
                      <div className="missing-columns">
                        <p><strong>Missing required columns:</strong></p>
                        <ul>
                          {validation.missing_columns.map((col, colIndex) => (
                            <li key={colIndex}>{col}</li>
                          ))}
                        </ul>
                      </div>
                    )}
                  </>
                )}
              </>
            ) : (
              <>
                {validation.valid ? (
                  <p>All {validation.total_records} card tokens are correctly formatted.</p>
                ) : (
                  <>
                    <p>Bluesnap card tokens must be exactly 13 numerical characters.</p>
                    <div className="missing-columns">
                      <p><strong>Found {validation.incorrect_count} card tokens with incorrect format.</strong></p>
                      <p>Click the download icon to get a report of all incorrect records.</p>
                    </div>
                  </>
                )}
              </>
            )}
          </div>
        </div>
      ))}

      {error && (
        <div className="error-message">
          {error}
        </div>
      )}
    </div>
  );
};

export default FileUpload; 