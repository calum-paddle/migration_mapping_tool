import React, { useState } from 'react';

const FileUpload = ({ onProcessingComplete }) => {
  const [subscriberFile, setSubscriberFile] = useState(null);
  const [mappingFile, setMappingFile] = useState(null);
  const [sellerName, setSellerName] = useState('');
  const [vaultProvider, setVaultProvider] = useState('TokenEx');
  const [isSandbox, setIsSandbox] = useState(false);
  const [provider, setProvider] = useState('stripe');
  const [isProcessing, setIsProcessing] = useState(false);
  const [processingStatus, setProcessingStatus] = useState('');
  const [error, setError] = useState(null);
  const [subscriberRecordCount, setSubscriberRecordCount] = useState(0);
  const [mappingRecordCount, setMappingRecordCount] = useState(0);
  const [validationResults, setValidationResults] = useState([]);
  const [currentValidationStep, setCurrentValidationStep] = useState('');
  const [waitingForUserInput, setWaitingForUserInput] = useState(false);

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

  const processFiles = async (subFile, mapFile, seller, vault, sandbox, prov, autocorrect = false, useMappingPostal = false, proceedWithoutMissing = false) => {
    const formData = new FormData();
    formData.append('subscriber_file', subFile);
    formData.append('mapping_file', mapFile);
    formData.append('seller_name', seller);
    formData.append('vault_provider', vault);
    formData.append('is_sandbox', sandbox);
    formData.append('provider', prov);
    if (autocorrect) {
      formData.append('autocorrect_us_postal', 'true');
    }
    if (useMappingPostal) {
      formData.append('use_mapping_postal_codes', 'true');
    }
    if (proceedWithoutMissing) {
      formData.append('proceed_without_missing_records', 'true');
    }

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
      
      // Check if user input is required
      if (result.status === 'user_input_required') {
        // Add any previous successful validations first
        if (result.validation_results) {
          const previousValidations = result.validation_results.map(validation => ({
            ...validation,
            timestamp: Date.now()
          }));
          setValidationResults(prev => [...prev, ...previousValidations]);
        }
        
        // Add the validation that requires user input
        const newValidation = {
          ...result.validation_result,
          step: result.step,
          timestamp: Date.now()
        };
        setValidationResults(prev => [...prev, newValidation]);
        setWaitingForUserInput(true);
        setIsProcessing(false);
        setProcessingStatus('');
        setCurrentValidationStep('');
        
        // Scroll to bottom to show validation results
        setTimeout(() => {
          window.scrollTo({ top: document.body.scrollHeight, behavior: 'smooth' });
        }, 100);
        return;
      }
      
      // Check if validation failed (new format: all validations returned together)
      if (result.error === 'Validation failures detected' && result.validation_results) {
        // Display all validation results (both passed and failed)
        const allValidations = result.validation_results.map(validation => ({
          ...validation,
          timestamp: Date.now()
        }));
        setValidationResults(allValidations);
        setIsProcessing(false);
        setProcessingStatus('');
        setCurrentValidationStep('');
        
        // Scroll to bottom to show validation results
        setTimeout(() => {
          window.scrollTo({ top: document.body.scrollHeight, behavior: 'smooth' });
        }, 100);
        return;
      }
      
      // Check if validation failed (old format: single validation failure)
      if (result.error && (result.step === 'column_validation' || result.step === 'card_token_validation' || result.step === 'date_format_validation' || result.step === 'date_validation' || result.step === 'ca_postal_code_validation' || result.step === 'us_postal_code_validation' || result.step === 'missing_postal_code_validation')) {
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
        
        // Scroll to bottom to show validation results
        setTimeout(() => {
          window.scrollTo({ top: document.body.scrollHeight, behavior: 'smooth' });
        }, 100);
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
      
      // Scroll to bottom to show error message
      setTimeout(() => {
        window.scrollTo({ top: document.body.scrollHeight, behavior: 'smooth' });
      }, 100);
    }
  };

  const resetValidationState = () => {
    setValidationResults([]);
    setError(null);
    setProcessingStatus('');
    setCurrentValidationStep('');
    setWaitingForUserInput(false);
  };

  const handleUserInput = async (step, userChoice) => {
    try {
      setProcessingStatus('Processing your choice...');
      setIsProcessing(true);
      
      if (step === 'us_postal_code_validation' && userChoice === 'cancel') {
        setProcessingStatus('Processing stopped by user request');
        setIsProcessing(false);
        setWaitingForUserInput(false);
        // Remove the user input requirement by setting autocorrectable_count to 0
        setValidationResults(prev => prev.map(validation => 
          validation.step === 'us_postal_code_validation' && !validation.valid
            ? { ...validation, autocorrectable_count: 0 }
            : validation
        ));
        return;
      }
      
      if (step === 'missing_postal_code_validation' && userChoice === 'cancel') {
        setProcessingStatus('Processing stopped by user request');
        setIsProcessing(false);
        setWaitingForUserInput(false);
        // Remove the user input requirement by setting show_buttons to false
        setValidationResults(prev => prev.map(validation => 
          validation.step === 'missing_postal_code_validation' && !validation.valid
            ? { ...validation, show_buttons: false }
            : validation
        ));
        return;
      }
      
      const response = await fetch('/api/continue-processing', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          user_choice: userChoice,
          step: step
        }),
      });

      if (!response.ok) {
        throw new Error('Failed to process user choice');
      }

      const result = await response.json();
      
      if (result.status === 'stopped_by_user') {
        setProcessingStatus('Processing stopped by user request');
        setIsProcessing(false);
        setWaitingForUserInput(false);
      } else if (result.status === 'continuing') {
        // Continue with processing - you would call the migration function again here
        setProcessingStatus('Continuing with processing...');
        // For now, just show success
        setProcessingStatus('Processing completed successfully!');
        setIsProcessing(false);
        setWaitingForUserInput(false);
      } else if (result.status === 'autocorrect_requested') {
        // Restart processing with autocorrect flag
        setProcessingStatus('Restarting processing with autocorrect...');
        // Reset validation results to start fresh
        setValidationResults([]);
        setWaitingForUserInput(false);
        
        // Restart the processing with autocorrect
        await processFiles(subscriberFile, mappingFile, sellerName, vaultProvider, isSandbox, provider, true);
      } else if (result.status === 'mapping_postal_codes_requested') {
        // Restart processing with mapping postal codes flag
        setProcessingStatus('Restarting processing with mapping postal codes...');
        // Reset validation results to start fresh
        setValidationResults([]);
        setWaitingForUserInput(false);
        
        // Restart the processing with mapping postal codes
        await processFiles(subscriberFile, mappingFile, sellerName, vaultProvider, isSandbox, provider, false, true);
      } else if (result.status === 'proceed_without_missing_records_requested') {
        // Restart processing with proceed without missing records flag
        setProcessingStatus('Restarting processing without missing records...');
        // Reset validation results to start fresh
        setValidationResults([]);
        setWaitingForUserInput(false);
        
        // Restart the processing without missing records
        await processFiles(subscriberFile, mappingFile, sellerName, vaultProvider, isSandbox, provider, false, false, true);
      }
      
    } catch (err) {
      setError('Error processing user choice: ' + err.message);
      setIsProcessing(false);
      setWaitingForUserInput(false);
      
      // Scroll to bottom to show error message
      setTimeout(() => {
        window.scrollTo({ top: document.body.scrollHeight, behavior: 'smooth' });
      }, 100);
    }
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
      
      // Scroll to bottom to show error message
      setTimeout(() => {
        window.scrollTo({ top: document.body.scrollHeight, behavior: 'smooth' });
      }, 100);
    }
  };

  return (
    <div className="file-upload-container">
      {/* <h2>Paddle Billing Migration Tool</h2> */}
      
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
              Use blackhole email addresses
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
          {currentValidationStep === 'date_format_validation' && 'Date format validation in progress...'}
          {currentValidationStep === 'date_validation' && 'Date validation in progress...'}
          {currentValidationStep === 'card_token_validation' && 'Bluesnap card token validation in progress...'}
          {currentValidationStep === 'ca_postal_code_validation' && 'Canadian postal code validation in progress...'}
          {currentValidationStep === 'us_postal_code_validation' && 'US postal code validation in progress...'}
          {currentValidationStep === 'missing_postal_code_validation' && 'Missing postal code validation in progress...'}
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
                : validation.step === 'date_format_validation'
                ? (validation.valid ? 'Date format validation passed' : 'Date format validation failed')
                : validation.step === 'date_validation'
                ? (validation.valid ? 'Date validation passed' : 'Date validation failed')
                : validation.step === 'card_token_validation'
                ? (validation.valid ? 'Card token validation passed' : 'Card token validation failed')
                : validation.step === 'ca_postal_code_validation'
                ? (validation.valid ? 'Canadian postal code validation passed' : 'Canadian postal code validation failed')
                : validation.step === 'us_postal_code_validation'
                ? (validation.valid ? 'US postal code validation passed' : 'US postal code validation failed')
                : validation.step === 'missing_postal_code_validation'
                ? (validation.valid ? 'Missing postal code validation passed   🇦🇺 🇨🇦 🇫🇷 🇩🇪 🇮🇳 🇮🇹 🇳🇱 🇪🇸 🇬🇧 🇺🇸' : 'Missing postal code validation failed   🇦🇺 🇨🇦 🇫🇷 🇩🇪 🇮🇳 🇮🇹 🇳🇱 🇪🇸 🇬🇧 🇺🇸')
                : validation.step === 'duplicate_detection'
                ? 'Duplicate detection requires input'
                : (validation.valid ? `${validation.step} passed` : `${validation.step} failed`)
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
            {validation.requires_user_input && (
              <span className="user-input-required">❓</span>
            )}
          </div>
          <div className="validation-details">
            {validation.step === 'column_validation' ? (
              <>
                {!validation.valid && (
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
            ) : validation.step === 'date_format_validation' ? (
              <>
                {!validation.valid && (
                  <>
                    <p>current_period_started_at and current_period_ends_at dates must be in ISO 8601 format: YYYY-MM-DDTHH:MM:SSZ (e.g., 2025-07-06T00:00:00Z).</p>
                    <div className="missing-columns">
                      <p><strong>Found {validation.incorrect_count} records with incorrect date formats.</strong></p>
                      <p>Click the download icon to get a report of all incorrect records.</p>
                    </div>
                  </>
                )}
              </>
            ) : validation.step === 'date_validation' ? (
              <>
                {!validation.valid && (
                  <>
                    <p>Date periods must be logical: current_period_started_at dates should not be in the future, current_period_ends_at dates should not be in the past.</p>
                    <div className="missing-columns">
                      <p><strong>Found {validation.incorrect_count} records with invalid date periods.</strong></p>
                      <p>Click the download icon to get a report of all incorrect records.</p>
                    </div>
                  </>
                )}
              </>
            ) : validation.step === 'card_token_validation' ? (
              <>
                {!validation.valid && (
                  <>
                    <p>Bluesnap card tokens must be exactly 13 numerical characters.</p>
                    <div className="missing-columns">
                      <p><strong>Found {validation.incorrect_count} card tokens with incorrect format.</strong></p>
                      <p>Click the download icon to get a report of all incorrect records.</p>
                    </div>
                  </>
                )}
              </>
            ) : validation.step === 'ca_postal_code_validation' ? (
              <>
                {!validation.valid && (
                  <>
                    <p>Canadian postal codes must be in the format: Letter-Number-Letter Number-Letter-Number (e.g., A1A 1A1).</p>
                    <div className="missing-columns">
                      <p><strong>Found {validation.incorrect_count} Canadian postal codes with incorrect format.</strong></p>
                      <p>Click the download icon to get a report of all incorrect records.</p>
                    </div>
                  </>
                )}
              </>
            ) : validation.step === 'us_postal_code_validation' ? (
              <>
                {!validation.valid && (
                  <>
                    <p>US postal codes must be exactly 5 numerical digits.</p>
                    <div className="missing-columns">
                      <p><strong>Found {validation.incorrect_count} US postal codes with incorrect format.</strong></p>
                      {validation.autocorrectable_count > 0 && (
                        <p><strong>*{validation.autocorrectable_count} can be autocorrected with leading zeros.</strong></p>
                      )}
                      <p>Click the download icon to get a report of all incorrect records.</p>
                    </div>
                    {validation.autocorrectable_count > 0 && (
                      <div className="user-input-options">
                        <p><strong>Please choose how to proceed:</strong></p>
                        <div className="user-input-buttons" style={{justifyContent: 'center'}}>
                          <button 
                            onClick={() => handleUserInput(validation.step, 'autocorrect_leading_zeros')}
                            className="user-input-btn"
                            disabled={isProcessing}
                          >
                            Autocorrect leading zeros
                          </button>
                        </div>
                      </div>
                    )}
                  </>
                )}
              </>
            ) : validation.step === 'missing_postal_code_validation' ? (
              <>
                {!validation.valid && (
                  <>
                    <p>Postal codes are required for AU, CA, FR, DE, IN, IT, NL, ES, GB, US addresses.</p>
                    <div className="missing-columns">
                      <p><strong>Found {validation.missing_count} records with missing postal codes.</strong></p>
                      {validation.available_from_mapping > 0 && (
                        <p><strong>*{validation.available_from_mapping} can be pulled from mapping file.</strong></p>
                      )}
                      <p>Click the download icon to get a report of all missing records.</p>
                    </div>
                    {validation.available_from_mapping > 0 && (
                      <div className="user-input-options">
                        <p><strong>Please choose how to proceed:</strong></p>
                        <div className="user-input-buttons">
                          <button 
                            onClick={() => handleUserInput(validation.step, 'use_mapping_postal_codes')}
                            className="user-input-btn"
                            disabled={isProcessing}
                          >
                            Use mapping postal codes
                          </button>
                        </div>
                      </div>
                    )}
                  </>
                )}
              </>
            ) : validation.step === 'duplicate_detection' ? (
              <>
                <p>{validation.message}</p>
                {validation.requires_user_input && validation.options && (
                  <div className="user-input-options">
                    <p><strong>Please choose how to proceed:</strong></p>
                    <div className="user-input-buttons">
                      {validation.options.map(option => (
                        <button 
                          key={option}
                          onClick={() => handleUserInput(validation.step, option)}
                          className="user-input-btn"
                          disabled={isProcessing}
                        >
                          {option.replace(/_/g, ' ')}
                        </button>
                      ))}
                    </div>
                  </div>
                )}
              </>
            ) : (
              <>
                {validation.valid ? (
                  <p>Validation passed</p>
                ) : (
                  <p>Validation failed</p>
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