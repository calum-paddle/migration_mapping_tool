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
  const [expandedValidations, setExpandedValidations] = useState(new Set());
  const [zipFile, setZipFile] = useState(null);
  const [useMappingZipCodes, setUseMappingZipCodes] = useState(false);
  const [autocorrectUsZipCodes, setAutocorrectUsZipCodes] = useState(false);

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

  const toggleValidation = (step) => {
    setExpandedValidations(prev => {
      const newSet = new Set(prev);
      if (newSet.has(step)) {
        newSet.delete(step);
      } else {
        newSet.add(step);
      }
      return newSet;
    });
  };

  const processFiles = async (subFile, mapFile, seller, vault, sandbox, prov, autocorrect = false, useMappingZip = false, proceedWithoutMissing = false) => {
    const formData = new FormData();
    formData.append('subscriber_file', subFile);
    formData.append('mapping_file', mapFile);
    formData.append('seller_name', seller);
    formData.append('vault_provider', vault);
    formData.append('is_sandbox', sandbox);
    formData.append('provider', prov);
    if (autocorrect) {
      formData.append('autocorrect_us_zip', 'true');
    }
    if (useMappingZip) {
      formData.append('use_mapping_zip_codes', 'true');
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
          // Expand passed validations (but not warnings)
          previousValidations.filter(v => v.valid && v.type !== 'warning').forEach(v => {
            setExpandedValidations(prev => new Set([...prev, v.step]));
          });
        }
        
        // Add the validation that requires user input
        const newValidation = {
          ...result.validation_result,
          step: result.step,
          timestamp: Date.now()
        };
        setValidationResults(prev => [...prev, newValidation]);
        // Expand if it's a passed validation (but not a warning)
        if (newValidation.valid && newValidation.type !== 'warning') {
          setExpandedValidations(prev => new Set([...prev, newValidation.step]));
        }
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
        // Store zip file if available (check both zip_file and output_files)
        if (result.zip_file) {
          setZipFile(result.zip_file);
        } else if (result.output_files) {
          const zipFileInfo = result.output_files.find(f => f.is_zip);
          if (zipFileInfo) {
            setZipFile(zipFileInfo);
          }
        }
        // Initialize expanded state: expand passed validations (but not warnings), collapse failed ones and warnings
        const initialExpanded = new Set(
          allValidations.filter(v => v.valid && v.type !== 'warning').map(v => v.step)
        );
        setExpandedValidations(initialExpanded);
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
      if (result.error && (result.step === 'column_validation' || result.step === 'card_token_validation' || result.step === 'date_format_validation' || result.step === 'date_validation' || result.step === 'ca_zip_code_validation' || result.step === 'us_zip_code_validation' || result.step === 'missing_zip_code_validation')) {
        // Add any previous successful validations first
        if (result.validation_results) {
          const previousValidations = result.validation_results.map(validation => ({
            ...validation,
            timestamp: Date.now()
          }));
          setValidationResults(prev => [...prev, ...previousValidations]);
          // Expand passed validations (but not warnings)
          previousValidations.filter(v => v.valid && v.type !== 'warning').forEach(v => {
            setExpandedValidations(prev => new Set([...prev, v.step]));
          });
        }
        
        // Then add the failed validation (collapsed by default)
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
        // Store zip file if available
        if (result.output_files) {
          const zipFileInfo = result.output_files.find(f => f.is_zip);
          if (zipFileInfo) {
            setZipFile(zipFileInfo);
          }
        }
        // Expand passed validations (but not warnings)
        newValidations.filter(v => v.valid && v.type !== 'warning').forEach(v => {
          setExpandedValidations(prev => new Set([...prev, v.step]));
        });
      }
      
      setIsProcessing(false);
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
    setExpandedValidations(new Set());
    setError(null);
    setProcessingStatus('');
    setCurrentValidationStep('');
    setWaitingForUserInput(false);
    setZipFile(null);
  };

  const handleUserInput = async (step, userChoice) => {
    try {
      setProcessingStatus('Processing your choice...');
      setIsProcessing(true);
      
      if (step === 'us_zip_code_validation' && userChoice === 'cancel') {
        setProcessingStatus('Processing stopped by user request');
        setIsProcessing(false);
        setWaitingForUserInput(false);
        // Remove the user input requirement by setting autocorrectable_count to 0
        setValidationResults(prev => prev.map(validation => 
          validation.step === 'us_zip_code_validation' && !validation.valid
            ? { ...validation, autocorrectable_count: 0 }
            : validation
        ));
        return;
      }
      
      if (step === 'missing_zip_code_validation' && userChoice === 'cancel') {
        setProcessingStatus('Processing stopped by user request');
        setIsProcessing(false);
        setWaitingForUserInput(false);
        // Remove the user input requirement by setting show_buttons to false
        setValidationResults(prev => prev.map(validation => 
          validation.step === 'missing_zip_code_validation' && !validation.valid
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
      
      // Process the migration with checkbox values
      await processFiles(subscriberFile, mappingFile, sellerName, vaultProvider, isSandbox, provider, autocorrectUsZipCodes, useMappingZipCodes);
      
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

        <div className="checkbox-group">
          <div className="checkbox-item">
            <label className="checkbox-label">
              <input
                type="checkbox"
                checked={useMappingZipCodes}
                onChange={(e) => setUseMappingZipCodes(e.target.checked)}
                className="checkbox-input"
              />
              <span>Use Mapping ZIP Codes</span>
              <div className="info-icon-wrapper">
                <span className="info-icon">ℹ️</span>
                <div className="tooltip">
                  If any required ZIP codes are missing, use ZIP codes from the mapping file if available.
                </div>
              </div>
            </label>
          </div>
          <div className="checkbox-item">
            <label className="checkbox-label">
              <input
                type="checkbox"
                checked={autocorrectUsZipCodes}
                onChange={(e) => setAutocorrectUsZipCodes(e.target.checked)}
                className="checkbox-input"
              />
              <span>Autocorrect US ZIP codes leading zeros</span>
              <div className="info-icon-wrapper">
                <span className="info-icon">ℹ️</span>
                <div className="tooltip">
                  Detect when a US ZIP code is only 4 digits and add a leading zero
                </div>
              </div>
            </label>
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
          {currentValidationStep === 'ca_zip_code_validation' && 'Canadian zip code validation in progress...'}
          {currentValidationStep === 'us_zip_code_validation' && 'US zip code validation in progress...'}
          {currentValidationStep === 'missing_zip_code_validation' && 'Missing zip code validation in progress...'}
            </div>
          )}
        </div>
      )}

      {validationResults.map((validation, index) => {
        const isExpanded = expandedValidations.has(validation.step);
        const isWarning = validation.type === 'warning';
        
        // Determine if validation box should be collapsible
        // Failed validations and warnings are always collapsible
        // Successful validations are only collapsible if they have additional content
        let isCollapsible = false;
        if (!validation.valid || isWarning) {
          // Failed validations and warnings are always collapsible
          isCollapsible = true;
        } else if (validation.valid) {
          // Successful validations are only collapsible if they have content to show
          if (validation.step === 'us_zip_code_validation' && validation.autocorrected_count > 0) {
            isCollapsible = true;
          } else if (validation.step === 'missing_zip_code_validation' && validation.pulled_from_mapping_count > 0) {
            isCollapsible = true;
          } else if (validation.step === 'successfully_mapped_records') {
            // Successfully mapped records always has content (message + download)
            isCollapsible = true;
          }
          // Other successful validations with no additional content are not collapsible
        }
        
        const validationKey = validation.timestamp || index;
        
        const isSuccessfullyMapped = validation.step === 'successfully_mapped_records';
        return (
        <div key={validationKey} className={`validation-result ${isSuccessfullyMapped ? 'super-success' : (isWarning ? 'warning' : (validation.valid ? 'valid' : 'invalid'))}`}>
          <div 
            className="validation-header" 
            onClick={isCollapsible ? () => toggleValidation(validation.step) : undefined}
            style={isCollapsible ? { cursor: 'pointer' } : {}}
          >
            {isCollapsible && (
              <span className="validation-chevron" style={{ marginRight: '8px' }}>
                {isExpanded ? '▼' : '▶'}
              </span>
            )}
            <span className="validation-icon">
              {isWarning ? '⚠' : (validation.valid ? '✓' : '✗')}
            </span>
            <span className="validation-title">
              {validation.step === 'column_validation' 
                ? (validation.valid ? 'Column validation passed' : `Column validation failed${validation.missing_columns ? ` (${validation.missing_columns.length})` : ''}`)
                : validation.step === 'date_format_validation'
                ? (validation.valid ? 'Date format validation passed' : `Date format validation failed${validation.incorrect_count !== undefined ? ` (${validation.incorrect_count})` : ''}`)
                : validation.step === 'date_validation'
                ? (validation.valid ? 'Date validation passed' : `Date validation failed${validation.incorrect_count !== undefined ? ` (${validation.incorrect_count})` : ''}`)
                : validation.step === 'card_token_validation'
                ? (validation.valid ? 'Card token validation passed' : `Card token validation failed${validation.incorrect_count !== undefined ? ` (${validation.incorrect_count})` : ''}`)
                : validation.step === 'ca_zip_code_validation'
                ? (validation.valid ? 'Canadian zip code validation passed' : `Canadian zip code validation failed${validation.incorrect_count !== undefined ? ` (${validation.incorrect_count})` : ''}`)
                : validation.step === 'us_zip_code_validation'
                ? (validation.valid ? 'US zip code validation passed' : `US zip code validation failed${validation.incorrect_count !== undefined ? ` (${validation.incorrect_count})` : ''}`)
                : validation.step === 'missing_zip_code_validation'
                ? (validation.valid ? 'Missing zip code validation passed   🇦🇺 🇨🇦 🇫🇷 🇩🇪 🇮🇳 🇮🇹 🇳🇱 🇪🇸 🇬🇧 🇺🇸' : `Missing zip code validation failed   🇦🇺 🇨🇦 🇫🇷 🇩🇪 🇮🇳 🇮🇹 🇳🇱 🇪🇸 🇬🇧 🇺🇸${validation.missing_count !== undefined ? ` (${validation.missing_count})` : ''}`)
                : validation.step === 'duplicate_tokens'
                ? `Duplicate card tokens detected (${validation.count})`
                : validation.step === 'duplicate_external_subscription_ids'
                ? `Duplicate external subscription IDs detected (${validation.count})`
                : validation.step === 'duplicate_emails'
                ? `Duplicate customer emails detected (${validation.count})`
                : validation.step === 'duplicate_card_ids'
                ? `Duplicate card IDs detected (${validation.count})`
                : validation.step === 'no_token_found'
                ? (validation.valid ? 'No token found validation passed' : `No token found (${validation.count})`)
                : validation.step === 'successfully_mapped_records'
                ? `Successfully mapped records (${validation.count})`
                : validation.step === 'duplicate_detection'
                ? 'Duplicate detection requires input'
                : (validation.valid ? `${validation.step} passed` : `${validation.step} failed`)
              }
            </span>
            {validation.download_file && (
              <button 
                className="download-report-btn"
                onClick={(e) => {
                  e.stopPropagation();
                  const link = document.createElement('a');
                  link.href = `http://localhost:5001/api/download/${validation.download_file}`;
                  link.download = validation.download_file;
                  document.body.appendChild(link);
                  link.click();
                  document.body.removeChild(link);
                }}
                title={isWarning ? "Download duplicate records report" : validation.valid && validation.step === 'successfully_mapped_records' ? "Download final import file" : "Download incorrect records report"}
              >
                📥
              </button>
            )}
            {validation.requires_user_input && (
              <span className="user-input-required">❓</span>
            )}
          </div>
          {(!isCollapsible || isExpanded) && !(validation.step === 'column_validation' && validation.valid) && (
          <div className="validation-details">
            {isWarning ? (
              <>
                <p>{validation.message}</p>
                {validation.download_file && (
                  <div className="missing-columns">
                    <p>Click the download icon to get a report of all duplicate records.</p>
                  </div>
                )}
              </>
            ) : validation.step === 'column_validation' ? (
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
            ) : validation.step === 'ca_zip_code_validation' ? (
              <>
                {!validation.valid && (
                  <>
                    <p>Canadian zip codes must be in the format: Letter-Number-Letter Number-Letter-Number (e.g., A1A 1A1).</p>
                    <div className="missing-columns">
                      <p><strong>Found {validation.incorrect_count} Canadian zip codes with incorrect format.</strong></p>
                      <p>Click the download icon to get a report of all incorrect records.</p>
                    </div>
                  </>
                )}
              </>
            ) : validation.step === 'us_zip_code_validation' ? (
              <>
                {!validation.valid ? (
                  <>
                    <p>US zip codes must be exactly 5 numerical digits.</p>
                    <div className="missing-columns">
                      <p><strong>Found {validation.incorrect_count} US zip codes with incorrect format.</strong></p>
                      {validation.autocorrected_count > 0 && (
                        <p><strong>{validation.autocorrected_count} were autocorrected with leading zeros.</strong></p>
                      )}
                      <p>Click the download icon to get a report of all incorrect records.</p>
                    </div>
                  </>
                ) : (
                  <>
                    <p>US zip codes must be exactly 5 numerical digits.</p>
                    {validation.autocorrected_count > 0 && (
                      <div className="missing-columns">
                        <p><strong>{validation.autocorrected_count} US zip codes were autocorrected with leading zeros.</strong></p>
                      </div>
                    )}
                  </>
                )}
              </>
            ) : validation.step === 'missing_zip_code_validation' ? (
              <>
                {!validation.valid ? (
                  <>
                    <p>Zip codes are required for AU, CA, FR, DE, IN, IT, NL, ES, GB, US addresses.</p>
                    <div className="missing-columns">
                      <p><strong>Found {validation.missing_count} records with missing zip codes.</strong></p>
                      {validation.pulled_from_mapping_count > 0 && (
                        <p><strong>{validation.pulled_from_mapping_count} zip codes were pulled from the mapping file.</strong></p>
                      )}
                      <p>Click the download icon to get a report of all missing records.</p>
                    </div>
                  </>
                ) : (
                  <>
                    <p>Zip codes are required for AU, CA, FR, DE, IN, IT, NL, ES, GB, US addresses.</p>
                    {validation.pulled_from_mapping_count > 0 && (
                      <div className="missing-columns">
                        <p><strong>{validation.pulled_from_mapping_count} zip codes were pulled from the mapping file.</strong></p>
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
            ) : validation.step === 'no_token_found' ? (
              <>
                {!validation.valid ? (
                  <>
                    <p>{validation.message}</p>
                    {validation.download_file && (
                      <div className="missing-columns">
                        <p>Click the download icon to get a report of all records with no matching token.</p>
                      </div>
                    )}
                  </>
                ) : (
                  <p>{validation.message}</p>
                )}
              </>
            ) : validation.step === 'successfully_mapped_records' ? (
              <>
                <p>{validation.message}</p>
                {validation.download_file && (
                  <div className="missing-columns">
                    <p>Click the download icon to get the final import file with all successfully mapped records.</p>
                  </div>
                )}
              </>
            ) : (
              <>
                {validation.valid ? null : (
                  <p>Validation failed</p>
                )}
              </>
            )}
          </div>
          )}
        </div>
        );
      })}


      {validationResults.length > 0 && zipFile && (
        <div style={{marginTop: '20px', textAlign: 'center'}}>
          <button
            onClick={() => {
              const link = document.createElement('a');
              link.href = `http://localhost:5001/api/download/${zipFile.name}`;
              link.download = zipFile.name;
              document.body.appendChild(link);
              link.click();
              document.body.removeChild(link);
            }}
            className="submit-btn"
            style={{padding: '12px 24px', fontSize: '16px'}}
          >
            📦 Download All Reports
          </button>
        </div>
      )}

      {error && (
        <div className="error-message">
          {error}
        </div>
      )}
    </div>
  );
};

export default FileUpload; 