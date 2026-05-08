import React, { useState } from 'react';

/** Steps returned when the API uses the legacy single-error payload (one failing step per response). */
const LEGACY_SINGLE_STEP_VALIDATION_STEPS = [
  'column_validation',
  'card_token_presence_validation',
  'customer_email_presence_validation',
  'status_presence_validation',
  'currency_code_presence_validation',
  'collection_mode_presence_validation',
  'subscription_external_id_presence_validation',
  'date_format_validation',
  'date_validation',
  'address_country_code_validation',
  'price_id_validation',
  'unsupported_countries_validation',
  'ca_zip_code_validation',
  'us_zip_code_validation',
  'missing_zip_code_validation',
];

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
  const [expandedValidations, setExpandedValidations] = useState(new Set());
  const [zipFile, setZipFile] = useState(null);
  const [useMappingZipCodes, setUseMappingZipCodes] = useState(false);
  const [autocorrectUsZipCodes, setAutocorrectUsZipCodes] = useState(false);
  const [stripIsoDateFractionalSuffix, setStripIsoDateFractionalSuffix] = useState(false);
  const [anonymiseEmailInSandbox, setAnonymiseEmailInSandbox] = useState(false);
  const [subscriberCsvCheckOnlyNoTokens, setSubscriberCsvCheckOnlyNoTokens] = useState(false);

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

  const processFiles = async (subFile, mapFile, seller, vault, sandbox, prov, autocorrect = false, useMappingZip = false, anonymiseEmail = false, stripIsoFractional = false, subscriberCheckOnly = false) => {
    const formData = new FormData();
    formData.append('subscriber_file', subFile);
    if (!subscriberCheckOnly && mapFile) {
      formData.append('mapping_file', mapFile);
    }
    if (subscriberCheckOnly) {
      formData.append('subscriber_csv_check_only', 'true');
    }
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
    formData.append('anonymise_email', sandbox && anonymiseEmail ? 'true' : 'false');
    if (stripIsoFractional) {
      formData.append('strip_iso_date_fractional_suffix', 'true');
    }

    try {
      setProcessingStatus(subscriberCheckOnly ? 'Checking subscriber CSV…' : 'Uploading files...');
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
          // All collapsible boxes start collapsed
        }
        
        // Add the validation that requires user input
        const newValidation = {
          ...result.validation_result,
          step: result.step,
          timestamp: Date.now()
        };
        setValidationResults(prev => [...prev, newValidation]);
        // All collapsible boxes start collapsed
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
        // Initialize expanded state: all collapsible boxes start collapsed except successfully mapped records
        const initialExpanded = new Set(
          allValidations.filter(v => v.step === 'successfully_mapped_records' && v.valid).map(v => v.step)
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
      if (result.error && LEGACY_SINGLE_STEP_VALIDATION_STEPS.includes(result.step)) {
        // Add any previous successful validations first
        if (result.validation_results) {
          const previousValidations = result.validation_results.map(validation => ({
            ...validation,
            timestamp: Date.now()
          }));
          setValidationResults(prev => [...prev, ...previousValidations]);
          // All collapsible boxes start collapsed
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
        // Store zip file if available (same as validation-failure path)
        if (result.zip_file) {
          setZipFile(result.zip_file);
        } else if (result.output_files) {
          const zipFileInfo = result.output_files.find(f => f.is_zip);
          if (zipFileInfo) {
            setZipFile(zipFileInfo);
          }
        }
        // All collapsible boxes start collapsed except successfully mapped records
        const initialExpanded = new Set(
          newValidations.filter(v => v.step === 'successfully_mapped_records' && v.valid).map(v => v.step)
        );
        setExpandedValidations(initialExpanded);
      }
      
      setIsProcessing(false);
      setProcessingStatus(
        result.subscriber_check_only
          ? 'Subscriber CSV checks completed.'
          : 'Processing completed successfully!'
      );
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
    setZipFile(null);
  };

  const handleUserInput = async (step, userChoice) => {
    try {
      setProcessingStatus('Processing your choice...');
      setIsProcessing(true);
      
      if (step === 'us_zip_code_validation' && userChoice === 'cancel') {
        setProcessingStatus('Processing stopped by user request');
        setIsProcessing(false);
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
      } else if (result.status === 'continuing') {
        // Continue with processing - you would call the migration function again here
        setProcessingStatus('Continuing with processing...');
        // For now, just show success
        setProcessingStatus('Processing completed successfully!');
        setIsProcessing(false);
      }
    } catch (err) {
      setError('Error processing user choice: ' + err.message);
      setIsProcessing(false);
      
      // Scroll to bottom to show error message
      setTimeout(() => {
        window.scrollTo({ top: document.body.scrollHeight, behavior: 'smooth' });
      }, 100);
    }
  };

  const handleSubmit = async (e) => {
    e.preventDefault();
    
    if (!subscriberFile || !sellerName || !vaultProvider) {
      setError('Please fill in all required fields and upload the subscriber export.');
      return;
    }
    if (!subscriberCsvCheckOnlyNoTokens && !mappingFile) {
      setError('Please upload a mapping file, or enable “Subscriber Data CSV Check Only – No tokens”.');
      return;
    }

    // Reset all validation state when starting a new process
    resetValidationState();
    setIsProcessing(true);
    setProcessingStatus(subscriberCsvCheckOnlyNoTokens ? 'Checking subscriber CSV…' : 'Processing migration...');

    try {
      // Check if server is running
      const healthResponse = await fetch('/api/health');
      if (!healthResponse.ok) {
        throw new Error('Backend server is not responding. Please ensure the Python server is running on port 5001.');
      }
      
      // Process the migration with checkbox values
      await processFiles(
        subscriberFile,
        mappingFile,
        sellerName,
        vaultProvider,
        isSandbox,
        provider,
        autocorrectUsZipCodes,
        useMappingZipCodes,
        anonymiseEmailInSandbox,
        stripIsoDateFractionalSuffix,
        subscriberCsvCheckOnlyNoTokens
      );
      
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
              Ixopay (fka TokenEx)
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
            <>
              <div className="sandbox-message">
                Optionally anonymise customer emails with blackhole addresses using the toggle below.
              </div>
              <div className="checkbox-item sandbox-anonymise-toggle">
                <label className="checkbox-label">
                  <input
                    type="checkbox"
                    checked={anonymiseEmailInSandbox}
                    onChange={(e) => setAnonymiseEmailInSandbox(e.target.checked)}
                    className="checkbox-input"
                  />
                  <span>Anonymise email addresses</span>
                </label>
              </div>
            </>
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

        <div className={`form-group${subscriberCsvCheckOnlyNoTokens ? ' mapping-upload-disabled' : ''}`}>
          <label htmlFor="mappingFile">Token File:</label>
          <div className="file-input-wrapper">
            <input
              type="file"
              id="mappingFile"
              accept=".csv,.txt"
              onChange={(e) => handleFileChange(e, 'mapping')}
              required={!subscriberCsvCheckOnlyNoTokens}
              disabled={subscriberCsvCheckOnlyNoTokens}
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

        <div className="form-group subscriber-check-only-option">
          <label className="checkbox-label">
            <input
              type="checkbox"
              checked={subscriberCsvCheckOnlyNoTokens}
              onChange={(e) => {
                const on = e.target.checked;
                setSubscriberCsvCheckOnlyNoTokens(on);
                if (on) {
                  setMappingFile(null);
                  setMappingRecordCount(0);
                }
              }}
              className="checkbox-input"
            />
            <span>Subscriber Data CSV Check Only - No tokens</span>
          </label>
        </div>

        <div className="checkbox-group">
          <div className="checkbox-item">
            <label className={`checkbox-label${subscriberCsvCheckOnlyNoTokens ? ' checkbox-disabled' : ''}`}>
              <input
                type="checkbox"
                checked={useMappingZipCodes}
                onChange={(e) => setUseMappingZipCodes(e.target.checked)}
                disabled={subscriberCsvCheckOnlyNoTokens}
                className="checkbox-input"
              />
              <span>Use ZIP Codes from Token file</span>
              <div className="info-icon-wrapper">
                <span className="info-icon">ℹ️</span>
                <div className="tooltip">
                  If any required ZIP codes are missing, use ZIP codes from the token file if available.
                </div>
              </div>
            </label>
          </div>
          <div className="checkbox-item">
            <label className="checkbox-label">
              <input
                type="checkbox"
                checked={stripIsoDateFractionalSuffix}
                onChange={(e) => setStripIsoDateFractionalSuffix(e.target.checked)}
                className="checkbox-input"
              />
              <span>Remove fractional seconds from dates (common Stripe format)</span>
              <div className="info-icon-wrapper">
                <span className="info-icon">ℹ️</span>
                <div className="tooltip">
                  When enabled, normalises ISO timestamps on started_at, paused_at, current_period_started_at, and current_period_ends_at by stripping fractional seconds (anything like .000 or .123 before the Z). Example: 2026-04-09T10:10:08.123Z becomes 2026-04-09T10:10:08Z. Values that already have no fractional part are left unchanged.
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
              <span>{subscriberCsvCheckOnlyNoTokens ? 'Checking…' : 'Processing...'}</span>
            </div>
          ) : (
            subscriberCsvCheckOnlyNoTokens ? 'Check CSV File' : 'Process Migration'
          )}
        </button>
      </form>

      {processingStatus && (
        <div className="processing-status">
          {processingStatus}
          {currentValidationStep && (
            <div className="validation-progress">
                        {currentValidationStep === 'column_validation' && 'Column validation in progress...'}
          {currentValidationStep === 'address_country_code_validation' && 'Address country code validation in progress...'}
          {currentValidationStep === 'price_id_validation' && 'Price ID validation in progress...'}
          {currentValidationStep === 'card_token_presence_validation' && 'Card token (required value) validation in progress...'}
          {currentValidationStep === 'customer_email_presence_validation' && 'Customer email validation in progress...'}
          {currentValidationStep === 'status_presence_validation' && 'Status validation (active, trialing, or paused) in progress...'}
          {currentValidationStep === 'currency_code_presence_validation' && 'Currency code validation in progress...'}
          {currentValidationStep === 'collection_mode_presence_validation' && 'Collection mode validation in progress...'}
          {currentValidationStep === 'subscription_external_id_presence_validation' && 'Subscription external ID validation in progress...'}
          {currentValidationStep === 'date_format_validation' && 'Date format validation in progress...'}
          {currentValidationStep === 'date_validation' && 'Date validation in progress...'}
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
        const isUnableToValidate = typeof validation.error === 'string' && validation.error.startsWith('Unable to validate as column is missing');
        
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

        if (validation.step === 'successfully_mapped_records' && !validation.valid) {
          isCollapsible = false;
        }
        if (validation.type === 'super_failure') {
          isCollapsible = false;
        }
        
        const validationKey = validation.timestamp || index;
        
        const isSuccessfullyMappedSuccess =
          validation.step === 'successfully_mapped_records' && validation.valid;
        const isSuperFailureStyle =
          validation.type === 'super_failure' ||
          (validation.step === 'successfully_mapped_records' && !validation.valid);
        return (
        <div key={validationKey} className={`validation-result ${isSuccessfullyMappedSuccess ? 'super-success' : isSuperFailureStyle ? 'super-failure' : (isWarning ? 'warning' : (validation.valid ? 'valid' : 'invalid'))}`}>
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
              {validation.step === 'merge_key_columns_validation'
                ? 'Unable to merge — required columns missing'
              : validation.step === 'subscriber_header_normalization'
                ? 'Unable to process — ambiguous subscription columns'
              : validation.step === 'column_validation' 
                ? (validation.valid ? 'Column validation passed' : `Column validation failed${validation.missing_columns ? ` (${validation.missing_columns.length})` : ''}`)
                : validation.step === 'address_country_code_validation'
                ? (validation.valid ? 'Address country code validation passed' : `Address country code validation failed${validation.incorrect_count !== undefined ? ` (${validation.incorrect_count})` : ''}`)
                : validation.step === 'price_id_validation'
                ? (validation.valid ? 'Price ID validation passed' : `Price ID validation failed${validation.incorrect_count !== undefined ? ` (${validation.incorrect_count})` : ''}`)
                : validation.step === 'card_token_presence_validation'
                ? (validation.valid ? 'Card token value validation passed' : (isUnableToValidate ? 'Card token value validation - Unable to validate' : `Card token value validation failed${validation.incorrect_count !== undefined ? ` (${validation.incorrect_count})` : ''}`))
                : validation.step === 'customer_email_presence_validation'
                ? (validation.valid ? 'Customer email validation passed' : (isUnableToValidate ? 'Customer email validation - Unable to validate' : `Customer email validation failed${validation.incorrect_count !== undefined ? ` (${validation.incorrect_count})` : ''}`))
                : validation.step === 'status_presence_validation'
                ? (validation.valid ? 'Status validation passed' : (isUnableToValidate ? 'Status validation - Unable to validate' : `Status validation failed${validation.incorrect_count !== undefined ? ` (${validation.incorrect_count})` : ''}`))
                : validation.step === 'currency_code_presence_validation'
                ? (validation.valid ? 'Currency code validation passed' : (isUnableToValidate ? 'Currency code validation - Unable to validate' : `Currency code validation failed${validation.incorrect_count !== undefined ? ` (${validation.incorrect_count})` : ''}`))
                : validation.step === 'collection_mode_presence_validation'
                ? (validation.valid ? 'Collection mode validation passed' : (isUnableToValidate ? 'Collection mode validation - Unable to validate' : `Collection mode validation failed${validation.incorrect_count !== undefined ? ` (${validation.incorrect_count})` : ''}`))
                : validation.step === 'subscription_external_id_presence_validation'
                ? (validation.valid ? 'Subscription external ID validation passed' : (isUnableToValidate ? 'Subscription external ID validation - Unable to validate' : `Subscription external ID validation failed${validation.incorrect_count !== undefined ? ` (${validation.incorrect_count})` : ''}`))
                : validation.step === 'date_format_validation'
                ? (validation.valid ? 'Date format validation passed' : `Date format validation failed${validation.incorrect_count !== undefined ? ` (${validation.incorrect_count})` : ''}`)
                : validation.step === 'date_validation'
                ? (validation.valid ? 'Date validation passed' : `Date validation failed${validation.incorrect_count !== undefined ? ` (${validation.incorrect_count})` : ''}`)
                : validation.step === 'ca_zip_code_validation'
                ? (validation.valid ? 'Canadian zip code validation passed' : `Canadian zip code validation failed${validation.incorrect_count !== undefined ? ` (${validation.incorrect_count})` : ''}`)
                : validation.step === 'us_zip_code_validation'
                ? (validation.valid ? 'US zip code validation passed' : `US zip code validation failed${validation.incorrect_count !== undefined ? ` (${validation.incorrect_count})` : ''}`)
                : validation.step === 'unsupported_countries_validation'
                ? (validation.valid ? 'Unsupported countries validation passed' : `Unsupported countries validation failed${validation.incorrect_count !== undefined ? ` (${validation.incorrect_count})` : ''}`)
                : validation.step === 'missing_zip_code_validation'
                ? (() => {
                    const countriesDict = validation.required_countries_dict || {
                      'AU': '🇦🇺', 'CA': '🇨🇦', 'FR': '🇫🇷', 'DE': '🇩🇪', 'IN': '🇮🇳', 
                      'IT': '🇮🇹', 'NL': '🇳🇱', 'ES': '🇪🇸', 'GB': '🇬🇧', 'US': '🇺🇸'
                    };
                    const flags = Object.values(countriesDict).join(' ');
                    return validation.valid 
                      ? `Missing zip code validation passed   ${flags}` 
                      : `Missing zip code validation failed   ${flags}${validation.missing_count !== undefined ? ` (${validation.missing_count})` : ''}`;
                  })()
                : validation.step === 'duplicate_tokens'
                ? `Duplicate card tokens detected (${validation.count})`
                : validation.step === 'duplicate_external_subscription_ids'
                ? `Duplicate external subscription IDs detected (${validation.count})`
                : validation.step === 'duplicate_emails'
                ? `Duplicate customer emails detected (${validation.count})`
                : validation.step === 'duplicate_card_ids'
                ? `Duplicate card IDs detected (${validation.count})`
                : validation.step === 'status_paused_warning'
                ? `Paused subscription status (${validation.count})`
                : validation.step === 'no_token_found'
                ? (validation.valid ? 'No token found validation passed' : `No token found (${validation.count})`)
                : validation.step === 'successfully_mapped_records'
                ? (validation.valid
                  ? `Successfully mapped records (${validation.count})`
                  : 'Unable to successfully map any records')
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
                title={isWarning ? (validation.step === 'status_paused_warning' ? 'Download paused status report' : 'Download duplicate records report') : validation.valid && validation.step === 'successfully_mapped_records' ? "Download final import file" : "Download incorrect records report"}
              >
                📥
              </button>
            )}
            {validation.requires_user_input && (
              <span className="user-input-required">❓</span>
            )}
          </div>
          {(!isCollapsible || isExpanded) && 
           !(validation.step === 'column_validation' && validation.valid) && 
           !(validation.step === 'address_country_code_validation' && validation.valid) &&
           !(validation.step === 'price_id_validation' && validation.valid) &&
           !(validation.step === 'card_token_presence_validation' && validation.valid) &&
           !(validation.step === 'customer_email_presence_validation' && validation.valid) &&
           !(validation.step === 'status_presence_validation' && validation.valid) &&
           !(validation.step === 'currency_code_presence_validation' && validation.valid) &&
           !(validation.step === 'collection_mode_presence_validation' && validation.valid) &&
           !(validation.step === 'subscription_external_id_presence_validation' && validation.valid) &&
           !(validation.step === 'date_format_validation' && validation.valid) &&
           !(validation.step === 'date_validation' && validation.valid) &&
           !(validation.step === 'ca_zip_code_validation' && validation.valid) &&
           !(validation.step === 'unsupported_countries_validation' && validation.valid) &&
           !(validation.step === 'us_zip_code_validation' && validation.valid && (!validation.autocorrected_count || validation.autocorrected_count === 0)) &&
           !(validation.step === 'missing_zip_code_validation' && validation.valid && (!validation.pulled_from_mapping_count || validation.pulled_from_mapping_count === 0)) && (
          <div className="validation-details">
            {isWarning ? (
              <>
                <p>{validation.message}</p>
                {validation.download_file && (
                  <div className="missing-columns">
                    <p>
                      {validation.step === 'status_paused_warning'
                        ? 'Click the download icon to get a report of all paused records.'
                        : 'Click the download icon to get a report of all duplicate records.'}
                    </p>
                  </div>
                )}
              </>
            ) : validation.step === 'merge_key_columns_validation' ? (
              <>
                <p>{validation.message}</p>
              </>
            ) : validation.step === 'subscriber_header_normalization' ? (
              <>
                <p>{validation.message}</p>
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
            ) : validation.step === 'address_country_code_validation' ? (
              <>
                {!validation.valid && (
                  <>
                    <p>Every row must have an <code>address_country_code</code> value, and it must be exactly two letters (A–Z), for example <code>US</code> or <code>GB</code>. Empty values, numbers, or longer codes are not allowed.</p>
                    {validation.error && (
                      <div className="missing-columns">
                        <p><strong>Error:</strong> {validation.error}</p>
                      </div>
                    )}
                    <div className="missing-columns">
                      <p><strong>Found {validation.incorrect_count !== undefined ? validation.incorrect_count : 0} records with invalid or missing country codes.</strong></p>
                      <p>Click the download icon to get a report of all incorrect records.</p>
                    </div>
                  </>
                )}
              </>
            ) : validation.step === 'price_id_validation' ? (
              <>
                {!validation.valid && (
                  <>
                    <p>Every row needs a price ID (<code>price_id_1</code>) starting with <code>pri_</code>. For each line, if <code>price_id_N</code> is filled, the matching <code>quantity_N</code> must be a non-negative integer (whole number).</p>
                    {validation.error && (
                      <div className="missing-columns">
                        <p><strong>Error:</strong> {validation.error}</p>
                      </div>
                    )}
                    <div className="missing-columns">
                      <p><strong>Found {validation.incorrect_count !== undefined ? validation.incorrect_count : 0} records with invalid or missing price IDs.</strong></p>
                      <p>Click the download icon to get a report of all incorrect records.</p>
                    </div>
                  </>
                )}
              </>
            ) : validation.step === 'card_token_presence_validation' ? (
              <>
                {!validation.valid && (
                  <div className="missing-columns">
                    {validation.error ? (
                      <p><strong>{validation.error}</strong></p>
                    ) : (
                      <>
                        <p><strong>Found {validation.incorrect_count !== undefined ? validation.incorrect_count : 0} subscription rows with no card token value.</strong></p>
                        <p>Click the download icon to get a report of all incorrect records.</p>
                      </>
                    )}
                  </div>
                )}
              </>
            ) : validation.step === 'customer_email_presence_validation' ? (
              <>
                {!validation.valid && (
                  <div className="missing-columns">
                    {validation.error ? (
                      <p><strong>{validation.error}</strong></p>
                    ) : (
                      <>
                        <p><strong>Found {validation.incorrect_count !== undefined ? validation.incorrect_count : 0} rows with missing customer email.</strong></p>
                        <p>Click the download icon to get a report of all incorrect records.</p>
                      </>
                    )}
                  </div>
                )}
              </>
            ) : validation.step === 'status_presence_validation' ? (
              <>
                {!validation.valid && (
                  <div className="missing-columns">
                    {validation.error ? (
                      <p><strong>{validation.error}</strong></p>
                    ) : (
                      <>
                        <p><strong>Found {validation.incorrect_count !== undefined ? validation.incorrect_count : 0} rows with invalid or missing status.</strong></p>
                        <p>Allowed values are <code>active</code>, <code>trialing</code>, and <code>paused</code> (case-insensitive).</p>
                        <p>Click the download icon to get a report of all incorrect records.</p>
                      </>
                    )}
                  </div>
                )}
              </>
            ) : validation.step === 'currency_code_presence_validation' ? (
              <>
                {!validation.valid && (
                  <div className="missing-columns">
                    {validation.error ? (
                      <p><strong>{validation.error}</strong></p>
                    ) : (
                      <>
                        <p><strong>Found {validation.incorrect_count !== undefined ? validation.incorrect_count : 0} rows with missing currency code.</strong></p>
                        <p>Click the download icon to get a report of all incorrect records.</p>
                      </>
                    )}
                  </div>
                )}
              </>
            ) : validation.step === 'collection_mode_presence_validation' ? (
              <>
                {!validation.valid && (
                  <div className="missing-columns">
                    {validation.error ? (
                      <p><strong>{validation.error}</strong></p>
                    ) : (
                      <>
                        <p><strong>Found {validation.incorrect_count !== undefined ? validation.incorrect_count : 0} rows with missing collection mode.</strong></p>
                        <p>Click the download icon to get a report of all incorrect records.</p>
                      </>
                    )}
                  </div>
                )}
              </>
            ) : validation.step === 'subscription_external_id_presence_validation' ? (
              <>
                {!validation.valid && (
                  <div className="missing-columns">
                    {validation.error ? (
                      <p><strong>{validation.error}</strong></p>
                    ) : (
                      <>
                        <p><strong>Found {validation.incorrect_count !== undefined ? validation.incorrect_count : 0} rows with missing subscription external ID.</strong></p>
                        <p>Click the download icon to get a report of all incorrect records.</p>
                      </>
                    )}
                  </div>
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
            ) : validation.step === 'unsupported_countries_validation' ? (
              <>
                {!validation.valid && (
                  <>
                    <p>Records with unsupported country codes are not allowed. The following countries are not supported: {validation.unsupported_countries ? validation.unsupported_countries.join(', ') : 'AF, AQ, BY, MM, CF, CU, CD, HT, IR, IQ, LY, ML, AN, NI, KP, RU, SO, SS, SD, SY, VE, YE, ZW'}.</p>
                    <p>{validation.unsupported_countries_dict ? Object.values(validation.unsupported_countries_dict).join(' ') : '🇦🇫 🇦🇶 🇧🇾 🇲🇲 🇨🇫 🇨🇺 🇨🇩 🇭🇹 🇮🇷 🇮🇶 🇱🇾 🇲🇱 🇳🇮 🇰🇵 🇷🇺 🇸🇴 🇸🇸 🇸🇩 🇸🇾 🇻🇪 🇾🇪 🇿🇼'}</p>
                    <div className="missing-columns">
                      <p><strong>Found {validation.incorrect_count} records with unsupported country codes.</strong></p>
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
                  validation.autocorrected_count > 0 ? (
                    <div className="missing-columns">
                      <p><strong>{validation.autocorrected_count} US zip codes were autocorrected with leading zeros.</strong></p>
                    </div>
                  ) : null
                )}
              </>
            ) : validation.step === 'missing_zip_code_validation' ? (
              <>
                {!validation.valid ? (
                  <>
                    <p>Zip codes are required for {validation.required_countries ? validation.required_countries.join(', ') : 'AU, CA, FR, DE, IN, IT, NL, ES, GB, US'} addresses.</p>
                    {validation.required_countries && validation.required_countries.length > 0 && (
                      <p>
                        {validation.required_countries_dict ? Object.values(validation.required_countries_dict).join(' ') : '🇦🇺 🇨🇦 🇫🇷 🇩🇪 🇮🇳 🇮🇹 🇳🇱 🇪🇸 🇬🇧 🇺🇸'}
                      </p>
                    )}
                    <div className="missing-columns">
                      <p><strong>Found {validation.missing_count} records with missing zip codes.</strong></p>
                      {validation.pulled_from_mapping_count > 0 && (
                        <p><strong>{validation.pulled_from_mapping_count} zip codes were pulled from the mapping file.</strong></p>
                      )}
                      <p>Click the download icon to get a report of all missing records.</p>
                    </div>
                  </>
                ) : (
                  validation.pulled_from_mapping_count > 0 ? (
                    <div className="missing-columns">
                      <p><strong>{validation.pulled_from_mapping_count} zip codes were pulled from the mapping file.</strong></p>
                    </div>
                  ) : null
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