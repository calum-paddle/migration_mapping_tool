import React, { useState } from 'react';

const FileUpload = ({ onProcessingComplete }) => {
  const [subscriberFile, setSubscriberFile] = useState(null);
  const [mappingFile, setMappingFile] = useState(null);
  const [sellerName, setSellerName] = useState('');
  const [vaultProvider, setVaultProvider] = useState('');
  const [isSandbox, setIsSandbox] = useState(false);
  const [provider, setProvider] = useState('stripe');
  const [useMappingZipcodes, setUseMappingZipcodes] = useState(false);
  const [hasMissingPostalCodes, setHasMissingPostalCodes] = useState(false);
  const [missingPostalCodeCount, setMissingPostalCodeCount] = useState(0);
  const [isProcessing, setIsProcessing] = useState(false);
  const [processingStatus, setProcessingStatus] = useState('');
  const [error, setError] = useState(null);
  
  // New state for zip code validation
  const [showMissingZeroModal, setShowMissingZeroModal] = useState(false);
  const [showIncorrectFormatModal, setShowIncorrectFormatModal] = useState(false);
  const [missingZeroIssues, setMissingZeroIssues] = useState([]);
  const [incorrectFormatIssues, setIncorrectFormatIssues] = useState([]);
  const [pendingFormData, setPendingFormData] = useState(null);

  // New state for Bluesnap card_token validation
  const [showCardTokenFormatModal, setShowCardTokenFormatModal] = useState(false);
  const [cardTokenFormatIssues, setCardTokenFormatIssues] = useState([]);

  // New state for Canadian postal code validation
  const [showCanadianPostalModal, setShowCanadianPostalModal] = useState(false);
  const [canadianPostalIssues, setCanadianPostalIssues] = useState([]);

  // New state for date validation
  const [showDateValidationModal, setShowDateValidationModal] = useState(false);
  const [dateValidationIssues, setDateValidationIssues] = useState([]);

  const handleFileChange = (e, fileType) => {
    const file = e.target.files[0];
    if (!file) return;

    // Check if it's a CSV file (either .csv extension or text file with comma-separated content)
    const isCSV = file.name.toLowerCase().endsWith('.csv') ||
                  (file.type === 'text/plain' && file.name.toLowerCase().endsWith('.txt'));

    if (!isCSV) {
      alert('Please select a valid CSV file. The file should have a .csv extension or be a text file.');
      return;
    }

    // Additional validation for CSV content
    const reader = new FileReader();
    reader.onload = (event) => {
      const content = event.target.result;
      const lines = content.split('\n');
      const firstLine = lines[0];
      
      // Check if the first line contains commas (indicating CSV format)
      if (!firstLine.includes(',')) {
        alert('The selected file does not appear to be in CSV format. Please select a valid CSV file.');
        return;
      }

      if (fileType === 'subscriber') {
        setSubscriberFile(file);
        // Check for missing postal codes when subscriber file is selected
        checkForMissingPostalCodes(file);
      } else if (fileType === 'mapping') {
        setMappingFile(file);
      }
    };
    reader.readAsText(file);
  };

  // Reset missing postal codes state when subscriber file is cleared
  const clearSubscriberFile = () => {
    setSubscriberFile(null);
    setHasMissingPostalCodes(false);
    setMissingPostalCodeCount(0);
  };

  const checkForMissingPostalCodes = (file) => {
    const reader = new FileReader();
    reader.onload = (event) => {
      try {
        const content = event.target.result;
        const lines = content.split('\n');
        const headers = lines[0].split(',').map(h => h.trim().replace(/"/g, ''));
        
        // Find the column indices
        const countryCodeIndex = headers.findIndex(h => h.toLowerCase() === 'address_country_code');
        const postalCodeIndex = headers.findIndex(h => h.toLowerCase() === 'address_postal_code');
        
        if (countryCodeIndex === -1 || postalCodeIndex === -1) {
          console.log('Required columns not found in subscriber file');
          setHasMissingPostalCodes(false);
          setMissingPostalCodeCount(0);
          return;
        }
        
        // Supported countries for postal code mapping
        const supportedCountries = ['AU', 'CA', 'FR', 'DE', 'IN', 'IT', 'NL', 'ES', 'GB', 'US'];
        
        let missingCount = 0;
        
        // Check each data row
        for (let i = 1; i < lines.length; i++) {
          const line = lines[i].trim();
          if (!line) continue;
          
          const values = line.split(',').map(v => v.trim().replace(/"/g, ''));
          const countryCode = values[countryCodeIndex];
          const postalCode = values[postalCodeIndex];
          
          // Check if it's a supported country with missing postal code
          if (supportedCountries.includes(countryCode) && 
              (!postalCode || postalCode === '' || postalCode === 'nan' || postalCode === 'None')) {
            missingCount++;
          }
        }
        
        setMissingPostalCodeCount(missingCount);
        setHasMissingPostalCodes(missingCount > 0);
        
        if (missingCount > 0) {
          console.log(`Found ${missingCount} records with missing postal codes for supported countries`);
        } else {
          console.log('No missing postal codes found for supported countries');
        }
        
      } catch (error) {
        console.error('Error checking for missing postal codes:', error);
        setHasMissingPostalCodes(false);
        setMissingPostalCodeCount(0);
      }
    };
    reader.readAsText(file);
  };

  // New function to validate US zip codes
  const validateUSZipCodes = async (file) => {
    return new Promise((resolve) => {
      const reader = new FileReader();
      reader.onload = (e) => {
        const text = e.target.result;
        const lines = text.split('\n');
        const headers = lines[0].split(',').map(h => h.trim().replace(/"/g, ''));
        
        // Find column indices
        const countryCodeIndex = headers.findIndex(h => h.toLowerCase() === 'address_country_code');
        const postalCodeIndex = headers.findIndex(h => h.toLowerCase() === 'address_postal_code');
        
        if (countryCodeIndex === -1 || postalCodeIndex === -1) {
          resolve({ hasIssues: false, issues: [] });
          return;
        }
        
        const issues = [];
        
        for (let i = 1; i < lines.length; i++) {
          const line = lines[i];
          if (!line.trim()) continue;
          
          const values = line.split(',').map(v => v.trim().replace(/"/g, ''));
          const countryCode = values[countryCodeIndex];
          const postalCode = values[postalCodeIndex];
          
          if (countryCode === 'US' && postalCode) {
            // Check for 4-digit zip codes (missing leading zero)
            if (/^\d{4}$/.test(postalCode)) {
              issues.push({
                line: i + 1,
                postalCode: postalCode,
                correctedCode: '0' + postalCode,
                type: 'missing_zero'
              });
            }
            // Check for incorrect format (not 4 or 5 digits, or non-numeric)
            else if (!/^\d{5}$/.test(postalCode)) {
              issues.push({
                line: i + 1,
                postalCode: postalCode,
                correctedCode: null,
                type: 'incorrect_format'
              });
            }
          }
        }
        
        resolve({ hasIssues: issues.length > 0, issues });
      };
      reader.readAsText(file);
    });
  };

  // New function to validate Bluesnap card_token format
  const validateBluesnapCardTokens = async (file) => {
    return new Promise((resolve) => {
      const reader = new FileReader();
      reader.onload = (e) => {
        const text = e.target.result;
        const lines = text.split('\n');
        const headers = lines[0].split(',').map(h => h.trim().replace(/"/g, ''));
        
        // Find card_token column index
        const cardTokenIndex = headers.findIndex(h => h.toLowerCase() === 'card_token');
        
        if (cardTokenIndex === -1) {
          resolve({ hasIssues: false, issues: [] });
          return;
        }
        
        const issues = [];
        
        for (let i = 1; i < lines.length; i++) {
          const line = lines[i];
          if (!line.trim()) continue;
          
          const values = line.split(',').map(v => v.trim().replace(/"/g, ''));
          const cardToken = values[cardTokenIndex];
          
          if (cardToken) {
            // Check if card_token is not exactly 13 numerical digits
            if (!/^\d{13}$/.test(cardToken)) {
              issues.push({
                line: i + 1,
                cardToken: cardToken,
                type: 'incorrect_format'
              });
            }
          }
        }
        
        resolve({ hasIssues: issues.length > 0, issues });
      };
      reader.readAsText(file);
    });
  };

  // New function to validate Canadian postal codes
  const validateCanadianPostalCodes = async (file) => {
    return new Promise((resolve) => {
      const reader = new FileReader();
      reader.onload = (e) => {
        const text = e.target.result;
        const lines = text.split('\n');
        const headers = lines[0].split(',').map(h => h.trim().replace(/"/g, ''));
        
        // Find column indices
        const countryCodeIndex = headers.findIndex(h => h.toLowerCase() === 'address_country_code');
        const postalCodeIndex = headers.findIndex(h => h.toLowerCase() === 'address_postal_code');
        
        if (countryCodeIndex === -1 || postalCodeIndex === -1) {
          resolve({ hasIssues: false, issues: [] });
          return;
        }
        
        const issues = [];
        
        for (let i = 1; i < lines.length; i++) {
          const line = lines[i];
          if (!line.trim()) continue;
          
          const values = line.split(',').map(v => v.trim().replace(/"/g, ''));
          const countryCode = values[countryCodeIndex];
          const postalCode = values[postalCodeIndex];
          
          if (countryCode === 'CA' && postalCode) {
            // Check if postal code doesn't match Canadian format (A1A 1A1 or A1A1A1)
            const canadianFormat = /^[A-Za-z]\d[A-Za-z]\s?\d[A-Za-z]\d$/;
            if (!canadianFormat.test(postalCode)) {
              issues.push({
                line: i + 1,
                postalCode: postalCode,
                type: 'incorrect_format'
              });
            }
          }
        }
        
        resolve({ hasIssues: issues.length > 0, issues });
      };
      reader.readAsText(file);
    });
  };

  const handleSubmit = async (e) => {
    e.preventDefault();
    
    if (!subscriberFile || !mappingFile || !sellerName || !vaultProvider) {
      setError('Please fill in all required fields and upload both files.');
      return;
    }

    setIsProcessing(true);
    setError(null);
    setProcessingStatus('Processing migration...');

    try {
      // Check if server is running
      const healthResponse = await fetch('/api/health');
      if (!healthResponse.ok) {
        throw new Error('Backend server is not responding. Please ensure the Python server is running on port 5001.');
      }
      
      // Process the migration (validation will be handled by backend after mapping)
      await processFiles(subscriberFile, mappingFile, sellerName, vaultProvider, isSandbox, provider, useMappingZipcodes);
      
    } catch (err) {
      setError('Error processing migration: ' + err.message);
      setIsProcessing(false);
      setProcessingStatus('');
    }
  };

  // Function to handle missing zero correction
  const handleMissingZeroCorrection = async (action) => {
    setShowMissingZeroModal(false);
    
    if (action === 'cancel') {
      setIsProcessing(false);
      setProcessingStatus('');
      setError('Processing cancelled.');
      return;
    }
    
    if (action === 'ignore') {
      // Add US missing zero to the list of skipped types and continue with next validation
      const currentSkippedTypes = pendingFormData.skippedTypes || [];
      const skippedTypes = [...currentSkippedTypes, 'us_missing_zero'];
      await processFiles(pendingFormData.subscriberFile, pendingFormData.mappingFile, pendingFormData.sellerName, pendingFormData.vaultProvider, pendingFormData.isSandbox, pendingFormData.provider, pendingFormData.useMappingZipcodes, skippedTypes);
      return;
    }
    
    if (action === 'autocorrect') {
      setIsProcessing(true);
      setProcessingStatus('Correcting zip codes...');
      
      try {
        // Create corrected file
        const reader = new FileReader();
        reader.onload = async (e) => {
          const text = e.target.result;
          const lines = text.split('\n');
          const headers = lines[0].split(',').map(h => h.trim().replace(/"/g, ''));
          
          const countryCodeIndex = headers.findIndex(h => h.toLowerCase() === 'address_country_code');
          const postalCodeIndex = headers.findIndex(h => h.toLowerCase() === 'address_postal_code');
          
          const correctedLines = [lines[0]]; // Keep header
          
          for (let i = 1; i < lines.length; i++) {
            const line = lines[i];
            if (!line.trim()) {
              correctedLines.push(line);
              continue;
            }
            
            const values = line.split(',').map(v => v.trim().replace(/"/g, ''));
            const countryCode = values[countryCodeIndex];
            const postalCode = values[postalCodeIndex];
            
            // Only fix 4-digit zip codes (missing leading zeros)
            if (countryCode === 'US' && postalCode && /^\d{4}$/.test(postalCode)) {
              values[postalCodeIndex] = '0' + postalCode;
            }
            
            correctedLines.push(values.join(','));
          }
          
          const correctedText = correctedLines.join('\n');
          const correctedFile = new File([correctedText], pendingFormData.subscriberFile.name, {
            type: 'text/csv'
          });
          
          // Add US missing zero to the list of skipped types and continue with next validation
          const currentSkippedTypes = pendingFormData.skippedTypes || [];
          const skippedTypes = [...currentSkippedTypes, 'us_missing_zero'];
          await processFiles(correctedFile, pendingFormData.mappingFile, pendingFormData.sellerName, pendingFormData.vaultProvider, pendingFormData.isSandbox, pendingFormData.provider, pendingFormData.useMappingZipcodes, skippedTypes);
        };
        reader.readAsText(pendingFormData.subscriberFile);
      } catch (err) {
        setError('Error correcting zip codes: ' + err.message);
        setIsProcessing(false);
        setProcessingStatus('');
      }
    }
  };

  // Function to handle incorrect format issues
  const handleIncorrectFormatCorrection = async (action) => {
    setShowIncorrectFormatModal(false);
    
    if (action === 'cancel') {
      setIsProcessing(false);
      setProcessingStatus('');
      setError('Processing cancelled. Please correct the zip code format issues and try again.');
      return;
    }
    
    if (action === 'proceed') {
      // Use the corrected file if it exists, otherwise use original
      let fileToProcess = pendingFormData.subscriberFile;
      
      // Check if we have a corrected file from missing zero fixes
      if (missingZeroIssues.length > 0) {
        // We need to create the corrected file again since we don't store it
        const reader = new FileReader();
        reader.onload = async (e) => {
          const text = e.target.result;
          const lines = text.split('\n');
          const headers = lines[0].split(',').map(h => h.trim().replace(/"/g, ''));
          
          const countryCodeIndex = headers.findIndex(h => h.toLowerCase() === 'address_country_code');
          const postalCodeIndex = headers.findIndex(h => h.toLowerCase() === 'address_postal_code');
          
          const correctedLines = [lines[0]]; // Keep header
          
          for (let i = 1; i < lines.length; i++) {
            const line = lines[i];
            if (!line.trim()) {
              correctedLines.push(line);
              continue;
            }
            
            const values = line.split(',').map(v => v.trim().replace(/"/g, ''));
            const countryCode = values[countryCodeIndex];
            const postalCode = values[postalCodeIndex];
            
            // Only fix 4-digit zip codes (missing leading zeros)
            if (countryCode === 'US' && postalCode && /^\d{4}$/.test(postalCode)) {
              values[postalCodeIndex] = '0' + postalCode;
            }
            
            correctedLines.push(values.join(','));
          }
          
          const correctedText = correctedLines.join('\n');
          const correctedFile = new File([correctedText], pendingFormData.subscriberFile.name, {
            type: 'text/csv'
          });
          
          // Add US incorrect format to the list of skipped types
          const currentSkippedTypes = pendingFormData.skippedTypes || [];
          const skippedTypes = [...currentSkippedTypes, 'us_incorrect_format'];
          await processFiles(correctedFile, pendingFormData.mappingFile, pendingFormData.sellerName, pendingFormData.vaultProvider, pendingFormData.isSandbox, pendingFormData.provider, pendingFormData.useMappingZipcodes, skippedTypes);
        };
        reader.readAsText(pendingFormData.subscriberFile);
      } else {
        // No missing zero issues, proceed with original file
        // Add US incorrect format to the list of skipped types
        const currentSkippedTypes = pendingFormData.skippedTypes || [];
        const skippedTypes = [...currentSkippedTypes, 'us_incorrect_format'];
        await processFiles(fileToProcess, pendingFormData.mappingFile, pendingFormData.sellerName, pendingFormData.vaultProvider, pendingFormData.isSandbox, pendingFormData.provider, pendingFormData.useMappingZipcodes, skippedTypes);
      }
    }
  };

  // Function to handle card token format issues
  const handleCardTokenFormatCorrection = async (action) => {
    setShowCardTokenFormatModal(false);
    
    if (action === 'cancel') {
      setIsProcessing(false);
      setProcessingStatus('');
      setError('Processing cancelled. Please correct the card token format issues and try again.');
      return;
    }
    
    if (action === 'correct') {
      setIsProcessing(false);
      setProcessingStatus('');
      setError('Please correct the card token format issues in your file and try again.');
      return;
    }
  };

  // Function to handle Canadian postal code format issues
  const handleCanadianPostalCorrection = async (action) => {
    setShowCanadianPostalModal(false);
    
    if (action === 'cancel') {
      setIsProcessing(false);
      setProcessingStatus('');
      setError('Processing cancelled. Please correct the Canadian postal code format issues and try again.');
      return;
    }
    
    if (action === 'proceed') {
      // Proceed with processing despite Canadian postal code issues
      // Add Canadian validation to the list of skipped types
      const currentSkippedTypes = pendingFormData.skippedTypes || [];
      const skippedTypes = [...currentSkippedTypes, 'canadian_incorrect_format'];
      await processFiles(pendingFormData.subscriberFile, pendingFormData.mappingFile, pendingFormData.sellerName, pendingFormData.vaultProvider, pendingFormData.isSandbox, pendingFormData.provider, pendingFormData.useMappingZipcodes, skippedTypes);
    }
  };

  // Function to handle date validation issues
  const handleDateValidationCorrection = async (action) => {
    setShowDateValidationModal(false);
    
    if (action === 'cancel') {
      setIsProcessing(false);
      setProcessingStatus('');
      setError('Processing cancelled. Please correct the date validation issues and try again.');
      return;
    }
    
    if (action === 'proceed') {
      // Proceed with processing despite date issues
      // Add date validation to the list of skipped types
      const currentSkippedTypes = pendingFormData.skippedTypes || [];
      const skippedTypes = [...currentSkippedTypes, 'date_validation_issues'];
      await processFiles(pendingFormData.subscriberFile, pendingFormData.mappingFile, pendingFormData.sellerName, pendingFormData.vaultProvider, pendingFormData.isSandbox, pendingFormData.provider, pendingFormData.useMappingZipcodes, skippedTypes);
    }
  };

  const processFiles = async (subFile, mapFile, seller, vault, sandbox, prov, useMappingZipcodes, skipValidationTypes = []) => {
    const formData = new FormData();
    formData.append('subscriber_file', subFile);
    formData.append('mapping_file', mapFile);
    formData.append('seller_name', seller);
    formData.append('vault_provider', vault);
    formData.append('is_sandbox', sandbox);
    formData.append('provider', prov);
    formData.append('use_mapping_zipcodes', useMappingZipcodes);
    formData.append('skip_validation_types', JSON.stringify(skipValidationTypes));

    try {
      setProcessingStatus('Uploading files...');
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
          // If response is not JSON (like HTML error page), get the text
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
      
      // Check if validation is required
      if (result.validation_required) {
        const validationResults = result.postal_validation_results;
        
        // Get the current skipped types from the request
        const currentSkippedTypes = skipValidationTypes || [];
        
        // Store pending data for subsequent processing
        setPendingFormData({
          subscriberFile: subFile,
          mappingFile: mapFile,
          sellerName: seller,
          vaultProvider: vault,
          isSandbox: sandbox,
          provider: prov,
          useMappingZipcodes,
          skippedTypes: currentSkippedTypes 
        });
        
        // Show validation modals in order
        let modalShown = false;
        
        if (validationResults.us_missing_zero && validationResults.us_missing_zero.length > 0) {
          setMissingZeroIssues(validationResults.us_missing_zero);
          setShowMissingZeroModal(true);
          modalShown = true;
        }
        
        if (!modalShown && validationResults.us_incorrect_format && validationResults.us_incorrect_format.length > 0) {
          setIncorrectFormatIssues(validationResults.us_incorrect_format);
          setShowIncorrectFormatModal(true);
          modalShown = true;
        }
        
        if (!modalShown && validationResults.canadian_incorrect_format && validationResults.canadian_incorrect_format.length > 0) {
          setCanadianPostalIssues(validationResults.canadian_incorrect_format);
          setShowCanadianPostalModal(true);
          modalShown = true;
        }
        
        if (!modalShown && validationResults.bluesnap_card_token_format && validationResults.bluesnap_card_token_format.length > 0) {
          setCardTokenFormatIssues(validationResults.bluesnap_card_token_format);
          setShowCardTokenFormatModal(true);
          modalShown = true;
        }
        
        if (!modalShown && validationResults.date_validation_issues && validationResults.date_validation_issues.length > 0) {
          setDateValidationIssues(validationResults.date_validation_issues);
          setShowDateValidationModal(true);
          modalShown = true;
        }
        
        setIsProcessing(false);
        setProcessingStatus('');
        return;
      }
      
      onProcessingComplete(result);
      setProcessingStatus('Processing completed successfully!');
    } catch (err) {
      setError('Error: ' + err.message);
    } finally {
      setIsProcessing(false);
    }
  };

  return (
    <div className="file-upload-container">
      <form onSubmit={handleSubmit} className="upload-form">
        <div className="form-group">
          <label htmlFor="sellerName">Seller Name *</label>
          <input
            type="text"
            id="sellerName"
            value={sellerName}
            onChange={(e) => setSellerName(e.target.value)}
            required
            placeholder="Enter seller name"
          />
        </div>

        <div className="form-group">
          <label htmlFor="vaultProvider">Vault Provider *</label>
          <input
            type="text"
            id="vaultProvider"
            value={vaultProvider}
            onChange={(e) => setVaultProvider(e.target.value)}
            required
            placeholder="Enter vault provider name"
          />
        </div>

        <div className="form-group">
          <label>Payment Service Provider</label>
          <div className="provider-buttons">
            <button
              type="button"
              className={`provider-btn ${provider === 'stripe' ? 'active' : ''}`}
              onClick={() => setProvider('stripe')}
            >
              Stripe
            </button>
            <button
              type="button"
              className={`provider-btn ${provider === 'bluesnap' ? 'active' : ''}`}
              onClick={() => setProvider('bluesnap')}
            >
              Bluesnap
            </button>
          </div>
        </div>

        <div className="toggle-group">
          <div className="form-group">
            <label>Environment</label>
            <div className="toggle-container">
              <span className={!isSandbox ? 'active' : ''}>Production</span>
              <label className="switch">
                <input
                  type="checkbox"
                  checked={isSandbox}
                  onChange={(e) => setIsSandbox(e.target.checked)}
                />
                <span className="slider round"></span>
              </label>
              <span className={isSandbox ? 'active' : ''}>Sandbox</span>
            </div>
          </div>

          {hasMissingPostalCodes && (
            <div className="form-group missing-postal-codes-section">
              <label className="missing-postal-codes-label">Necessary postal codes missing ({missingPostalCodeCount} records)</label>
              <div className="toggle-container">
                <span className={!useMappingZipcodes ? 'active' : ''}>Use original data</span>
                <label className="switch">
                  <input
                    type="checkbox"
                    checked={useMappingZipcodes}
                    onChange={(e) => setUseMappingZipcodes(e.target.checked)}
                  />
                  <span className="slider round"></span>
                </label>
                <span className={useMappingZipcodes ? 'active' : ''}>Use mapping zipcodes if available</span>
              </div>
              <div className="toggle-description">
                Fill missing postal codes from mapping file for: AU, CA, FR, DE, IN, IT, NL, ES, GB, US (only if mapping data is available)
              </div>
            </div>
          )}
        </div>

        <div className="form-group">
          <label htmlFor="subscriberFile">Subscriber Export File (CSV) *</label>
          <input
            type="file"
            id="subscriberFile"
            accept=".csv,.txt,text/csv,text/plain"
            onChange={(e) => handleFileChange(e, 'subscriber')}
            required
          />
        </div>

        <div className="form-group">
          <label htmlFor="mappingFile">Mapping File (CSV) *</label>
          <input
            type="file"
            id="mappingFile"
            accept=".csv,.txt,text/csv,text/plain"
            onChange={(e) => handleFileChange(e, 'mapping')}
            required
          />
        </div>

        <button type="submit" disabled={isProcessing || !subscriberFile || !mappingFile} className="submit-btn">
          {isProcessing ? 'Processing...' : 'Process Migration'}
        </button>
      </form>

      {isProcessing && (
        <div className="processing-status">
          <div className="spinner"></div>
          <p>{processingStatus}</p>
        </div>
      )}

      {error && (
        <div className="error-message">
          <p>{error}</p>
        </div>
      )}

      {/* Missing Zero Modal */}
      {showMissingZeroModal && (
        <div className="modal-overlay">
          <div className="modal">
            <div className="modal-header">
              <h3>Potential Missing Leading Zeros for US Zip Codes <span className="issue-count">({missingZeroIssues.length})</span></h3>
            </div>
            <div className="modal-body">
              <p>The following US zip codes appear to be missing leading zeros:</p>
              <div className="zip-code-list">
                {missingZeroIssues.map((issue, index) => (
                  <div key={index} className="zip-code-item">
                    <span className="line-number">Line {issue.line}:</span>
                    <span className="original-code">{issue.postalCode}</span>
                    <span className="arrow">→</span>
                    <span className="corrected-code">{issue.correctedCode}</span>
                  </div>
                ))}
              </div>
              <p>What would you like to do with these zip codes?</p>
            </div>
            <div className="modal-footer three-buttons">
              <button
                className="btn-secondary"
                onClick={() => handleMissingZeroCorrection('cancel')}
              >
                Cancel
              </button>
              <button
                className="btn-secondary"
                onClick={() => handleMissingZeroCorrection('ignore')}
              >
                Ignore
              </button>
              <button
                className="btn-primary"
                onClick={() => handleMissingZeroCorrection('autocorrect')}
              >
                Autocorrect
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Incorrect Format Modal */}
      {showIncorrectFormatModal && (
        <div className="modal-overlay">
          <div className="modal">
            <div className="modal-header">
              <h3>Incorrect US Zip Code Format <span className="issue-count">({incorrectFormatIssues.length})</span></h3>
            </div>
            <div className="modal-body">
              <p>The following US zip codes have incorrect format (should be 5 digits):</p>
              <div className="zip-code-list">
                {incorrectFormatIssues.map((issue, index) => (
                  <div key={index} className="zip-code-item">
                    <span className="line-number">Line {issue.line}:</span>
                    <span className="original-code">{issue.postalCode}</span>
                    <span className="arrow">→</span>
                    <span className="corrected-code needs-correction">Needs correction</span>
                  </div>
                ))}
              </div>
              <p>These zip codes need manual correction. Would you like to cancel and fix them?</p>
            </div>
            <div className="modal-footer">
              <button
                className="btn-secondary"
                onClick={() => handleIncorrectFormatCorrection('cancel')}
              >
                Cancel and Correct
              </button>
              <button
                className="btn-primary"
                onClick={() => handleIncorrectFormatCorrection('proceed')}
              >
                Proceed Anyway
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Card Token Format Modal */}
      {showCardTokenFormatModal && (
        <div className="modal-overlay">
          <div className="modal">
            <div className="modal-header">
              <h3>Incorrect Bluesnap Card Token Format <span className="issue-count">({cardTokenFormatIssues.length})</span></h3>
            </div>
            <div className="modal-body">
              <p>The following card tokens have incorrect format (should be exactly 13 numerical digits):</p>
              <div className="zip-code-list">
                {cardTokenFormatIssues.map((issue, index) => (
                  <div key={index} className="zip-code-item">
                    <span className="line-number">Line {issue.line}:</span>
                    <span className="original-code">{issue.cardToken}</span>
                    <span className="arrow">→</span>
                    <span className="corrected-code needs-correction">Needs correction</span>
                  </div>
                ))}
              </div>
              <p>These card tokens need manual correction. Please fix them before proceeding.</p>
            </div>
            <div className="modal-footer">
              <button
                className="btn-secondary"
                onClick={() => handleCardTokenFormatCorrection('cancel')}
              >
                Cancel
              </button>
              <button
                className="btn-primary"
                onClick={() => handleCardTokenFormatCorrection('correct')}
              >
                Correct
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Canadian Postal Code Modal */}
      {showCanadianPostalModal && (
        <div className="modal-overlay">
          <div className="modal">
            <div className="modal-header">
              <h3>Incorrect Canadian Postal Code Format <span className="issue-count">({canadianPostalIssues.length})</span></h3>
            </div>
            <div className="modal-body">
              <p>The following Canadian postal codes have incorrect format (should be A1A 1A1 or A1A1A1):</p>
              <div className="zip-code-list">
                {canadianPostalIssues.map((issue, index) => (
                  <div key={index} className="zip-code-item">
                    <span className="line-number">Line {issue.line}:</span>
                    <span className="original-code">{issue.postalCode}</span>
                    <span className="arrow">→</span>
                    <span className="corrected-code needs-correction">Needs correction</span>
                  </div>
                ))}
              </div>
              <p>These postal codes need manual correction. Would you like to cancel and fix them?</p>
            </div>
            <div className="modal-footer">
              <button
                className="btn-secondary"
                onClick={() => handleCanadianPostalCorrection('cancel')}
              >
                Cancel and Correct
              </button>
              <button
                className="btn-primary"
                onClick={() => handleCanadianPostalCorrection('proceed')}
              >
                Proceed Anyway
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Date Validation Modal */}
      {showDateValidationModal && (
        <div className="modal-overlay">
          <div className="modal date-validation-modal">
            <div className="modal-header">
              <h3>Date Validation Issues <span className="issue-count">({dateValidationIssues.length})</span></h3>
            </div>
            <div className="modal-body">
              <p>The following records have date validation issues:</p>
              <div className="zip-code-list">
                {dateValidationIssues.map((issue, index) => (
                  <div key={index} className="zip-code-item">
                    <span className="line-number">Line {issue.line}:</span>
                    <span className="original-code">{issue.field}: {issue.value}</span>
                    <span className="arrow">→</span>
                    <span className="corrected-code needs-correction">
                      {issue.issue === 'start_date_not_in_past' ? 'Start date is in the future' :
                       issue.issue === 'end_date_not_in_future' ? 'End date is in the past' :
                       issue.issue === 'invalid_date_format' ? 'Invalid date format (cannot parse date)' :
                       'Unknown date issue'}
                    </span>
                    <div className="email-info">
                      <strong>Email:</strong> {issue.email}
                    </div>
                  </div>
                ))}
              </div>
              <p>These date issues need manual correction. Would you like to cancel and fix them?</p>
            </div>
            <div className="modal-footer">
              <button
                className="btn-secondary"
                onClick={() => handleDateValidationCorrection('cancel')}
              >
                Cancel and Correct
              </button>
              <button
                className="btn-primary"
                onClick={() => handleDateValidationCorrection('proceed')}
              >
                Proceed Anyway
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
};

export default FileUpload; 