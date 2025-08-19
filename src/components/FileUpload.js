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

    if (fileType === 'subscriber') {
      setSubscriberFile(file);
    } else if (fileType === 'mapping') {
      setMappingFile(file);
    }
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
      
      onProcessingComplete(result);
      setProcessingStatus('Processing completed successfully!');
    } catch (err) {
      setError('Error processing migration: ' + err.message);
      setIsProcessing(false);
      setProcessingStatus('');
    }
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
        </div>

        <div className="form-group">
          <label>Environment:</label>
          <div className="toggle-group">
            <div className="toggle-container">
              <span>Production</span>
              <label className="switch">
                <input
                  type="checkbox"
                  checked={isSandbox}
                  onChange={(e) => setIsSandbox(e.target.checked)}
                />
                <span className="slider round"></span>
              </label>
              <span>Sandbox</span>
            </div>
            {isSandbox && (
              <div className="sandbox-message">
                Anonymize email addresses
              </div>
            )}
          </div>
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
          <label htmlFor="subscriberFile">Subscriber Export File:</label>
          <input
            type="file"
            id="subscriberFile"
            accept=".csv,.txt"
            onChange={(e) => handleFileChange(e, 'subscriber')}
            required
          />
          {subscriberFile && <p className="file-info">Selected: {subscriberFile.name}</p>}
        </div>

        <div className="form-group">
          <label htmlFor="mappingFile">Mapping File:</label>
          <input
            type="file"
            id="mappingFile"
            accept=".csv,.txt"
            onChange={(e) => handleFileChange(e, 'mapping')}
            required
          />
          {mappingFile && <p className="file-info">Selected: {mappingFile.name}</p>}
        </div>

        <button type="submit" disabled={isProcessing} className="submit-btn">
          {isProcessing ? 'Processing...' : 'Process Migration'}
        </button>
      </form>

      {processingStatus && (
        <div className="processing-status">
          {processingStatus}
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