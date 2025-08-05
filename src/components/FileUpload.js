import React from 'react';
import './FileUpload.css';

const FileUpload = ({
  subscriberFile,
  setSubscriberFile,
  mappingFile,
  setMappingFile,
  sellerName,
  setSellerName,
  vaultProvider,
  setVaultProvider,
  isSandbox
}) => {
  const handleFileChange = (setFile) => (event) => {
    const file = event.target.files[0];
    if (file && file.type === 'text/csv') {
      setFile(file);
    } else {
      alert('Please select a valid CSV file');
    }
  };

  return (
    <div className="file-upload-container">
      <div className="upload-section">
        <h3>Upload Files</h3>
        
        <div className="file-input-group">
          <label htmlFor="seller-name">Seller Name</label>
          <input
            type="text"
            id="seller-name"
            value={sellerName}
            onChange={(e) => setSellerName(e.target.value)}
            placeholder="Enter seller name"
            className="text-input"
          />
        </div>

        <div className="file-input-group">
          <label htmlFor="vault-provider">Vault Provider Name</label>
          <input
            type="text"
            id="vault-provider"
            value={vaultProvider}
            onChange={(e) => setVaultProvider(e.target.value)}
            placeholder="Enter vault provider name"
            className="text-input"
          />
        </div>

        <div className="file-input-group">
          <label htmlFor="subscriber-file">Subscriber Export File (CSV)</label>
          <div className="file-input-wrapper">
            <input
              type="file"
              id="subscriber-file"
              accept=".csv"
              onChange={handleFileChange(setSubscriberFile)}
              className="file-input"
            />
            <label htmlFor="subscriber-file" className="file-input-label">
              {subscriberFile ? subscriberFile.name : 'Choose CSV file'}
            </label>
          </div>
          {subscriberFile && (
            <div className="file-info">
              <span className="file-name">{subscriberFile.name}</span>
              <span className="file-size">({(subscriberFile.size / 1024).toFixed(1)} KB)</span>
            </div>
          )}
        </div>

        <div className="file-input-group">
          <label htmlFor="mapping-file">
            {isSandbox ? 'Test Mapping File (CSV)' : 'TokenEx Mapping File (CSV)'}
          </label>
          <div className="file-input-wrapper">
            <input
              type="file"
              id="mapping-file"
              accept=".csv"
              onChange={handleFileChange(setMappingFile)}
              className="file-input"
            />
            <label htmlFor="mapping-file" className="file-input-label">
              {mappingFile ? mappingFile.name : 'Choose CSV file'}
            </label>
          </div>
          {mappingFile && (
            <div className="file-info">
              <span className="file-name">{mappingFile.name}</span>
              <span className="file-size">({(mappingFile.size / 1024).toFixed(1)} KB)</span>
            </div>
          )}
        </div>

        <div className="upload-info">
          <h4>Environment: {isSandbox ? 'Sandbox' : 'Production'}</h4>
          <p>
            {isSandbox 
              ? 'Sandbox mode will anonymize customer data and use test tokens for processing.'
              : 'Production mode will process real customer data with actual payment tokens.'
            }
          </p>
        </div>
      </div>
    </div>
  );
};

export default FileUpload; 