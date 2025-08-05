import React, { useState } from 'react';
import './App.css';
import FileUpload from './components/FileUpload';
import ProcessingStatus from './components/ProcessingStatus';
import ResultsSummary from './components/ResultsSummary';
import DownloadLinks from './components/DownloadLinks';

function App() {
  const [isSandbox, setIsSandbox] = useState(false);
  const [selectedProvider, setSelectedProvider] = useState('stripe');
  const [subscriberFile, setSubscriberFile] = useState(null);
  const [mappingFile, setMappingFile] = useState(null);
  const [sellerName, setSellerName] = useState('');
  const [vaultProvider, setVaultProvider] = useState('');
  const [isProcessing, setIsProcessing] = useState(false);
  const [results, setResults] = useState(null);
  const [error, setError] = useState(null);

  const handleProcess = async () => {
    if (!subscriberFile || !mappingFile || !sellerName || !vaultProvider) {
      setError('Please provide all required files, seller name, and vault provider name');
      return;
    }

    setIsProcessing(true);
    setError(null);
    setResults(null);

    try {
      // Create FormData for file upload
      const formData = new FormData();
      formData.append('subscriber_file', subscriberFile);
      formData.append('mapping_file', mappingFile);
      formData.append('seller_name', sellerName);
      formData.append('vault_provider', vaultProvider);
      formData.append('is_sandbox', isSandbox.toString());
      formData.append('provider', selectedProvider);

      // Make HTTP call to backend
      const response = await fetch('http://localhost:5001/api/process-migration', {
        method: 'POST',
        body: formData,
      });

      if (!response.ok) {
        const errorData = await response.json();
        throw new Error(errorData.error || 'Processing failed');
      }

      const result = await response.json();
      setResults(result);
    } catch (err) {
      setError(err.message);
    } finally {
      setIsProcessing(false);
    }
  };

  return (
    <div className="App">
      <header className="App-header">
        <div className="header-content">
          <div className="logo-section">
            <div className="logo">
              <img 
                src="https://images.seeklogo.com/logo-png/45/2/paddle-logo-png_seeklogo-451808.png" 
                alt="Paddle Logo" 
                className="paddle-logo"
              />
            </div>
            <h1>Migration Mapping Tool</h1>
          </div>
        </div>
      </header>

      <main className="App-main">
        <div className="content-wrapper">
                      <div className="page-header">
              <h2>Data Migration</h2>
              <p>Upload your CSV files to map payment tokens to subscription data</p>
              <h3 className="psp-heading">Choose PSP</h3>
              <div className="provider-buttons">
              <button 
                className={`provider-button ${selectedProvider === 'stripe' ? 'active' : ''}`}
                onClick={() => setSelectedProvider('stripe')}
              >
                Stripe
              </button>
              <button 
                className={`provider-button ${selectedProvider === 'bluesnap' ? 'active' : ''}`}
                onClick={() => setSelectedProvider('bluesnap')}
              >
                Bluesnap
              </button>
            </div>
          </div>

          <div className="migration-container">
            <div className="environment-toggle">
              <label className="toggle-label">
                <input
                  type="checkbox"
                  checked={isSandbox}
                  onChange={(e) => setIsSandbox(e.target.checked)}
                />
                <span className="toggle-slider"></span>
                <span className="toggle-text">
                  {isSandbox ? 'Sandbox Mode' : 'Production Mode'}
                </span>
              </label>
            </div>

            <FileUpload
              subscriberFile={subscriberFile}
              setSubscriberFile={setSubscriberFile}
              mappingFile={mappingFile}
              setMappingFile={setMappingFile}
              sellerName={sellerName}
              setSellerName={setSellerName}
              vaultProvider={vaultProvider}
              setVaultProvider={setVaultProvider}
              isSandbox={isSandbox}
            />

            {error && (
              <div className="error-message">
                {error}
              </div>
            )}

            <button
              className="process-button"
              onClick={handleProcess}
              disabled={isProcessing || !subscriberFile || !mappingFile || !sellerName || !vaultProvider}
            >
              {isProcessing ? 'Processing...' : 'Process Migration'}
            </button>

            {isProcessing && (
              <ProcessingStatus />
            )}

            {results && (
              <>
                <ResultsSummary results={results} isSandbox={isSandbox} />
                <DownloadLinks results={results} />
              </>
            )}
          </div>
        </div>
      </main>
    </div>
  );
}

export default App; 