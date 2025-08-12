import React from 'react';
import './ResultsSummary.css';

const ResultsSummary = ({ results, onReset }) => {
  const formatFileSize = (bytes) => {
    if (bytes === 0) return '0 Bytes';
    const k = 1024;
    const sizes = ['Bytes', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
  };

  const downloadFile = (fileInfo) => {
    const link = document.createElement('a');
    link.href = fileInfo.url;
    link.download = fileInfo.name;
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
  };

  return (
    <div className="results-container">
      <div className="results-header">
        <h2>Processing Complete!</h2>
        <button className="reset-btn" onClick={onReset}>
          Process Another File
        </button>
      </div>

      <div className="results-summary">
        <div className="summary-stats">
          <div className="stat-item">
            <span className="stat-label">Total Records:</span>
            <span className="stat-value">{results.total_processed !== undefined ? results.total_processed : 'N/A'}</span>
          </div>
          <div className="stat-item">
            <span className="stat-label">Success Records:</span>
            <span className="stat-value">{results.success_count !== undefined ? results.success_count : 'N/A'}</span>
          </div>
          <div className="stat-item">
            <span className="stat-label">No Tokens Found:</span>
            <span className="stat-value">{results.no_tokens_count !== undefined ? results.no_tokens_count : 'N/A'}</span>
          </div>
          <div className="stat-item">
            <span className="stat-label">Duplicate Tokens:</span>
            <span className="stat-value">{results.duplicate_tokens_count !== undefined ? results.duplicate_tokens_count : 'N/A'}</span>
          </div>
          <div className="stat-item">
            <span className="stat-label">Duplicate Card IDs:</span>
            <span className="stat-value">{results.duplicate_card_ids_count !== undefined ? results.duplicate_card_ids_count : 'N/A'}</span>
          </div>
          <div className="stat-item">
            <span className="stat-label">Duplicate Subscription IDs:</span>
            <span className="stat-value">{results.duplicate_external_subscription_ids_count !== undefined ? results.duplicate_external_subscription_ids_count : 'N/A'}</span>
          </div>
          <div className="stat-item">
            <span className="stat-label">Duplicate Emails:</span>
            <span className="stat-value">{results.duplicate_emails_count !== undefined ? results.duplicate_emails_count : 'N/A'}</span>
          </div>
        </div>
      </div>

      <div className="download-section">
        <h3>Download Files</h3>
        <div className="file-list">
          {results.output_files && results.output_files.map((file, index) => (
            <div key={index} className="file-item">
              <div className="file-info">
                <span className="file-name">{file.name}</span>
                <span className="file-size">({formatFileSize(file.size)})</span>
              </div>
              <button 
                className="download-btn"
                onClick={() => downloadFile(file)}
              >
                Download
              </button>
            </div>
          ))}
        </div>
      </div>

      {results.processing_time && (
        <div className="processing-info">
          <p>Processing completed in {typeof results.processing_time === 'number' ? results.processing_time.toFixed(2) : results.processing_time} seconds</p>
        </div>
      )}
    </div>
  );
};

export default ResultsSummary; 