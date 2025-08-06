import React from 'react';
import './DownloadLinks.css';

const DownloadLinks = ({ results }) => {
  const { output_files } = results;

  if (!output_files || output_files.length === 0) {
    return null;
  }

  const getFileIcon = (filename) => {
    if (filename.includes('final_import')) return '📄';
    if (filename.includes('no_token')) return '⚠️';
    if (filename.includes('duplicate')) return '🔄';
    return '📁';
  };

  const getFileDescription = (filename) => {
    if (filename.includes('final_import')) return 'Final import file with processed data';
    if (filename.includes('no_token_found')) return 'Records with missing payment tokens';
    if (filename.includes('duplicate_tokens')) return 'Records with duplicate payment tokens';
    if (filename.includes('duplicate_card_ids')) return 'Records with duplicate card IDs';
    if (filename.includes('duplicate_external_subscription_ids')) return 'Records with duplicate subscription IDs';
    if (filename.includes('duplicate_emails')) return 'Records with duplicate email addresses';
    return 'Processed data file';
  };

  return (
    <div className="download-links">
      <div className="download-header">
        <h3>Download Output Files</h3>
        <p>Click on any file below to download the processed data</p>
      </div>

      <div className="download-grid">
        {output_files.map((file, index) => (
          <div key={index} className="download-card">
            <div className="file-icon">
              {getFileIcon(file.name)}
            </div>
            <div className="file-info">
              <div className="file-name">{file.name}</div>
              <div className="file-description">
                {getFileDescription(file.name)}
              </div>
              {file.size && (
                <div className="file-size">
                  {(file.size / 1024).toFixed(1)} KB
                </div>
              )}
            </div>
            <div className="download-action">
              <a 
                href={file.url} 
                download={file.name}
                className="download-button"
                onClick={() => {
                  console.log('Download clicked for:', file.name);
                  console.log('Download URL:', file.url);
                }}
              >
                Download
              </a>
            </div>
          </div>
        ))}
      </div>

      <div className="download-note">
        <p>
          <strong>Note:</strong> All files are in CSV format and can be opened in Excel, 
          Google Sheets, or any spreadsheet application.
        </p>
      </div>
    </div>
  );
};

export default DownloadLinks; 