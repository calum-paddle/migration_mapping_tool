import React from 'react';
import './ResultsSummary.css';

const ResultsSummary = ({ results, isSandbox }) => {
  const { 
    success_count, 
    no_tokens_count, 
    duplicate_tokens_count, 
    duplicate_card_ids_count, 
    duplicate_external_subscription_ids_count, 
    duplicate_emails_count,
    total_processed 
  } = results;

  return (
    <div className="results-summary">
      <div className="results-header">
        <h3>Migration Results</h3>
        <span className="environment-badge">
          {isSandbox ? 'Sandbox' : 'Production'}
        </span>
      </div>

      <div className="results-stats">
        <div className="stat-card primary">
          <div className="stat-number">{success_count}</div>
          <div className="stat-label">Successful Migrations</div>
        </div>

        <div className="stat-card warning">
          <div className="stat-number">{no_tokens_count}</div>
          <div className="stat-label">No Tokens Found</div>
        </div>

        <div className="stat-card info">
          <div className="stat-number">{total_processed}</div>
          <div className="stat-label">Total Processed</div>
        </div>
      </div>

      <div className="duplicate-summary">
        <h4>Duplicate Detection Results</h4>
        <div className="duplicate-stats">
          <div className="duplicate-item">
            <span className="duplicate-label">Duplicate Tokens:</span>
            <span className="duplicate-count">{duplicate_tokens_count}</span>
          </div>
          <div className="duplicate-item">
            <span className="duplicate-label">Duplicate Card IDs:</span>
            <span className="duplicate-count">{duplicate_card_ids_count}</span>
          </div>
          <div className="duplicate-item">
            <span className="duplicate-label">Duplicate Subscription IDs:</span>
            <span className="duplicate-count">{duplicate_external_subscription_ids_count}</span>
          </div>
          <div className="duplicate-item">
            <span className="duplicate-label">Duplicate Emails:</span>
            <span className="duplicate-count">{duplicate_emails_count}</span>
          </div>
        </div>
      </div>

      <div className="processing-info">
        <div className="info-item">
          <strong>Environment:</strong> {isSandbox ? 'Sandbox (Data Anonymized)' : 'Production'}
        </div>
        <div className="info-item">
          <strong>Processing Time:</strong> {results.processing_time || 'N/A'}
        </div>
        <div className="info-item">
          <strong>Files Generated:</strong> {results.output_files?.length || 0}
        </div>
      </div>
    </div>
  );
};

export default ResultsSummary; 