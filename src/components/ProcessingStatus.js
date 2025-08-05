import React from 'react';
import './ProcessingStatus.css';

const ProcessingStatus = () => {
  return (
    <div className="processing-status">
      <div className="processing-spinner">
        <div className="spinner"></div>
      </div>
      <div className="processing-text">
        <h4>Processing Migration</h4>
        <p>Please wait while we process your files...</p>
      </div>
      <div className="processing-steps">
        <div className="step">
          <div className="step-icon">✓</div>
          <span>Files uploaded</span>
        </div>
        <div className="step active">
          <div className="step-icon">⟳</div>
          <span>Processing data</span>
        </div>
        <div className="step">
          <div className="step-icon">⏳</div>
          <span>Generating output files</span>
        </div>
      </div>
    </div>
  );
};

export default ProcessingStatus; 