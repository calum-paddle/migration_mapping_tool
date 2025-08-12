import React, { useState } from 'react';
import './App.css';
import FileUpload from './components/FileUpload';
import ResultsSummary from './components/ResultsSummary';

function App() {
  const [results, setResults] = useState(null);

  const handleProcessingComplete = (processingResults) => {
    setResults(processingResults);
  };

  return (
    <div className="App">
      <header className="App-header">
        <div className="header-content">
          <div className="logo-section">
            <div className="logo">P</div>
            <h1>Migration Mapping Tool</h1>
          </div>
        </div>
      </header>

      <main className="App-main">
        <div className="main-content">
          <div className="description-section">
            <p>Upload your CSV files to map payment tokens to subscription data</p>
          </div>

          {!results ? (
            <FileUpload onProcessingComplete={handleProcessingComplete} />
          ) : (
            <ResultsSummary 
              results={results} 
              onReset={() => setResults(null)}
            />
          )}
        </div>
      </main>
    </div>
  );
}

export default App; 