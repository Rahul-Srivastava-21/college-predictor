import { useState } from 'react';
import ChanceCalculator from './components/ChanceCalculator';
import CollegeFinder from './components/CollegeFinder';
import { FaGraduationCap } from 'react-icons/fa';
import './App.css';

function App() {
  const [activeTab, setActiveTab] = useState('chance');

  return (
    <div className="app">
      <header className="app-header">
        <div className="header-content">
          <FaGraduationCap className="logo-icon" />
          <div className="header-text">
            <h1>KCET & COMEDK Admission Predictor</h1>
            <p>AI-powered college admission predictions for Karnataka engineering colleges</p>
          </div>
        </div>
      </header>

      <nav className="tabs-nav">
        <button
          className={`tab-button ${activeTab === 'chance' ? 'active' : ''}`}
          onClick={() => setActiveTab('chance')}
        >
          Admission Chance Calculator
        </button>
        <button
          className={`tab-button ${activeTab === 'finder' ? 'active' : ''}`}
          onClick={() => setActiveTab('finder')}
        >
          College Finder
        </button>
      </nav>

      <main className="app-content">
        {activeTab === 'chance' ? <ChanceCalculator /> : <CollegeFinder />}
      </main>

      <footer className="app-footer">
        <p className="disclaimer">
          *Predictions are based on historical data and ML models. Actual cutoffs may vary.
        </p>
      </footer>
    </div>
  );
}

export default App;
