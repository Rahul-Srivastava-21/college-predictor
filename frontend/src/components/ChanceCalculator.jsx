import { useState, useEffect } from 'react';
import Select from 'react-select';
import { predictWithChance, getCollegesList } from '../services/api';
import { FaUniversity, FaCalculator, FaCheckCircle, FaExclamationTriangle } from 'react-icons/fa';
import './ChanceCalculator.css';

const ChanceCalculator = () => {
  const [formData, setFormData] = useState({
    User_Rank: '',
    College_Code: '',
    College_Name: '',
    Branch: '',
    Category: '',
    Exam_Type: 'CET',
    Year: 2024,
    Round: 1,
    Quota_Seats: 60,
  });

  const [result, setResult] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [collegesData, setCollegesData] = useState({ colleges: [], branches: [], categories: [] });

  // Category options based on exam type
  const categoryOptions = {
    CET: ['GM', 'GMK', 'GMR', '1G', '1K', '1R', '2AG', '2AK', '2AR', '2BG', '2BK', '2BR', '3AG', '3AK', '3AR', '3BG', '3BK', '3BR', 'SCG', 'SCK', 'SCR', 'STG', 'STK', 'STR'],
    COMEDK: ['GM']
  };

  useEffect(() => {
    console.log('ChanceCalculator mounted, loading colleges...');
    loadCollegesData();
  }, [formData.Exam_Type]);

  const loadCollegesData = async () => {
    console.log(`Starting loadCollegesData for ${formData.Exam_Type}`);
    try {
      // Check cache first
      const cacheKey = `colleges_data_${formData.Exam_Type}`;
      const cached = localStorage.getItem(cacheKey);
      
      if (cached) {
        const cachedData = JSON.parse(cached);
        // Check if cache is less than 24 hours old
        if (Date.now() - cachedData.timestamp < 24 * 60 * 60 * 1000) {
          console.log(`✓ Loaded from cache: ${cachedData.data.colleges.length} colleges, ${cachedData.data.branches.length} branches`);
          setCollegesData(cachedData.data);
          return;
        } else {
          console.log('Cache expired, fetching fresh data...');
        }
      } else {
        console.log('No cache found, fetching from API...');
      }

      // Fetch from API
      console.log(`→ Making API call to /colleges/list?exam_type=${formData.Exam_Type}`);
      const data = await getCollegesList(formData.Exam_Type);
      console.log('← API response received:', data);
      
      if (data.success) {
        console.log(`✓ API returned: ${data.colleges.length} colleges, ${data.branches.length} branches`);
        setCollegesData(data);
        // Cache the data
        localStorage.setItem(cacheKey, JSON.stringify({
          data: data,
          timestamp: Date.now()
        }));
      } else {
        console.error('API returned success=false:', data);
      }
    } catch (err) {
      console.error('❌ Failed to load colleges data:', err);
      console.error('Error details:', err.message, err.response);
    }
  };

  const handleInputChange = (e) => {
    const { name, value } = e.target;
    setFormData(prev => ({
      ...prev,
      [name]: name === 'User_Rank' || name === 'Year' || name === 'Round' || name === 'Quota_Seats' 
        ? parseInt(value) || '' 
        : value
    }));
  };

  const handleCollegeSelect = async (selectedOption) => {
    if (selectedOption) {
      const collegeCode = selectedOption.value;
      const collegeName = selectedOption.label.split(' - ')[1] || selectedOption.label;
      
      setFormData(prev => ({
        ...prev,
        College_Code: collegeCode,
        College_Name: collegeName,
        Branch: '' // Reset branch when college changes
      }));

      // Load branches for this specific college
      try {
        console.log(`Loading branches for college ${collegeCode}...`);
        const data = await getCollegesList(formData.Exam_Type, collegeCode);
        if (data.success) {
          console.log(`✓ Loaded ${data.branches.length} branches for ${collegeCode}`);
          setCollegesData(prev => ({
            ...prev,
            branches: data.branches
          }));
        }
      } catch (err) {
        console.error('Failed to load branches for college:', err);
      }
    } else {
      setFormData(prev => ({ ...prev, College_Code: '', College_Name: '', Branch: '' }));
      // Reload all branches when college is cleared
      loadCollegesData();
    }
  };

  const handleBranchSelect = (selectedOption) => {
    if (selectedOption) {
      setFormData(prev => ({ ...prev, Branch: selectedOption.value }));
    } else {
      setFormData(prev => ({ ...prev, Branch: '' }));
    }
  };

  const handleSubmit = async (e) => {
    e.preventDefault();
    setLoading(true);
    setError(null);
    setResult(null);

    try {
      const data = await predictWithChance(formData);
      if (data.success) {
        setResult(data);
      } else {
        setError(data.error || 'Prediction failed');
      }
    } catch (err) {
      setError(err.response?.data?.error || err.message || 'Failed to connect to server');
    } finally {
      setLoading(false);
    }
  };

  const getChanceColor = (level) => {
    const colors = {
      'High': '#10b981',
      'Good': '#22c55e',
      'Moderate': '#f59e0b',
      'Reach': '#ef4444',
      'Low': '#dc2626'
    };
    return colors[level] || '#6b7280';
  };

  return (
    <div className="chance-calculator">
      <div className="calculator-header">
        <FaUniversity className="header-icon" />
        <h2>Admission Chance Calculator</h2>
        <p>Calculate your probability of getting admission to a specific college</p>
      </div>

      <form onSubmit={handleSubmit} className="calculator-form">
        <div className="form-row">
          <div className="form-group">
            <label>Your Rank *</label>
            <input
              type="number"
              name="User_Rank"
              value={formData.User_Rank}
              onChange={handleInputChange}
              placeholder="Enter your rank"
              required
            />
          </div>

          <div className="form-group">
            <label>Exam Type *</label>
            <select name="Exam_Type" value={formData.Exam_Type} onChange={handleInputChange} required>
              <option value="CET">CET/KCET</option>
              <option value="COMEDK">COMEDK</option>
            </select>
          </div>
        </div>

        <div className="form-row">
          <div className="form-group">
            <label>College *</label>
            <Select
              options={collegesData.colleges.map(college => ({
                value: college.College_Code,
                label: `${college.College_Code} - ${college.College_Name}`
              }))}
              onChange={handleCollegeSelect}
              placeholder="Search or select college..."
              isClearable
              isSearchable
              value={formData.College_Code ? {
                value: formData.College_Code,
                label: `${formData.College_Code} - ${formData.College_Name}`
              } : null}
              styles={{
                control: (base) => ({ ...base, minHeight: '48px', borderRadius: '8px', borderWidth: '2px' }),
                option: (base, state) => ({ ...base, padding: '10px' })
              }}
            />
          </div>
        </div>

        <div className="form-row">
          <div className="form-group">
            <label>Branch *</label>
            <Select
              options={collegesData.branches.map(branch => ({
                value: branch,
                label: branch
              }))}
              onChange={handleBranchSelect}
              placeholder="Search or select branch..."
              isClearable
              isSearchable
              value={formData.Branch ? { value: formData.Branch, label: formData.Branch } : null}
              styles={{
                control: (base) => ({ ...base, minHeight: '48px', borderRadius: '8px', borderWidth: '2px' }),
                option: (base, state) => ({ ...base, padding: '10px' })
              }}
            />
          </div>

          <div className="form-group">
            <label>Category *</label>
            <select name="Category" value={formData.Category} onChange={handleInputChange} required>
              <option value="">Select Category</option>
              {categoryOptions[formData.Exam_Type].map(cat => (
                <option key={cat} value={cat}>{cat}</option>
              ))}
            </select>
          </div>
        </div>

        <div className="form-row">
          <div className="form-group">
            <label>Year *</label>
            <input
              type="number"
              name="Year"
              value={formData.Year}
              onChange={handleInputChange}
              min="2024"
              max="2026"
              required
            />
          </div>

          <div className="form-group">
            <label>Round *</label>
            <select name="Round" value={formData.Round} onChange={handleInputChange} required>
              <option value={0}>Round 0</option>
              <option value={1}>Round 1</option>
              <option value={2}>Round 2</option>
              <option value={3}>Round 3</option>
            </select>
          </div>
        </div>

        <button type="submit" className="submit-btn" disabled={loading}>
          <FaCalculator />
          {loading ? 'Calculating...' : 'Calculate Admission Chance'}
        </button>
      </form>

      {error && (
        <div className="error-message">
          <FaExclamationTriangle /> {error}
        </div>
      )}

      {result && (
        <div className="result-container">
          <div className="result-header">
            <h3>Prediction Results</h3>
          </div>

          <div className="chance-display" style={{ borderColor: getChanceColor(result.chance.level) }}>
            <div className="chance-percentage" style={{ color: getChanceColor(result.chance.level) }}>
              {result.chance.percentage}%
            </div>
            <div className="chance-level" style={{ color: getChanceColor(result.chance.level) }}>
              {result.chance.level} Chance
            </div>
          </div>

          <div className="cutoff-comparison">
            <div className="comparison-item">
              <span className="label">Your Rank:</span>
              <span className="value user-rank">{result.user_rank}</span>
            </div>
            <div className="comparison-item">
              <span className="label">Predicted Cutoff:</span>
              <span className="value predicted-cutoff">{result.predicted_cutoff}</span>
            </div>
            <div className="comparison-item">
              <span className="label">Difference:</span>
              <span className={`value ${result.chance.details.rank_difference > 0 ? 'positive' : 'negative'}`}>
                {result.chance.details.rank_difference > 0 ? '+' : ''}{result.chance.details.rank_difference}
              </span>
            </div>
          </div>

          <div className="explanation-section">
            <h4>Analysis</h4>
            <p className="explanation-text">{result.chance.explanation}</p>
            
            <div className="factors-list">
              {result.chance.factors.map((factor, index) => (
                <div key={index} className="factor-item">
                  {factor.startsWith('✓') ? <FaCheckCircle className="icon-success" /> : 
                   factor.startsWith('⚠') ? <FaExclamationTriangle className="icon-warning" /> : 
                   <span className="icon-neutral">•</span>}
                  <span>{factor.replace(/^[✓⚠•]\s*/, '')}</span>
                </div>
              ))}
            </div>
          </div>

          <div className="details-section">
            <h4>Statistical Details</h4>
            <div className="details-grid">
              <div className="detail-item">
                <span className="detail-label">Historical Volatility:</span>
                <span className="detail-value">±{result.chance.details.volatility} ranks</span>
              </div>
              <div className="detail-item">
                <span className="detail-label">Trend Slope:</span>
                <span className="detail-value">{result.chance.details.trend_slope} ranks/year</span>
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  );
};

export default ChanceCalculator;
