import { useState, useEffect } from 'react';
import Select from 'react-select';
import { findColleges, getCollegesList } from '../services/api';
import { FaSearch, FaUniversity, FaCheckCircle, FaExclamationCircle, FaExclamationTriangle } from 'react-icons/fa';
import './CollegeFinder.css';

const CollegeFinder = () => {
  const [formData, setFormData] = useState({
    User_Rank: '',
    Branch_Preferences: [],
    Category: '',
    Exam_Type: 'CET',
    Year: 2024,
    Round: 1,
    Location: '',
  });

  const [results, setResults] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [branchOptions, setBranchOptions] = useState([]);
  const [sortBy, setSortBy] = useState('chance'); // 'chance', 'cutoff', 'safety'
  const [currentPage, setCurrentPage] = useState(1);
  const itemsPerPage = 20;

  // Category options based on exam type
  const categoryOptions = {
    CET: ['GM', 'GMK', 'GMR', '1G', '1K', '1R', '2AG', '2AK', '2AR', '2BG', '2BK', '2BR', '3AG', '3AK', '3AR', '3BG', '3BK', '3BR', 'SCG', 'SCK', 'SCR', 'STG', 'STK', 'STR'],
    COMEDK: ['GM']
  };

  useEffect(() => {
    console.log('CollegeFinder mounted, loading branches...');
    loadBranchesAndCategories();
  }, [formData.Exam_Type]);

  const loadBranchesAndCategories = async () => {
    console.log(`Starting loadBranchesAndCategories for ${formData.Exam_Type}`);
    try {
      // Check cache first
      const cacheKey = `branches_data_${formData.Exam_Type}`;
      const cached = localStorage.getItem(cacheKey);
      
      if (cached) {
        const cachedData = JSON.parse(cached);
        // Check if cache is less than 24 hours old
        if (Date.now() - cachedData.timestamp < 24 * 60 * 60 * 1000) {
          console.log(`✓ Loaded ${cachedData.branches.length} branches from cache`);
          setBranchOptions(cachedData.branches);
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
        console.log(`✓ API returned ${data.branches.length} branches`);
        setBranchOptions(data.branches);
        // Cache the data
        localStorage.setItem(cacheKey, JSON.stringify({
          branches: data.branches,
          timestamp: Date.now()
        }));
      } else {
        console.error('API returned success=false:', data);
      }
    } catch (err) {
      console.error('❌ Failed to load data:', err);
      console.error('Error details:', err.message, err.response);
    }
  };

  const loadCollegesData = async () => {
    try {
      const cacheKey = `colleges_${formData.Exam_Type}`;

      // Clear cache for colleges data
      localStorage.removeItem(cacheKey);
      console.log(`Cache cleared for key: ${cacheKey}`);

      const cachedData = JSON.parse(localStorage.getItem(cacheKey));

      if (cachedData) {
        // Check if cache is less than 24 hours old
        if (Date.now() - cachedData.timestamp < 24 * 60 * 60 * 1000) {
          console.log(`✓ Loaded ${cachedData.colleges.length} colleges from cache`);
          setCollegesData({
            ...cachedData,
            colleges: cachedData.colleges.filter(college => college.College_Name !== 'MISSING_COLLEGE')
          });
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
        console.log(`✓ API returned ${data.colleges.length} colleges`);
        setCollegesData({
          ...data,
          colleges: data.colleges.filter(college => college.College_Name !== 'MISSING_COLLEGE')
        });
        // Cache the data
        localStorage.setItem(cacheKey, JSON.stringify({
          ...data,
          colleges: data.colleges.filter(college => college.College_Name !== 'MISSING_COLLEGE'),
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
      [name]: name === 'User_Rank' || name === 'Year' || name === 'Round'
        ? parseInt(value) || ''
        : value
    }));
  };

  const handleBranchToggle = (branch) => {
    setFormData(prev => ({
      ...prev,
      Branch_Preferences: prev.Branch_Preferences.includes(branch)
        ? prev.Branch_Preferences.filter(b => b !== branch)
        : [...prev.Branch_Preferences, branch]
    }));
  };

  const handleBranchMultiSelect = (selectedOptions) => {
    setFormData(prev => ({
      ...prev,
      Branch_Preferences: selectedOptions ? selectedOptions.map(opt => opt.value) : []
    }));
  };

  const handleSubmit = async (e) => {
    e.preventDefault();
    
    if (formData.Branch_Preferences.length === 0) {
      setError('Please select at least one branch preference');
      return;
    }

    setLoading(true);
    setError(null);
    setResults(null);
    setCurrentPage(1);

    try {
      const data = await findColleges(formData);
      if (data.success) {
        // Filter out colleges with 'MISSING_COLLEGE' in either college_name or College_Name
        setResults({
          ...data,
          colleges: data.colleges.filter(
            college =>
              college.college_name !== 'MISSING_COLLEGE' &&
              college.College_Name !== 'MISSING_COLLEGE'
          )
        });
      } else {
        setError(data.error || 'Failed to find colleges');
      }
    } catch (err) {
      setError(err.response?.data?.error || err.message || 'Failed to connect to server');
    } finally {
      setLoading(false);
    }
  };

  const getSafetyColor = (level) => {
    const colors = {
      'Safe': '#10b981',
      'Moderate': '#f59e0b',
      'Reach': '#ef4444'
    };
    return colors[level] || '#6b7280';
  };

  const getSafetyIcon = (level) => {
    return level === 'Safe' ? <FaCheckCircle /> : <FaExclamationCircle />;
  };

  // Sorting logic
  const getSortedColleges = () => {
    if (!results || !results.colleges) return [];
    
    const sorted = [...results.colleges];
    
    switch(sortBy) {
      case 'chance':
        // Best chance first (highest to lowest)
        return sorted.sort((a, b) => b.admission_chance - a.admission_chance);
      case 'cutoff':
        // Lowest cutoff first (easier to get in)
        return sorted.sort((a, b) => a.predicted_cutoff - b.predicted_cutoff);
      case 'safety':
        // Safe -> Moderate -> Reach
        const safetyOrder = { 'Safe': 0, 'Moderate': 1, 'Reach': 2 };
        return sorted.sort((a, b) => {
          const orderDiff = safetyOrder[a.safety_level] - safetyOrder[b.safety_level];
          if (orderDiff !== 0) return orderDiff;
          return b.admission_chance - a.admission_chance;
        });
      default:
        return sorted;
    }
  };

  // Pagination logic
  const getPaginatedColleges = () => {
    const sortedColleges = getSortedColleges();
    const startIndex = (currentPage - 1) * itemsPerPage;
    const endIndex = startIndex + itemsPerPage;
    return sortedColleges.slice(startIndex, endIndex);
  };

  const totalPages = results ? Math.ceil(getSortedColleges().length / itemsPerPage) : 0;

  return (
    <div className="college-finder">
      <div className="finder-header">
        <FaSearch className="header-icon" />
        <h2>College Finder</h2>
        <p>Find colleges where you can get admission based on your rank</p>
      </div>

      <form onSubmit={handleSubmit} className="finder-form">
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
            <label>Category *</label>
            <select name="Category" value={formData.Category} onChange={handleInputChange} required>
              <option value="">Select Category</option>
              {categoryOptions[formData.Exam_Type].map(cat => (
                <option key={cat} value={cat}>{cat}</option>
              ))}
            </select>
          </div>

          <div className="form-group">
            <label>Location Filter (Optional)</label>
            <input
              type="text"
              name="Location"
              value={formData.Location}
              onChange={handleInputChange}
              placeholder="e.g., Bangalore, Mysore"
            />
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

        <div className="branch-selection">
          <label>Branch Preferences * (Select one or more)</label>
          <Select
            options={branchOptions.map(branch => ({
              value: branch,
              label: branch
            }))}
            onChange={handleBranchMultiSelect}
            placeholder="Search and select branches..."
            isMulti
            isClearable
            isSearchable
            value={formData.Branch_Preferences.map(branch => ({
              value: branch,
              label: branch
            }))}
            styles={{
              control: (base) => ({ ...base, minHeight: '48px', borderRadius: '8px', borderWidth: '2px' }),
              option: (base, state) => ({ ...base, padding: '10px' }),
              multiValue: (base) => ({ ...base, backgroundColor: '#8b5cf6', color: 'white' }),
              multiValueLabel: (base) => ({ ...base, color: 'white' }),
              multiValueRemove: (base) => ({ ...base, color: 'white', ':hover': { backgroundColor: '#7c3aed', color: 'white' } })
            }}
          />
          <p className="selected-count">
            {formData.Branch_Preferences.length} branch(es) selected
          </p>
        </div>

        <button type="submit" className="submit-btn" disabled={loading}>
          <FaSearch />
          {loading ? 'Searching...' : 'Find Colleges'}
        </button>
      </form>

      {error && (
        <div className="error-message">
          <FaExclamationCircle /> {error}
        </div>
      )}

      {results && (
        <div className="results-container">
          <div className="results-header">
            <h3>Found {results.total_options} College Options</h3>
            <p>For rank: <strong>{results.user_rank}</strong></p>
          </div>

          {results.colleges.length > 0 && (
            <div className="sort-controls">
              <label>Sort by:</label>
              <select value={sortBy} onChange={(e) => { setSortBy(e.target.value); setCurrentPage(1); }}>
                <option value="chance">Best Chance First</option>
                <option value="cutoff">Lowest Cutoff First</option>
                <option value="safety">Safety Level</option>
              </select>
              <span className="showing-count">
                Showing {((currentPage - 1) * itemsPerPage) + 1}-{Math.min(currentPage * itemsPerPage, getSortedColleges().length)} of {getSortedColleges().length}
              </span>
            </div>
          )}

          {results.colleges.length === 0 ? (
            <div className="no-results">
              <p>No colleges found matching your criteria. Try adjusting your filters.</p>
            </div>
          ) : (
            <>
            <div className="colleges-list">
              {getPaginatedColleges().map((college, index) => (
                <div key={index} className={`college-card${college.is_unstable ? ' unstable' : ''}`}>
                  <div className="college-header">
                    <FaUniversity className="college-icon" />
                    <div className="college-info">
                      <h4>{college.college_name}
                        {college.is_unstable && (
                          <span
                            className="unstable-badge"
                            title="Prediction for this branch is unstable due to high historical variability. Interpret with caution."
                            style={{
                              color: '#f59e0b',
                              marginLeft: '8px',
                              fontSize: '1.1em',
                              verticalAlign: 'middle',
                              cursor: 'pointer',
                              display: 'inline-flex',
                              alignItems: 'center'
                            }}
                          >
                            <FaExclamationTriangle style={{ marginRight: 3 }} />
                            <span style={{ fontWeight: 600 }}>Unstable</span>
                          </span>
                        )}
                      </h4>
                      <p className="college-code">{college.college_code}</p>
                    </div>
                    <div 
                      className="safety-badge" 
                      style={{ backgroundColor: getSafetyColor(college.safety_level) }}
                    >
                      {getSafetyIcon(college.safety_level)}
                      {college.safety_level}
                    </div>
                  </div>

                  <div className="college-details">
                    <div className="detail-row">
                      <span className="label">Branch:</span>
                      <span className="value">{college.branch}</span>
                    </div>
                    <div className="detail-row">
                      <span className="label">Predicted Cutoff:</span>
                      <span className="value cutoff">{college.predicted_cutoff}</span>
                    </div>
                    <div className="detail-row">
                      <span className="label">Your Advantage:</span>
                      <span className={`value ${college.rank_difference > 0 ? 'positive' : 'negative'}`}>
                        {college.rank_difference > 0 ? '+' : ''}{college.rank_difference} ranks
                      </span>
                    </div>
                    <div className="detail-row">
                      <span className="label">Admission Chance:</span>
                      <span className="value chance">{college.admission_chance}%</span>
                    </div>
                    <div className="detail-row">
                      <span className="label">Historical Volatility:</span>
                      <span className="value">±{college.volatility} ranks</span>
                    </div>
                    {college.is_unstable && (
                      <div className="detail-row">
                        <span className="label" style={{ color: '#f59e0b' }}>⚠ Unstable Prediction:</span>
                        <span className="value" style={{ color: '#f59e0b' }}>
                          This branch has high historical variability. Use prediction with caution.
                        </span>
                      </div>
                    )}
                  </div>
                </div>
              ))}
            </div>

            {totalPages > 1 && (
              <div className="pagination">
                <button 
                  onClick={() => setCurrentPage(prev => Math.max(1, prev - 1))}
                  disabled={currentPage === 1}
                  className="page-btn"
                >
                  Previous
                </button>
                
                <div className="page-numbers">
                  {[...Array(totalPages)].map((_, i) => {
                    const pageNum = i + 1;
                    // Show first page, last page, current page, and pages around current
                    if (
                      pageNum === 1 ||
                      pageNum === totalPages ||
                      (pageNum >= currentPage - 2 && pageNum <= currentPage + 2)
                    ) {
                      return (
                        <button
                          key={pageNum}
                          onClick={() => setCurrentPage(pageNum)}
                          className={`page-btn ${currentPage === pageNum ? 'active' : ''}`}
                        >
                          {pageNum}
                        </button>
                      );
                    } else if (pageNum === currentPage - 3 || pageNum === currentPage + 3) {
                      return <span key={pageNum} className="page-dots">...</span>;
                    }
                    return null;
                  })}
                </div>

                <button 
                  onClick={() => setCurrentPage(prev => Math.min(totalPages, prev + 1))}
                  disabled={currentPage === totalPages}
                  className="page-btn"
                >
                  Next
                </button>
              </div>
            )}
            </>
          )}
        </div>
      )}
    </div>
  );
};

export default CollegeFinder;
