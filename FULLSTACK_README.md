# College Predictor - Full Stack Application

## 🎓 Features

### 1. Admission Chance Calculator
- Enter your rank and specific college-branch combination
- Get **percentage probability** of admission
- See detailed **explanation** of how the chance is calculated using:
  - Historical volatility analysis
  - Trend analysis (increasing/decreasing cutoffs)
  - Round-to-round stability
  - Your rank position relative to predicted cutoff

### 2. College Finder (Reverse Search)
- Enter your rank and branch preferences
- Get a **ranked list of colleges** where you can get admission
- Colleges categorized as:
  - **Safe** (90%+ chance)
  - **Moderate** (60-70% chance)
  - **Reach** (30-50% chance)
- Filter by location (Bangalore, Mysore, etc.)
- Select multiple branch preferences

## 🚀 Running the Application

### Backend (FastAPI)

```bash
# Navigate to app directory
cd "D:\Major Project\college-predictor\app"

# Activate virtual environment
& "D:\Major Project\college-predictor\ml-gpu\Scripts\Activate.ps1"

# Run the FastAPI server
uvicorn main:app --reload
```

Backend will run on: `http://localhost:8000`

### Frontend (React + Vite)

```bash
# Open new terminal
cd "D:\Major Project\college-predictor\frontend"

# Start development server
npm run dev
```

Frontend will run on: `http://localhost:5173`

## 📡 API Endpoints

### 1. `/predict/chance` (POST)
Calculate admission chance for specific college

**Request:**
```json
{
  "User_Rank": 1500,
  "College_Code": "E005",
  "College_Name": "R. V. College of Engineering",
  "Branch": "CS Computers",
  "Category": "GM",
  "Exam_Type": "CET",
  "Year": 2024,
  "Round": 1,
  "Quota_Seats": 60
}
```

**Response:**
```json
{
  "success": true,
  "predicted_cutoff": 292,
  "user_rank": 1500,
  "chance": {
    "percentage": 15.3,
    "level": "Reach",
    "explanation": "Based on historical patterns...",
    "factors": [
      "⚠ Your rank (1500) is 1208 ranks below predicted cutoff (292)",
      "✓ Low volatility (±150 ranks) - stable cutoffs",
      "• Stable trend over recent years"
    ],
    "details": {
      "rank_difference": -1208,
      "volatility": 150,
      "trend_slope": 25
    }
  }
}
```

### 2. `/colleges/find` (POST)
Find colleges based on rank

**Request:**
```json
{
  "User_Rank": 5000,
  "Branch_Preferences": ["CS Computers", "IS Information Science"],
  "Category": "GM",
  "Exam_Type": "CET",
  "Year": 2024,
  "Round": 1,
  "Location": "Bangalore"
}
```

**Response:**
```json
{
  "success": true,
  "user_rank": 5000,
  "total_options": 25,
  "colleges": [
    {
      "college_code": "E004",
      "college_name": "Dr. Ambedkar Institute Of Technology",
      "branch": "CS Computers",
      "predicted_cutoff": 7554,
      "safety_level": "Safe",
      "admission_chance": 90,
      "rank_difference": 2554,
      "volatility": 800
    }
  ]
}
```

### 3. `/colleges/list` (GET)
Get all available colleges, branches, categories

```bash
GET /colleges/list?exam_type=CET
```

## 🧮 Chance Calculation Formula

```python
if user_rank < predicted_cutoff:
    base_chance = 90%
    buffer = (predicted_cutoff - user_rank) / volatility
    final_chance = min(99%, base_chance + buffer * 10%)

elif abs(rank_diff) <= volatility:
    final_chance = 40-70% (based on position within buffer)

else:
    final_chance = max(5%, 40% - (gap / volatility) * 50%)
```

**Factors Considered:**
- ✅ Historical volatility (±X ranks)
- ✅ 3-year trend analysis (increasing/decreasing)
- ✅ Round-to-round stability
- ✅ College tier and competitiveness
- ✅ Program maturity (new vs established)

## 📊 Transparency Features

The application shows **exactly how** predictions are made:

1. **Feature Explanations:**
   - "Your rank is X ranks better than predicted cutoff"
   - "Historical volatility: ±Y ranks (low/medium/high)"
   - "3-year trend: Increasing/Decreasing by Z ranks/year"

2. **Statistical Details:**
   - Historical volatility
   - Trend slope
   - Round stability metrics
   - Number of years of historical data

3. **Visual Indicators:**
   - Color-coded chance levels (Green/Yellow/Red)
   - Safety badges (Safe/Moderate/Reach)
   - Progress indicators

## 🎨 Frontend Features

- **Responsive Design:** Works on desktop and mobile
- **Real-time Validation:** Form validation before submission
- **Loading States:** Visual feedback during API calls
- **Error Handling:** User-friendly error messages
- **Auto-complete:** College name fills automatically when code is selected
- **Multiple Selection:** Choose multiple branch preferences
- **Filter Options:** Location filter for college finder

## 🛠 Tech Stack

**Backend:**
- FastAPI (Python)
- LightGBM + XGBoost ensemble models
- Pandas for data processing
- Historical cutoff database (combined_cutoffs.csv)

**Frontend:**
- React 19
- Vite build tool
- Axios for API calls
- React Icons
- Custom CSS (no framework dependency)

## 📝 Notes

- Predictions are based on historical data (2020-2024)
- Uses 32 engineered features including:
  - Previous year cutoffs
  - Rolling averages and trends
  - College tier and branch popularity
  - Target encoding of college-branch combinations
- Ensemble of LightGBM + XGBoost models (weighted average)

## 🔍 Testing

Test with real data:
- **Premium College:** E005 (RV College) - CS Computers - Expected cutoff: ~250-300
- **Good College:** E001 (UVCE) - CS Computers - Expected cutoff: ~1800-2000
- **Moderate College:** E004 (Dr. AIT) - CS Computers - Expected cutoff: ~7500-8000

---

**Built with ❤️ using Machine Learning**
