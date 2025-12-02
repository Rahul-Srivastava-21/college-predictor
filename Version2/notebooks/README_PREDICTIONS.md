# KCET Cutoff Prediction - Quick Start Guide

## 📦 Using `predict_cutoff.py`

The prediction module is now available as a standalone Python file that you can import and use anywhere!

---

## 🚀 Quick Start

### 1. Basic Import

```python
from predict_cutoff import predict_college_cutoff
```

### 2. Make a Simple Prediction

```python
result = predict_college_cutoff(
    college_code="E105",
    branch="CS Computers",
    category="GM",
    round_num=1
)

print(f"Predicted Cutoff: {result['predicted_cutoff']:,.0f}")
```

---

## 📚 Available Functions

### `predict_college_cutoff()` - Full Prediction

**Parameters:**
- `college_code` (str): College code (e.g., "E001", "E105")
- `branch` (str): Branch name (e.g., "CS Computers", "AI Artificial Intelligence")
- `category` (str): Category (e.g., "GM", "1G", "2AG", "SC", "ST")
- `round_num` (int): Round number (1, 2, or 3) - default: 1
- `year` (int): Year for prediction - default: 2025
- `college_tier` (int): Tier (1, 2, 3) - auto-detected if not provided
- `prev_round_rank` (float): Previous round cutoff (required for Round 2/3)
- `model_choice` (str): Model to use - default: "lightgbm"
- `use_imputation` (bool): Use imputation for Round 1 - default: True
- `verbose` (bool): Show detailed output - default: True

**Returns:**
Dictionary with:
- `predicted_cutoff`: Predicted rank
- `input_details`: Input parameters used
- `confidence_info`: Model accuracy metrics
- `historical_features`: Historical statistics used

**Example:**
```python
result = predict_college_cutoff(
    college_code="E001",
    branch="AI Artificial Intelligence",
    category="1G",
    round_num=1,
    verbose=True
)
```

---

### `quick_predict()` - Minimal Output

**Parameters:** Same as above (college_code, branch, category, round_num)

**Returns:** Float (just the predicted cutoff)

**Example:**
```python
cutoff = quick_predict("E105", "CS Computers", "GM", round_num=1)
print(f"Cutoff: {cutoff:,.0f}")  # Output: Cutoff: 9,858
```

---

### `batch_predict()` - Multiple Predictions

**Parameters:**
- `predictions_list` (list): List of dictionaries with prediction parameters

**Returns:** pandas DataFrame with all results

**Example:**
```python
predictions = [
    {"college_code": "E105", "branch": "CS Computers", "category": "GM", "round_num": 1},
    {"college_code": "E060", "branch": "CS Computers", "category": "GM", "round_num": 1},
    {"college_code": "E005", "branch": "CS Computers", "category": "GM", "round_num": 1},
]

results_df = batch_predict(predictions)
print(results_df)
```

---

### `get_historical_data()` - View Historical Cutoffs

**Parameters:**
- `college_code` (str): College code
- `branch` (str): Branch name
- `category` (str): Category
- `years` (list): Optional list of years to include

**Returns:** pandas DataFrame with historical data

**Example:**
```python
history = get_historical_data("E105", "CS Computers", "GM", years=[2022, 2023, 2024])
print(history)
```

---

## 💡 Common Use Cases

### Case 1: Round 1 Prediction (Most Common)
```python
# Automatic imputation - easiest way!
result = predict_college_cutoff("E105", "CS Computers", "GM", round_num=1)
print(f"Predicted: {result['predicted_cutoff']:,.0f}")
```

### Case 2: Round 2 Prediction
```python
# Get Round 1 first
round1 = quick_predict("E105", "CS Computers", "GM", round_num=1)

# Use it for Round 2
result = predict_college_cutoff(
    college_code="E105",
    branch="CS Computers",
    category="GM",
    round_num=2,
    prev_round_rank=round1  # Use Round 1 result
)
```

### Case 3: Multiple Predictions at Once
```python
predictions = [
    {"college_code": "E001", "branch": "AI Artificial Intelligence", "category": "1G", "round_num": 1},
    {"college_code": "E005", "branch": "CS Computers", "category": "GM", "round_num": 1},
    {"college_code": "E105", "branch": "CS Computers", "category": "GM", "round_num": 1},
]

results = batch_predict(predictions)
results.to_csv("my_predictions.csv", index=False)
```

### Case 4: Comparing with Historical Data
```python
# Get prediction
predicted = quick_predict("E105", "CS Computers", "GM", round_num=1)

# Get history
history = get_historical_data("E105", "CS Computers", "GM")

print(f"2025 Prediction: {predicted:,.0f}")
print(f"\nHistorical data:")
print(history[['Year', 'Round', 'Cutoff_Rank']])
```

---

## 🎯 Example: Full Workflow

```python
from predict_cutoff import predict_college_cutoff, get_historical_data

# Step 1: View historical trend
history = get_historical_data("E105", "CS Computers", "GM", years=[2022, 2023, 2024])
print("Historical cutoffs:")
print(history[['Year', 'Round', 'Cutoff_Rank']])

# Step 2: Make prediction
result = predict_college_cutoff(
    college_code="E105",
    branch="CS Computers",
    category="GM",
    round_num=1,
    verbose=True
)

# Step 3: Analyze result
print(f"\nPrediction: {result['predicted_cutoff']:,.0f}")
print(f"Margin of error: ±{result['confidence_info']['expected_mae']:,.0f}")
print(f"Imputation source: {result['input_details']['imputation_source']}")

# Step 4: Check if realistic
last_year = history[history['Year'] == 2024]['Cutoff_Rank'].values[0]
change = result['predicted_cutoff'] - last_year
print(f"\nChange from 2024: {change:+,.0f} ({(change/last_year)*100:+.1f}%)")
```

---

## ⚙️ Configuration

### Custom College Tier Mapping

Edit `TIER_MAPPING` in `predict_cutoff.py`:

```python
TIER_MAPPING = {
    "E001": 1, "E002": 1, "E005": 1,  # Top tier
    "E105": 2, "E060": 2,              # Mid tier
    # Add more as needed
}
```

### Change Data Paths

Modify at the top of `predict_cutoff.py`:

```python
DATA_DIR = BASE_DIR / "data"
MODEL_DIR = BASE_DIR / "models"
```

---

## 🔧 Troubleshooting

### Error: "Model not found"
**Solution:** Ensure models are trained and saved in `models/` directory:
- `lightgbm_model.txt`
- `xgboost_model.json`
- `random_forest_baseline.pkl`

### Error: "Data file not found"
**Solution:** Check that data files exist:
- `data/stage2_clean/train_features_v4.csv`
- `data/stage2_clean/val_features_v4.csv`
- `data/stage2_clean/test_features_v4.csv`

### Warning: "Branch name not found"
**Solution:** Check exact branch name in historical data. Common variations:
- "CS Computers" vs "Computer Science"
- "AI Artificial Intelligence" vs "AI & Data Science"

---

## 📊 Understanding Results

### Prediction Dictionary Structure
```python
{
    'predicted_cutoff': 9858.0,                    # Main prediction
    'model_used': 'lightgbm',                      # Model name
    'input_details': {
        'college_code': 'E105',
        'branch': 'CS Computers',
        'category': 'GM',
        'round': 1,
        'imputation_source': 'exact_match',        # How prev_round_rank was determined
        'prev_round_rank_used': 9819.0             # Value used
    },
    'confidence_info': {
        'expected_mae': 1267,                      # Expected error margin
        'model_r2': 0.9958,                        # Model accuracy (R² score)
        'note': 'Prediction with Solution 2 (Imputation)'
    }
}
```

### Imputation Sources
- `exact_match`: Found exact college+branch+category in 2024 data ✅
- `college_branch_fallback`: Used median of college+branch combinations
- `college_fallback`: Used median of all college data
- `branch_fallback`: Used median of all branch data
- `overall_median_fallback`: Used overall median (rare)

---

## 📝 Notes

1. **Solution 2 (Imputation) is active by default** for Round 1 predictions
2. **Expected accuracy**: ±1,267 ranks (MAE) for LightGBM model
3. **For highly competitive colleges** (Tier 1), variation can be higher
4. **Historical features** automatically calculated from 2020-2024 data
5. **Round 2/3 predictions** require `prev_round_rank` from previous round

---

## 🎓 Best Practices

1. ✅ **Always check historical data** before making predictions
2. ✅ **Use batch predictions** for multiple colleges (more efficient)
3. ✅ **Set verbose=False** when running many predictions
4. ✅ **Consider the error margin** (±1,267 ranks) when interpreting results
5. ✅ **Compare predictions with trends** to validate reasonableness

---

## 📞 Support

For issues or questions:
1. Check historical data availability for your college/branch
2. Verify model files exist in `models/` directory
3. Review error messages carefully
4. Compare with historical trends for validation

---

**Last Updated:** December 2025  
**Version:** 1.0 (Solution 2 - Imputation)
