# 🎓 E001 (UVCE Bangalore) - Model Validation Report
## 2025 Round 1 Cutoff Predictions vs Actual Results

---

## 📊 Executive Summary

This report evaluates the performance of our KCET cutoff prediction model against **real 2025 Round 1 cutoffs** for E001 (University of Visvesvaraya College of Engineering, Bangalore - A State Autonomous Public University on IIT Model).

### 🔍 Key Findings

| Metric | Value | Status |
|--------|-------|--------|
| **Total Test Cases** | 126 predictions | ✅ Comprehensive |
| **Mean Absolute Error (MAE)** | 21,710 ranks | ⚠️ High |
| **Median Absolute Error** | 19,633 ranks | ⚠️ High |
| **MAPE (Mean Abs % Error)** | 146.64% | ❌ Very High |
| **R² Score** | 0.0934 | ❌ Poor |
| **RMSE** | 28,418 ranks | ⚠️ High |

**Overall Assessment**: ❌ **Model shows significant prediction errors for E001, particularly for CS/CSE branches**

---

## 🔬 Detailed Analysis

### 1. Branch-Wise Performance

| Branch | MAE (Ranks) | Performance | Issues Identified |
|--------|-------------|-------------|-------------------|
| **ME** (Mechanical) | 11,700 | 🟡 Moderate | Best performing branch |
| **EEE** (Electrical) | 12,020 | 🟡 Moderate | Second best |
| **ECE** (Electronics) | 15,229 | 🟠 Fair | Some errors |
| **ISE** (Info Science) | 15,660 | 🟠 Fair | Moderate errors |
| **AI** (AI & Data Science) | 17,126 | 🟠 Fair | Imputation issues |
| **CSE** (CS Engineering) | 19,501 | 🔴 Poor | High errors |
| **CS** (Computers) | **59,308** | 🔴 **Critical** | **Severe underprediction** |

### 2. Critical Issue: CS Branch Predictions

**Problem Identified**: The model severely **underpredicts** CS (Computers) branch cutoffs by ~90%

#### Sample CS Predictions:
| Category | Actual | Predicted | Error | Error % |
|----------|--------|-----------|-------|---------|
| 1G | 70,257 | 7,822 | -62,435 | **-88.9%** |
| GM | 45,486 | 3,663 | -41,823 | **-91.9%** |
| 2AG | 63,957 | 7,010 | -56,947 | **-89.0%** |
| 3AG | 52,560 | 4,210 | -48,350 | **-92.0%** |

**Root Cause**: The model is using **2024 Round 1 cutoffs** as imputation source, but E001's CS branch cutoffs have **dramatically increased** in 2025, likely due to:
- Increased demand for CS seats
- Changes in seat allocation
- E001's special status (IIT Model, State Autonomous)

### 3. AI Branch Performance

**Issue**: AI branch predictions show **college_fallback** imputation instead of exact matches

#### Sample AI Predictions:
| Category | Actual | Predicted | Error | Error % | Imputation |
|----------|--------|-----------|-------|---------|------------|
| GM | 4,628 | 29,061 | +24,433 | **+527.9%** | college_fallback |
| 1G | 7,516 | 29,626 | +22,110 | **+294.2%** | college_fallback |
| 3AG | 5,191 | 29,494 | +24,303 | **+468.2%** | college_fallback |

**Root Cause**: The imputation lookup **doesn't have E001 AI branch data** from 2024, so it falls back to college-wide median (~29,000), which is far too high.

### 4. CSE (Computer Science Engineering) Performance

Similar to CS, CSE also shows significant errors:
- **MAE**: 19,501 ranks
- All predictions use **college_fallback** imputation
- Overpredicts most categories by 200-700%

---

## 🎯 Accuracy by Category

### Top 10 Most Accurate Categories:
| Rank | Category | MAE | Typical Use |
|------|----------|-----|-------------|
| 1 | 3AK | 5,867 | 3A Category, Karnataka |
| 2 | 2BK | 10,062 | 2B Category, Karnataka |
| 3 | SCG | 14,551 | Scheduled Caste, General |
| 4 | 3AR | 16,045 | 3A Category, Rural |
| 5 | SCK | 18,455 | SC, Karnataka |
| 6 | SCR | 18,563 | SC, Rural |
| 7 | 2BG | 18,625 | 2B, General |
| 8 | GMK | 18,903 | General Merit, Karnataka |
| 9 | STR | 20,424 | ST, Rural |
| 10 | STG | 20,429 | ST, General |

**Note**: Even the "best" categories have errors of 5,000-20,000 ranks, which is significant.

---

## ❌ Top 10 Worst Predictions

| Branch | Category | Actual | Predicted | Error | Error % |
|--------|----------|--------|-----------|-------|---------|
| CSE | GM | 3,473 | 29,061 | +25,588 | **+736.8%** |
| AI | 3AG | 5,191 | 29,494 | +24,303 | **+468.2%** |
| CSE | GMK | 4,239 | 29,378 | +25,139 | **+593.0%** |
| AI | 3BG | 4,788 | 29,494 | +24,706 | **+516.0%** |
| AI | GM | 4,628 | 29,061 | +24,433 | **+527.9%** |
| CSE | 3AG | 4,061 | 29,494 | +25,433 | **+626.3%** |
| CSE | 3BG | 4,329 | 29,494 | +25,165 | **+581.3%** |
| CSE | 1G | 6,561 | 29,626 | +23,065 | **+351.6%** |
| CSE | 2AG | 5,714 | 29,773 | +24,059 | **+421.0%** |
| AI | 1G | 7,516 | 29,626 | +22,110 | **+294.2%** |

**Pattern**: All worst predictions involve **AI or CSE branches** with **college_fallback imputation**, leading to massive overpredictions.

---

## ✅ Top 10 Best Predictions

| Branch | Category | Actual | Predicted | Error | Error % |
|--------|----------|--------|-----------|-------|---------|
| CSE | SCK | 29,894 | 29,883 | -11 | **-0.04%** |
| ECE | 3BR | 8,883 | 9,201 | +318 | **+3.6%** |
| EEE | 2AK | 29,635 | 26,564 | -3,071 | **-10.4%** |
| CSE | SCR | 24,589 | 29,883 | +5,294 | **+21.5%** |
| AI | SCR | 33,087 | 29,883 | -3,204 | **-9.7%** |
| AI | STR | 23,537 | 29,883 | +6,346 | **+27.0%** |
| AI | SCK | 41,199 | 29,883 | -11,316 | **-27.5%** |
| ISE | 2AR | 8,169 | 9,986 | +1,817 | **+22.2%** |
| ME | 2BR | 107,428 | 115,702 | +8,274 | **+7.7%** |
| ECE | 3AR | 10,000 | 11,738 | +1,738 | **+17.4%** |

**Pattern**: Best predictions are for **reserved categories (SC, ST)** where historical patterns are more stable, and non-CS/CSE/AI branches.

---

## 🔍 Error Distribution

| Error Range | Count | Percentage | Visual |
|-------------|-------|------------|--------|
| **< 10%** | 1 | 0.8% | ▓ |
| **10-25%** | 10 | 7.9% | ▓▓▓▓▓▓▓ |
| **25-50%** | 13 | 10.3% | ▓▓▓▓▓▓▓▓▓▓ |
| **50-100%** | 18 | 14.3% | ▓▓▓▓▓▓▓▓▓▓▓▓▓▓ |
| **> 100%** | 84 | **66.7%** | ▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓ |

**Critical**: **66.7%** of predictions have errors exceeding 100%, indicating systematic issues.

---

## 🚨 Root Cause Analysis

### 1. **Missing Historical Data for New Branches**
- **AI (Artificial Intelligence)** branch appears to be relatively new at E001
- No exact match found in 2024 data → falls back to college median
- College median (~29,000) doesn't represent AI branch reality

### 2. **CS Branch Cutoff Shift**
- E001 CS cutoffs have **increased dramatically** in 2025
- Historical 2024 data shows much lower cutoffs (~7,000-8,000)
- Model uses exact 2024 matches but fails to predict the surge
- Possible reasons:
  - E001's special status (IIT Model, State Autonomous)
  - Increased prestige/demand
  - Seat matrix changes
  - Policy changes

### 3. **Imputation Strategy Limitations**
- **college_fallback** uses median across all branches
- This creates ~29,000 prediction for all CSE/AI fallbacks
- Doesn't account for branch-specific demand patterns
- Fails for colleges with high variance between branches

### 4. **College Tier Mismatch**
- E001 is classified as **Tier 1** in our mapping
- But its cutoff patterns don't match other Tier 1 colleges
- Special autonomous status creates unique behavior

---

## 📈 Comparison with Model's Expected Performance

| Metric | Expected (On Test Set) | E001 Actual | Difference |
|--------|------------------------|-------------|------------|
| MAE | 1,267 ranks | 21,710 ranks | **+17x worse** |
| R² | 0.9958 | 0.0934 | **-90% degradation** |

**Conclusion**: The model performs **17 times worse** on E001 than on its test set, indicating:
- E001 has unique characteristics not well-represented in training data
- New branches (AI) and dramatic cutoff shifts aren't captured
- Model generalizes poorly to colleges with rapid changes

---

## 💡 Recommendations

### Immediate Actions:

1. **⚠️ Add Disclaimer for E001 Predictions**
   - Flag E001 predictions as "Low Confidence"
   - Warn users about potential errors of ±20,000 ranks
   - Especially for CS, CSE, and AI branches

2. **🔄 Update Training Data**
   - Include E001 2025 Round 1 data once available
   - Add AI branch historical data
   - Retrain model with updated cutoffs

3. **🎯 Improve Imputation Strategy**
   - Create separate imputation logic for new branches
   - Use branch-family grouping (CS/CSE/AI together)
   - Consider year-over-year growth trends
   - Add college-specific adjustment factors

4. **🏷️ College-Specific Handling**
   - Create special logic for autonomous/IIT-model colleges
   - Flag colleges with known rapid changes
   - Use different models for different college tiers

### Long-Term Improvements:

5. **📊 Trend Analysis Module**
   - Implement Solution 3 (Trend Analysis) for new branches
   - Predict cutoff growth based on regional patterns
   - Use seat matrix and exam statistics

6. **🤖 Ensemble Approach**
   - Combine multiple prediction strategies
   - Use weighted average based on data availability
   - Add confidence intervals to predictions

7. **🔍 Feature Engineering**
   - Add "years since branch introduction" feature
   - Include college autonomy status
   - Factor in state policy changes
   - Add seat matrix year-over-year changes

8. **✅ Separate Models by Branch Family**
   - Train dedicated model for CS/CSE/AI/ISE branches
   - Different model for traditional branches (ME, EEE, ECE)
   - Accounts for different demand dynamics

---

## 📁 Data Files Generated

1. **e001_validation_results.csv** - Detailed prediction results for all 126 cases
   - Columns: Branch, Category, Actual, Predicted, Error, Error_Pct, Imputation

---

## 🎯 Conclusions

### What Works:
✅ Model performs reasonably for traditional branches (ME, EEE, ECE)  
✅ Reserved category predictions (SC, ST) are more stable  
✅ Predictions for less competitive seats show better accuracy  

### What Doesn't Work:
❌ CS branch predictions are off by **~90%** (severe underprediction)  
❌ AI branch has **no historical data**, causing 400-700% overprediction  
❌ CSE branch predictions suffer from college_fallback issues  
❌ Model fails to capture E001's unique autonomous college behavior  
❌ Imputation strategy fails for colleges with high branch variance  

### Overall Verdict:
**🔴 NOT PRODUCTION READY for E001 predictions**

The model shows systematic failures for:
- New/emerging branches (AI, Data Science)
- High-demand CS/CSE branches
- Colleges with rapid cutoff changes
- Autonomous institutions with unique characteristics

### Recommended Action:
1. **Disable E001 predictions** until model is retrained
2. OR display **large confidence interval (±25,000 ranks)**
3. OR show **"Experimental"** tag for E001 CS/CSE/AI predictions

---

*Report Generated: December 2, 2025*  
*Model: LightGBM with Solution 2 (Imputation)*  
*Data Source: E001 2025 Round 1 Official Cutoffs*  
*Test Cases: 126 predictions across 7 branches*

