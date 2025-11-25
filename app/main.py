# ============================================
# main.py
# ============================================
"""
FastAPI app for KCET/COMEDK cutoff prediction using ensemble models.
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from typing import List, Optional
import joblib
import xgboost as xgb
import numpy as np
import pandas as pd
import logging
from pathlib import Path
from feature_generator import build_feature_vector, df_history, global_mean

# --------------------------------------------
# Logging
# --------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("kcet_api")

# --------------------------------------------
# Paths
import json
# --------------------------------------------
BASE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = BASE_DIR.parent

META_PATH = PROJECT_ROOT / "notebooks" / "models" / "ensemble" / "weighted_ensemble_meta.joblib"
LGB_PATH = PROJECT_ROOT / "notebooks" / "models" / "lightGBM_model.joblib"
XGB_PATH = PROJECT_ROOT / "notebooks" / "kcet_ml_project" / "models" / "xgboost_stage3" / "xgb_booster_stage3_final_bestiter713.json"

# --------------------------------------------
# Load models
# --------------------------------------------
logger.info("Loading ensemble models...")

# Load unstable branches (by College_Branch_target_enc)
UNSTABLE_BRANCHES_PATH = (PROJECT_ROOT / "notebooks" / "kcet_ml_project" / "models" / "xgboost_stage3" / "package_v1_20251119T100521" / "unstable_branches_stage3.json")
try:
    with open(UNSTABLE_BRANCHES_PATH, "r") as f:
        _unstable_json = json.load(f)
        UNSTABLE_BRANCHES_SET = set(float(x) for x in _unstable_json["unstable_branches"])
    logger.info(f"Loaded {len(UNSTABLE_BRANCHES_SET)} unstable branches for Stage-3 model.")
except Exception as e:
    logger.warning(f"Could not load unstable branches list: {e}")
    UNSTABLE_BRANCHES_SET = set()

meta = joblib.load(META_PATH)
lgb_model = joblib.load(LGB_PATH)

xgb_booster = xgb.Booster()
xgb_booster.load_model(str(XGB_PATH))

features = meta["features"]
weights = meta["weights"]

logger.info(f"Models loaded successfully. Features: {len(features)}")

# --------------------------------------------
# FastAPI setup
# --------------------------------------------
app = FastAPI(
    title="KCET & COMEDK Ensemble Cutoff Predictor",
    description="Predicts cutoff ranks using ensemble of LightGBM + XGBoost",
    version="2.0.0",
)

# CORS middleware for React frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://localhost:3000"],  # Vite default port
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --------------------------------------------
# Pydantic Models
# --------------------------------------------
class UserInput(BaseModel):
    """User input schema - minimal 8 fields needed."""
    College_Code: str
    College_Name: str
    Branch: str
    Category: str
    Exam_Type: str
    Year: int
    Round: int
    Quota_Seats: int = 60

class ChanceInput(BaseModel):
    """Input for chance calculator."""
    User_Rank: int = Field(..., description="Student's CET/COMEDK rank")
    College_Code: str
    College_Name: str
    Branch: str
    Category: str
    Exam_Type: str
    Year: int
    Round: int
    Quota_Seats: int = 60

class CollegeFinderInput(BaseModel):
    """Input for college finder (reverse search)."""
    User_Rank: int = Field(..., description="Student's rank")
    Branch_Preferences: List[str] = Field(..., description="List of preferred branches")
    Category: str
    Exam_Type: str
    Year: int
    Round: int
    Location: Optional[str] = None

# --------------------------------------------
# Helper functions
# --------------------------------------------
def predict_lightgbm(df: pd.DataFrame):
    """LightGBM prediction only."""
    lgb_pred = lgb_model.predict(df[features])
    return float(lgb_pred[0])

def predict_weighted(df: pd.DataFrame):
    """Weighted average of LightGBM + XGBoost."""
    dmat = xgb.DMatrix(df[features])
    xgb_pred = xgb_booster.predict(dmat)
    lgb_pred = lgb_model.predict(df[features])
    final_pred = weights["xgb"] * xgb_pred + weights["lgb"] * lgb_pred
    return float(final_pred[0])

def predict_stacking(df: pd.DataFrame):
    """Stacked ensemble: use LightGBM and XGBoost predictions as features."""
    # Base model predictions
    dmat = xgb.DMatrix(df[features])
    xgb_pred = xgb_booster.predict(dmat)[0]
    lgb_pred = lgb_model.predict(df[features])[0]
    
    # Stack predictions (simple weighted average as meta-learner)
    # In production, you could train a separate meta-model here
    final_pred = 0.6 * lgb_pred + 0.4 * xgb_pred
    return float(final_pred)

def calculate_admission_chance(user_rank: int, predicted_cutoff: float, features_dict: dict) -> dict:
    """
    Calculate admission probability and explanation based on multiple factors.
    
    Returns:
        dict with 'percentage', 'level', 'explanation', 'factors'
    """
    # Ensure all values are proper numeric types (not numpy or string)
    # Get volatility, if it's unreasonably high (> 50k), cap it at a reasonable level
    raw_volatility = float(features_dict.get('Historical_Std_Raw', 5000))
    volatility = min(raw_volatility, 15000)  # Cap volatility at 15k to avoid global std contamination
    trend = float(features_dict.get('trend3Y_slope_L1Y', 0))
    round_std = float(features_dict.get('cutoff_roll2R_std_L1R', 0))
    
    # Ensure predicted_cutoff and user_rank are also proper numeric types
    predicted_cutoff = float(predicted_cutoff)
    user_rank = float(user_rank)
    
    rank_difference = predicted_cutoff - user_rank
    
    # Volatility-based adjustment factor (more lenient)
    # Higher volatility = more uncertainty = slight reduction in confidence
    if volatility < 1000:
        volatility_confidence = 1.0  # Low volatility, high confidence
    elif volatility < 3000:
        volatility_confidence = 0.95  # Moderate volatility, minimal reduction
    elif volatility < 5000:
        volatility_confidence = 0.9  # Higher volatility, slight reduction
    else:
        volatility_confidence = 0.85  # Very high volatility, moderate reduction
    
    # Base chance calculation with volatility consideration (more optimistic)
    if user_rank < predicted_cutoff:
        # User has better (lower) rank than cutoff
        # Calculate how much better relative to volatility
        buffer_size = rank_difference / max(volatility, 100)
        
        if buffer_size >= 2.0:
            # Rank is 2x volatility better - very safe
            base_chance = 97.0 * volatility_confidence
        elif buffer_size >= 1.0:
            # Rank is 1x volatility better - safe
            base_chance = 90.0 * volatility_confidence
        elif buffer_size >= 0.5:
            # Rank is 0.5x volatility better - good chance
            base_chance = 82.0 * volatility_confidence
        else:
            # Rank is slightly better but within volatility range
            base_chance = 72.0 * volatility_confidence
        
        final_chance = min(99.0, base_chance)
        level = "High" if final_chance >= 75 else "Good"
        
    elif abs(rank_difference) <= volatility:
        # Within volatility range - chance depends on position within range
        position_ratio = abs(rank_difference) / max(volatility, 1)
        
        # At predicted cutoff (position_ratio=0): 70% chance with volatility adjustment
        # At edge of volatility (position_ratio=1): 40% chance with volatility adjustment
        base_chance = (70.0 - (position_ratio * 30)) * volatility_confidence
        final_chance = max(25.0, base_chance)  # Minimum 25% within volatility range
        level = "Moderate"
        
    else:
        # User rank is worse than predicted cutoff + volatility
        # Calculate how far beyond the volatility buffer
        gap_beyond_volatility = abs(rank_difference) - volatility
        gap_ratio = gap_beyond_volatility / max(volatility, 100)
        
        # More lenient decay based on how far beyond volatility
        if gap_ratio < 0.5:
            base_chance = 35.0 * volatility_confidence
        elif gap_ratio < 1.0:
            base_chance = 22.0 * volatility_confidence
        elif gap_ratio < 2.0:
            base_chance = 12.0 * volatility_confidence
        else:
            base_chance = 5.0 * volatility_confidence
        
        final_chance = max(2.0, base_chance)
        level = "Low" if final_chance < 20 else "Reach"
    
    # Build explanation
    factors = []
    
    # Rank comparison
    if rank_difference > 0:
        factors.append(f"✓ Your rank ({user_rank}) is {int(rank_difference)} ranks better than predicted cutoff ({int(predicted_cutoff)})")
    elif rank_difference < 0:
        factors.append(f"⚠ Your rank ({user_rank}) is {int(abs(rank_difference))} ranks below predicted cutoff ({int(predicted_cutoff)})")
    else:
        factors.append(f"• Your rank ({user_rank}) matches predicted cutoff")
    
    # Volatility analysis with impact on chance
    if volatility < 1000:
        factors.append(f"✓ Low volatility (±{int(volatility)} ranks) - stable cutoffs increase confidence")
    elif volatility < 3000:
        factors.append(f"• Moderate volatility (±{int(volatility)} ranks) - slight uncertainty")
    elif volatility < 5000:
        factors.append(f"⚠ Higher volatility (±{int(volatility)} ranks) - reduced confidence by ~20%")
    else:
        factors.append(f"⚠ High volatility (±{int(volatility)} ranks) - reduced confidence by ~30%, unpredictable")
    
    # Rank buffer explanation
    if user_rank < predicted_cutoff:
        buffer_in_volatility = rank_difference / max(volatility, 100)
        if buffer_in_volatility >= 2.0:
            factors.append(f"✓ Your rank is {buffer_in_volatility:.1f}x volatility better - very safe margin")
        elif buffer_in_volatility >= 1.0:
            factors.append(f"✓ Your rank is {buffer_in_volatility:.1f}x volatility better - safe margin")
        else:
            factors.append(f"• Your rank advantage is {buffer_in_volatility:.1f}x volatility - moderate margin")
    
    # Trend analysis
    if trend < -500:
        factors.append(f"✓ Cutoffs decreasing trend ({int(abs(trend))} ranks/year) - easier admission")
    elif trend > 500:
        factors.append(f"⚠ Cutoffs increasing trend (+{int(trend)} ranks/year) - more competitive")
    else:
        factors.append("• Stable trend over recent years")
    
    # Round stability
    if features_dict.get('is_first_round_L1R', 0) == 1:
        factors.append("• First round - no previous round data available")
    elif round_std > 0:
        factors.append(f"• Round-to-round variation: ±{int(round_std)} ranks")
    
    explanation = f"Based on historical patterns, volatility-adjusted analysis, and rank comparison, your admission probability is {final_chance:.1f}%. Volatility of ±{int(volatility)} ranks has been factored into this calculation."
    
    return {
        "percentage": round(final_chance, 1),
        "level": level,
        "explanation": explanation,
        "factors": factors,
        "details": {
            "user_rank": user_rank,
            "predicted_cutoff": int(predicted_cutoff),
            "rank_difference": int(rank_difference),
            "volatility": int(volatility),
            "trend_slope": int(trend)
        }
    }

# --------------------------------------------
# API routes
# --------------------------------------------
@app.get("/")
def root():
    return {
        "message": "KCET/COMEDK Ensemble Predictor API is running!",
        "available_routes": [
            "/predict/lightgbm - LightGBM only",
            "/predict/weighted - Weighted ensemble (LightGBM + XGBoost)",
            "/predict/stacking - Stacking ensemble"
        ]
    }

@app.post("/predict/lightgbm")
def predict_lgbm(input_data: UserInput):
    """Predict using LightGBM model only."""
    try:
        df = build_feature_vector(input_data.dict())
        pred = predict_lightgbm(df)
        logger.info(f"LightGBM prediction: {pred}")
        return {
            "model": "LightGBM",
            "predicted_cutoff": pred,
            "input": input_data.dict()
        }
    except Exception as e:
        logger.exception("LightGBM prediction failed")
        return {"error": str(e)}

@app.post("/predict/weighted")
def predict_ensemble(input_data: UserInput):
    """Predict using weighted average ensemble (LightGBM + XGBoost)."""
    try:
        df = build_feature_vector(input_data.dict())
        pred = predict_weighted(df)
        logger.info(f"Weighted ensemble prediction: {pred}")
        return {
            "model": "Weighted Ensemble",
            "weights": weights,
            "predicted_cutoff": pred,
            "input": input_data.dict()
        }
    except Exception as e:
        logger.exception("Weighted ensemble prediction failed")
        return {"error": str(e)}

@app.post("/predict/stacking")
def predict_stack(input_data: UserInput):
    """Predict using stacking ensemble approach."""
    try:
        df = build_feature_vector(input_data.dict())
        pred = predict_stacking(df)
        logger.info(f"Stacking ensemble prediction: {pred}")
        return {
            "model": "Stacking Ensemble",
            "predicted_cutoff": pred,
            "input": input_data.dict()
        }
    except Exception as e:
        logger.exception("Stacking ensemble prediction failed")
        return {"error": str(e)}

@app.post("/predict/chance")
def predict_with_chance(input_data: ChanceInput):
    """
    Predict cutoff and calculate admission chance for student's rank.
    Returns prediction + percentage chance + explanation.
    """
    try:
        # Build feature vector
        user_dict = input_data.dict()
        user_rank = user_dict.pop('User_Rank')
        
        logger.info(f"Building features for rank {user_rank}, input: {user_dict}")
        df = build_feature_vector(user_dict)
        
        logger.info(f"Predicting cutoff...")
        pred = predict_weighted(df)
        logger.info(f"Predicted cutoff: {pred}, type: {type(pred)}")
        
        # Extract features for explanation
        features_dict = df.iloc[0].to_dict()
        logger.info(f"Features extracted, checking types...")
        logger.info(f"Historical_Std_Raw: {features_dict.get('Historical_Std_Raw')} (type: {type(features_dict.get('Historical_Std_Raw'))})")
        logger.info(f"n_years_hist: {features_dict.get('n_years_hist_L1Y')}, Historical_Count: {features_dict.get('Historical_Count_Raw')}")
        
        # Calculate admission chance
        logger.info(f"Calculating admission chance...")
        chance_result = calculate_admission_chance(user_rank, pred, features_dict)
        
        logger.info(f"Chance prediction: {chance_result['percentage']}% for rank {user_rank}")
        
        return {
            "success": True,
            "predicted_cutoff": int(pred),
            "user_rank": user_rank,
            "chance": chance_result,
            "college_info": {
                "college_code": input_data.College_Code,
                "college_name": input_data.College_Name,
                "branch": input_data.Branch,
                "category": input_data.Category,
                "exam_type": input_data.Exam_Type,
                "year": input_data.Year,
                "round": input_data.Round
            }
        }
    except Exception as e:
        logger.exception("Chance prediction failed")
        return {"success": False, "error": str(e)}

@app.post("/colleges/find")
def find_colleges(input_data: CollegeFinderInput):
    """
    Find colleges where student can get admission based on rank.
    Reverse prediction: given rank, find all eligible college-branch combinations.
    """
    try:
        user_rank = input_data.User_Rank
        branches = input_data.Branch_Preferences
        category = input_data.Category
        exam_type = input_data.Exam_Type
        year = input_data.Year
        round_num = input_data.Round
        location_filter = input_data.Location
        
        logger.info(f"Finding colleges for rank {user_rank}, branches: {branches}")
        
        # Get unique college-branch combinations from historical data
        historical_combos = df_history[
            (df_history['Branch'].isin(branches)) &
            (df_history['Category'] == category) &
            (df_history['Exam_Type'] == exam_type)
        ][['College_Code', 'College_Name', 'Branch']].drop_duplicates()
        
        results = []
        
        for _, row in historical_combos.iterrows():
            try:
                # Skip if location filter doesn't match
                if location_filter and location_filter.lower() not in row['College_Name'].lower():
                    continue

                # Build feature vector for this combo
                test_input = {
                    'College_Code': row['College_Code'],
                    'College_Name': row['College_Name'],
                    'Branch': row['Branch'],
                    'Category': category,
                    'Exam_Type': exam_type,
                    'Year': year,
                    'Round': round_num,
                    'Quota_Seats': 60
                }

                df = build_feature_vector(test_input)
                predicted_cutoff = predict_weighted(df)

                # Extract features for chance calculation
                features_dict = df.iloc[0].to_dict()
                volatility = features_dict.get('Historical_Std_Raw', 5000)

                # Unstable branch detection using College_Branch_target_enc
                cb_enc = float(features_dict.get('College_Branch_target_enc', -1))
                is_unstable = cb_enc in UNSTABLE_BRANCHES_SET

                # Calculate buffer zone
                buffer = max(500, volatility * 0.3)

                # Determine safety level
                if user_rank <= predicted_cutoff - buffer:
                    safety = "Safe"
                    chance_pct = 90
                elif user_rank <= predicted_cutoff + buffer:
                    safety = "Moderate"
                    chance_pct = 60
                elif user_rank <= predicted_cutoff + (buffer * 2):
                    safety = "Reach"
                    chance_pct = 30
                else:
                    continue  # Skip if too far

                results.append({
                    "college_code": row['College_Code'],
                    "college_name": row['College_Name'],
                    "branch": row['Branch'],
                    "predicted_cutoff": int(predicted_cutoff),
                    "safety_level": safety,
                    "admission_chance": chance_pct,
                    "rank_difference": int(predicted_cutoff - user_rank),
                    "volatility": int(volatility),
                    "is_unstable": is_unstable
                })

            except Exception as e:
                logger.warning(f"Failed to predict for {row['College_Code']}-{row['Branch']}: {e}")
                continue
        
        # Sort by safety level then by predicted cutoff
        safety_order = {"Safe": 0, "Moderate": 1, "Reach": 2}
        results.sort(key=lambda x: (safety_order[x["safety_level"]], x["predicted_cutoff"]))
        
        logger.info(f"Found {len(results)} eligible colleges, returning all results")
        
        return {
            "success": True,
            "user_rank": user_rank,
            "total_options": len(results),
            "colleges": results  # Return all results instead of limiting to 50
        }
        
    except Exception as e:
        logger.exception("College finder failed")
        return {"success": False, "error": str(e)}

@app.get("/colleges/list")
def list_colleges(exam_type: str = "CET", college_code: str = None):
    """
    Get list of all available colleges, branches, and metadata.
    If college_code is provided, returns branches for that specific college only.
    Used to populate dropdowns in frontend.
    """
    try:
        # Filter for year 2024 only - consistent for both CET and COMEDK
        filtered_data = df_history[
            (df_history['Exam_Type'] == exam_type) & 
            (df_history['Year'] == 2024)
        ]

        # Filter out colleges with missing names and remove duplicates
        colleges_df = filtered_data[['College_Code', 'College_Name']]
        colleges_df = colleges_df.dropna(subset=['College_Name', 'College_Code'])
        colleges_df = colleges_df[colleges_df['College_Name'] != 'MISSING_COLLEGE']
        colleges = colleges_df.drop_duplicates().to_dict('records')

        # If college_code is provided, filter branches for that specific college
        if college_code:
            branch_data = filtered_data[filtered_data['College_Code'] == college_code]
            logger.info(f"Filtering branches for college {college_code}: {len(branch_data)} records")
        else:
            branch_data = filtered_data

        # Filter out NaN/null values, clean strings, and convert before sorting
        branches_raw = branch_data['Branch'].dropna().astype(str).unique()
        branches = [branch.replace('\r', ' ').replace('\n', ' ').strip() for branch in branches_raw]
        branches = [b for b in branches if b]  # Remove empty strings

        categories = filtered_data['Category'].dropna().astype(str).unique().tolist()

        logger.info(f"Returning: {len(colleges)} colleges, {len(branches)} branches, {len(categories)} categories")

        return {
            "success": True,
            "colleges": colleges,
            "branches": branches,
            "categories": categories
        }

    except Exception as e:
        logger.exception("Failed to list colleges")
        return {"success": False, "error": str(e)}

# --------------------------------------------
# Example run
# --------------------------------------------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
