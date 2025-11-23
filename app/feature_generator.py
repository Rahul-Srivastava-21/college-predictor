# ============================================
# feature_generator.py
# ============================================
"""
Feature generator for KCET/COMEDK cutoff prediction.
Takes 8 user inputs and generates all 32 features required by LightGBM model.

User Inputs:
    - College_Code: str (e.g., "E001")
    - College_Name: str (e.g., "B.M.S. College of Engineering")
    - Branch: str (e.g., "Computer Science and Engineering")
    - Category: str (e.g., "GM", "2AG", "SC", etc.)
    - Exam_Type: str ("KCET" or "COMEDK")
    - Year: int (e.g., 2024)
    - Round: int (e.g., 1, 2, 3)
    - Quota_Seats: int (e.g., 60)

Output:
    - DataFrame (1x32) with all required numeric features for prediction
"""

import pandas as pd
import numpy as np
import json
import logging
from pathlib import Path

# --------------------------------------------
# Logging configuration
# --------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("feature_generator")

# --------------------------------------------
# Constants & paths
# --------------------------------------------
BASE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = Path("D:\\Major Project\\college-predictor")
DATA_DIR = PROJECT_ROOT / "notebooks" / "kcet_ml_project" / "data"
MAPPINGS_DIR = DATA_DIR / "mappings_regenerated"

# Historical data for feature calculation
HISTORICAL_DATA_PATH = PROJECT_ROOT / "notebooks" / "combined_cutoffs.csv"

# Mapping files
TARGET_ENCODING_PATH = MAPPINGS_DIR / "target_encoding_maps.json"

# --------------------------------------------
# Load mappings and historical data
# --------------------------------------------
def load_mappings():
    """Load all encoding maps and metadata."""
    logger.info("Loading encoding maps...")
    
    # Target encoding maps
    with open(TARGET_ENCODING_PATH, 'r') as f:
        target_maps = json.load(f)
    
    # Label encoding maps
    label_encodings = {
        'Exam_Type': {'KCET': 0, 'CET': 0, 'COMEDK': 1},
        'Volatility_Category': {'Low': 0, 'Medium': 1, 'High': 2},
        'Program_Maturity': {'New': 0, 'Growing': 1, 'Established': 2},
        'Category_Score': {
            'GM': 1.0, '1G': 0.95, '2AG': 0.9, '2BG': 0.85,
            '3AG': 0.8, '3BG': 0.75, 'SC': 0.7, 'ST': 0.65
        }
    }
    
    # College tier map (default tier 3 if not found)
    college_tier_map = {}
    
    # Branch popularity map (default 0.5 if not found)
    branch_popularity_map = {}
    
    logger.info("✅ Encoding maps loaded successfully")
    
    return target_maps, label_encodings, college_tier_map, branch_popularity_map

def load_historical_data():
    """Load historical cutoff data for feature calculation."""
    logger.info(f"Loading historical data from {HISTORICAL_DATA_PATH}...")
    df = pd.read_csv(HISTORICAL_DATA_PATH)
    logger.info(f"✅ Loaded historical data: {df.shape}")
    return df

# Load once at module import
target_maps, label_encodings, college_tier_map, branch_popularity_map = load_mappings()
df_history = load_historical_data()
global_mean = target_maps['metadata']['global_mean']

logger.info(f"Global mean cutoff rank: {global_mean:.2f}")

# --------------------------------------------
# Feature calculation functions
# --------------------------------------------

def calculate_inter_year_features(college_code, branch, category, exam_type, year):
    """Calculate features based on previous years' data."""
    logger.info(f"Calculating inter-year features for {college_code}-{branch}-{category}")
    
    # Filter historical data for this program
    program_history = df_history[
        (df_history['College_Code'] == college_code) &
        (df_history['Branch'] == branch) &
        (df_history['Category'] == category) &
        (df_history['Exam_Type'] == exam_type) &
        (df_history['Year'] < year)
    ].sort_values('Year')
    
    if len(program_history) > 0:
        # Previous year cutoffs
        cutoff_L1Y = program_history[program_history['Year'] == year - 1]['Cutoff_Rank'].mean()
        cutoff_L2Y = program_history[program_history['Year'] == year - 2]['Cutoff_Rank'].mean()
        cutoff_L3Y = program_history[program_history['Year'] == year - 3]['Cutoff_Rank'].mean()
        
        # Rolling statistics
        last_3_years = program_history[program_history['Year'] >= year - 3]['Cutoff_Rank']
        cutoff_roll3Y_mean = last_3_years.mean()
        cutoff_roll3Y_std = last_3_years.std() if len(last_3_years) > 1 else 0.0
        
        # Historical depth
        n_years_hist = len(program_history['Year'].unique())
        
        # Trend calculation
        if len(last_3_years) >= 2:
            years_arr = program_history[program_history['Year'] >= year - 3]['Year'].values
            cutoffs_arr = last_3_years.values
            trend3Y_slope = np.polyfit(years_arr, cutoffs_arr, 1)[0] if len(years_arr) == len(cutoffs_arr) else 0.0
        else:
            trend3Y_slope = 0.0
        
        is_low_history = 1 if n_years_hist < 2 else 0
        
        # Overall historical stats
        historical_mean_primary = program_history['Cutoff_Rank'].mean()
        historical_std_raw = program_history['Cutoff_Rank'].std()
        historical_count_raw = len(program_history)
        
        # Percentile rank
        all_means = df_history.groupby(['College_Code', 'Branch', 'Category'])['Cutoff_Rank'].mean()
        historical_mean_percentile = (all_means <= historical_mean_primary).mean()
        
    else:
        # No history - use defaults
        cutoff_L1Y = cutoff_L2Y = cutoff_L3Y = np.nan
        cutoff_roll3Y_mean = cutoff_roll3Y_std = 0.0
        n_years_hist = 0
        trend3Y_slope = 0.0
        is_low_history = 1
        historical_mean_primary = global_mean
        # Use a reasonable default volatility instead of 0 or global std
        historical_std_raw = 5000.0  # Moderate default volatility
        historical_count_raw = 0
        historical_mean_percentile = 0.5
    
    # Branch aggregates
    branch_history = df_history[
        (df_history['Branch'] == branch) &
        (df_history['Year'] == year - 1)
    ]
    branch_prevY_mean = branch_history['Cutoff_Rank'].mean() if len(branch_history) > 0 else global_mean
    
    # College aggregates
    college_history = df_history[
        (df_history['College_Code'] == college_code) &
        (df_history['Year'] == year - 1)
    ]
    college_prevY_allbranches_mean = college_history['Cutoff_Rank'].mean() if len(college_history) > 0 else global_mean
    
    return {
        'cutoff_lag1Y_L1Y': cutoff_L1Y,
        'cutoff_lag2Y_L2Y': cutoff_L2Y,
        'cutoff_roll3Y_mean_L1Y': cutoff_roll3Y_mean,
        'cutoff_roll3Y_std_L1Y': cutoff_roll3Y_std,
        'n_years_hist_L1Y': n_years_hist,
        'trend3Y_slope_L1Y': trend3Y_slope,
        'is_low_history_L1Y': is_low_history,
        'branch_prevY_mean_L1Y': branch_prevY_mean,
        'college_prevY_allbranches_mean_L1Y': college_prevY_allbranches_mean,
        'Historical_Mean_Primary': historical_mean_primary,
        'Historical_Mean_Percentile': historical_mean_percentile,
        'Historical_Std_Raw': historical_std_raw,
        'Historical_Count_Raw': historical_count_raw
    }


def calculate_intra_year_features(college_code, branch, category, exam_type, year, round_num):
    """Calculate features based on previous rounds in the same year."""
    logger.info(f"Calculating intra-year features for Round {round_num}")
    
    same_year_history = df_history[
        (df_history['College_Code'] == college_code) &
        (df_history['Branch'] == branch) &
        (df_history['Category'] == category) &
        (df_history['Exam_Type'] == exam_type) &
        (df_history['Year'] == year) &
        (df_history['Round'] < round_num)
    ].sort_values('Round')
    
    if len(same_year_history) > 0:
        cutoff_lag1R = same_year_history.iloc[-1]['Cutoff_Rank']
        cutoff_roll2R_mean = same_year_history.tail(2)['Cutoff_Rank'].mean()
        cutoff_roll2R_std = same_year_history.tail(2)['Cutoff_Rank'].std() if len(same_year_history) > 1 else 0.0
        n_rounds_hist = len(same_year_history)
        is_first_round = 0
    else:
        cutoff_lag1R = 0.0
        cutoff_roll2R_mean = 0.0
        cutoff_roll2R_std = 0.0
        n_rounds_hist = 0
        is_first_round = 1
    
    return {
        'cutoff_lag1R_L1R': cutoff_lag1R,
        'cutoff_roll2R_mean_L1R': cutoff_roll2R_mean,
        'cutoff_roll2R_std_L1R': cutoff_roll2R_std,
        'n_rounds_hist_L1R': n_rounds_hist,
        'is_first_round_L1R': is_first_round
    }


def build_feature_vector(user_input: dict) -> pd.DataFrame:
    """
    Build the complete 32-feature vector from minimal user input.
    
    Parameters:
    -----------
    user_input : dict with keys:
        - College_Code: str
        - College_Name: str (not used in features but kept for context)
        - Branch: str
        - Category: str
        - Exam_Type: str
        - Year: int
        - Round: int
        - Quota_Seats: int
    
    Returns:
    --------
    pd.DataFrame with 1 row and 32 columns (all features required by LightGBM)
    """
    
    logger.info(f"Building feature vector from user input: {user_input}")
    
    # Extract inputs
    college_code = user_input['College_Code']
    branch = user_input['Branch']
    category = user_input['Category']
    exam_type = user_input['Exam_Type']
    year = user_input['Year']
    round_num = user_input['Round']
    quota_seats = user_input.get('Quota_Seats', 60)
    
    # ========================================================================
    # 1. Temporal Features
    # ========================================================================
    years_since_2020 = year - 2020
    is_recent = 1 if year >= 2022 else 0
    year_squared = year ** 2
    
    # ========================================================================
    # 2. Encoded Categorical Features
    # ========================================================================
    exam_type_encoded = label_encodings['Exam_Type'].get(exam_type.upper(), 0)
    category_score = label_encodings['Category_Score'].get(category, 0.5)
    
    # Target encodings
    college_code_target_enc = target_maps['College_Code_target_enc'].get(
        str(college_code), global_mean
    )
    
    college_branch_key = f"{college_code}_{branch}"
    college_branch_target_enc = target_maps['College_Branch_target_enc'].get(
        college_branch_key, global_mean
    )
    
    # ========================================================================
    # 3. Calculate Historical Features
    # ========================================================================
    inter_year_features = calculate_inter_year_features(
        college_code, branch, category, exam_type, year
    )
    
    intra_year_features = calculate_intra_year_features(
        college_code, branch, category, exam_type, year, round_num
    )
    
    # ========================================================================
    # 4. Derived Features
    # ========================================================================
    historical_std_raw = inter_year_features['Historical_Std_Raw']
    n_years_hist = inter_year_features['n_years_hist_L1Y']
    
    # Volatility category
    if historical_std_raw < 500:
        volatility_category_encoded = 0  # Low
    elif historical_std_raw < 1500:
        volatility_category_encoded = 1  # Medium
    else:
        volatility_category_encoded = 2  # High
    
    # Program maturity
    if n_years_hist >= 5:
        program_maturity_encoded = 2  # Established
        is_established = 1
    elif n_years_hist >= 2:
        program_maturity_encoded = 1  # Growing
        is_established = 0
    else:
        program_maturity_encoded = 0  # New
        is_established = 0
    
    # College tier and branch popularity (use defaults if not in maps)
    college_tier_numeric = college_tier_map.get(college_code, 3)
    branch_popularity = branch_popularity_map.get(branch, 0.5)
    
    # ========================================================================
    # 5. Assemble All 32 Features in Correct Order
    # ========================================================================
    features = {
        'Year': year,
        'Round': round_num,
        'Exam_Type': exam_type_encoded,
        'Years_Since_2020': years_since_2020,
        'Is_Recent': is_recent,
        'Year_Squared': year_squared,
        'Historical_Mean_Primary': inter_year_features['Historical_Mean_Primary'],
        'Historical_Mean_Percentile': inter_year_features['Historical_Mean_Percentile'],
        'College_Tier_Numeric': college_tier_numeric,
        'Category_Score': category_score,
        'Historical_Std_Raw': historical_std_raw,
        'Volatility_Category': volatility_category_encoded,
        'Historical_Count_Raw': inter_year_features['Historical_Count_Raw'],
        'Program_Maturity': program_maturity_encoded,
        'Is_Established': is_established,
        'Branch_Popularity': branch_popularity,
        'cutoff_lag1Y_L1Y': inter_year_features['cutoff_lag1Y_L1Y'],
        'cutoff_lag2Y_L2Y': inter_year_features['cutoff_lag2Y_L2Y'],
        'cutoff_roll3Y_mean_L1Y': inter_year_features['cutoff_roll3Y_mean_L1Y'],
        'cutoff_roll3Y_std_L1Y': inter_year_features['cutoff_roll3Y_std_L1Y'],
        'n_years_hist_L1Y': inter_year_features['n_years_hist_L1Y'],
        'trend3Y_slope_L1Y': inter_year_features['trend3Y_slope_L1Y'],
        'is_low_history_L1Y': inter_year_features['is_low_history_L1Y'],
        'branch_prevY_mean_L1Y': inter_year_features['branch_prevY_mean_L1Y'],
        'college_prevY_allbranches_mean_L1Y': inter_year_features['college_prevY_allbranches_mean_L1Y'],
        'cutoff_lag1R_L1R': intra_year_features['cutoff_lag1R_L1R'],
        'cutoff_roll2R_mean_L1R': intra_year_features['cutoff_roll2R_mean_L1R'],
        'cutoff_roll2R_std_L1R': intra_year_features['cutoff_roll2R_std_L1R'],
        'n_rounds_hist_L1R': intra_year_features['n_rounds_hist_L1R'],
        'is_first_round_L1R': intra_year_features['is_first_round_L1R'],
        'College_Code_target_enc': college_code_target_enc,
        'College_Branch_target_enc': college_branch_target_enc
    }
    
    # Convert to DataFrame
    df = pd.DataFrame([features])
    
    # Replace NaN values with global mean
    df = df.fillna(global_mean)
    
    # Ensure all values are proper Python floats (not numpy types)
    for col in df.columns:
        df[col] = df[col].astype(float)
    
    logger.info(f"✅ Generated {len(features)} features successfully")
    
    return df


if __name__ == "__main__":
    sample_input = {
        'College_Code': 'E001',
        'College_Name': 'B.M.S. College of Engineering',
        'Branch': 'Computer Science and Engineering',
        'Category': 'GM',
        'Exam_Type': 'KCET',
        'Year': 2024,
        'Round': 1,
        'Quota_Seats': 60
    }
    
    print("\n" + "="*80)
    print("TESTING FEATURE GENERATOR")
    print("="*80)
    print(f"\nInput: {sample_input}")
    
    features_df = build_feature_vector(sample_input)
    print(f"\nGenerated Features ({features_df.shape[1]} columns):")
    print(features_df.T)  # Transpose for better readability
