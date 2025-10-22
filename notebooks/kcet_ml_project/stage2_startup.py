# STAGE 2 STARTUP SCRIPT: Load all data and configurations
# Run this cell first in your new Stage 2 notebook

import pandas as pd
import numpy as np
import joblib
import json
import pickle
from datetime import datetime

print("LOADING STAGE 2 DATA & CONFIGURATIONS")
print("="*80)

# Load datasets
try:
    df_raw = pd.read_pickle('kcet_ml_project/data/df_raw.pkl')
    print(f"Loaded df_raw: {df_raw.shape}")
except:
    df_raw = pd.read_csv('kcet_ml_project/data/df_raw.csv')
    print(f"Loaded df_raw from CSV: {df_raw.shape}")

try:
    df_optimized = pd.read_pickle('kcet_ml_project/data/df_optimized.pkl')
    print(f"Loaded df_optimized: {df_optimized.shape}")
except:
    df_optimized = pd.read_csv('kcet_ml_project/data/df_optimized.csv')
    print(f"Loaded df_optimized from CSV: {df_optimized.shape}")

# Load configurations
with open('kcet_ml_project/configs/column_definitions.json', 'r') as f:
    column_definitions = json.load(f)

with open('kcet_ml_project/configs/project_metadata.json', 'r') as f:
    project_metadata = json.load(f)

with open('kcet_ml_project/configs/stage2_plan.json', 'r') as f:
    stage2_plan = json.load(f)

# Extract column definitions
NUMERIC_COLUMNS = column_definitions['numeric_columns']
CATEGORICAL_COLUMNS = column_definitions['categorical_columns']
TARGET_COLUMN = column_definitions['target_column']
GROUP_COLUMNS = column_definitions['key_grouping_columns']

print(f"\nDATA SUMMARY:")
print(f"   Records: {len(df_optimized):,}")
print(f"   Features: {len(df_optimized.columns)}")
print(f"   Numeric cols: {len(NUMERIC_COLUMNS)}")
print(f"   Categorical cols: {len(CATEGORICAL_COLUMNS)}")

print(f"\nSTAGE 2 OBJECTIVES:")
print(f"   Target MAE: {stage2_plan['expected_improvements']['target_mae']:,}")
print(f"   Target R2: {stage2_plan['expected_improvements']['target_r2']}")
print(f"   New features: {stage2_plan['expected_improvements']['new_features_count']}")

print(f"\nSTAGE 2 READY TO BEGIN!")

# Try to load trained models (optional)
try:
    trained_models = {}
    import os
    model_files = os.listdir('kcet_ml_project/models/')
    for model_file in model_files:
        if model_file.endswith('.joblib'):
            model_name = model_file.replace('.joblib', '')
            trained_models[model_name] = joblib.load(f'kcet_ml_project/models/{model_file}')
            print(f"Loaded trained model: {model_name}")
    
    if trained_models:
        print(f"\nAvailable trained models: {list(trained_models.keys())}")
except:
    print("\nNo trained models found (optional)")
