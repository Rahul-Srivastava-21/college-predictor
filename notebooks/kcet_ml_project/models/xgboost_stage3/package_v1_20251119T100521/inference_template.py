"""
Inference template for packaged XGBoost model (Stage 3).
Place this alongside the package files and call predict_df(df) with a pandas DataFrame.
It expects:
 - xgb_wrapper_stage3_final_with_adaptive_fallback.joblib (wrapper)
 - xgb_booster_stage3_final_bestiter713.json (booster JSON)
 - per_branch_blend_stage3.json
 - branch_historical_mean.csv
"""

import json
from pathlib import Path
import joblib
import xgboost as xgb
import pandas as pd
import numpy as np

PKG_DIR = Path(__file__).resolve().parent

# Load wrapper and booster
wrapper = joblib.load(PKG_DIR / "xgb_wrapper_stage3_final_with_adaptive_fallback.joblib")
booster_path = wrapper.get("booster_path")
features = wrapper.get("features")
bst = xgb.Booster()
bst.load_model(str(PKG_DIR / Path(booster_path).name))

# Load per-branch blend mapping and branch hist means
blend_map = {}
per_branch_file = PKG_DIR / "per_branch_blend_stage3.json"
if per_branch_file.exists():
    with open(per_branch_file) as f:
        blend_map = json.load(f)
branch_hist = {}
hist_csv = PKG_DIR / "branch_historical_mean.csv"
if hist_csv.exists():
    bh = pd.read_csv(hist_csv)
    branch_hist = dict(zip(bh["College_Branch_target_enc"].astype(float), bh["hist_mean"]))

def predict_df(df, blend_default=1.0, min_hist_count=5):
    """
    df: pandas DataFrame including the same features used for training.
    Returns: numpy array of final predictions (after adaptive fallback)
    """
    # Ensure features present
    X = df.copy()
    missing = [c for c in features if c not in X.columns]
    if missing:
        raise ValueError(f"Missing features in input DataFrame: {missing}")
    dmat = xgb.DMatrix(X[features])
    preds = bst.predict(dmat)
    # compute hist mean per-row (from packaged branch_hist fallback)
    branch_vals = X["College_Branch_target_enc"].astype(float).values
    hist_vals = np.array([branch_hist.get(float(b), np.nan) for b in branch_vals])
    # apply per-branch blends where available; else leave model pred
    final = preds.copy()
    for i, b in enumerate(branch_vals):
        b_key = str(float(b))
        blend = 1.0
        if b_key in blend_map:
            blend = float(blend_map[b_key])
        elif float(b) in blend_map:
            blend = float(blend_map[float(b)])
        if blend < 1.0:
            hm = hist_vals[i]
            if pd.isna(hm):
                # fallback to Historical_Mean_Primary if present
                if "Historical_Mean_Primary" in X.columns:
                    hm = float(X.iloc[i]["Historical_Mean_Primary"])
                else:
                    hm = float(np.nanmean(list(branch_hist.values())))  # last resort
            final[i] = blend * preds[i] + (1.0 - blend) * hm
    return np.asarray(final)

# Example usage:
# import pandas as pd
# df = pd.read_csv("some_input.csv")
# preds = predict_df(df)
# print(preds[:10])
