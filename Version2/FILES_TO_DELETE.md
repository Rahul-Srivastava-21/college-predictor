# Version2 - Files Safe to Delete

## Analysis Date: December 2, 2025

This document identifies files that can be safely deleted from the Version2 folder to reduce storage and improve project organization.

---

## ✅ **CRITICAL FILES - DO NOT DELETE**

### Models (Required for Predictions)
```
Version2/models/
├── lightgbm_model.txt          ✅ KEEP - Primary model used in production
├── xgboost_model.json          ✅ KEEP - Alternative model
└── random_forest_baseline.pkl  ✅ KEEP - Baseline model
```

### Data (Required by predict_cutoff.py)
```
Version2/data/stage2_clean/
├── train_features_v4.csv       ✅ KEEP - Used by predict_cutoff.py
├── val_features_v4.csv         ✅ KEEP - Used by predict_cutoff.py
└── test_features_v4.csv        ✅ KEEP - Used by predict_cutoff.py
```

### Production Code
```
Version2/notebooks/
└── predict_cutoff.py           ✅ KEEP - Production prediction module
```

---

## 🗑️ **FILES SAFE TO DELETE**

### 1. **Duplicate/Intermediate Data Files** (Can Delete: ~500MB+)

#### data/processed_data/ - OLD VERSIONS
```bash
❌ DELETE:
├── KCET_split_cleaned.csv                    # Superseded by stage2_clean
├── KCET_split_cleaned_filtered.csv           # Superseded by stage2_clean
├── KCET_split_engineering_only.csv           # Intermediate file
├── KCET_stage2_dataset.csv                   # Superseded by v4 files
├── KCET_stage2_final.csv                     # Superseded by v4 files
├── KCET_stage2_final_with_encoders.csv       # Superseded by v4 files
├── KCET_stage2_Tier1.csv                     # Tier-specific (not needed)
├── KCET_stage2_tier1_features.csv            # Tier-specific (not needed)
├── KCET_stage2_Tier2.csv                     # Tier-specific (not needed)
├── KCET_stage2_tier2_features.csv            # Tier-specific (not needed)
├── KCET_stage2_Tier3.csv                     # Tier-specific (not needed)
├── KCET_stage2_tier3_features.csv            # Tier-specific (not needed)
└── Stage1Files/                              # Old stage files (entire folder)

✅ KEEP:
└── college_mapping_2024.json                 # May be useful for reference
```

#### data/stage2_clean/ - OLD VERSIONS
```bash
❌ DELETE (Old versions - v1, v2, v3):
├── train_features.csv           # Use v4 instead
├── train_features_v2.csv        # Use v4 instead
├── train_features_v3.csv        # Use v4 instead
├── val_features.csv             # Use v4 instead
├── val_features_v2.csv          # Use v4 instead
├── val_features_v3.csv          # Use v4 instead
├── test_features.csv            # Use v4 instead
├── test_features_v2.csv         # Use v4 instead
├── test_features_v3.csv         # Use v4 instead
├── feature_metadata.json        # Use v4 instead
├── feature_metadata_v2.json     # Use v4 instead
├── feature_metadata_v3.json     # Use v4 instead

✅ KEEP (v4 files + identifiers):
├── train_features_v4.csv        # Current production version
├── val_features_v4.csv          # Current production version
├── test_features_v4.csv         # Current production version
├── feature_metadata_v4.json     # Current metadata
├── train_identifiers.csv        # Useful for tracking
├── val_identifiers.csv          # Useful for tracking
├── test_identifiers.csv         # Useful for tracking
├── split_metadata.json          # Useful for tracking
├── leakage_test_results.json    # Important validation
└── cell1_validation_report.json # Important validation
```

#### data/stage2_outputs/ - INTERMEDIATE FILES
```bash
❌ DELETE (Everything - intermediate processing artifacts):
├── branch_counts_preview.csv
├── branch_year_medians_prior.csv
├── branch_year_medians_prior.pkl
├── college_medians.pkl
├── college_year_medians_prior.csv
├── college_year_medians_prior.pkl
├── df_clean.pkl
├── df_stage2_sorted.csv
├── enc_Branch_temporal.json
├── enc_Category_temporal.json
├── enc_College_Code_temporal.json
├── KCET_stage2_final.csv
├── KCET_stage2_final_with_encoders.csv
├── KCET_stage2_final_with_encoders_v2.csv
├── KCET_stage2_final_with_encoders_v3.csv
├── KCET_stage2_final_with_encoders_v4.csv
├── KCET_stage2_Tier1.csv
├── KCET_stage2_tier1_features.csv
├── KCET_stage2_Tier2.csv
├── KCET_stage2_tier2_features.csv
├── KCET_stage2_Tier3.csv
├── KCET_stage2_tier3_features.csv
├── manifest.json
├── split_test_raw.csv
├── split_train_raw.csv
├── split_val_raw.csv
├── stage1_df.pkl
├── stage2_encoders_manifest.json
├── stage2_feature_list.json
├── stage2_feature_list_v2.json
├── stage2_fe_diag.json
├── stage2_fe_step1.csv
├── stage2_fe_step2.csv
├── stage2_final_features_v4.pkl
├── stage2_final_manifest.json
├── stage2_final_manifest_checked.json
├── stage2_final_manifest_v2.json
├── stage2_final_manifest_v3.json
├── stage2_final_manifest_v4.json
├── stage2_globals.json
├── stage2_input_validation.json
├── stage2_numeric_scaler.joblib
├── stage2_scaler_v4.pkl
├── temporal_encoders.pkl
├── temporal_encoders_final_v4.pkl
├── temporal_encoders_v2.pkl
├── temporal_encoders_v3.pkl
├── temporal_encoders_v4.pkl
├── Tier1/ (entire folder)
├── Tier2/ (entire folder)
├── Tier3/ (entire folder)
├── tier_enc_medians.json
├── tier_map.json
├── unstable_branches.pkl
├── unstable_branches_list.json
├── unstable_branches_list_v2.json
├── unstable_branches_list_v3.json
├── unstable_branches_list_v4.json
└── year_max_ranks.json

**Entire folder can be deleted** - These are intermediate processing files
not needed for production predictions.
```

### 2. **Duplicate/Exploration Notebooks** (Can Delete)

```bash
Version2/notebooks/

❌ DELETE (Exploration/Development notebooks):
├── Stage1EDA-KCET.ipynb                 # EDA - keep if you want reference
├── Stage2FE-KCET.ipynb                  # Feature engineering - old version
├── Stage2FE-New-KCET.ipynb              # Feature engineering - new version
├── StageF2E-KCET-DUP.ipynb              # Duplicate file
├── Stage3-ModelTraining-KCET.ipynb      # Model training - keep for reference
├── data_correction.ipynb                # Data correction notebook
├── removal_2024_colleges.ipynb          # Data cleanup notebook
└── __pycache__/                         # Python cache (auto-regenerated)

✅ KEEP (Production & Documentation):
├── predict_cutoff.py                    # Production module
├── PRODUCTION_READY_UPDATES.md          # Important documentation
├── README_PREDICTIONS.md                # Important documentation
├── E001_VALIDATION_REPORT.md            # Validation results (update needed)
├── ROOT_CAUSE_ANALYSIS.md               # Analysis (update needed)
├── e001_ai_corrected_validation.csv     # Validation data
├── e001_ce_validation.csv               # Validation data
├── e001_cs_validation.csv               # Validation data
├── e001_ec_validation.csv               # Validation data
├── e001_ee_validation.csv               # Validation data
├── e001_ie_validation.csv               # Validation data
└── e001_me_validation.csv               # Validation data
```

### 3. **Optional: Model Subfolder** (Can Delete if not needed)

```bash
Version2/models/tier1/
❌ DELETE - Tier-specific models not used in production
```

---

## 📊 **ESTIMATED STORAGE SAVINGS**

| Category | Estimated Size | Files Count |
|----------|---------------|-------------|
| Old data versions (v1-v3) | ~300-500 MB | 9 files |
| processed_data/ old files | ~200-300 MB | 13+ files |
| stage2_outputs/ folder | ~500-800 MB | 60+ files |
| Duplicate notebooks | ~50-100 MB | 5 notebooks |
| __pycache__ | ~1-5 MB | Multiple |
| **TOTAL SAVINGS** | **~1-2 GB** | **85+ files** |

---

## 🔧 **SAFE DELETION COMMANDS**

### Option 1: Delete Safely (Move to Backup First)

```bash
# Create backup folder
mkdir -p ../Version2_backup_$(date +%Y%m%d)

# Backup before deletion
cp -r Version2/data/stage2_outputs ../Version2_backup_$(date +%Y%m%d)/
cp -r Version2/data/processed_data/Stage1Files ../Version2_backup_$(date +%Y%m%d)/

# Then delete
rm -rf Version2/data/stage2_outputs
rm -rf Version2/data/processed_data/Stage1Files
```

### Option 2: Delete Specific Files

```bash
cd Version2/data/stage2_clean

# Delete old versions (v1, v2, v3)
rm -f train_features.csv train_features_v2.csv train_features_v3.csv
rm -f val_features.csv val_features_v2.csv val_features_v3.csv
rm -f test_features.csv test_features_v2.csv test_features_v3.csv
rm -f feature_metadata.json feature_metadata_v2.json feature_metadata_v3.json
```

### Option 3: Delete Entire Folders

```bash
cd Version2

# Delete entire stage2_outputs (safest to delete)
rm -rf data/stage2_outputs

# Delete Stage1Files
rm -rf data/processed_data/Stage1Files

# Delete tier1 models folder
rm -rf models/tier1

# Delete Python cache
rm -rf notebooks/__pycache__
```

---

## ⚠️ **IMPORTANT NOTES**

1. **Before deleting anything**: Make sure you have a backup or Git commit
2. **Test predictions** after deletion to ensure everything still works:
   ```python
   from predict_cutoff import predict_college_cutoff
   result = predict_college_cutoff("E001", "CS Computers", "GM", 1, 2025)
   print(result['predicted_cutoff'])
   ```

3. **Git tracking**: If using Git, add deleted folders to `.gitignore`:
   ```
   Version2/data/stage2_outputs/
   Version2/data/processed_data/Stage1Files/
   Version2/notebooks/__pycache__/
   ```

---

## 📋 **FINAL MINIMAL STRUCTURE (After Cleanup)**

```
Version2/
├── data/
│   ├── KCET_split.csv                      # Original raw data
│   ├── processed_data/
│   │   └── college_mapping_2024.json       # Useful mapping
│   └── stage2_clean/
│       ├── train_features_v4.csv           # Required
│       ├── val_features_v4.csv             # Required
│       ├── test_features_v4.csv            # Required
│       ├── feature_metadata_v4.json
│       ├── *_identifiers.csv (3 files)
│       ├── split_metadata.json
│       ├── leakage_test_results.json
│       └── cell1_validation_report.json
├── models/
│   ├── lightgbm_model.txt                  # Required
│   ├── xgboost_model.json                  # Required
│   └── random_forest_baseline.pkl          # Required
├── notebooks/
│   ├── predict_cutoff.py                   # Required - Production code
│   ├── PRODUCTION_READY_UPDATES.md
│   ├── README_PREDICTIONS.md
│   ├── e001_*_validation.csv (7 files)
│   └── Stage3-ModelTraining-KCET.ipynb     # Optional - for reference
└── results/
    └── *.json (4 files)                    # Model comparison results
```

**Estimated final size**: ~200-300 MB (down from ~1.5-2.5 GB)

---

## ✅ **RECOMMENDATION**

**Priority 1 - Safe to delete immediately**:
- ✅ Delete entire `Version2/data/stage2_outputs/` folder (~500-800 MB)
- ✅ Delete `Version2/notebooks/__pycache__/`
- ✅ Delete old data versions (v1, v2, v3) from `stage2_clean/`

**Priority 2 - After backup**:
- ✅ Delete `Version2/data/processed_data/Stage1Files/`
- ✅ Delete tier-specific files from `processed_data/`
- ✅ Delete duplicate notebooks (keep only Stage3 for reference)

**Priority 3 - Keep for now** (but not needed for production):
- ⚠️ Notebooks (Stage1, Stage2) - useful for understanding the process
- ⚠️ Validation reports - useful for documentation

---

**Need help with deletion? Let me know and I can create the exact commands for your situation!**
