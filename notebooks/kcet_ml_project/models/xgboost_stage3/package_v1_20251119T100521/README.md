KCET Stage3 XGBoost packaging
Package: package_v1_20251119T100521
Created: 20251119T100521

Contents:
- model booster(s), wrappers, per-branch blending map, branch historical means
- SHAP artifacts, diagnostics, and checksums

Usage:
- Copy package folder to your deployment host.
- Use inference_template.py as a starting point for loading model and running predictions.
- Validate predictions on your infra before serving.

Notes:
- This package includes an adaptive per-branch blend mapping calibrated on Val(2023). Keep that file with the package.
