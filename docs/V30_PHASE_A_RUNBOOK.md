# V30 Phase-A Runbook

Version: v1.1.0
Updated: 2026-02-24

## Goal
Generate V30 daily feature table from DB market features table.

## Command
```bash
python scripts/run_v30_build_features.py \
  --input-table market_features_daily \
  --breadth-csv ../UTRBE/output/full_2006_2026.csv \
  --breadth-value-col breadth \
  --output-csv output/v30_features_daily.csv \
  --table-name v30_features_daily
```

## Outputs
- `output/v30_features_daily.csv`
- `output/v30_features_build_meta.json`
- DB table `v30_features_daily`

## Notes
- This is Phase-A bootstrap output, not final structural/shock model inference.
- Feature table is sorted newest-to-oldest for handoff readability.
- Step3 uses external breadth source (`full_2006_2026.csv`) as real breadth input (not in-table proxy).
