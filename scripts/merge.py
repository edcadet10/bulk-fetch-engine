#!/usr/bin/env python3
"""Merge all results/batch_*.json into a single CSV."""
import csv
import json
from pathlib import Path

RESULTS_DIR = Path('results')
OUT_CSV = Path('merged_results.csv')

all_rows = []
for f in sorted(RESULTS_DIR.glob('batch_*.json')):
    with f.open('r', encoding='utf-8') as fh:
        all_rows.extend(json.load(fh))

if not all_rows:
    print('No results found')
    raise SystemExit(0)

# Collect all unique keys
keys = set()
for r in all_rows:
    keys.update(r.keys())
fieldnames = sorted(keys)

with OUT_CSV.open('w', encoding='utf-8', newline='') as f:
    w = csv.DictWriter(f, fieldnames=fieldnames)
    w.writeheader()
    for r in all_rows:
        w.writerow({k: r.get(k, '') for k in fieldnames})

print(f'Merged {len(all_rows)} records into {OUT_CSV}')
