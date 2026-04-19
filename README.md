# bulk-fetch-engine

Distributed HTTP fetcher using GitHub Actions runners. Each runner provides a fresh source IP, so per-IP rate limits don't compound.

## How it works

- `data/targets.csv` — list of records to fetch (id, type, etc.)
- `.github/workflows/fetch.yml` — matrix workflow, N parallel jobs, each takes a slice
- `scripts/fetch_batch.py` — fetches one slice, retries with backoff, saves JSON
- `results/` — output JSON files per batch
- `scripts/merge.py` — combines all batch outputs into a single CSV

## Usage

```bash
# 1. Update data/targets.csv with your input list
# 2. Trigger the workflow:
gh workflow run fetch.yml -f batch_size=200 -f parallel_jobs=20
# 3. After completion:
gh run download
python scripts/merge.py
```

## Tuning

- `batch_size`: records per job (default 200, ~5 min runtime)
- `parallel_jobs`: matrix dimension (default 20, max 256 for free tier)
- `delay_ms`: per-request throttle (default 1500)
