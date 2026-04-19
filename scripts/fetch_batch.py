#!/usr/bin/env python3
"""
Fetch a single slice of detail pages.
Reads contractor_ids.csv, picks the slice based on JOB_INDEX/TOTAL_JOBS env vars,
fetches each one, parses, and saves results to results/batch_<index>.json.
"""
import os
import sys
import csv
import json
import time
import re
from pathlib import Path
import requests
from bs4 import BeautifulSoup

INPUT_CSV = Path('data/targets.csv')
RESULTS_DIR = Path('results')
RESULTS_DIR.mkdir(exist_ok=True)

JOB_INDEX = int(os.environ.get('JOB_INDEX', '0'))
TOTAL_JOBS = int(os.environ.get('TOTAL_JOBS', '1'))
BATCH_SIZE = int(os.environ.get('BATCH_SIZE', '200'))
DELAY_MS = int(os.environ.get('DELAY_MS', '1500'))
MAX_RETRIES = 2

DETAIL_URL = 'https://search.msboc.us/Detail.cfm?ContractorID={cid}&ContractorType={ct}&varDataSource={ds}'
SCRAPERAPI_URL = 'https://api.scraperapi.com/'
SCRAPERAPI_KEY = os.environ.get('SCRAPERAPI_KEY', '')

import urllib.parse

session = requests.Session()


def via_scraperapi(target_url: str) -> str:
    """Wrap target URL through ScraperAPI proxy."""
    if not SCRAPERAPI_KEY:
        return target_url
    return f'{SCRAPERAPI_URL}?api_key={SCRAPERAPI_KEY}&url={urllib.parse.quote(target_url)}'


def prime_sessions():
    """Not needed when using ScraperAPI (each request gets fresh IP+cookies)."""
    if SCRAPERAPI_KEY:
        print('  using ScraperAPI — skipping session prime')
        return


def parse_detail(html: str, contractor_id: str, contractor_type: str) -> dict:
    soup = BeautifulSoup(html, 'html.parser')
    text = re.sub(r'\s+', ' ', soup.get_text(separator=' ', strip=True))

    def field(label, stoppers=('Address', 'Phone', 'Fax', 'Expiration', 'Minority',
                                'First Issue', 'Status', 'Class(es)', 'Officers')):
        stop_pattern = '|'.join(re.escape(s) for s in stoppers)
        pat = re.compile(rf'{re.escape(label)}\s+(.+?)(?=\s+(?:{stop_pattern})\s|$)', re.IGNORECASE)
        m = pat.search(text)
        return m.group(1).strip() if m else ''

    out = {
        'contractor_id': contractor_id,
        'contractor_type': contractor_type,
        'address': field('Address'),
        'phone': field('Phone'),
        'fax': field('Fax'),
        'expiration_date': field('Expiration'),
        'minority': field('Minority'),
        'first_issue': field('First Issue'),
        'detail_status': field('Status'),
    }
    county_match = re.search(r'(\w+)\s+County', text)
    out['county'] = county_match.group(1) if county_match else ''

    # Classifications + qualifying party
    classes = []
    in_class_section = False
    for tr in soup.find_all('tr'):
        tr_text = tr.get_text(strip=True)
        if 'Classification' in tr_text and 'Qualifying' in tr_text:
            in_class_section = True
            continue
        if in_class_section:
            if 'Officers' in tr_text:
                in_class_section = False
                continue
            cells = [td.get_text(strip=True) for td in tr.find_all('td') if td.get_text(strip=True)]
            if len(cells) >= 2 and re.match(r'^[A-Z][A-Z &(),./-]+$', cells[0]):
                classes.append({'classification': cells[0], 'qualifying_name': cells[1]})

    out['classifications'] = '; '.join(c['classification'] for c in classes)
    out['qualifying_names'] = '; '.join(c['qualifying_name'] for c in classes)
    out['classifications_json'] = json.dumps(classes)

    officers_idx = text.find('Officers')
    out['officers'] = text[officers_idx + 8:officers_idx + 300].strip() if officers_idx >= 0 else ''

    return out


def fetch_one(contractor_id: str, contractor_type: str) -> dict | None:
    ds = 'BOCRes' if contractor_type.lower() == 'residential' else 'BOC'
    target = DETAIL_URL.format(cid=contractor_id, ct=contractor_type, ds=ds)
    url = via_scraperapi(target)
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = session.get(url, timeout=60)
            if r.status_code == 403:
                if attempt < MAX_RETRIES:
                    time.sleep(5)
                    continue
                return {'contractor_id': contractor_id, 'contractor_type': contractor_type, 'error': 'HTTP 403'}
            r.raise_for_status()
            if len(r.text) < 500:
                raise ValueError('Response too short')
            return parse_detail(r.text, contractor_id, contractor_type)
        except Exception as e:
            if attempt < MAX_RETRIES:
                time.sleep(3)
            else:
                return {'contractor_id': contractor_id, 'contractor_type': contractor_type, 'error': str(e)}


def main():
    if not INPUT_CSV.exists():
        print(f'ERROR: {INPUT_CSV} not found', file=sys.stderr)
        sys.exit(1)

    # Load all targets
    with INPUT_CSV.open('r', encoding='utf-8', newline='') as f:
        all_rows = list(csv.DictReader(f))

    # Compute this job's slice using stride pattern
    # Job 0 takes records 0, TOTAL_JOBS, 2*TOTAL_JOBS, ...
    my_slice = [r for i, r in enumerate(all_rows) if i % TOTAL_JOBS == JOB_INDEX][:BATCH_SIZE]
    print(f'Job {JOB_INDEX}/{TOTAL_JOBS}: processing {len(my_slice)} records (BATCH_SIZE={BATCH_SIZE})')

    if not my_slice:
        print('Nothing to do for this job')
        return

    prime_sessions()

    results = []
    failed_count = 0
    start = time.time()

    for i, row in enumerate(my_slice):
        cid = row['contractor_id']
        ct = row.get('contractor_type', 'Commercial')
        result = fetch_one(cid, ct)
        if result and 'error' not in result:
            result['license_number'] = row.get('license_number', '')
            result['business_name'] = row.get('business_name', '')
            result['status_bucket'] = row.get('status_bucket', '')
            results.append(result)
        else:
            # Just record the error and keep going — ScraperAPI will rotate IP next request
            failed_count += 1
            results.append(result or {'contractor_id': cid, 'error': 'unknown'})

        if (i + 1) % 25 == 0:
            elapsed = time.time() - start
            print(f'  {i+1}/{len(my_slice)} | rate {(i+1)/elapsed:.1f}/s | failed {failed_count}')

        time.sleep(DELAY_MS / 1000)

    out_path = RESULTS_DIR / f'batch_{JOB_INDEX:03d}.json'
    with out_path.open('w', encoding='utf-8') as f:
        json.dump(results, f, indent=2)
    print(f'Saved {len(results)} results to {out_path} ({failed_count} errors)')


if __name__ == '__main__':
    main()
