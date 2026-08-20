#!/usr/bin/env python3
"""Part 2.1: download a month of NYC TLC Yellow Taxi trip records.

Source: https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_<YYYY-MM>.parquet
- the same CloudFront-hosted Parquet files TLC's own trip-record-data
page links to. Downloads are idempotent (skipped if the target file
already exists, unless --force) and streamed to disk rather than loaded
into memory - these files run tens of megabytes each.

Scope, dataset choice, and why urllib (not requests) - see DEFENSE.md #26.
"""
import argparse
import sys
import urllib.error
import urllib.request
from pathlib import Path

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"


def download(year_month, output_dir, force=False):
    filename = f"yellow_tripdata_{year_month}.parquet"
    url = f"{BASE_URL}/{filename}"
    output_path = Path(output_dir) / filename

    if output_path.exists() and not force:
        print(f"already downloaded: {output_path} ({output_path.stat().st_size} bytes) - use --force to re-download")
        return output_path

    output_path.parent.mkdir(parents=True, exist_ok=True)
    print(f"downloading {url}")
    try:
        with urllib.request.urlopen(url, timeout=60) as response:
            if response.status != 200:
                raise SystemExit(f"FAIL: {url} returned HTTP {response.status}, not 200")
            total = int(response.headers.get("Content-Length", 0))
            tmp_path = output_path.with_suffix(".parquet.partial")
            written = 0
            with open(tmp_path, "wb") as f:
                while True:
                    chunk = response.read(1024 * 1024)
                    if not chunk:
                        break
                    f.write(chunk)
                    written += len(chunk)
            tmp_path.rename(output_path)
    except urllib.error.HTTPError as e:
        raise SystemExit(f"FAIL: {url} returned HTTP {e.code} ({e.reason}) - check --year-month is a published month")
    except urllib.error.URLError as e:
        raise SystemExit(f"FAIL: could not reach {url}: {e.reason}")

    if total and written != total:
        raise SystemExit(f"FAIL: downloaded {written} bytes, expected {total} (Content-Length) - incomplete download")

    print(f"downloaded {output_path} ({written} bytes)")
    return output_path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--year-month", default="2025-01", help="e.g. 2025-01 (default: 2025-01, confirmed published)")
    parser.add_argument("--output-dir", default="data/tlc")
    parser.add_argument("--force", action="store_true", help="re-download even if the file already exists")
    args = parser.parse_args()

    download(args.year_month, args.output_dir, force=args.force)


if __name__ == "__main__":
    main()
