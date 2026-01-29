#!/usr/bin/env python3
"""
Download URLs from a file (one per line), sanitize CRLF/^M, save them with rewritten names,
and produce a CSV mapping: old_url,new_name

Example rewrite:
  https://wiki.itldc.com/wp-content/uploads/2022/07/Screenshot-2022-07-19-at-11.58.50.png
-> ./wiki/legacy/2022-07-Screenshot-2022-07-19-at-11.58.50.png

Usage:
  python3 legacy_dl.py -i input.csv -o match.csv
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Optional, Tuple


def sanitize_line(line: str) -> str:
    # Handles normal CRLF, plus literal "^M" text at end of line if present
    s = line.strip()
    if s.endswith("^M"):
        s = s[:-2].rstrip()
    return s.strip()


def url_to_new_path(url: str, base_dir: Path) -> Path:
    """
    From a URL containing .../wp-content/uploads/YYYY/MM/filename.ext
    build: base_dir/wiki/legacy/YYYY-MM-filename.ext
    """
    parsed = urllib.parse.urlparse(url)
    path = parsed.path  # e.g. /wp-content/uploads/2022/07/file.png

    parts = [p for p in path.split("/") if p]
    # Find "wp-content/uploads"
    # Expect: wp-content, uploads, YYYY, MM, ...
    try:
        idx = parts.index("wp-content")
    except ValueError as e:
        raise ValueError(f"URL path doesn't contain 'wp-content': {path}") from e

    if idx + 1 >= len(parts) or parts[idx + 1] != "uploads":
        raise ValueError(f"URL path doesn't contain 'wp-content/uploads': {path}")

    if idx + 4 >= len(parts):
        raise ValueError(f"URL path too short for uploads/YYYY/MM/file: {path}")

    year = parts[idx + 2]
    month = parts[idx + 3]
    filename = parts[-1]  # keep original filename (no query; already in path)

    if not (year.isdigit() and len(year) == 4):
        raise ValueError(f"Invalid year '{year}' in path: {path}")
    if not (month.isdigit() and len(month) == 2):
        raise ValueError(f"Invalid month '{month}' in path: {path}")

    out_dir = base_dir / "wiki" / "legacy"
    out_dir.mkdir(parents=True, exist_ok=True)

    new_name = f"{year}-{month}-{filename}"
    return out_dir / new_name


def unique_path(path: Path) -> Path:
    """If file exists, add -2, -3, ... before suffix."""
    if not path.exists():
        return path
    stem = path.stem
    suffix = path.suffix
    parent = path.parent
    n = 2
    while True:
        candidate = parent / f"{stem}-{n}{suffix}"
        if not candidate.exists():
            return candidate
        n += 1


def download(url: str, dest: Path, timeout: int = 30, retries: int = 3) -> None:
    # Basic polite headers (some hosts dislike default urllib UA)
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "legacy-downloader/1.0 (+python urllib)"},
        method="GET",
    )

    last_err: Optional[BaseException] = None
    for attempt in range(1, retries + 1):
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                if getattr(resp, "status", 200) >= 400:
                    raise urllib.error.HTTPError(
                        url, resp.status, f"HTTP {resp.status}", resp.headers, None
                    )
                dest.parent.mkdir(parents=True, exist_ok=True)
                with open(dest, "wb") as f:
                    while True:
                        chunk = resp.read(1024 * 256)
                        if not chunk:
                            break
                        f.write(chunk)
            return
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError) as e:
            last_err = e
            if attempt < retries:
                time.sleep(1.0 * attempt)
            else:
                raise

    if last_err:
        raise last_err


def process_file(input_path: Path, output_csv: Path, base_dir: Path, timeout: int, retries: int) -> Tuple[int, int]:
    ok = 0
    failed = 0

    with open(input_path, "r", encoding="utf-8", errors="replace") as fin, \
         open(output_csv, "w", encoding="utf-8", newline="") as fout:
        writer = csv.writer(fout)
        writer.writerow(["old_url", "new_name"])

        for lineno, raw in enumerate(fin, start=1):
            url = sanitize_line(raw)
            if not url:
                continue

            # Allow lines like " 'https://...png^M' " or "\"https://...\""
            url = url.strip().strip('"').strip("'").strip()

            try:
                new_path = url_to_new_path(url, base_dir)
                new_path = unique_path(new_path)
                download(url, new_path, timeout=timeout, retries=retries)

                # Write mapping. new_name is relative path from base_dir for portability.
                rel = new_path.relative_to(base_dir)
                writer.writerow([url, str(rel)])
                ok += 1
                print(f"[OK] {url} -> {rel}", flush=True)
            except Exception as e:
                failed += 1
                # Still log mapping attempt? Keep CSV clean; print error to stderr.
                print(f"[FAIL:{lineno}] {url} - {e}", file=sys.stderr, flush=True)

    return ok, failed


def main() -> int:
    ap = argparse.ArgumentParser(description="Download wiki legacy uploads and rewrite filenames.")
    ap.add_argument("-i", "--input", required=True, help="Input file with URLs (one per line).")
    ap.add_argument("-o", "--output", required=True, help="Output CSV with old_url,new_name mapping.")
    ap.add_argument("--base-dir", default=".", help="Base directory (default: current directory).")
    ap.add_argument("--timeout", type=int, default=30, help="HTTP timeout seconds (default: 30).")
    ap.add_argument("--retries", type=int, default=3, help="Download retries (default: 3).")
    args = ap.parse_args()

    base_dir = Path(args.base_dir).resolve()
    input_path = Path(args.input).expanduser()
    output_csv = Path(args.output).expanduser()

    if not input_path.exists():
        print(f"Input file not found: {input_path}", file=sys.stderr)
        return 2

    ok, failed = process_file(input_path, output_csv, base_dir, args.timeout, args.retries)

    print(f"Done. OK={ok} FAIL={failed}. Mapping saved to: {output_csv}")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())

