"""Fetch the Danish AIS monthly ZIP for a given year/month into data/raw/."""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

import requests
from tqdm import tqdm

AIS_BASE_URL = "http://aisdata.ais.dk"
CHUNK_BYTES = 1 << 20
RETRY_ATTEMPTS = 3
RETRY_DELAY_S = 5
HEAD_TIMEOUT_S = 30
GET_TIMEOUT_S = 120


def file_url(year: int, month: int) -> str:
    """Build the AIS monthly ZIP URL for a given year and month."""
    return f"{AIS_BASE_URL}/{year}/aisdk-{year}-{month:02d}.zip"


def _remote_size(url: str) -> int:
    """Return the remote file's Content-Length in bytes via an HTTP HEAD request."""
    r = requests.head(url, allow_redirects=True, timeout=HEAD_TIMEOUT_S)
    r.raise_for_status()
    return int(r.headers["content-length"])


def _download_once(url: str, dest: Path) -> None:
    """Stream the URL to `dest` once, with size-based cache check and atomic rename."""
    expected = _remote_size(url)
    if dest.exists() and dest.stat().st_size == expected:
        print(f"cached   {dest.name} ({expected / 1e9:.2f} GB)")
        return

    tmp = dest.with_suffix(dest.suffix + ".part")
    with requests.get(url, stream=True, timeout=GET_TIMEOUT_S) as r:
        r.raise_for_status()
        with open(tmp, "wb") as f, tqdm(
            total=expected, unit="B", unit_scale=True, desc=dest.name
        ) as bar:
            for chunk in r.iter_content(CHUNK_BYTES):
                f.write(chunk)
                bar.update(len(chunk))

    actual = tmp.stat().st_size
    if actual != expected:
        tmp.unlink()
        raise RuntimeError(
            f"size mismatch for {dest.name}: got {actual}, expected {expected}"
        )
    tmp.replace(dest)


def download_month(year: int, month: int, dest_dir: Path) -> Path:
    """Download the monthly AIS ZIP for (year, month) into `dest_dir`, retrying on transient errors."""
    dest_dir.mkdir(parents=True, exist_ok=True)
    url = file_url(year, month)
    dest = dest_dir / Path(url).name

    last_err: Exception | None = None
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            _download_once(url, dest)
            return dest
        except (requests.RequestException, RuntimeError, OSError) as e:
            last_err = e
            print(f"attempt {attempt}/{RETRY_ATTEMPTS} failed: {e}")
            if attempt < RETRY_ATTEMPTS:
                time.sleep(RETRY_DELAY_S)
    raise RuntimeError(f"failed after {RETRY_ATTEMPTS} attempts: {last_err}")


def check_month(year: int, month: int) -> None:
    """HEAD-only sanity check that prints the remote file size without downloading."""
    url = file_url(year, month)
    size = _remote_size(url)
    print(f"{Path(url).name:30s} {size / 1e9:6.2f} GB  ({url})")


def main(argv: list[str] | None = None) -> int:
    """CLI entrypoint: download the configured month's ZIP, or HEAD-check it with --check."""
    p = argparse.ArgumentParser(description="Download Danish AIS monthly ZIP.")
    p.add_argument("--year", type=int, default=2021)
    p.add_argument("--month", type=int, default=12)
    p.add_argument("--dest", type=Path, default=Path("/app/data/raw"))
    p.add_argument("--check", action="store_true", help="HEAD-only, no download")
    args = p.parse_args(argv)

    if args.check:
        check_month(args.year, args.month)
    else:
        download_month(args.year, args.month, args.dest)
    return 0


if __name__ == "__main__":
    sys.exit(main())
