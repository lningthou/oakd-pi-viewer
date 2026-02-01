"""Background pre-processor: watches S3 for new recordings and processes them ahead of time."""

import json
import logging
import time
from pathlib import Path

from . import s3, cache
from .mcap_reader import get_metadata
from .processing import process_rgb, process_depth, process_imu

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger(__name__)

POLL_INTERVAL = 60  # seconds


def _is_recording_folder(listing: dict) -> bool:
    """Check if a listing contains an MCAP file."""
    return any(f["name"].endswith(".mcap") and f["size"] > 0 for f in listing.get("files", []))


def _find_mcap_key(listing: dict) -> tuple[str, int] | None:
    """Return (key, size) of the first non-empty MCAP file."""
    for f in listing.get("files", []):
        if f["name"].endswith(".mcap") and f["size"] > 0:
            return f["key"], f["size"]
    return None


def _upload_results(recording_id: str):
    """Upload processed outputs back to S3 alongside the original recording."""
    import boto3
    from .config import settings

    client = boto3.client("s3", region_name=settings.aws_region)
    prefix = recording_id
    if settings.s3_prefix:
        prefix = f"{settings.s3_prefix}/{recording_id}".strip("/")

    for name, path in [
        ("rgb.mp4", cache.get_rgb_path(recording_id)),
        ("depth.mp4", cache.get_depth_path(recording_id)),
        ("imu.json", cache.get_imu_path(recording_id)),
    ]:
        if path.exists():
            key = f"{prefix}/{name}"
            log.info(f"Uploading {path} -> s3://{settings.s3_bucket}/{key}")
            client.upload_file(str(path), settings.s3_bucket, key)


def process_one(recording_id: str):
    """Download and process a single recording."""
    if cache.is_processed(recording_id):
        log.info(f"Already processed: {recording_id}")
        return

    cache.ensure_dir(recording_id)
    mcap_path = cache.get_mcap_path(recording_id)

    # Clean stale empty file
    if mcap_path.exists() and mcap_path.stat().st_size == 0:
        mcap_path.unlink()

    # Download MCAP
    if not mcap_path.exists():
        listing = s3.list_prefix(recording_id)
        result = _find_mcap_key(listing)
        if not result:
            log.warning(f"No valid MCAP in {recording_id}, skipping")
            return
        mcap_key, mcap_size = result
        log.info(f"Downloading {recording_id} ({mcap_size / 1e9:.1f} GB)")
        s3.download_file(mcap_key, mcap_path)

    # Process
    log.info(f"Processing {recording_id}")

    def prog(stage, progress, detail):
        if progress == 1.0 or int(progress * 100) % 25 == 0:
            log.info(f"  [{stage}] {progress:.0%} - {detail}")

    import concurrent.futures
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
        futures = [
            pool.submit(process_rgb, mcap_path, cache.get_rgb_path(recording_id), prog),
            pool.submit(process_depth, mcap_path, cache.get_depth_path(recording_id), prog),
            pool.submit(process_imu, mcap_path, cache.get_imu_path(recording_id), prog),
        ]
        for f in concurrent.futures.as_completed(futures):
            f.result()  # raise if failed

    # Upload results back to S3
    _upload_results(recording_id)
    log.info(f"Done: {recording_id}")


def run():
    """Poll S3 for unprocessed recordings and process them."""
    log.info("Pre-processor starting, polling for new recordings...")

    while True:
        try:
            listing = s3.list_prefix("")
            for folder in listing.get("folders", []):
                recording_id = folder["prefix"]

                # Check if already has processed outputs in S3
                rec_listing = s3.list_prefix(recording_id)
                has_rgb = any(f["name"] == "rgb.mp4" for f in rec_listing.get("files", []))
                has_mcap = any(
                    f["name"].endswith(".mcap") and f["size"] > 0
                    for f in rec_listing.get("files", [])
                )

                if has_rgb or not has_mcap:
                    continue

                log.info(f"New unprocessed recording: {recording_id}")
                try:
                    process_one(recording_id)
                except Exception:
                    log.exception(f"Failed to process {recording_id}")

        except Exception:
            log.exception("Poll cycle failed")

        log.info(f"Sleeping {POLL_INTERVAL}s before next poll")
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    run()
