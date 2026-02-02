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


def _find_mcap_key(s3_files: dict) -> tuple[str, int] | None:
    """Return (key, size) of the first non-empty MCAP file from an s3_files dict."""
    for name, f in s3_files.items():
        if name.endswith(".mcap") and f["size"] > 0:
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


def process_one(recording_id: str, s3_files: dict[str, dict] | None = None):
    """Download and process a single recording. Only processes missing outputs."""
    cache.ensure_dir(recording_id)

    # Determine what's already done (in S3 or locally)
    if s3_files is None:
        listing = s3.list_prefix(recording_id)
        s3_files = {f["name"]: f for f in listing.get("files", [])}

    has_rgb_s3 = "rgb.mp4" in s3_files
    has_depth_s3 = "depth.mp4" in s3_files
    has_imu_s3 = "imu.json" in s3_files
    has_rgb_local = cache.get_rgb_path(recording_id).exists()
    has_depth_local = cache.get_depth_path(recording_id).exists()
    has_imu_local = cache.get_imu_path(recording_id).exists()

    need_rgb = not has_rgb_s3 and not has_rgb_local
    need_depth = not has_depth_s3 and not has_depth_local
    need_imu = not has_imu_s3 and not has_imu_local

    if not need_rgb and not need_depth and not need_imu:
        log.info(f"All outputs exist for {recording_id}, skipping")
        return

    log.info(f"Need to process {recording_id}: rgb={need_rgb} depth={need_depth} imu={need_imu}")

    mcap_path = cache.get_mcap_path(recording_id)

    # Clean stale empty file
    if mcap_path.exists() and mcap_path.stat().st_size == 0:
        mcap_path.unlink()

    # Download MCAP
    if not mcap_path.exists():
        result = _find_mcap_key(s3_files)
        if not result:
            log.warning(f"No valid MCAP in {recording_id}, skipping")
            return
        mcap_key, mcap_size = result
        log.info(f"Downloading {recording_id} ({mcap_size / 1e9:.1f} GB)")
        s3.download_file(mcap_key, mcap_path)

    # Process only what's needed
    def prog(stage, progress, detail):
        if progress == 1.0 or int(progress * 100) % 25 == 0:
            log.info(f"  [{stage}] {progress:.0%} - {detail}")

    import concurrent.futures
    futures = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
        if need_rgb:
            futures.append(pool.submit(process_rgb, mcap_path, cache.get_rgb_path(recording_id), prog))
        if need_depth:
            futures.append(pool.submit(process_depth, mcap_path, cache.get_depth_path(recording_id), prog))
        if need_imu:
            futures.append(pool.submit(process_imu, mcap_path, cache.get_imu_path(recording_id), prog))
        for f in concurrent.futures.as_completed(futures):
            f.result()  # raise if failed

    # Upload results back to S3
    _upload_results(recording_id)

    # Clean up MCAP to save disk space
    if mcap_path.exists():
        log.info(f"Cleaning up MCAP: {mcap_path} ({mcap_path.stat().st_size / 1e9:.1f} GB)")
        mcap_path.unlink()

    log.info(f"Done: {recording_id}")


def run():
    """Poll S3 for unprocessed recordings and process them."""
    log.info("Pre-processor starting, polling for new recordings...")
    failed: set[str] = set()  # Skip recordings that have permanently failed

    while True:
        try:
            listing = s3.list_prefix("")
            for folder in listing.get("folders", []):
                recording_id = folder["prefix"]

                if recording_id in failed:
                    continue

                # Check what outputs exist in S3
                rec_listing = s3.list_prefix(recording_id)
                s3_files = {f["name"]: f for f in rec_listing.get("files", [])}
                has_rgb = "rgb.mp4" in s3_files
                has_depth = "depth.mp4" in s3_files
                has_imu = "imu.json" in s3_files
                has_mcap = any(
                    k.endswith(".mcap") and s3_files[k]["size"] > 0
                    for k in s3_files
                )

                if not has_mcap:
                    continue
                if has_rgb and has_depth and has_imu:
                    continue

                log.info(f"Recording needs processing: {recording_id} (rgb={has_rgb} depth={has_depth} imu={has_imu})")
                try:
                    process_one(recording_id, s3_files=s3_files)
                except Exception:
                    log.exception(f"Failed to process {recording_id}, skipping permanently")
                    failed.add(recording_id)

        except Exception:
            log.exception("Poll cycle failed")

        log.info(f"Sleeping {POLL_INTERVAL}s before next poll")
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    run()
