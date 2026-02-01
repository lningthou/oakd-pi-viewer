"""Disk cache for processed recording outputs."""

import logging
from pathlib import Path

from .config import settings

log = logging.getLogger(__name__)


def _recording_dir(recording_id: str) -> Path:
    """Get cache directory for a recording."""
    return settings.cache_dir / recording_id


def is_processed(recording_id: str) -> bool:
    """Check if a recording has all processed outputs cached."""
    d = _recording_dir(recording_id)
    return (
        (d / "rgb.mp4").exists()
        and (d / "depth.mp4").exists()
        and (d / "imu.json").exists()
    )


def get_mcap_path(recording_id: str) -> Path:
    """Path where the raw MCAP is cached."""
    return _recording_dir(recording_id) / "recording.mcap"


def get_rgb_path(recording_id: str) -> Path:
    return _recording_dir(recording_id) / "rgb.mp4"


def get_depth_path(recording_id: str) -> Path:
    return _recording_dir(recording_id) / "depth.mp4"


def get_imu_path(recording_id: str) -> Path:
    return _recording_dir(recording_id) / "imu.json"


def ensure_dir(recording_id: str) -> Path:
    """Create and return the cache directory for a recording."""
    d = _recording_dir(recording_id)
    d.mkdir(parents=True, exist_ok=True)
    return d
