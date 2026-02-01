"""Configuration via environment variables."""

import os
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class Settings:
    s3_bucket: str = field(default_factory=lambda: os.environ.get("OAKD_S3_BUCKET", ""))
    s3_prefix: str = field(default_factory=lambda: os.environ.get("OAKD_S3_PREFIX", ""))
    aws_region: str = field(default_factory=lambda: os.environ.get("AWS_DEFAULT_REGION", "us-east-1"))
    cache_dir: Path = field(
        default_factory=lambda: Path(os.environ.get("OAKD_CACHE_DIR", "/tmp/oakd-viewer-cache"))
    )
    host: str = field(default_factory=lambda: os.environ.get("OAKD_HOST", "0.0.0.0"))
    port: int = field(default_factory=lambda: int(os.environ.get("OAKD_PORT", "8000")))
    debug: bool = field(default_factory=lambda: os.environ.get("OAKD_DEBUG", "").lower() in ("1", "true"))

    # Video settings
    camera_fps: int = 30
    resolution: tuple[int, int] = (640, 480)
    imu_downsample: int = 4  # 200Hz -> 50Hz


settings = Settings()
