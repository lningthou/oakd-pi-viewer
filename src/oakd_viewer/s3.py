"""S3 browsing and file download."""

import logging
from pathlib import Path

import boto3

from .config import settings

log = logging.getLogger(__name__)

_client = None


def _get_client():
    global _client
    if _client is None:
        _client = boto3.client("s3", region_name=settings.aws_region)
    return _client


def list_prefix(prefix: str = "") -> dict:
    """List folders and files at an S3 prefix. Returns {folders: [...], files: [...]}."""
    client = _get_client()
    full_prefix = f"{settings.s3_prefix}/{prefix}".strip("/")
    if full_prefix:
        full_prefix += "/"

    paginator = client.get_paginator("list_objects_v2")
    pages = paginator.paginate(
        Bucket=settings.s3_bucket,
        Prefix=full_prefix,
        Delimiter="/",
    )

    folders = []
    files = []

    for page in pages:
        for cp in page.get("CommonPrefixes", []):
            folder_path = cp["Prefix"]
            # Strip the base prefix to get relative path
            rel = folder_path
            if settings.s3_prefix:
                rel = folder_path[len(settings.s3_prefix) :].strip("/")
            name = folder_path.rstrip("/").rsplit("/", 1)[-1]
            folders.append({"name": name, "prefix": rel.rstrip("/")})

        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key == full_prefix:
                continue
            name = key.rsplit("/", 1)[-1]
            rel = key
            if settings.s3_prefix:
                rel = key[len(settings.s3_prefix) :].strip("/")
            files.append({
                "name": name,
                "key": rel,
                "size": obj["Size"],
                "last_modified": obj["LastModified"].isoformat(),
            })

    return {"folders": folders, "files": files}


def download_file(s3_key: str, dest: Path) -> Path:
    """Download an S3 object to a local path. Returns the dest path."""
    client = _get_client()
    full_key = s3_key
    if settings.s3_prefix:
        full_key = f"{settings.s3_prefix}/{s3_key}".strip("/")

    dest.parent.mkdir(parents=True, exist_ok=True)
    log.info(f"Downloading s3://{settings.s3_bucket}/{full_key} -> {dest}")
    client.download_file(settings.s3_bucket, full_key, str(dest))
    return dest


def get_object_bytes(s3_key: str) -> bytes:
    """Download an S3 object into memory."""
    client = _get_client()
    full_key = s3_key
    if settings.s3_prefix:
        full_key = f"{settings.s3_prefix}/{s3_key}".strip("/")

    resp = client.get_object(Bucket=settings.s3_bucket, Key=full_key)
    return resp["Body"].read()
