"""Processing pipeline: remux H.265→MP4, depth→colormap MP4, IMU→JSON."""

import asyncio
import json
import logging
import struct
import subprocess
from pathlib import Path
from typing import Callable

import cv2
import lz4.frame as lz4f
import numpy as np

from .config import settings
from .mcap_reader import iter_messages, count_messages

log = logging.getLogger(__name__)

# Progress callback type: (stage: str, progress: float 0-1, detail: str) -> None
ProgressCallback = Callable[[str, float, str], None]


def _noop_progress(stage: str, progress: float, detail: str):
    pass


def process_rgb(mcap_path: Path, output_path: Path, progress: ProgressCallback = _noop_progress):
    """Extract H.265 NAL units from /oak/rgb messages and remux into MP4 (no transcode)."""
    total = count_messages(mcap_path, "/oak/rgb")
    if total == 0:
        raise ValueError("No RGB messages found in MCAP")

    fps = settings.camera_fps

    # Remux: copy the H.265 bitstream directly into MP4 container — no re-encoding
    cmd = [
        "ffmpeg", "-y",
        "-f", "hevc",
        "-r", str(fps),
        "-i", "pipe:0",
        "-c:v", "copy",
        "-movflags", "+faststart",
        "-tag:v", "hvc1",
        output_path.as_posix(),
    ]

    proc = subprocess.Popen(cmd, stdin=subprocess.PIPE, stderr=subprocess.PIPE)
    try:
        for i, (ts, data) in enumerate(iter_messages(mcap_path, "/oak/rgb")):
            proc.stdin.write(data)
            if i % 100 == 0:
                progress("rgb", i / total, f"Frame {i}/{total}")
    finally:
        proc.stdin.close()
        proc.wait()

    if proc.returncode != 0:
        stderr = proc.stderr.read().decode(errors="replace")
        raise RuntimeError(f"ffmpeg RGB remux failed (rc={proc.returncode}): {stderr[-500:]}")

    progress("rgb", 1.0, "Done")
    log.info(f"RGB remux complete: {output_path}")


_MIN_DEPTH_MM = 200
_MAX_DEPTH_MM = 1000
_DEPTH_RANGE = float(_MAX_DEPTH_MM - _MIN_DEPTH_MM)

# Small morphological kernel for optional hole cleanup
_KERNEL_3 = cv2.getStructuringElement(cv2.MORPH_ELLIPSE, (3, 3))


def _colorize_depth_frame(depth_raw: np.ndarray) -> np.ndarray:
    """Colorize a uint16 depth frame (mm) using turbo colormap.

    Clips to display range, normalizes valid pixels to 0-255,
    applies light median cleanup, then turbo colormap.
    Invalid pixels are black.
    """
    d = depth_raw
    valid = (d > 0) & (d >= _MIN_DEPTH_MM) & (d <= _MAX_DEPTH_MM)

    # Normalize valid pixels to uint8
    u8 = np.zeros(d.shape, dtype=np.uint8)
    u8[valid] = np.clip((d[valid].astype(np.float32) - _MIN_DEPTH_MM) * 255.0 / _DEPTH_RANGE, 0, 255).astype(np.uint8)

    # Light cleanup on normalized map
    u8 = cv2.medianBlur(u8, 3)

    # Optional: morphological close on valid mask to reduce tiny holes
    valid_closed = cv2.morphologyEx(valid.view(np.uint8), cv2.MORPH_CLOSE, _KERNEL_3).astype(bool)

    vis = cv2.applyColorMap(u8, cv2.COLORMAP_TURBO)
    vis[~valid_closed] = [0, 0, 0]
    return vis


def process_depth(mcap_path: Path, output_path: Path, progress: ProgressCallback = _noop_progress):
    """Extract LZ4-compressed depth frames, colorize with turbo, encode to H.264 MP4."""
    total = count_messages(mcap_path, "/oak/depth")
    if total == 0:
        raise ValueError("No depth messages found in MCAP")

    fps = settings.camera_fps
    w, h = settings.resolution

    cmd = [
        "ffmpeg", "-y",
        "-f", "rawvideo",
        "-pix_fmt", "bgr24",
        "-s", f"{w}x{h}",
        "-r", str(fps),
        "-i", "pipe:0",
        "-c:v", "libx264",
        "-preset", "ultrafast",
        "-crf", "23",
        "-g", "15",
        "-keyint_min", "15",
        "-pix_fmt", "yuv420p",
        "-movflags", "+faststart",
        output_path.as_posix(),
    ]

    proc = subprocess.Popen(cmd, stdin=subprocess.PIPE, stderr=subprocess.PIPE)
    frame_bytes = w * h * 2  # uint16

    try:
        for i, (ts, data) in enumerate(iter_messages(mcap_path, "/oak/depth")):
            raw = lz4f.decompress(data)
            depth = np.frombuffer(raw[:frame_bytes], dtype=np.uint16).reshape(h, w)

            colored = _colorize_depth_frame(depth)
            proc.stdin.write(colored.tobytes())

            if i % 30 == 0:
                progress("depth", i / total, f"Frame {i}/{total}")
    finally:
        proc.stdin.close()
        proc.wait()

    if proc.returncode != 0:
        stderr = proc.stderr.read().decode(errors="replace")
        raise RuntimeError(f"ffmpeg depth encode failed (rc={proc.returncode}): {stderr[-500:]}")

    progress("depth", 1.0, "Done")
    log.info(f"Depth colormap complete: {output_path}")


def process_imu(mcap_path: Path, output_path: Path, progress: ProgressCallback = _noop_progress):
    """Extract IMU samples, downsample 4x, write JSON."""
    total = count_messages(mcap_path, "/oak/imu")
    if total == 0:
        raise ValueError("No IMU messages found in MCAP")

    downsample = settings.imu_downsample
    timestamps = []
    accel_x, accel_y, accel_z = [], [], []
    gyro_x, gyro_y, gyro_z = [], [], []

    first_ts = None
    for i, (ts, data) in enumerate(iter_messages(mcap_path, "/oak/imu")):
        if i % downsample != 0:
            continue

        if first_ts is None:
            first_ts = ts

        ax, ay, az, gx, gy, gz = struct.unpack("<6d", data)
        t_s = (ts - first_ts) / 1e9

        timestamps.append(round(t_s, 4))
        accel_x.append(round(ax, 4))
        accel_y.append(round(ay, 4))
        accel_z.append(round(az, 4))
        gyro_x.append(round(gx, 4))
        gyro_y.append(round(gy, 4))
        gyro_z.append(round(gz, 4))

        if i % 1000 == 0:
            progress("imu", i / total, f"Sample {i}/{total}")

    result = {
        "timestamps": timestamps,
        "accel": {"x": accel_x, "y": accel_y, "z": accel_z},
        "gyro": {"x": gyro_x, "y": gyro_y, "z": gyro_z},
        "sample_rate_hz": round(200 / downsample),
        "sample_count": len(timestamps),
    }

    with open(output_path, "w") as f:
        json.dump(result, f)

    progress("imu", 1.0, "Done")
    log.info(f"IMU extraction complete: {output_path} ({len(timestamps)} samples)")


async def process_recording(
    mcap_path: Path,
    rgb_path: Path,
    depth_path: Path,
    imu_path: Path,
    progress: ProgressCallback = _noop_progress,
):
    """Run all three processing stages in parallel."""
    progress("processing", 0.0, "Starting parallel processing")

    await asyncio.gather(
        asyncio.to_thread(process_rgb, mcap_path, rgb_path, progress),
        asyncio.to_thread(process_depth, mcap_path, depth_path, progress),
        asyncio.to_thread(process_imu, mcap_path, imu_path, progress),
    )

    progress("done", 1.0, "Processing complete")
