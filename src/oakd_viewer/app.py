"""FastAPI application: routes, SSE progress, video serving."""

import asyncio
import json
import logging
import uuid
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

from . import s3, cache, processing
from .mcap_reader import get_metadata

log = logging.getLogger(__name__)

app = FastAPI(title="OAK-D Recording Viewer")

# Mount static files
_static_dir = Path(__file__).resolve().parent.parent.parent / "static"
app.mount("/static", StaticFiles(directory=str(_static_dir)), name="static")

# Active processing jobs: job_id -> {recording_id, events: asyncio.Queue, done: bool}
_jobs: dict[str, dict] = {}
# Dedup: recording_id -> job_id (prevent duplicate processing)
_active_recordings: dict[str, str] = {}


@app.get("/", response_class=HTMLResponse)
async def index():
    return FileResponse(_static_dir / "index.html")


@app.get("/api/browse")
async def browse(prefix: str = ""):
    """List S3 folders/files at prefix."""
    try:
        result = await asyncio.to_thread(s3.list_prefix, prefix)
        return result
    except Exception as e:
        log.exception("S3 browse failed")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/metadata/{recording_id:path}")
async def metadata(recording_id: str):
    """Return metadata JSON. Tries S3 metadata file first, falls back to MCAP metadata."""
    # Try to find a metadata_*.json in the recording folder
    try:
        listing = await asyncio.to_thread(s3.list_prefix, recording_id)
        for f in listing.get("files", []):
            if f["name"].startswith("metadata_") and f["name"].endswith(".json"):
                data = await asyncio.to_thread(s3.get_object_bytes, f["key"])
                return json.loads(data)
    except Exception as e:
        log.warning(f"Could not fetch metadata from S3: {e}")

    # Fall back to MCAP metadata if we have the file cached
    mcap_path = cache.get_mcap_path(recording_id)
    if mcap_path.exists():
        try:
            meta = await asyncio.to_thread(get_metadata, mcap_path)
            return meta
        except Exception as e:
            log.warning(f"Could not read MCAP metadata: {e}")

    raise HTTPException(status_code=404, detail="Metadata not found")


@app.post("/api/process/{recording_id:path}")
async def start_processing(recording_id: str):
    """Start processing a recording. Returns immediately with job_id or 'ready' if cached."""
    if cache.is_processed(recording_id):
        return {"status": "ready", "recording_id": recording_id}

    # Check if already processing
    if recording_id in _active_recordings:
        job_id = _active_recordings[recording_id]
        return {"status": "processing", "job_id": job_id, "recording_id": recording_id}

    job_id = str(uuid.uuid4())[:8]
    event_queue: asyncio.Queue = asyncio.Queue()
    _jobs[job_id] = {"recording_id": recording_id, "events": event_queue, "done": False}
    _active_recordings[recording_id] = job_id

    asyncio.create_task(_run_processing(job_id, recording_id, event_queue))

    return {"status": "processing", "job_id": job_id, "recording_id": recording_id}


async def _run_processing(job_id: str, recording_id: str, event_queue: asyncio.Queue):
    """Background task: download MCAP, process, cache results."""
    try:
        cache.ensure_dir(recording_id)
        mcap_path = cache.get_mcap_path(recording_id)

        # Find the .mcap file in the recording folder
        # Remove stale 0-byte files from previous failed downloads
        if mcap_path.exists() and mcap_path.stat().st_size == 0:
            mcap_path.unlink()

        if not mcap_path.exists():
            await event_queue.put({"stage": "download", "progress": 0, "detail": "Finding MCAP file"})
            listing = await asyncio.to_thread(s3.list_prefix, recording_id)
            mcap_key = None
            mcap_size = 0
            for f in listing.get("files", []):
                if f["name"].endswith(".mcap"):
                    mcap_key = f["key"]
                    mcap_size = f.get("size", 0)
                    break
            if not mcap_key:
                raise ValueError(f"No .mcap file found in {recording_id}")
            if mcap_size == 0:
                raise ValueError(f"Recording MCAP file is empty (0 bytes) — this recording may have been aborted")

            await event_queue.put({"stage": "download", "progress": 0.1, "detail": f"Downloading MCAP ({mcap_size / 1e9:.1f} GB)"})
            await asyncio.to_thread(s3.download_file, mcap_key, mcap_path)
            await event_queue.put({"stage": "download", "progress": 1.0, "detail": "Download complete"})

        # Progress callback that pushes to the SSE queue
        def on_progress(stage: str, progress: float, detail: str):
            try:
                event_queue.put_nowait({"stage": stage, "progress": progress, "detail": detail})
            except asyncio.QueueFull:
                pass

        await processing.process_recording(
            mcap_path,
            cache.get_rgb_path(recording_id),
            cache.get_depth_path(recording_id),
            cache.get_imu_path(recording_id),
            progress=on_progress,
        )

        await event_queue.put({"stage": "done", "progress": 1.0, "detail": "Ready"})

    except Exception as e:
        log.exception(f"Processing failed for {recording_id}")
        await event_queue.put({"stage": "error", "progress": 0, "detail": str(e)})

    finally:
        _jobs[job_id]["done"] = True
        _active_recordings.pop(recording_id, None)


@app.get("/api/jobs/{job_id}")
async def job_events(job_id: str):
    """SSE stream of processing progress."""
    if job_id not in _jobs:
        raise HTTPException(status_code=404, detail="Job not found")

    job = _jobs[job_id]

    async def event_stream():
        while True:
            try:
                event = await asyncio.wait_for(job["events"].get(), timeout=30.0)
                yield f"data: {json.dumps(event)}\n\n"
                if event.get("stage") in ("done", "error"):
                    break
            except asyncio.TimeoutError:
                yield f"data: {json.dumps({'stage': 'heartbeat', 'progress': 0, 'detail': ''})}\n\n"

        # Cleanup after stream ends
        _jobs.pop(job_id, None)

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.get("/api/video/rgb/{recording_id:path}")
async def serve_rgb(recording_id: str, request: Request):
    """Serve cached RGB MP4 with range request support."""
    path = cache.get_rgb_path(recording_id)
    if not path.exists():
        raise HTTPException(status_code=404, detail="RGB video not processed yet")
    return _range_response(path, request, "video/mp4")


@app.get("/api/video/depth/{recording_id:path}")
async def serve_depth(recording_id: str, request: Request):
    """Serve cached depth colormap MP4 with range request support."""
    path = cache.get_depth_path(recording_id)
    if not path.exists():
        raise HTTPException(status_code=404, detail="Depth video not processed yet")
    return _range_response(path, request, "video/mp4")


@app.get("/api/imu/{recording_id:path}")
async def serve_imu(recording_id: str):
    """Serve cached IMU JSON."""
    path = cache.get_imu_path(recording_id)
    if not path.exists():
        raise HTTPException(status_code=404, detail="IMU data not processed yet")
    return FileResponse(path, media_type="application/json")


def _range_response(path: Path, request: Request, media_type: str):
    """Serve a file with HTTP range request support for video seeking."""
    file_size = path.stat().st_size
    range_header = request.headers.get("range")

    if range_header:
        # Parse range header: "bytes=start-end"
        range_spec = range_header.replace("bytes=", "")
        parts = range_spec.split("-")
        start = int(parts[0]) if parts[0] else 0
        end = int(parts[1]) if parts[1] else file_size - 1
        end = min(end, file_size - 1)
        length = end - start + 1

        def iter_chunk():
            with open(path, "rb") as f:
                f.seek(start)
                remaining = length
                while remaining > 0:
                    chunk_size = min(65536, remaining)
                    data = f.read(chunk_size)
                    if not data:
                        break
                    remaining -= len(data)
                    yield data

        return StreamingResponse(
            iter_chunk(),
            status_code=206,
            media_type=media_type,
            headers={
                "Content-Range": f"bytes {start}-{end}/{file_size}",
                "Content-Length": str(length),
                "Accept-Ranges": "bytes",
            },
        )

    return FileResponse(path, media_type=media_type)
