# OAK-D Recording Viewer

Web viewer for OAK-D Pi MCAP recordings stored in S3. Browse recordings, play back synchronized RGB + depth colormap video, and visualize IMU data.

## Setup

### System dependencies

```bash
sudo apt install -y ffmpeg    # Ubuntu/Debian
brew install ffmpeg            # macOS
```

### Install

```bash
uv sync
```

### Configure

Set environment variables:

```bash
export OAKD_S3_BUCKET=your-bucket-name
export OAKD_S3_PREFIX=recordings          # optional S3 key prefix
export AWS_DEFAULT_REGION=us-east-1
export OAKD_CACHE_DIR=/tmp/oakd-viewer-cache
```

AWS credentials must be available (instance role, env vars, or `~/.aws/credentials`).

### Run

```bash
uv run python -m oakd_viewer
```

Server starts on `http://0.0.0.0:8000`.

## EC2 deployment

For best performance, deploy in the same region as your S3 bucket.

```bash
# Install system deps
sudo apt update && sudo apt install -y ffmpeg python3.11

# Install uv
curl -LsSf https://astral.sh/uv/install.sh | sh

# Clone and install
git clone <repo-url> oakd-pi-viewer
cd oakd-pi-viewer
uv sync

# Run with instance role (recommended)
export OAKD_S3_BUCKET=your-bucket
uv run python -m oakd_viewer
```

## Architecture

1. **Browse** S3 bucket for recording folders via sidebar
2. **Process** on first view: downloads MCAP, transcodes H.265 → H.264 MP4 (RGB), generates depth colormap MP4, extracts IMU to JSON
3. **Cache** all outputs on disk — repeat views load instantly
4. **Play** synchronized RGB + depth video with IMU overlay charts
