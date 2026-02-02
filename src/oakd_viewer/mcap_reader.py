"""MCAP file reader: iterate messages by topic.

Supports both normal MCAP files (with footer/summary) and truncated files
from interrupted recordings (no footer). Falls back to forward-scanning
via NonSeekingReader when the summary is unavailable.
"""

import logging
from pathlib import Path
from typing import Iterator

from mcap.reader import make_reader, NonSeekingReader

log = logging.getLogger(__name__)


def _has_summary(mcap_path: Path) -> bool:
    """Check if an MCAP file has a readable summary (footer)."""
    try:
        with open(mcap_path, "rb") as f:
            reader = make_reader(f)
            summary = reader.get_summary()
            return summary is not None and summary.statistics is not None
    except Exception:
        return False


def iter_messages(mcap_path: Path, topic: str) -> Iterator[tuple[int, bytes]]:
    """Yield (timestamp_ns, data_bytes) for all messages on a given topic.

    Falls back to NonSeekingReader for footer-less MCAP files.
    """
    if _has_summary(mcap_path):
        with open(mcap_path, "rb") as f:
            reader = make_reader(f)
            for schema, channel, message in reader.iter_messages(topics=[topic]):
                yield message.log_time, message.data
    else:
        log.warning(f"No MCAP summary/footer in {mcap_path.name}, using forward scan")
        with open(mcap_path, "rb") as f:
            reader = NonSeekingReader(f, validate_crcs=False)
            try:
                for schema, channel, message in reader.iter_messages(topics=[topic]):
                    yield message.log_time, message.data
            except Exception as e:
                log.warning(f"Forward scan stopped early: {e}")


def get_metadata(mcap_path: Path) -> dict:
    """Extract MCAP metadata records as a dict.

    For footer-less files, scans forward to collect what metadata is available.
    """
    result = {}

    if _has_summary(mcap_path):
        with open(mcap_path, "rb") as f:
            reader = make_reader(f)
            summary = reader.get_summary()
            if summary and summary.metadata:
                for mid, meta in summary.metadata.items():
                    result[meta.name] = dict(meta.metadata)

            if summary and summary.statistics:
                stats = summary.statistics
                result["_stats"] = {
                    "message_count": stats.message_count,
                    "channel_count": len(summary.channels) if summary.channels else 0,
                    "start_time_ns": stats.message_start_time,
                    "end_time_ns": stats.message_end_time,
                }
                if stats.message_start_time and stats.message_end_time:
                    duration_s = (stats.message_end_time - stats.message_start_time) / 1e9
                    result["_stats"]["duration_s"] = round(duration_s, 2)
    else:
        log.warning(f"No MCAP summary/footer in {mcap_path.name}, computing stats via forward scan")
        count = 0
        first_ts = None
        last_ts = None
        topics = set()
        with open(mcap_path, "rb") as f:
            reader = NonSeekingReader(f, validate_crcs=False)
            try:
                for schema, channel, message in reader.iter_messages():
                    count += 1
                    topics.add(channel.topic)
                    if first_ts is None:
                        first_ts = message.log_time
                    last_ts = message.log_time
            except Exception:
                pass
        result["_stats"] = {
            "message_count": count,
            "channel_count": len(topics),
            "start_time_ns": first_ts or 0,
            "end_time_ns": last_ts or 0,
            "truncated": True,
        }
        if first_ts and last_ts:
            result["_stats"]["duration_s"] = round((last_ts - first_ts) / 1e9, 2)

    return result


def count_messages(mcap_path: Path, topic: str) -> int:
    """Count messages on a specific topic.

    Uses summary if available, otherwise counts by iterating (slower but works
    on footer-less files).
    """
    if _has_summary(mcap_path):
        with open(mcap_path, "rb") as f:
            reader = make_reader(f)
            summary = reader.get_summary()
            if summary and summary.channels and summary.statistics:
                for cid, ch in summary.channels.items():
                    if ch.topic == topic:
                        counts = summary.statistics.channel_message_counts
                        return counts.get(cid, 0)
        return 0

    # Footer-less: count by iterating
    log.info(f"Counting messages on {topic} via forward scan (no footer)...")
    count = 0
    with open(mcap_path, "rb") as f:
        reader = NonSeekingReader(f, validate_crcs=False)
        try:
            for schema, channel, message in reader.iter_messages(topics=[topic]):
                count += 1
        except Exception:
            pass
    log.info(f"Found {count} messages on {topic}")
    return count
