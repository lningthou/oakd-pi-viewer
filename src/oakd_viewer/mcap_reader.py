"""MCAP file reader: iterate messages by topic."""

import logging
from pathlib import Path
from typing import Iterator

from mcap.reader import make_reader

log = logging.getLogger(__name__)


def iter_messages(mcap_path: Path, topic: str) -> Iterator[tuple[int, bytes]]:
    """Yield (timestamp_ns, data_bytes) for all messages on a given topic."""
    with open(mcap_path, "rb") as f:
        reader = make_reader(f)
        for schema, channel, message in reader.iter_messages(topics=[topic]):
            yield message.log_time, message.data


def get_metadata(mcap_path: Path) -> dict:
    """Extract MCAP metadata records as a dict."""
    result = {}
    with open(mcap_path, "rb") as f:
        reader = make_reader(f)
        summary = reader.get_summary()
        if summary and summary.metadata:
            for mid, meta in summary.metadata.items():
                result[meta.name] = dict(meta.metadata)

        # Also collect basic stats
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

    return result


def count_messages(mcap_path: Path, topic: str) -> int:
    """Count messages on a specific topic."""
    with open(mcap_path, "rb") as f:
        reader = make_reader(f)
        summary = reader.get_summary()
        if summary and summary.channels and summary.statistics:
            for cid, ch in summary.channels.items():
                if ch.topic == topic:
                    counts = summary.statistics.channel_message_counts
                    return counts.get(cid, 0)
    return 0
