"""Helpers for resolving MeshCore channel display labels."""

from __future__ import annotations

import os
import re
from functools import lru_cache

from meshcore_hub.collector.letsmesh_decoder import LetsMeshPacketDecoder


def parse_decoder_key_entries(raw: str | None) -> list[str]:
    """Parse COLLECTOR_LETSMESH_DECODER_KEYS into key entries."""
    if not raw:
        return []
    return [part.strip() for part in re.split(r"[,\s]+", raw) if part.strip()]


@lru_cache(maxsize=1)
def build_channel_labels() -> dict[int, str]:
    """Build channel labels from built-in and configured decoder keys."""
    decoder = LetsMeshPacketDecoder(
        enabled=False,
        channel_keys=parse_decoder_key_entries(
            os.getenv("COLLECTOR_LETSMESH_DECODER_KEYS")
        ),
    )
    return decoder.channel_labels_by_index()


def resolve_channel_label(
    channel_idx: int | None = None,
    channel_hash: str | None = None,
) -> str | None:
    """Resolve a configured label from channel index or hash."""
    labels = build_channel_labels()

    normalized_hash = (
        channel_hash.strip().upper() if isinstance(channel_hash, str) else ""
    )
    if normalized_hash:
        try:
            return labels.get(int(normalized_hash, 16))
        except ValueError:
            pass

    if channel_idx is None:
        return None
    return labels.get(channel_idx)


def format_channel_label(
    channel_name: str | None,
    channel_hash: str | None,
    channel_idx: int | None,
) -> str | None:
    """Format a display label for channel messages."""
    if channel_name and channel_name.strip():
        cleaned = channel_name.strip()
        if cleaned.lower() == "public":
            return "Public"
        return cleaned if cleaned.startswith("#") else f"#{cleaned}"

    resolved_label = resolve_channel_label(
        channel_idx=channel_idx, channel_hash=channel_hash
    )
    if resolved_label:
        return resolved_label
    if channel_idx is not None:
        return f"Ch {channel_idx}"
    if channel_hash:
        return f"Ch {channel_hash.upper()}"
    return None
