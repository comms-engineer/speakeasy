"""Compact channel summary helpers for announce-time prefiltering.

The summary is intentionally tiny and fixed-size so Reticulum announces stay
small regardless of how many channels a hub carries.
"""

from __future__ import annotations

import hashlib
from typing import Iterable


CHANNEL_SUMMARY_BYTES = 16
CHANNEL_SUMMARY_HASHES = 3


def _normalize_channel(name: str) -> str:
    return str(name or "").strip().lstrip("#").lower()


def _bit_positions(channel: str,
                   summary_bytes: int = CHANNEL_SUMMARY_BYTES,
                   hashes: int = CHANNEL_SUMMARY_HASHES) -> list[int]:
    bits = int(summary_bytes) * 8
    seed = hashlib.sha256(channel.encode("utf-8")).digest()
    positions = []
    for i in range(max(1, int(hashes))):
        start = (i * 4) % len(seed)
        value = int.from_bytes(seed[start:start + 4], "big", signed=False)
        positions.append(value % bits)
    return positions


def build_channel_summary(channels: Iterable[str],
                          summary_bytes: int = CHANNEL_SUMMARY_BYTES,
                          hashes: int = CHANNEL_SUMMARY_HASHES) -> bytes:
    """Builds a fixed-size probabilistic summary for a channel set."""
    bitset = 0
    unique = {_normalize_channel(name) for name in channels}
    for channel in unique:
        if not channel:
            continue
        for pos in _bit_positions(channel, summary_bytes=summary_bytes, hashes=hashes):
            bitset |= 1 << pos
    return int(bitset).to_bytes(int(summary_bytes), "big", signed=False)


def channel_maybe_present(channel: str, summary: bytes,
                          hashes: int = CHANNEL_SUMMARY_HASHES) -> bool:
    """Returns True when the summary indicates channel may be present."""
    if not isinstance(summary, (bytes, bytearray)) or not summary:
        return False
    normalized = _normalize_channel(channel)
    if not normalized:
        return False
    bitset = int.from_bytes(bytes(summary), "big", signed=False)
    for pos in _bit_positions(normalized, summary_bytes=len(summary), hashes=hashes):
        if (bitset & (1 << pos)) == 0:
            return False
    return True