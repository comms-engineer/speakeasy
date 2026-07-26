"""
Bridge to Reticulum's own blackhole list.

Reticulum keeps a table of blackholed identities in
`RNS.Transport.blackholed_identities`, populated from
`~/.reticulum/storage/blackhole` at Transport start and managed with
`rnpath -B <hash>` / `rnpath -U <hash>`. RNS itself only applies it to
*pathing* -- announces and paths for a blackholed identity are dropped -- which
does nothing for a spammer whose records reach us relayed inside a hub's frames,
carrying no path of their own.

So Speakeasy consults the same list at the record layer: a blackholed identity's
messages, profiles, bulletins and channel requests are refused, and its keys are
neither learned nor gossiped onward. Reusing RNS's list rather than keeping a
private one means a user blocks someone once, with the standard tooling, and
every RNS application on that node honours it.
"""
import logging
import time
from typing import Optional, Set, Union
import RNS

logger = logging.getLogger("speakeasy.blackhole")


def _as_bytes(identity_hash: Union[str, bytes, None]) -> Optional[bytes]:
    if isinstance(identity_hash, bytes):
        return identity_hash
    if isinstance(identity_hash, str):
        try:
            return bytes.fromhex(identity_hash)
        except ValueError:
            return None
    return None


def is_blackholed(identity_hash: Union[str, bytes, None]) -> bool:
    """
    True when RNS holds a live blackhole entry for `identity_hash`.

    Expired entries are treated as lifted: RNS prunes them on its own job loop,
    so between sweeps the table can still hold an entry whose `until` has
    passed.
    """
    key = _as_bytes(identity_hash)
    if not key:
        return False

    entry = RNS.Transport.blackholed_identities.get(key)
    if entry is None:
        return False

    until = entry.get("until") if isinstance(entry, dict) else None
    if until is not None and float(until) < time.time():
        return False
    return True


def blackholed_hashes() -> Set[str]:
    """Hex identity hashes currently blackholed, for UI and bulk filtering."""
    return {
        key.hex() for key in list(RNS.Transport.blackholed_identities.keys())
        if is_blackholed(key)
    }


def blackhole(identity_hash: Union[str, bytes], reason: str = "",
              duration_hours: Optional[float] = None) -> bool:
    """Adds an identity to the node-wide RNS blackhole list."""
    key = _as_bytes(identity_hash)
    if not key:
        return False
    until = time.time() + float(duration_hours) * 3600 if duration_hours else None
    return bool(RNS.Transport.blackhole_identity(key, until=until, reason=reason or None))


def unblackhole(identity_hash: Union[str, bytes]) -> bool:
    key = _as_bytes(identity_hash)
    if not key:
        return False
    return bool(RNS.Transport.unblackhole_identity(key))
