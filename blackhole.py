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
neither learned nor gossiped onward. Anything the node's operator blackholes with
the standard tooling is therefore honoured by Speakeasy too.

This module deliberately only *reads* that list. RNS's blackhole table is
node-wide state owned by the master instance, and every Speakeasy process is
normally a shared-instance client: writing to it from here changes only the
calling process's copy -- `rnpath -b` (which reads the master) would not show the
block -- and the next `rnpath -B` rewrites the shared file wholesale, silently
dropping whatever Speakeasy wrote. A user blocking someone in their own client is
also not the same decision as an operator blackholing an identity for the whole
node, so client blocks are kept in the client's own database (see
`SpeakeasyDB.block_identity`) and `is_blocked()` honours both.
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


def is_blocked(identity_hash: Union[str, bytes, None], db=None) -> bool:
    """
    True when either this node's RNS blackhole list or `db`'s own block list
    refuses `identity_hash`. Both are consulted per record rather than cached, so
    a block applied to a running node takes effect on the next frame.
    """
    if is_blackholed(identity_hash):
        return True
    key = _as_bytes(identity_hash)
    return bool(db and key and db.is_blocked(key.hex()))


def blocked_hashes(db=None) -> Set[str]:
    """Every hex hash blocked on this node, from both sources."""
    blocked = blackholed_hashes()
    if db:
        blocked |= set(db.blocked_identities())
    return blocked
