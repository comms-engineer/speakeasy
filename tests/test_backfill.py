"""Historical backfill: a hub joining an existing mesh must inherit history.

Before this, anti-entropy only ever reconciled the *current* epoch, so a peer
that linked at 14:05 could never learn about anything posted at 13:00 -- the two
hubs agreed perfectly about the only epoch they ever compared.
"""

import hashlib
import time

import RNS

import signing
from fed_engine import MAX_SYNC_EPOCHS, Opcode, S2SProtocolEngine, WireCodec
from speakeasy_db import BandwidthClass, SpeakeasyDB
from test_federation import Hub, exchange


def backdated_message(hub: Hub, author: RNS.Identity, channel: str, content: str, age_sec: float):
    """Stores a correctly signed message as though it were posted `age_sec` ago."""
    sender_hash = author.hash.hex()
    ts = time.time() - age_sec
    msg_id = hashlib.sha256(f"{channel}:{content}:{ts}".encode("utf-8")).hexdigest()
    canonical = signing.canonical_message_bytes(msg_id, channel, sender_hash, ts, content)
    hub.db.add_message(
        msg_id=msg_id, channel=channel, sender_hash=sender_hash,
        content=content, timestamp=ts, signature=signing.sign_bytes(author, canonical),
    )
    return msg_id


def test_joining_hub_backfills_history_from_before_the_link(hubs):
    alpha, beta = hubs
    author = RNS.Identity()
    for hub in (alpha, beta):
        hub.db.upsert_identity(author.hash.hex(), "author", author.get_public_key())

    # Alpha holds several hours of history; beta is empty, as a hub that just
    # joined the mesh would be.
    old_ids = [
        backdated_message(alpha, author, "parlor", f"old-{i}", age_sec=3600 * (i + 1))
        for i in range(3)
    ]
    fresh = alpha.db.sign_and_insert_message(author, "parlor", "fresh")

    exchange(beta, alpha, [beta.engine.build_hello(["parlor"])])

    stored = {m["msg_id"] for m in beta.db.get_messages("parlor", limit=100, since=0)}
    assert fresh["msg_id"] in stored
    for msg_id in old_ids:
        assert msg_id in stored, "history predating the link was not backfilled"


def test_peer_volunteers_epochs_the_requester_cannot_name(hubs):
    """
    The joining hub has no messages, so it cannot name a single historical
    epoch. Backfill only works because the responder offers epochs it holds
    within the requester's stated horizon.
    """
    alpha, beta = hubs
    author = RNS.Identity()
    alpha.db.upsert_identity(author.hash.hex(), "author", author.get_public_key())
    backdated_message(alpha, author, "parlor", "ancient", age_sec=7200)

    # Beta names only the current epoch, exactly as an empty hub would.
    req = beta.engine.build_epoch_sync_req(
        [("parlor", beta.db.current_epoch())], since_epoch=beta.engine.sync_horizon_epoch()
    )
    frames = alpha.deliver(req).frames

    offered = set()
    for frame in frames:
        _, _, _, payload = WireCodec.unpack(frame)
        offered.update((entry[0], entry[1]) for entry in payload[0])

    old_epoch = alpha.db.epoch_for(time.time() - 7200)
    assert ("parlor", old_epoch) in offered


def test_sync_skips_empty_epochs(hubs):
    """A quiet fortnight must not put thousands of Merkle roots on the wire."""
    alpha, _ = hubs
    author = RNS.Identity()
    alpha.db.upsert_identity(author.hash.hex(), "author", author.get_public_key())
    backdated_message(alpha, author, "parlor", "one", age_sec=86400)

    targets = alpha.engine.sync_targets(["parlor"])

    # Current epoch plus the single populated one -- not a day's worth of buckets.
    assert len(targets) == 2
    assert len(targets) <= MAX_SYNC_EPOCHS


def test_sync_window_walks_back_with_offset(hubs):
    alpha, _ = hubs
    author = RNS.Identity()
    alpha.db.upsert_identity(author.hash.hex(), "author", author.get_public_key())
    for i in range(4):
        backdated_message(alpha, author, "parlor", f"m{i}", age_sec=600 * (i + 1))

    first = alpha.engine.sync_targets(["parlor"], offset=0)
    second = alpha.engine.sync_targets(["parlor"], offset=2)

    historical_first = [t for t in first if t[1] != alpha.db.current_epoch()]
    historical_second = [t for t in second if t[1] != alpha.db.current_epoch()]
    # The offset round skips the newest history and starts further back.
    assert historical_first[:2] != historical_second[:2]
    assert max(e for _, e in historical_second) < max(e for _, e in historical_first)


def test_history_beyond_the_horizon_is_not_requested(tmp_path):
    """
    A hub whose retention window is one day must not keep pulling week-old
    messages it would delete on the next sweep.
    """
    db = SpeakeasyDB(str(tmp_path / "h.db"), epoch_bucket_sec=300)
    db.add_channel("parlor", "test")
    identity = RNS.Identity()
    engine = S2SProtocolEngine(
        db=db, local_hash_bytes=identity.hash,
        bandwidth_class=BandwidthClass.HIGH_SPEED, sync_history_days=1.0,
    )
    author = RNS.Identity()
    db.upsert_identity(author.hash.hex(), "author", author.get_public_key())

    ts = time.time() - 7 * 86400
    canonical = signing.canonical_message_bytes("a" * 64, "parlor", author.hash.hex(), ts, "old")
    db.add_message(msg_id="a" * 64, channel="parlor", sender_hash=author.hash.hex(),
                   content="old", timestamp=ts, signature=signing.sign_bytes(author, canonical))

    epochs = [e for _, e in engine.sync_targets(["parlor"])]
    assert epochs == [db.current_epoch()]
    db.close()


def test_epoch_sync_resp_is_chunked_to_fit_the_mdu(hubs):
    alpha, _ = hubs
    for i in range(20):
        alpha.db.add_channel(f"chan{i}", "")
    entries = [(f"chan{i}", alpha.db.current_epoch()) for i in range(20)]

    frames = alpha.engine.build_epoch_sync_resp(entries)

    assert len(frames) > 1
    for frame in frames:
        opcode, _, _, _ = WireCodec.unpack(frame)
        assert opcode == Opcode.EPOCH_SYNC_RESP
        assert len(frame) <= 400
