"""Storage retention: a hub must stay inside a fixed disk budget.

Reticulum nodes commonly run on a Pi or a small SBC with an SD card, so an
unbounded message table is a real failure mode: the node dies of a full disk
rather than degrading by forgetting old chatter.
"""

import time

import RNS
import pytest

import signing
from speakeasy_db import SpeakeasyDB


@pytest.fixture
def db(tmp_path):
    database = SpeakeasyDB(str(tmp_path / "retention.db"), epoch_bucket_sec=300)
    database.add_channel("parlor", "test")
    database.add_channel("tech", "test")
    yield database
    database.close()


def add_message(db: SpeakeasyDB, author: RNS.Identity, channel: str, content: str,
                age_sec: float = 0.0) -> str:
    msg_id = f"{abs(hash((channel, content, age_sec))):064x}"[:64]
    ts = time.time() - age_sec
    sender = author.hash.hex()
    canonical = signing.canonical_message_bytes(msg_id, channel, sender, ts, content)
    db.add_message(msg_id=msg_id, channel=channel, sender_hash=sender, content=content,
                   timestamp=ts, signature=signing.sign_bytes(author, canonical))
    return msg_id


def test_ttl_prune_removes_only_expired_messages(db):
    author = RNS.Identity()
    old = add_message(db, author, "parlor", "old", age_sec=10 * 86400)
    recent = add_message(db, author, "parlor", "recent", age_sec=3600)

    removed = db.prune_messages(ttl_days=7)

    kept = {m["msg_id"] for m in db.get_messages("parlor", limit=50, since=0)}
    assert removed == 1
    assert old not in kept
    assert recent in kept


def test_per_channel_cap_protects_quiet_channels_from_busy_ones(db):
    """
    A global cap would let one flooded channel evict every other channel's
    history, so the cap is applied per channel.
    """
    author = RNS.Identity()
    for i in range(20):
        add_message(db, author, "parlor", f"flood-{i}", age_sec=20 - i)
    quiet = add_message(db, author, "tech", "rare message", age_sec=100)

    removed = db.prune_channel_overflow(max_per_channel=5)

    parlor = db.get_messages("parlor", limit=50, since=0)
    tech = {m["msg_id"] for m in db.get_messages("tech", limit=50, since=0)}
    assert removed == 15
    assert len(parlor) == 5
    assert quiet in tech, "a busy channel evicted a quiet channel's history"
    # The newest messages are the ones kept.
    assert {m["content"] for m in parlor} == {f"flood-{i}" for i in range(15, 20)}


def test_per_channel_cap_is_disabled_when_unset(db):
    author = RNS.Identity()
    for i in range(5):
        add_message(db, author, "parlor", f"m{i}", age_sec=i)

    assert db.prune_channel_overflow(0) == 0
    assert len(db.get_messages("parlor", limit=50, since=0)) == 5


def test_size_limit_sheds_oldest_history_first(db):
    author = RNS.Identity()
    for i in range(400):
        add_message(db, author, "parlor", f"msg-{i:04d}", age_sec=400 - i)

    baseline = db.db_size_bytes()
    limit = baseline // 2
    removed = db.enforce_size_limit(limit, batch=50)

    remaining = [m["content"] for m in db.get_messages("parlor", limit=1000, since=0)]
    assert removed > 0
    assert db.db_size_bytes() <= limit
    # Whatever survived is the newest history, not an arbitrary slice.
    assert remaining == sorted(remaining)
    assert "msg-0000" not in remaining


def test_size_limit_stops_instead_of_deleting_non_message_data(db, caplog):
    """
    A limit below the size of the schema itself must not turn into deleting
    identities, channels or profiles -- the node would lose the ability to
    verify anyone. It logs and gives up instead.
    """
    author = RNS.Identity()
    db.upsert_identity(author.hash.hex(), "author", author.get_public_key())

    with caplog.at_level("WARNING"):
        removed = db.enforce_size_limit(1)

    assert removed == 0
    assert db.get_public_key(author.hash.hex()) is not None
    assert db.get_channel_names()
    assert any("no prunable messages" in record.message for record in caplog.records)


def test_size_limit_ignores_pages_freed_by_an_earlier_prune(db):
    """
    Regression: a prune earlier in the same sweep leaves its pages on SQLite's
    freelist, so the file still measures its old size. Reading that made the
    size stage conclude the prune had achieved nothing and delete the entire
    remaining history while the real payload was a fraction of the budget.
    """
    author = RNS.Identity()
    for i in range(600):
        add_message(db, author, "parlor", f"msg-{i:04d}", age_sec=600 - i)
    inflated = db.db_size_bytes()

    db.prune_channel_overflow(max_per_channel=10)

    # The file has not shrunk yet, but the payload is well inside this budget.
    assert db.db_size_bytes() >= inflated
    assert db.db_payload_bytes() < inflated
    removed = db.enforce_size_limit(inflated // 2)

    assert removed == 0
    assert len(db.get_messages("parlor", limit=50, since=0)) == 10


def test_payload_bytes_tracks_deletions_before_vacuum(db):
    author = RNS.Identity()
    for i in range(300):
        add_message(db, author, "parlor", f"msg-{i}", age_sec=i)
    before = db.db_payload_bytes()

    db.prune_messages(ttl_days=0.0001)

    assert db.db_payload_bytes() < before
    assert db.db_size_bytes() >= before  # unchanged on disk until VACUUM


def test_size_limit_is_disabled_when_unset(db):
    author = RNS.Identity()
    msg_id = add_message(db, author, "parlor", "keep me")

    assert db.enforce_size_limit(0) == 0
    assert {m["msg_id"] for m in db.get_messages("parlor", limit=10, since=0)} == {msg_id}


def test_vacuum_returns_freed_pages_to_the_filesystem(db):
    author = RNS.Identity()
    for i in range(300):
        add_message(db, author, "parlor", f"msg-{i}", age_sec=i)
    grown = db.db_size_bytes()

    db.prune_messages(ttl_days=0.0001)
    db.vacuum()

    assert db.db_size_bytes() < grown


def test_purge_identity_removes_their_records_but_keeps_their_key(db):
    spammer = RNS.Identity()
    innocent = RNS.Identity()
    db.upsert_identity(spammer.hash.hex(), "spammer", spammer.get_public_key())
    for i in range(3):
        add_message(db, spammer, "parlor", f"spam-{i}", age_sec=i)
    kept = add_message(db, innocent, "parlor", "hello", age_sec=5)

    removed = db.purge_identity(spammer.hash.hex())

    remaining = {m["msg_id"] for m in db.get_messages("parlor", limit=50, since=0)}
    assert removed == 3
    assert remaining == {kept}
    assert db.get_public_key(spammer.hash.hex()) is not None
