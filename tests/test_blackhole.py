"""Reticulum blackholes applied at the record layer.

RNS applies its blackhole list to *pathing* only, which does nothing about a
spammer whose records reach us relayed inside a hub's frames. These tests cover
the application-layer half: blackholed authors are refused, and their keys are
neither learned nor gossiped onward.
"""

import time

import RNS
import pytest

import blackhole
from fed_engine import S2SProtocolEngine
from speakeasy_db import BandwidthClass


@pytest.fixture(autouse=True)
def clean_blackhole_table():
    """Blackholes live in RNS.Transport, so they leak between tests otherwise."""
    original = dict(RNS.Transport.blackholed_identities)
    RNS.Transport.blackholed_identities.clear()
    yield
    RNS.Transport.blackholed_identities.clear()
    RNS.Transport.blackholed_identities.update(original)


def block(identity: RNS.Identity, until=None):
    RNS.Transport.blackholed_identities[identity.hash] = {
        "source": identity.hash, "until": until, "reason": "test",
    }


def test_hex_and_bytes_hashes_are_equivalent():
    identity = RNS.Identity()
    block(identity)

    assert blackhole.is_blackholed(identity.hash)
    assert blackhole.is_blackholed(identity.hash.hex())
    assert not blackhole.is_blackholed(RNS.Identity().hash)
    assert not blackhole.is_blackholed(None)
    assert not blackhole.is_blackholed("not-hex")


def test_expired_entry_is_treated_as_lifted():
    """RNS prunes expired entries on its own loop, so the table lags reality."""
    identity = RNS.Identity()
    block(identity, until=time.time() - 60)

    assert not blackhole.is_blackholed(identity.hash)
    assert identity.hash.hex() not in blackhole.blackholed_hashes()


def test_timed_entry_still_in_force_is_honoured():
    identity = RNS.Identity()
    block(identity, until=time.time() + 3600)

    assert blackhole.is_blackholed(identity.hash)
    assert identity.hash.hex() in blackhole.blackholed_hashes()


def test_blackholed_author_message_is_not_stored(hubs):
    alpha, beta = hubs
    spammer = RNS.Identity()
    for hub in (alpha, beta):
        hub.db.upsert_identity(spammer.hash.hex(), "spammer", spammer.get_public_key())
    record = alpha.db.sign_and_insert_message(spammer, "parlor", "buy my coin")

    block(spammer)
    beta.deliver(alpha.engine.build_delta_push_chunks([record])[0])

    assert not beta.db.has_message(record["msg_id"])


def test_valid_signature_does_not_override_a_blackhole(hubs):
    """
    A blocked spammer's records are perfectly well signed, so the block has to
    be checked before verification rather than relying on it.
    """
    alpha, beta = hubs
    spammer = RNS.Identity()
    for hub in (alpha, beta):
        hub.db.upsert_identity(spammer.hash.hex(), "spammer", spammer.get_public_key())
    record = alpha.db.sign_and_insert_message(spammer, "parlor", "signed spam")
    assert alpha.db.has_message(record["msg_id"])

    block(spammer)
    result = beta.deliver(alpha.engine.build_delta_push_chunks([record])[0])

    assert result.accepted_msg_ids == []


def test_unblocking_restores_delivery(hubs):
    alpha, beta = hubs
    author = RNS.Identity()
    for hub in (alpha, beta):
        hub.db.upsert_identity(author.hash.hex(), "author", author.get_public_key())

    block(author)
    blocked = alpha.db.sign_and_insert_message(author, "parlor", "first")
    beta.deliver(alpha.engine.build_delta_push_chunks([blocked])[0])
    assert not beta.db.has_message(blocked["msg_id"])

    # No restart: the next frame after the block is lifted must land.
    RNS.Transport.blackholed_identities.clear()
    allowed = alpha.db.sign_and_insert_message(author, "parlor", "second")
    beta.deliver(alpha.engine.build_delta_push_chunks([allowed])[0])

    assert beta.db.has_message(allowed["msg_id"])


def test_blackholed_profile_is_refused(hubs):
    alpha, beta = hubs
    spammer = RNS.Identity()
    for hub in (alpha, beta):
        hub.db.upsert_identity(spammer.hash.hex(), "spammer", spammer.get_public_key())
    profile = alpha.db.sign_and_upsert_profile(spammer, "SpamBot", "visit my site")

    block(spammer)
    result = beta.deliver(alpha.engine.build_profile_sync(profile))

    assert result.accepted_profiles == []
    assert not beta.db.find_profile(spammer.hash.hex())


def test_blackholed_bulletin_is_refused(hubs):
    alpha, beta = hubs
    spammer = RNS.Identity()
    for hub in (alpha, beta):
        hub.db.upsert_identity(spammer.hash.hex(), "spammer", spammer.get_public_key())
    bulletin = alpha.db.sign_and_add_bulletin(spammer, "Free money", "click here")

    block(spammer)
    beta.deliver(alpha.engine.build_bulletin_post(bulletin))

    assert not any(b["bulletin_id"] == bulletin["bulletin_id"] for b in beta.db.get_bulletins())


def test_blackholed_identity_key_is_not_learned(hubs):
    """
    Learning the key would be the first step to verifying -- and relaying --
    their records, so the block is applied before the key is stored.
    """
    alpha, beta = hubs
    spammer = RNS.Identity()
    alpha.db.upsert_identity(spammer.hash.hex(), "spammer", spammer.get_public_key())

    block(spammer)
    frame = alpha.engine.build_identity_push(spammer.hash.hex())
    result = beta.deliver(frame)

    assert result.learned_identities == []
    assert beta.db.get_public_key(spammer.hash.hex()) is None


def test_blackholed_identity_is_not_gossiped_to_peers(hubs):
    alpha, _ = hubs
    spammer = RNS.Identity()
    innocent = RNS.Identity()
    for identity, alias in ((spammer, "spammer"), (innocent, "innocent")):
        alpha.db.upsert_identity(identity.hash.hex(), alias, identity.get_public_key())

    block(spammer)
    frames = alpha.engine.build_identity_frames([spammer.hash.hex(), innocent.hash.hex()])

    assert len(frames) == 1
    assert spammer.hash.hex().encode() not in b"".join(frames)


def test_blackholed_channel_request_is_refused(hubs):
    alpha, beta = hubs
    requester = RNS.Identity()
    beta.engine.accept_channel_requests = True

    block(requester)
    engine = S2SProtocolEngine(
        db=alpha.db, local_hash_bytes=requester.hash,
        bandwidth_class=BandwidthClass.HIGH_SPEED,
    )
    result = beta.deliver(engine.build_channel_req("spamchan", "buy things"))

    assert result.channel_requests == []
    assert "spamchan" not in beta.db.get_channel_names()


def test_local_block_survives_rns_rewriting_its_blackhole_file(hubs):
    """
    Regression: client blocks used to be written into RNS's node-wide blackhole
    table. That table belongs to the master instance, so a shared-instance
    client's write was invisible to `rnpath -b` and the next `rnpath -B`
    overwrote the file and dropped it. Local blocks live in the client's own
    database instead, and must outlive exactly that.
    """
    alpha, beta = hubs
    spammer = RNS.Identity()
    for hub in (alpha, beta):
        hub.db.upsert_identity(spammer.hash.hex(), "spammer", spammer.get_public_key())

    assert beta.db.block_identity(spammer.hash.hex(), reason="Blocked from Speakeasy")
    # RNS reloading the shared file from the master, wholesale.
    RNS.Transport.blackholed_identities.clear()
    RNS.Transport.blackholed_identities[RNS.Identity().hash] = {
        "source": b"", "until": None, "reason": "someone else",
    }

    record = alpha.db.sign_and_insert_message(spammer, "parlor", "still spam")
    beta.deliver(alpha.engine.build_delta_push_chunks([record])[0])

    assert beta.db.is_blocked(spammer.hash.hex())
    assert not beta.db.has_message(record["msg_id"])


def test_local_block_and_rns_blackhole_are_independent(hubs):
    alpha, beta = hubs
    locally = RNS.Identity()
    node_wide = RNS.Identity()
    beta.db.block_identity(locally.hash.hex())
    block(node_wide)

    assert blackhole.is_blocked(locally.hash, beta.db)
    assert blackhole.is_blocked(node_wide.hash, beta.db)
    # A local block is not, and must not claim to be, a node-wide blackhole.
    assert not blackhole.is_blackholed(locally.hash)
    assert blackhole.blocked_hashes(beta.db) == {locally.hash.hex(), node_wide.hash.hex()}

    assert beta.db.unblock_identity(locally.hash.hex())
    assert not blackhole.is_blocked(locally.hash, beta.db)
    # Lifting a local block cannot lift an operator's blackhole.
    assert not beta.db.unblock_identity(node_wide.hash.hex())
    assert blackhole.is_blocked(node_wide.hash, beta.db)


def test_blocking_twice_is_reported_as_already_blocked(hubs):
    _, beta = hubs
    spammer = RNS.Identity()

    assert beta.db.block_identity(spammer.hash.hex())
    assert not beta.db.block_identity(spammer.hash.hex())
    assert beta.db.blocked_identities() == [spammer.hash.hex()]


def test_stored_messages_from_a_blocked_sender_are_not_relayed(hubs):
    """
    A hub that verified and stored a record before the operator blocked its
    author must stop amplifying it, without needing the record deleted.
    """
    alpha, _ = hubs
    spammer = RNS.Identity()
    alpha.db.upsert_identity(spammer.hash.hex(), "spammer", spammer.get_public_key())
    record = alpha.db.sign_and_insert_message(spammer, "parlor", "old spam")

    block(spammer)
    frames = alpha.engine.build_relay_frames([record["msg_id"]], hop_count=1)

    assert frames == []
