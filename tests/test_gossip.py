"""
Key/profile gossip and the operator-approved channel flow.

The scenario that motivates all of this: alice and bob both talk to a hub but
have never met each other. Unless the hub propagates alice's public key, bob
cannot verify anything alice says, and every relayed message is discarded in
silence.
"""

import time

import RNS
import pytest

from fed_engine import DEFERRED_RECORD_LIMIT, Opcode, S2SProtocolEngine, WireCodec
from speakeasy_db import BandwidthClass, SpeakeasyDB


class Node:
    def __init__(self, path, allowed_channels=None, accept_channel_requests=False):
        self.identity = RNS.Identity()
        self.db = SpeakeasyDB(str(path))
        self.db.add_channel("parlor", "test channel")
        self.engine = S2SProtocolEngine(
            db=self.db,
            local_hash_bytes=self.identity.hash,
            bandwidth_class=BandwidthClass.HIGH_SPEED,
            allowed_channels=allowed_channels,
            accept_channel_requests=accept_channel_requests,
        )

    def deliver(self, frame):
        return self.engine.process_inbound_frame(frame)

    def close(self):
        self.db.close()


@pytest.fixture
def hub(tmp_path):
    node = Node(tmp_path / "hub.db", accept_channel_requests=True)
    yield node
    node.close()


@pytest.fixture
def bob(tmp_path):
    node = Node(tmp_path / "bob.db")
    yield node
    node.close()


def test_identity_push_teaches_a_key_bob_never_saw(hub, bob):
    alice = RNS.Identity()
    hub.db.upsert_identity(alice.hash.hex(), "alice", alice.get_public_key())

    assert bob.db.get_public_key(alice.hash.hex()) is None
    bob.deliver(hub.engine.build_identity_push(alice.hash.hex()))

    assert bob.db.get_public_key(alice.hash.hex()) == alice.get_public_key()


def test_identity_push_with_a_key_for_someone_else_is_ignored(hub, bob):
    alice = RNS.Identity()
    impostor = RNS.Identity()
    frame = WireCodec.pack(
        Opcode.IDENTITY_PUSH, hub.identity.hash,
        {0: alice.hash, 1: impostor.get_public_key(), 2: "alice"}
    )

    bob.deliver(frame)

    assert bob.db.get_public_key(alice.hash.hex()) is None


def test_gossiped_key_makes_a_relayed_message_verifiable(hub, bob):
    """The end-to-end bug this fixes: key first, then the message verifies."""
    alice = RNS.Identity()
    hub.db.upsert_identity(alice.hash.hex(), "alice", alice.get_public_key())
    message = hub.db.sign_and_insert_message(alice, "parlor", "hello bob")

    bob.deliver(hub.engine.build_identity_push(alice.hash.hex()))
    result = bob.deliver(hub.engine.build_relay_frames([message["msg_id"]], hop_count=1)[0])

    assert result.accepted_msg_ids == [message["msg_id"]]
    assert bob.db.get_message(message["msg_id"])["content"] == "hello bob"


def test_message_arriving_before_its_key_is_recovered_not_lost(hub, bob):
    """
    Ordering is not guaranteed on a mesh. A message that overtakes its author's
    key must be parked and re-verified, not dropped: the sender will never
    resend it.
    """
    alice = RNS.Identity()
    hub.db.upsert_identity(alice.hash.hex(), "alice", alice.get_public_key())
    message = hub.db.sign_and_insert_message(alice, "parlor", "out of order")

    result = bob.deliver(hub.engine.build_relay_frames([message["msg_id"]], hop_count=1)[0])

    assert result.accepted_msg_ids == []
    assert bob.db.get_message(message["msg_id"]) is None
    # Bob asks for the key he is missing rather than giving up.
    assert len(result.frames) == 1
    opcode, _, _, payload = WireCodec.unpack(result.frames[0])
    assert opcode == Opcode.IDENTITY_REQ
    assert payload[0] == [alice.hash]

    followup = bob.deliver(hub.engine.build_identity_push(alice.hash.hex()))

    assert followup.accepted_msg_ids == [message["msg_id"]]
    assert bob.db.get_message(message["msg_id"])["content"] == "out of order"


def test_identity_req_is_answered_with_the_requested_key(hub, bob):
    alice = RNS.Identity()
    hub.db.upsert_identity(alice.hash.hex(), "alice", alice.get_public_key())

    result = hub.deliver(bob.engine.build_identity_req([alice.hash.hex()]))

    assert len(result.frames) == 1
    opcode, _, _, payload = WireCodec.unpack(result.frames[0])
    assert opcode == Opcode.IDENTITY_PUSH
    assert payload[1] == alice.get_public_key()


def test_deferred_records_are_bounded(hub, bob):
    alice = RNS.Identity()
    hub.db.upsert_identity(alice.hash.hex(), "alice", alice.get_public_key())

    for i in range(DEFERRED_RECORD_LIMIT + 20):
        record = hub.db.sign_and_insert_message(alice, "parlor", f"msg {i}")
        # A distinct unknown sender per record, so nothing is ever retried.
        stranger = RNS.Identity()
        bob.engine._defer_message(stranger.hash.hex(), [
            bytes.fromhex(record["msg_id"]), "parlor", alice.hash,
            record["timestamp"], record["content"], record["signature"]
        ])

    parked = sum(len(v) for v in bob.engine.deferred_messages.values())
    assert parked <= DEFERRED_RECORD_LIMIT


def test_relayed_profile_keeps_the_author_as_origin(hub, bob):
    """
    A hub re-packing someone else's profile must not stamp its own hash on it,
    or the signature is checked against the wrong identity downstream.
    """
    alice = RNS.Identity()
    hub.db.upsert_identity(alice.hash.hex(), "alice", alice.get_public_key())
    hub.db.sign_and_upsert_profile(alice, "AliceRadio", "on air", "bio")

    frames = hub.engine.build_profile_frames([alice.hash.hex()])
    assert len(frames) == 1
    opcode, origin, _, _ = WireCodec.unpack(frames[0])
    assert opcode == Opcode.PROFILE_SYNC
    assert origin == alice.hash

    bob.deliver(frames[0])

    assert bob.db.find_profile(alice.hash.hex())["handle"] == "AliceRadio"
    assert bob.db.get_public_key(alice.hash.hex()) == alice.get_public_key()


# ---------------------------------------------------------------------------
# Channel requests and operator approval
# ---------------------------------------------------------------------------

def test_channel_request_is_queued_not_created(hub, bob):
    hub.deliver(bob.engine.build_channel_req("lounge", "off-topic chatter"))

    assert "lounge" not in hub.db.get_channel_names()
    pending = hub.db.get_channel_requests("pending")
    assert [(r["name"], r["requester_hash"]) for r in pending] == [("lounge", bob.identity.hash.hex())]


def test_channel_requests_are_ignored_when_not_accepted(tmp_path, bob):
    node = Node(tmp_path / "closed.db", accept_channel_requests=False)
    try:
        node.deliver(bob.engine.build_channel_req("lounge", "nope"))
        assert node.db.get_channel_requests("pending") == []
    finally:
        node.close()


def test_channel_requests_are_ignored_for_blocked_channels(hub, bob):
    hub.db.add_channel("spam", "blocked", status="blocked")
    hub.engine.channel_blocklist.add("spam")

    result = hub.deliver(bob.engine.build_channel_req("spam", "let me in"))

    assert result.channel_requests == []
    assert hub.db.get_channel_requests("pending") == []


def test_duplicate_channel_requests_do_not_stack(hub, bob):
    hub.deliver(bob.engine.build_channel_req("lounge", "first"))
    result = hub.deliver(bob.engine.build_channel_req("lounge", "again"))

    assert result.channel_requests == []
    assert len(hub.db.get_channel_requests("pending")) == 1


def test_approved_channel_propagates_and_is_verifiable(hub, bob):
    hub.deliver(bob.engine.build_channel_req("lounge", "off-topic chatter"))
    record = hub.db.sign_and_add_channel(hub.identity, "lounge", "off-topic chatter")
    hub.db.set_channel_request_status("lounge", "approved")

    result = bob.deliver(hub.engine.build_channel_add(record))

    assert result.accepted_channels == ["lounge"]
    assert "lounge" in bob.db.get_channel_names()
    assert bob.db.get_channel("lounge")["approver_hash"] == hub.identity.hash.hex()
    assert hub.db.get_channel_requests("pending") == []


def test_channel_add_propagation_is_idempotent(hub, bob):
    record = hub.db.sign_and_add_channel(hub.identity, "lounge", "off-topic")
    frame = hub.engine.build_channel_add(record)

    assert bob.deliver(frame).accepted_channels == ["lounge"]
    # A replay must not re-announce the channel, or three federated hubs
    # circulate the same approval forever.
    assert bob.deliver(frame).accepted_channels == []


def test_channel_approval_signature_is_checked(hub, bob):
    record = hub.db.sign_and_add_channel(hub.identity, "lounge", "off-topic")
    tampered = dict(record)
    tampered["description"] = "something else entirely"

    bob.deliver(hub.engine.build_channel_add(tampered))

    assert "lounge" not in bob.db.get_channel_names()


def test_approved_channel_carries_traffic_despite_static_policy(tmp_path, hub):
    """A hub whose config predates the approval must still accept the channel."""
    peer = Node(tmp_path / "strict.db", allowed_channels={"parlor"})
    author = RNS.Identity()
    try:
        peer.db.upsert_identity(author.hash.hex(), "alice", author.get_public_key())
        message = hub.db.sign_and_insert_message(author, "lounge", "post-approval")

        assert not peer.engine.channel_permitted("lounge")
        peer.deliver(hub.engine.build_channel_add(
            hub.db.sign_and_add_channel(hub.identity, "lounge", "off-topic")
        ))

        assert peer.engine.channel_permitted("lounge")
        result = peer.deliver(hub.engine.build_delta_push_chunks([message])[0])
        assert result.accepted_msg_ids == [message["msg_id"]]
    finally:
        peer.close()


def test_paused_channel_refuses_traffic(tmp_path, hub):
    peer = Node(tmp_path / "paused.db", allowed_channels={"parlor"})
    author = RNS.Identity()
    try:
        peer.db.set_channel_status("parlor", "paused")
        peer.db.upsert_identity(author.hash.hex(), "alice", author.get_public_key())
        message = hub.db.sign_and_insert_message(author, "parlor", "maintenance window")

        assert not peer.engine.channel_permitted("parlor")
        result = peer.deliver(hub.engine.build_delta_push_chunks([message])[0])
        assert result.accepted_msg_ids == []
    finally:
        peer.close()


def test_future_dated_channel_approval_is_rejected(hub, bob):
    record = hub.db.sign_and_add_channel(hub.identity, "lounge", "off-topic")
    record["created_at"] = time.time() + 86400

    bob.deliver(hub.engine.build_channel_add(record))

    assert "lounge" not in bob.db.get_channel_names()
