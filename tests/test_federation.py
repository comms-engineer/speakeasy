"""Two in-process hubs federating over a simulated (lossless, in-memory) link.

The hubs exchange the same frames the daemon would put on a Reticulum link, so
these tests cover the whole anti-entropy loop: HELLO -> EPOCH_SYNC_REQ ->
EPOCH_SYNC_RESP -> DELTA_REQ -> DELTA_PUSH.
"""

import time

import RNS
import pytest

from fed_engine import MAX_HOP_COUNT, Opcode, S2SProtocolEngine, WireCodec
from speakeasy_db import BandwidthClass, SpeakeasyDB


class Hub:
    def __init__(self, path, epoch_bucket_sec=300, allowed_channels=None):
        self.identity = RNS.Identity()
        self.db = SpeakeasyDB(str(path), epoch_bucket_sec=epoch_bucket_sec)
        self.db.add_channel("parlor", "test channel")
        self.engine = S2SProtocolEngine(
            db=self.db,
            local_hash_bytes=self.identity.hash,
            bandwidth_class=BandwidthClass.HIGH_SPEED,
            allowed_channels=allowed_channels,
        )

    def learn(self, other):
        self.db.upsert_identity(other.identity.hash.hex(), "peer", other.identity.get_public_key())

    def deliver(self, frame, accepted=None):
        return self.engine.process_inbound_frame(frame, accepted)

    def close(self):
        self.db.close()


def exchange(sender: Hub, receiver: Hub, frames, max_rounds=12):
    """Ping-pongs frames between two hubs until the conversation goes quiet."""
    pending = list(frames)
    src, dst = sender, receiver
    for _ in range(max_rounds):
        if not pending:
            break
        responses = []
        for frame in pending:
            _, out = dst.deliver(frame)
            responses.extend(out)
        pending = responses
        src, dst = dst, src
    return pending


@pytest.fixture
def hubs(tmp_path):
    alpha = Hub(tmp_path / "alpha.db")
    beta = Hub(tmp_path / "beta.db")
    yield alpha, beta
    alpha.close()
    beta.close()


def test_two_hubs_converge_on_channel_history(hubs):
    alpha, beta = hubs
    author = RNS.Identity()
    for hub in hubs:
        hub.db.upsert_identity(author.hash.hex(), "author", author.get_public_key())
    alpha.learn(beta)
    beta.learn(alpha)

    posted = [alpha.db.sign_and_insert_message(author, "parlor", f"round {i}") for i in range(5)]
    assert beta.db.get_messages("parlor") == []

    # Beta opens the conversation exactly as a freshly linked peer does.
    remaining = exchange(beta, alpha, [beta.engine.build_hello(beta.db.get_channel_names())])

    assert remaining == []
    assert {m["msg_id"] for m in beta.db.get_messages("parlor")} == {m["msg_id"] for m in posted}
    epoch = alpha.db.current_epoch()
    assert beta.db.get_epoch_merkle_root("parlor", epoch) == alpha.db.get_epoch_merkle_root("parlor", epoch)


def test_convergence_is_idempotent(hubs):
    alpha, beta = hubs
    author = RNS.Identity()
    for hub in hubs:
        hub.db.upsert_identity(author.hash.hex(), "author", author.get_public_key())

    alpha.db.sign_and_insert_message(author, "parlor", "only once")
    exchange(beta, alpha, [beta.engine.build_hello(["parlor"])])
    exchange(beta, alpha, [beta.engine.build_hello(["parlor"])])

    assert len(beta.db.get_messages("parlor")) == 1


def test_matching_epoch_roots_produce_no_delta_request(hubs):
    alpha, beta = hubs
    author = RNS.Identity()
    for hub in hubs:
        hub.db.upsert_identity(author.hash.hex(), "author", author.get_public_key())
    record = alpha.db.sign_and_insert_message(author, "parlor", "shared")
    beta.db.verify_and_add_message(**{k: record[k] for k in
                                      ("msg_id", "channel", "sender_hash", "content", "timestamp", "signature")})

    epoch = alpha.db.current_epoch()
    _, frames = beta.deliver(alpha.engine.build_epoch_sync_resp([("parlor", epoch)]))

    assert frames == []


def test_mismatched_epoch_bucket_aborts_sync(tmp_path):
    alpha = Hub(tmp_path / "a.db", epoch_bucket_sec=300)
    beta = Hub(tmp_path / "b.db", epoch_bucket_sec=3600)
    try:
        _, frames = alpha.deliver(beta.engine.build_hello(["parlor"]))
        assert frames == []
    finally:
        alpha.close()
        beta.close()


def test_forged_message_is_not_stored_or_acknowledged(hubs):
    alpha, beta = hubs
    author = RNS.Identity()
    impostor = RNS.Identity()
    beta.db.upsert_identity(author.hash.hex(), "author", author.get_public_key())

    honest = alpha.db.sign_and_insert_message(author, "parlor", "honest")
    forged = dict(honest)
    forged["msg_id"] = "d" * 64
    forged["content"] = "forged"
    forged["sender_hash"] = author.hash.hex()
    forged["signature"] = impostor.sign(b"whatever")

    accepted = []
    beta.deliver(alpha.engine.build_delta_push_chunks([honest, forged])[0], accepted)

    assert accepted == [honest["msg_id"]]
    assert beta.db.get_message("d" * 64) is None


def test_bad_signature_does_not_suppress_the_genuine_record(hubs):
    """A pre-sent frame carrying a known msg_id and a bogus signature must not
    poison the dedupe cache against the real record."""
    alpha, beta = hubs
    author = RNS.Identity()
    beta.db.upsert_identity(author.hash.hex(), "author", author.get_public_key())
    genuine = alpha.db.sign_and_insert_message(author, "parlor", "the real thing")

    poisoned = dict(genuine)
    poisoned["signature"] = b"\x00" * 64
    beta.deliver(alpha.engine.build_delta_push_chunks([poisoned])[0])
    assert beta.db.get_message(genuine["msg_id"]) is None

    accepted = []
    beta.deliver(alpha.engine.build_delta_push_chunks([genuine])[0], accepted)

    assert accepted == [genuine["msg_id"]]
    assert beta.db.get_message(genuine["msg_id"])["content"] == "the real thing"


def test_delta_push_beyond_hop_limit_is_dropped(hubs):
    alpha, beta = hubs
    author = RNS.Identity()
    beta.db.upsert_identity(author.hash.hex(), "author", author.get_public_key())
    record = alpha.db.sign_and_insert_message(author, "parlor", "looping")

    frame = alpha.engine.build_delta_push_chunks([record], hop_count=MAX_HOP_COUNT + 1)[0]
    beta.deliver(frame)

    assert beta.db.get_message(record["msg_id"]) is None


def test_relay_frames_are_rebuilt_from_verified_storage(hubs):
    alpha, beta = hubs
    author = RNS.Identity()
    beta.db.upsert_identity(author.hash.hex(), "author", author.get_public_key())
    record = alpha.db.sign_and_insert_message(author, "parlor", "relay me")

    accepted = []
    beta.deliver(alpha.engine.build_delta_push_chunks([record])[0], accepted)
    relay = beta.engine.build_relay_frames(accepted, hop_count=1)

    assert len(relay) == 1
    opcode, origin, hops, payload = WireCodec.unpack(relay[0])
    assert opcode == Opcode.DELTA_PUSH
    assert origin == beta.identity.hash
    assert hops == 1
    assert payload[0][0][0].hex() == record["msg_id"]


def test_profile_sync_teaches_public_key_then_verifies(hubs):
    """The organic-identity path: a peer's signed profile carries the key that
    makes its own signature -- and every later record -- verifiable."""
    alpha, beta = hubs
    author = RNS.Identity()
    engine = S2SProtocolEngine(db=alpha.db, local_hash_bytes=author.hash,
                               bandwidth_class=BandwidthClass.HIGH_SPEED)
    record = alpha.db.sign_and_upsert_profile(author, "operator", "on air", "bio")

    assert beta.db.get_public_key(author.hash.hex()) is None
    beta.deliver(engine.build_profile_sync(record))

    assert beta.db.get_public_key(author.hash.hex()) == author.get_public_key()
    assert beta.db.find_profile(author.hash.hex())["handle"] == "operator"

    message = alpha.db.sign_and_insert_message(author, "parlor", "now verifiable")
    accepted = []
    beta.deliver(alpha.engine.build_delta_push_chunks([message])[0], accepted)
    assert accepted == [message["msg_id"]]


def test_profile_sync_with_mismatched_key_is_not_stored(hubs):
    alpha, beta = hubs
    author = RNS.Identity()
    impostor = RNS.Identity()
    record = alpha.db.sign_and_upsert_profile(author, "operator", "", "")
    record["public_key"] = impostor.get_public_key()
    engine = S2SProtocolEngine(db=alpha.db, local_hash_bytes=author.hash,
                               bandwidth_class=BandwidthClass.HIGH_SPEED)

    beta.deliver(engine.build_profile_sync(record))

    assert beta.db.get_public_key(author.hash.hex()) is None
    assert beta.db.find_profile(author.hash.hex()) is None


def test_channel_add_respects_policy(tmp_path):
    hub = Hub(tmp_path / "policy.db", allowed_channels={"parlor"})
    peer = Hub(tmp_path / "peer.db")
    try:
        hub.deliver(peer.engine.build_channel_add("spam", "unwanted"))
        assert "spam" not in hub.db.get_channel_names()

        hub.deliver(peer.engine.build_channel_add("parlor", "allowed"))
        assert "parlor" in hub.db.get_channel_names()
    finally:
        hub.close()
        peer.close()


def test_epoch_bounds_partition_history(tmp_path):
    db = SpeakeasyDB(str(tmp_path / "epoch.db"), epoch_bucket_sec=300)
    identity = RNS.Identity()
    try:
        record = db.sign_and_insert_message(identity, "parlor", "now")
        epoch = db.epoch_for(record["timestamp"])

        assert db.get_epoch_message_ids("parlor", epoch) == [record["msg_id"]]
        assert db.get_epoch_message_ids("parlor", epoch - 1) == []
        assert db.get_missing_messages("parlor", epoch, {record["msg_id"]}) == []
        assert len(db.get_missing_messages("parlor", epoch, set())) == 1
    finally:
        db.close()


def test_oversized_content_is_refused_at_composition(tmp_path):
    from fed_engine import MAX_MESSAGE_CONTENT_BYTES

    db = SpeakeasyDB(str(tmp_path / "size.db"), max_message_bytes=MAX_MESSAGE_CONTENT_BYTES)
    identity = RNS.Identity()
    try:
        assert db.sign_and_insert_message(identity, "parlor", "x" * MAX_MESSAGE_CONTENT_BYTES)
        assert db.sign_and_insert_message(identity, "parlor", "x" * (MAX_MESSAGE_CONTENT_BYTES + 1)) is None
    finally:
        db.close()


def test_single_message_frame_fits_mdu_at_content_limit(tmp_path):
    from fed_engine import MAX_MDU_PAYLOAD, MAX_MESSAGE_CONTENT_BYTES

    hub = Hub(tmp_path / "fit.db")
    identity = RNS.Identity()
    try:
        record = hub.db.sign_and_insert_message(identity, "broadsheet", "x" * MAX_MESSAGE_CONTENT_BYTES)
        frames = hub.engine.build_delta_push_chunks([record])

        assert len(frames) == 1
        assert len(frames[0]) <= MAX_MDU_PAYLOAD
    finally:
        hub.close()


def test_prune_messages_respects_ttl(tmp_path):
    db = SpeakeasyDB(str(tmp_path / "ttl.db"))
    try:
        db.add_message("a" * 64, "parlor", "ff", "old", time.time() - (40 * 86400), b"")
        db.add_message("b" * 64, "parlor", "ff", "new", time.time(), b"")

        assert db.prune_messages(30) == 1
        assert [m["content"] for m in db.get_messages("parlor")] == ["new"]
    finally:
        db.close()
