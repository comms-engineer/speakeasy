import time

import RNS
import pytest

import signing
from speakeasy_db import SpeakeasyDB


@pytest.fixture
def identity():
    return RNS.Identity()


@pytest.fixture
def db(tmp_path):
    database = SpeakeasyDB(str(tmp_path / "signing.db"))
    yield database
    database.close()


def test_identity_from_public_key_round_trips(identity):
    recovered = signing.identity_from_public_key(identity.get_public_key())

    assert recovered is not None
    assert recovered.hash == identity.hash
    assert recovered.validate(identity.sign(b"payload"), b"payload")


def test_identity_from_public_key_accepts_hex(identity):
    recovered = signing.identity_from_public_key(identity.get_public_key().hex())

    assert recovered.hash == identity.hash


def test_verify_bytes_accepts_valid_signature(identity):
    data = signing.canonical_message_bytes("aa", "parlor", identity.hash.hex(), 1.5, "hello")

    assert signing.verify_bytes(identity.hash, identity.sign(data), data,
                                public_key_bytes=identity.get_public_key())


def test_verify_bytes_rejects_tampered_payload(identity):
    data = signing.canonical_message_bytes("aa", "parlor", identity.hash.hex(), 1.5, "hello")
    signature = identity.sign(data)
    tampered = signing.canonical_message_bytes("aa", "parlor", identity.hash.hex(), 1.5, "hell0")

    assert not signing.verify_bytes(identity.hash, signature, tampered,
                                    public_key_bytes=identity.get_public_key())


def test_verify_bytes_rejects_key_not_matching_claimed_hash(identity):
    """A peer must not be able to present its own key alongside another identity's hash."""
    impostor = RNS.Identity()
    data = b"canonical"

    assert not signing.verify_bytes(identity.hash, impostor.sign(data), data,
                                    public_key_bytes=impostor.get_public_key())


def test_canonical_bytes_are_timestamp_stable():
    a = signing.canonical_message_bytes("id", "parlor", "abcd", 1700000000.1230004, "hi")
    b = signing.canonical_message_bytes("id", "parlor", "abcd", 1700000000.123, "hi")

    assert a == b


def test_verify_and_add_message_rejects_forged_signature(db, identity):
    record = db.sign_and_insert_message(identity, "parlor", "authentic")
    db.upsert_identity(identity.hash.hex(), "sender", identity.get_public_key())

    assert not db.verify_and_add_message(
        msg_id="f" * 64, channel="parlor", sender_hash=identity.hash.hex(),
        content="forged", timestamp=record["timestamp"], signature=record["signature"]
    )
    assert db.get_message("f" * 64) is None


def test_verify_and_add_message_rejects_future_timestamp(db, identity):
    db.upsert_identity(identity.hash.hex(), "sender", identity.get_public_key())
    future = time.time() + 86400
    msg_id = "a" * 64
    canonical = signing.canonical_message_bytes(msg_id, "parlor", identity.hash.hex(), future, "later")

    assert not db.verify_and_add_message(
        msg_id=msg_id, channel="parlor", sender_hash=identity.hash.hex(),
        content="later", timestamp=future, signature=identity.sign(canonical)
    )


def test_profile_sync_rejects_stale_edit(db, identity):
    db.upsert_identity(identity.hash.hex(), "sender", identity.get_public_key())
    record = db.sign_and_upsert_profile(identity, "operator", "on air", "bio")

    assert not db.verify_and_upsert_profile(
        identity_hash=record["identity_hash"], handle=record["handle"], status=record["status"],
        bio=record["bio"], edited_at=record["edited_at"], signature=record["signature"]
    )


def test_bulletin_verification_round_trip(db, identity):
    db.upsert_identity(identity.hash.hex(), "author", identity.get_public_key())
    record = db.sign_and_add_bulletin(identity, "Notice", "Body text")

    peer = SpeakeasyDB(str(db.db_path) + ".peer")
    peer.upsert_identity(identity.hash.hex(), "author", identity.get_public_key())
    try:
        assert peer.verify_and_add_bulletin(
            bulletin_id=record["bulletin_id"], title=record["title"], body=record["body"],
            author_hash=record["author_hash"], timestamp=record["timestamp"],
            signature=record["signature"]
        )
        assert not peer.verify_and_add_bulletin(
            bulletin_id="b" * 64, title="Tampered", body=record["body"],
            author_hash=record["author_hash"], timestamp=record["timestamp"],
            signature=record["signature"]
        )
    finally:
        peer.close()


def test_unknown_identity_cannot_be_verified(db, identity):
    """Without a public key on file, records are dropped rather than trusted."""
    msg_id = "c" * 64
    ts = time.time()
    canonical = signing.canonical_message_bytes(msg_id, "parlor", identity.hash.hex(), ts, "hi")

    assert not db.verify_and_add_message(
        msg_id=msg_id, channel="parlor", sender_hash=identity.hash.hex(),
        content="hi", timestamp=ts, signature=identity.sign(canonical)
    )
