import RNS
import pytest

from fed_engine import MAX_MDU_PAYLOAD, Opcode, S2SProtocolEngine, WireCodec
from speakeasy_db import BandwidthClass, SpeakeasyDB, merkle_root, EMPTY_MERKLE_ROOT


@pytest.fixture
def engine(tmp_path):
    db = SpeakeasyDB(str(tmp_path / "codec.db"), epoch_bucket_sec=300)
    yield S2SProtocolEngine(db=db, local_hash_bytes=b"\x01" * 16,
                            bandwidth_class=BandwidthClass.HIGH_SPEED)
    db.close()


def test_envelope_round_trips():
    frame = WireCodec.pack(Opcode.DELTA_REQ, b"\x02" * 16, {0: "parlor", 1: 42}, hop_count=3)
    opcode, origin, hops, payload = WireCodec.unpack(frame)

    assert opcode == Opcode.DELTA_REQ
    assert origin == b"\x02" * 16
    assert hops == 3
    assert payload == {0: "parlor", 1: 42}


def test_hello_round_trips_and_carries_epoch_bucket(engine):
    opcode, origin, _, payload = WireCodec.unpack(engine.build_hello(["parlor", "backroom"]))

    assert opcode == Opcode.FED_HELLO
    assert origin == engine.local_hash_bytes
    assert payload[1] == BandwidthClass.HIGH_SPEED.value
    assert payload[2] == ["parlor", "backroom"]
    assert payload[3] == engine.db.epoch_bucket_sec


def test_every_builder_round_trips(engine):
    identity = RNS.Identity()
    engine.db.upsert_identity(identity.hash.hex(), "author", identity.get_public_key())
    bulletin = engine.db.sign_and_add_bulletin(identity, "Title", "Body")
    profile = engine.db.sign_and_upsert_profile(identity, "operator", "on air", "bio")

    builders = {
        Opcode.CHANNEL_ADD: engine.build_channel_add(
            engine.db.sign_and_add_channel(identity, "backroom", "quiet corner")
        ),
        Opcode.CHANNEL_REQ: engine.build_channel_req("lounge", "off-topic"),
        Opcode.IDENTITY_PUSH: engine.build_identity_push(identity.hash.hex()),
        Opcode.IDENTITY_REQ: engine.build_identity_req([identity.hash.hex()]),
        Opcode.PROFILE_SYNC: engine.build_profile_sync(profile),
        Opcode.BULLETIN_POST: engine.build_bulletin_post(bulletin),
        Opcode.EPOCH_SYNC_REQ: engine.build_epoch_sync_req([("parlor", 7)]),
        Opcode.EPOCH_SYNC_RESP: engine.build_epoch_sync_resp([("parlor", 7)]),
        Opcode.DELTA_REQ: engine.build_delta_req("parlor", 7),
    }

    for expected_opcode, frame in builders.items():
        opcode, origin, hops, payload = WireCodec.unpack(frame)
        assert opcode == expected_opcode
        assert origin == engine.local_hash_bytes
        assert hops == 0
        assert isinstance(payload, dict) and payload


def test_delta_push_chunks_respect_mdu(engine):
    identity = RNS.Identity()
    messages = [engine.db.sign_and_insert_message(identity, "parlor", f"message {i}" * 4)
                for i in range(25)]

    frames = engine.build_delta_push_chunks(messages)

    assert len(frames) > 1
    assert all(len(frame) <= MAX_MDU_PAYLOAD for frame in frames)

    recovered = []
    for frame in frames:
        opcode, _, _, payload = WireCodec.unpack(frame)
        assert opcode == Opcode.DELTA_PUSH
        recovered.extend(entry[0].hex() for entry in payload[0])

    assert recovered == [m["msg_id"] for m in messages]


def test_oversized_single_message_is_flagged_not_batched(engine, caplog):
    identity = RNS.Identity()
    huge = engine.db.sign_and_insert_message(identity, "parlor", "x" * (MAX_MDU_PAYLOAD * 2))

    with caplog.at_level("WARNING"):
        frames = engine.build_delta_push_chunks([huge])

    assert len(frames) == 1
    assert "exceeds MAX_MDU_PAYLOAD" in caplog.text


def test_malformed_frame_is_rejected(engine):
    result = engine.process_inbound_frame(b"not-msgpack")

    assert result.opcode is None
    assert result.frames == []


def test_merkle_root_is_order_independent_and_empty_is_zero():
    ids = ["aa" * 32, "bb" * 32, "cc" * 32]

    assert merkle_root(ids) == merkle_root(reversed(ids))
    assert merkle_root([]) == EMPTY_MERKLE_ROOT
    assert merkle_root(ids) != merkle_root(ids[:2])
