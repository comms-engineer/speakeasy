import enum
import logging
from collections import OrderedDict
from typing import Optional, Set
import msgpack
import signing
from speakeasy_db import BandwidthClass, SpeakeasyDB

logger = logging.getLogger("reti_speakeasy.fed")

MAX_MDU_PAYLOAD = 400

# Content ceiling that leaves room for the rest of a single-message DELTA_PUSH
# frame (envelope + 32-byte msg id + 16-byte sender hash + 64-byte signature +
# channel + timestamp). Messages larger than this can never be sent intact, so
# they are rejected at composition time rather than fragmenting unpredictably.
MAX_MESSAGE_CONTENT_BYTES = 200

# Frames carrying more hops than this are dropped: without a ceiling, three
# mutually federated hubs circulate the same DELTA_PUSH indefinitely.
MAX_HOP_COUNT = 8

# Bound on the recently-seen id cache. Deduplication is only an optimization --
# the messages table primary key is the authoritative dedupe -- so evicting
# oldest-first is safe.
SEEN_CACHE_LIMIT = 8192


class Opcode(enum.IntEnum):
    FED_HELLO = 0x01
    EPOCH_SYNC_REQ = 0x02
    EPOCH_SYNC_RESP = 0x03
    DELTA_REQ = 0x04
    DELTA_PUSH = 0x05
    CHANNEL_ADD = 0x06
    PROFILE_SYNC = 0x07
    BULLETIN_POST = 0x08


class WireCodec:
    """msgpack envelope: {0: opcode, 1: origin_hash, 2: hop_count, 3: payload}."""

    @staticmethod
    def pack(opcode: Opcode, origin_hash_bytes: bytes, payload: dict, hop_count: int = 0) -> bytes:
        envelope = {0: int(opcode), 1: origin_hash_bytes, 2: hop_count, 3: payload}
        return msgpack.packb(envelope, use_bin_type=True)

    @staticmethod
    def unpack(data: bytes) -> tuple[Opcode, bytes, int, dict]:
        unpacked = msgpack.unpackb(data, raw=False, strict_map_key=False)
        return Opcode(unpacked[0]), unpacked[1], unpacked[2], unpacked[3]


class S2SProtocolEngine:
    """
    Handles S2S state synchronizing, channel proposals, profile exchange, and bulletins.

    Trust model note: `origin_hash` in the envelope is self-reported by whoever
    sent the frame -- it is NOT cryptographically tied to the sending Link on
    its own. For PROFILE_SYNC, BULLETIN_POST, and DELTA_PUSH, the actual trust
    enforcement happens one layer down, in SpeakeasyDB's verify_and_* methods,
    which check a per-record Ed25519 signature (see signing.py) against
    `origin_hash` before anything gets stored or re-relayed. This engine does
    not itself decide what to trust -- it unpacks wire frames and hands raw,
    still-unverified data to the DB layer, which is the actual choke point.
    """

    def __init__(self, db: SpeakeasyDB, local_hash_bytes: bytes, bandwidth_class: BandwidthClass,
                 allowed_channels: Optional[Set[str]] = None, channel_blocklist: Optional[Set[str]] = None):
        self.db = db
        self.local_hash_bytes = local_hash_bytes
        self.bandwidth_class = bandwidth_class
        # CHANNEL_ADD is currently unsigned, so policy is the only thing
        # standing between a peer and arbitrary channel creation.
        self.allowed_channels = set(allowed_channels) if allowed_channels else None
        self.channel_blocklist = set(channel_blocklist or ())
        self.seen_msg_ids: OrderedDict = OrderedDict()

    def _mark_seen(self, msg_id: str):
        self.seen_msg_ids[msg_id] = None
        while len(self.seen_msg_ids) > SEEN_CACHE_LIMIT:
            self.seen_msg_ids.popitem(last=False)

    def channel_permitted(self, channel: str) -> bool:
        if channel in self.channel_blocklist:
            return False
        return self.allowed_channels is None or channel in self.allowed_channels

    def build_relay_frames(self, msg_ids: list, hop_count: int = 0) -> list:
        """
        Re-packs locally stored (therefore already verified) messages for
        fan-out. Relaying must never forward the peer's original bytes, since a
        frame can mix valid and forged records.
        """
        rows = []
        for msg_id in msg_ids:
            row = self.db.get_message(msg_id)
            if row:
                rows.append(row)
        return self.build_delta_push_chunks(rows, hop_count=hop_count) if rows else []

    def build_hello(self, active_channels: list[str]) -> bytes:
        payload = {
            0: 1,
            1: self.bandwidth_class.value,
            2: active_channels,
            3: int(self.db.epoch_bucket_sec),
        }
        return WireCodec.pack(Opcode.FED_HELLO, self.local_hash_bytes, payload)

    def build_channel_add(self, channel_name: str, description: str) -> bytes:
        payload = {0: channel_name, 1: description}
        return WireCodec.pack(Opcode.CHANNEL_ADD, self.local_hash_bytes, payload)

    def build_profile_sync(self, record: dict) -> bytes:
        payload = {
            0: record.get("handle", ""),
            1: record.get("status", ""),
            2: record.get("bio", ""),
            3: float(record["edited_at"]),
            4: record["signature"],
            5: record.get("public_key") or b"",
        }
        return WireCodec.pack(Opcode.PROFILE_SYNC, self.local_hash_bytes, payload)

    def build_bulletin_post(self, record: dict) -> bytes:
        """
        `record` is the dict returned by SpeakeasyDB.sign_and_add_bulletin() --
        must contain bulletin_id, title, body, timestamp, signature.
        """
        payload = {
            0: bytes.fromhex(record["bulletin_id"]),
            1: record["title"],
            2: record["body"],
            3: float(record["timestamp"]),
            4: record["signature"],
        }
        return WireCodec.pack(Opcode.BULLETIN_POST, self.local_hash_bytes, payload)

    def build_epoch_sync_req(self, channel_epochs: list[tuple[str, int]]) -> bytes:
        payload = {0: [[chan, epoch] for chan, epoch in channel_epochs]}
        return WireCodec.pack(Opcode.EPOCH_SYNC_REQ, self.local_hash_bytes, payload)

    def build_epoch_sync_resp(self, channel_epochs: list[tuple[str, int]]) -> bytes:
        resp_data = []
        for channel, epoch in channel_epochs:
            root_hex = self.db.get_epoch_merkle_root(channel, epoch)
            resp_data.append([channel, epoch, bytes.fromhex(root_hex)])
        return WireCodec.pack(Opcode.EPOCH_SYNC_RESP, self.local_hash_bytes, {0: resp_data})

    def build_delta_req(self, channel: str, epoch: int) -> bytes:
        known_ids = self.db.get_epoch_message_ids(channel, epoch)
        known_ids_bytes = [bytes.fromhex(mid) for mid in known_ids]
        return WireCodec.pack(Opcode.DELTA_REQ, self.local_hash_bytes, {0: channel, 1: epoch, 2: known_ids_bytes})

    def build_delta_push_chunks(self, missing_messages: list[dict], hop_count: int = 0) -> list[bytes]:
        """
        `missing_messages` items must each contain msg_id, channel, sender_hash,
        timestamp, content, signature -- i.e. either a full DB row (from
        get_missing_messages, which SELECT *s and so already has `signature`)
        or the dict returned by sign_and_insert_message().

        MDU-fit fix: the previous version only flushed the current batch and
        started a new one when `current_batch` was already non-empty, which
        meant a SINGLE message that on its own exceeded MAX_MDU_PAYLOAD sailed
        through unflagged and got sent as an oversized frame. Now every
        over-limit case flushes what's pending and is checked again on its
        own; if a single message still doesn't fit alone, it's logged loudly
        rather than silently shipped.
        """
        frames = []
        current_batch: list = []

        for msg in missing_messages:
            msg_tuple = [
                bytes.fromhex(msg["msg_id"]),
                msg["channel"],
                bytes.fromhex(msg["sender_hash"]),
                float(msg["timestamp"]),
                msg["content"],
                msg.get("signature") or b"",
            ]

            trial_batch = current_batch + [msg_tuple]
            trial_frame = WireCodec.pack(Opcode.DELTA_PUSH, self.local_hash_bytes, {0: trial_batch}, hop_count)

            if len(trial_frame) > MAX_MDU_PAYLOAD:
                if current_batch:
                    frames.append(WireCodec.pack(Opcode.DELTA_PUSH, self.local_hash_bytes, {0: current_batch}, hop_count))
                    current_batch = []

                solo_frame = WireCodec.pack(Opcode.DELTA_PUSH, self.local_hash_bytes, {0: [msg_tuple]}, hop_count)
                if len(solo_frame) > MAX_MDU_PAYLOAD:
                    logger.warning(
                        f"Message {msg['msg_id'][:10]} in #{msg['channel']} alone exceeds "
                        f"MAX_MDU_PAYLOAD ({len(solo_frame)} > {MAX_MDU_PAYLOAD} bytes) even after "
                        f"flushing the batch. Sending anyway, but expect this frame to be dropped, "
                        f"rejected, or unpredictably fragmented at the RNS interface layer. Enforce "
                        f"max_message_bytes at composition time to prevent this outright."
                    )
                current_batch = [msg_tuple]
            else:
                current_batch = trial_batch

        if current_batch:
            frames.append(WireCodec.pack(Opcode.DELTA_PUSH, self.local_hash_bytes, {0: current_batch}, hop_count))
        return frames

    def process_inbound_frame(self, raw_bytes: bytes, accepted_msg_ids: Optional[list] = None) -> tuple[Opcode, list[bytes]]:
        """
        Unpacks and applies one frame.

        :param accepted_msg_ids: If supplied, ids of messages that verified and
            were stored are appended to it, so the caller can relay exactly
            those records. RNS delivers packets on multiple threads, so this is
            a per-call output list rather than engine state.
        """
        try:
            opcode, origin_hash, hop_count, payload = WireCodec.unpack(raw_bytes)
        except Exception as e:
            logger.error(f"Malformed frame: {e}")
            return None, []

        outbound_frames = []

        if opcode == Opcode.FED_HELLO:
            raw_channels = payload.get(2, [])
            channels = []
            for c in raw_channels:
                if isinstance(c, dict):
                    name = c.get("name") or c.get("channel")
                    if name:
                        channels.append(str(name))
                elif isinstance(c, str):
                    channels.append(c)

            remote_bucket = payload.get(3)
            if remote_bucket and int(remote_bucket) != int(self.db.epoch_bucket_sec):
                logger.warning(
                    f"Peer {origin_hash.hex()[:10]} uses epoch_bucket_sec={remote_bucket} but this node "
                    f"uses {self.db.epoch_bucket_sec}; epoch roots can never agree. Skipping sync."
                )
                return opcode, outbound_frames

            current_epoch = self.db.current_epoch()
            local_chan_names = set(self.db.get_channel_names())

            sync_tuples = [(c, current_epoch) for c in channels if c in local_chan_names]
            if sync_tuples:
                outbound_frames.append(self.build_epoch_sync_req(sync_tuples))

        elif opcode == Opcode.CHANNEL_ADD:
            chan_name = payload[0]
            desc = payload[1]
            if isinstance(chan_name, dict):
                chan_name = chan_name.get("name", "unknown")
            if not self.channel_permitted(str(chan_name)):
                logger.warning(f"Refused CHANNEL_ADD for #{chan_name} from {origin_hash.hex()[:10]}: "
                               f"channel not permitted by local policy")
            elif self.db.add_channel(str(chan_name), str(desc)):
                logger.info(f"Replicated new federated channel: #{chan_name}")

        elif opcode == Opcode.PROFILE_SYNC:
            handle, status, bio = payload[0], payload[1], payload[2]
            edited_at = payload[3]
            signature = payload[4]
            pub_key = payload.get(5) if len(payload) > 5 else None

            if pub_key:
                reconstructed = signing.identity_from_public_key(pub_key)
                if reconstructed is None:
                    logger.warning(f"PROFILE_SYNC from {origin_hash.hex()[:10]} carried an unusable public key")
                elif reconstructed.hash != origin_hash:
                    logger.warning(f"PROFILE_SYNC from {origin_hash.hex()[:10]} carried a public key "
                                   f"belonging to {reconstructed.hash.hex()[:10]}; not stored")
                else:
                    self.db.upsert_identity(
                        identity_hash=origin_hash.hex(),
                        alias=str(handle),
                        public_key=pub_key
                    )

            ok = self.db.verify_and_upsert_profile(
                identity_hash=origin_hash.hex(), handle=handle, status=status, bio=bio,
                edited_at=edited_at, signature=signature
            )
            if not ok:
                logger.info(f"Discarded profile sync from {origin_hash.hex()[:10]} (failed verification or stale edit)")

        elif opcode == Opcode.BULLETIN_POST:
            bulletin_id = payload[0].hex()
            title, body, ts = payload[1], payload[2], payload[3]
            signature = payload[4]
            ok = self.db.verify_and_add_bulletin(
                bulletin_id=bulletin_id, title=title, body=body,
                author_hash=origin_hash.hex(), timestamp=ts, signature=signature
            )
            if not ok:
                logger.info(f"Discarded bulletin '{str(title)[:30]}' from {origin_hash.hex()[:10]} (failed verification)")

        elif opcode == Opcode.EPOCH_SYNC_REQ:
            outbound_frames.append(self.build_epoch_sync_resp(payload[0]))

        elif opcode == Opcode.EPOCH_SYNC_RESP:
            for channel, epoch, remote_root_bytes in payload[0]:
                if isinstance(channel, dict):
                    channel = channel.get("name", "")
                local_root = bytes.fromhex(self.db.get_epoch_merkle_root(str(channel), epoch))
                if local_root != remote_root_bytes:
                    outbound_frames.append(self.build_delta_req(str(channel), epoch))

        elif opcode == Opcode.DELTA_REQ:
            channel, epoch = payload[0], payload[1]
            if isinstance(channel, dict):
                channel = channel.get("name", "")
            remote_known_ids = {mid.hex() for mid in payload[2]}
            missing = self.db.get_missing_messages(str(channel), epoch, remote_known_ids)
            if missing:
                outbound_frames.extend(self.build_delta_push_chunks(missing, hop_count=hop_count + 1))

            # A DELTA_REQ also advertises everything the requester holds for
            # this epoch, which is the only point where the responder learns it
            # is behind. Without this counter-request, sync is one-directional:
            # whichever side received FED_HELLO pulls, and the side that opened
            # the conversation never receives history. Terminates because a
            # DELTA_REQ only ever produces pushes plus, at most, one
            # counter-request per epoch.
            local_known_ids = set(self.db.get_epoch_message_ids(str(channel), epoch))
            if remote_known_ids - local_known_ids:
                outbound_frames.append(self.build_delta_req(str(channel), epoch))

        elif opcode == Opcode.DELTA_PUSH:
            if hop_count > MAX_HOP_COUNT:
                logger.info(f"Dropped DELTA_PUSH from {origin_hash.hex()[:10]}: hop count {hop_count} "
                            f"exceeds MAX_HOP_COUNT ({MAX_HOP_COUNT})")
                return opcode, outbound_frames

            for msg_tuple in payload[0]:
                msg_id_hex, channel, sender_hash_hex, ts, content = (
                    msg_tuple[0].hex(), msg_tuple[1], msg_tuple[2].hex(), msg_tuple[3], msg_tuple[4]
                )
                signature = msg_tuple[5] if len(msg_tuple) > 5 else b""
                if isinstance(channel, dict):
                    channel = channel.get("name", "")

                if msg_id_hex in self.seen_msg_ids:
                    continue

                if not self.channel_permitted(str(channel)):
                    logger.info(f"Discarded message {msg_id_hex[:10]}: channel #{channel} not permitted")
                    continue

                # Only accepted ids are cached. Marking seen before verifying
                # would let anyone who has observed a public msg_id pre-send a
                # frame with that id and a bogus signature, permanently
                # suppressing the legitimate record.
                ok = self.db.verify_and_add_message(
                    msg_id=msg_id_hex, channel=str(channel), sender_hash=sender_hash_hex,
                    content=content, timestamp=ts, signature=signature
                )
                if ok:
                    self._mark_seen(msg_id_hex)
                    if accepted_msg_ids is not None:
                        accepted_msg_ids.append(msg_id_hex)
                else:
                    logger.info(f"Discarded message {msg_id_hex[:10]} in #{channel} "
                                f"from {sender_hash_hex[:10]} (failed verification)")

        return opcode, outbound_frames
