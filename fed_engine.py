import enum
import logging
import time
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set
import msgpack
import blackhole
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

# Records whose author is unknown are parked here while an IDENTITY_REQ is in
# flight, then re-verified once the key arrives. Without this, a record that
# overtakes its author's key is lost permanently -- the sender has no reason to
# ever send it again.
DEFERRED_RECORD_LIMIT = 512

# How far back a single anti-entropy round reaches. Syncing only the current
# epoch means a hub joining an existing mesh never receives anything posted
# before the link came up; syncing every epoch in the retention window at once
# would put thousands of Merkle roots on the wire. Instead each round covers the
# most recent populated epochs and successive rounds walk further back.
DEFAULT_SYNC_HISTORY_DAYS = 14.0
MAX_SYNC_EPOCHS = 48

# EPOCH_SYNC_RESP carries a 32-byte root per channel-epoch, so a wide sync
# overruns the MDU unless the response is chunked. Channel names vary in length,
# so chunks are sized by measuring the packed frame rather than by entry count.


class Opcode(enum.IntEnum):
    FED_HELLO = 0x01
    EPOCH_SYNC_REQ = 0x02
    EPOCH_SYNC_RESP = 0x03
    DELTA_REQ = 0x04
    DELTA_PUSH = 0x05
    CHANNEL_ADD = 0x06
    PROFILE_SYNC = 0x07
    BULLETIN_POST = 0x08
    IDENTITY_PUSH = 0x09
    IDENTITY_REQ = 0x0A
    CHANNEL_REQ = 0x0B


@dataclass
class FrameResult:
    """
    Outcome of one inbound frame.

    The caller needs more than the opcode to decide what to gossip: only
    records that actually verified locally may be re-broadcast, and the hub
    must be able to forward the *keys* behind them, otherwise a peer that has
    never met the author cannot verify anything it is sent.
    """
    opcode: Optional[Opcode]
    frames: List[bytes] = field(default_factory=list)
    accepted_msg_ids: List[str] = field(default_factory=list)
    accepted_profiles: List[str] = field(default_factory=list)
    accepted_channels: List[str] = field(default_factory=list)
    learned_identities: List[str] = field(default_factory=list)
    channel_requests: List[Dict[str, Any]] = field(default_factory=list)
    hello_channels: List[str] = field(default_factory=list)


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
    Channel and hop policy are the exception: those are properties of the link
    rather than of a signed record, so they are enforced here.
    """

    def __init__(self, db: SpeakeasyDB, local_hash_bytes: bytes, bandwidth_class: BandwidthClass,
                 allowed_channels: Optional[Set[str]] = None, channel_blocklist: Optional[Set[str]] = None,
                 accept_channel_requests: bool = False,
                 receive_federated_channel_nominations: bool = True,
                 sync_history_days: float = DEFAULT_SYNC_HISTORY_DAYS):
        self.db = db
        self.sync_history_days = float(sync_history_days)
        self.local_hash_bytes = local_hash_bytes
        self.bandwidth_class = bandwidth_class
        self.allowed_channels = set(allowed_channels) if allowed_channels else None
        self.channel_blocklist = set(channel_blocklist or ())
        self.accept_channel_requests = accept_channel_requests
        self.receive_federated_channel_nominations = bool(receive_federated_channel_nominations)
        self.seen_msg_ids: OrderedDict = OrderedDict()
        self.deferred_messages: OrderedDict = OrderedDict()

    def _refuse_blackholed(self, identity_hash_hex: str, what: str) -> bool:
        """
        True when a record should be dropped because the user blackholed its
        author. Checked per record rather than cached: `rnpath -B` can be run
        against a live node, and a block the user just applied should take
        effect on the next frame, not on the next restart.
        """
        if not blackhole.is_blocked(identity_hash_hex, self.db):
            return False
        logger.info(f"Dropped {what} from blackholed identity {identity_hash_hex[:10]}")
        return True

    def sync_horizon_epoch(self) -> int:
        """Oldest epoch this node will sync, derived from the history window."""
        if self.sync_history_days <= 0:
            return self.db.current_epoch()
        return self.db.epoch_for(time.time() - self.sync_history_days * 86400)

    def _mark_seen(self, msg_id: str):
        self.seen_msg_ids[msg_id] = None
        while len(self.seen_msg_ids) > SEEN_CACHE_LIMIT:
            self.seen_msg_ids.popitem(last=False)

    def channel_permitted(self, channel: str) -> bool:
        if channel in self.channel_blocklist:
            return False
        status = self.db.get_channel_status(channel)
        if status in {"paused", "blocked"}:
            return False
        if self.allowed_channels is None or channel in self.allowed_channels:
            return True
        # A channel an operator approved and federated is as valid as one named
        # in the local config; otherwise newly approved channels could never
        # carry traffic on peer hubs.
        record = self.db.get_channel(channel)
        return bool(record and record.get("signature"))

    def _defer_message(self, sender_hash: str, msg_tuple: list):
        pending = self.deferred_messages.setdefault(sender_hash, [])
        pending.append(msg_tuple)
        self.deferred_messages.move_to_end(sender_hash)
        total = sum(len(v) for v in self.deferred_messages.values())
        while total > DEFERRED_RECORD_LIMIT and self.deferred_messages:
            _, dropped = self.deferred_messages.popitem(last=False)
            total -= len(dropped)

    def build_relay_frames(self, msg_ids: list, hop_count: int = 0) -> list:
        """
        Re-packs locally stored (therefore already verified) messages for
        fan-out. Relaying must never forward the peer's original bytes, since a
        frame can mix valid and forged records.
        """
        rows = []
        for msg_id in msg_ids:
            row = self.db.get_message(msg_id)
            if row and not blackhole.is_blocked(row["sender_hash"], self.db):
                rows.append(row)
        return self.build_delta_push_chunks(rows, hop_count=hop_count) if rows else []

    def build_identity_push(self, identity_hash_hex: str) -> Optional[bytes]:
        """
        Frame carrying an identity's public key.

        No signature is needed: the key is self-authenticating, because the
        receiver only accepts it if it hashes to the claimed identity hash.
        """
        record = self.db.get_identity_record(identity_hash_hex)
        public_key = record.get("public_key") if record else None
        if not public_key:
            public_key = self.db.get_public_key(identity_hash_hex)
        if not public_key:
            return None
        alias = (record or {}).get("alias") or ""
        payload = {0: bytes.fromhex(identity_hash_hex), 1: bytes(public_key), 2: alias}
        return WireCodec.pack(Opcode.IDENTITY_PUSH, self.local_hash_bytes, payload)

    def build_identity_frames(self, identity_hashes) -> list:
        frames = []
        for identity_hash in dict.fromkeys(identity_hashes):
            # Never help a peer verify records from an identity this node has
            # blackholed: blocking someone should stop us amplifying them too.
            if blackhole.is_blocked(identity_hash, self.db):
                continue
            frame = self.build_identity_push(identity_hash)
            if frame:
                frames.append(frame)
        return frames

    def build_identity_req(self, identity_hashes) -> bytes:
        wanted = [bytes.fromhex(h) for h in dict.fromkeys(identity_hashes)]
        return WireCodec.pack(Opcode.IDENTITY_REQ, self.local_hash_bytes, {0: wanted})

    def build_channel_req(self, channel_name: str, description: str,
                          requester_hash: Optional[str] = None) -> bytes:
        payload = {0: channel_name, 1: description}
        if requester_hash:
            payload[2] = bytes.fromhex(requester_hash)
        return WireCodec.pack(Opcode.CHANNEL_REQ, self.local_hash_bytes, payload)

    def build_hello(self, active_channels: list[str]) -> bytes:
        payload = {
            0: 1,
            1: self.bandwidth_class.value,
            2: active_channels,
            3: int(self.db.epoch_bucket_sec),
        }
        return WireCodec.pack(Opcode.FED_HELLO, self.local_hash_bytes, payload)

    def build_channel_add(self, record: dict, hop_count: int = 0) -> bytes:
        """
        `record` is an operator-approved channel row (from
        SpeakeasyDB.sign_and_add_channel, or a stored channel), carrying the
        approving hub's signature so any hub down the line verifies the
        approval itself instead of trusting whoever relayed it.
        """
        approver_hash = record["approver_hash"]
        payload = {
            0: record["name"],
            1: record.get("description") or "",
            2: bytes.fromhex(approver_hash),
            3: float(record["created_at"]),
            4: record["signature"],
            5: self.db.get_public_key(approver_hash) or b"",
        }
        return WireCodec.pack(Opcode.CHANNEL_ADD, self.local_hash_bytes, payload, hop_count)

    def build_channel_frames(self, channel_names, hop_count: int = 0) -> list:
        """Re-packs locally stored, operator-approved channels for propagation."""
        frames = []
        for name in dict.fromkeys(channel_names):
            record = self.db.get_channel(name)
            if record and record.get("signature") and record.get("approver_hash"):
                frames.append(self.build_channel_add(record, hop_count=hop_count))
        return frames

    def build_profile_frames(self, identity_hashes) -> list:
        """Re-packs locally stored (therefore verified) profiles for gossip."""
        frames = []
        for identity_hash in dict.fromkeys(identity_hashes):
            if blackhole.is_blocked(identity_hash, self.db):
                continue
            record = self.db.find_profile(identity_hash)
            if not record or not record.get("signature"):
                continue
            record = dict(record)
            if not record.get("public_key"):
                record["public_key"] = self.db.get_public_key(identity_hash)
            frames.append(self.build_profile_sync(record, origin_hash_hex=identity_hash))
        return frames

    def build_profile_sync(self, record: dict, origin_hash_hex: Optional[str] = None) -> bytes:
        payload = {
            0: record.get("handle", ""),
            1: record.get("status", ""),
            2: record.get("bio", ""),
            3: float(record["edited_at"]),
            4: record["signature"],
            5: record.get("public_key") or b"",
        }
        # A relayed profile keeps the *author's* hash as origin, not the
        # relaying hub's, otherwise the signature is checked against the wrong
        # identity downstream and every gossiped profile is discarded.
        origin = bytes.fromhex(origin_hash_hex) if origin_hash_hex else self.local_hash_bytes
        return WireCodec.pack(Opcode.PROFILE_SYNC, origin, payload)

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

    def build_epoch_sync_req(self, channel_epochs: list[tuple[str, int]],
                             since_epoch: Optional[int] = None, offset: int = 0) -> bytes:
        """
        Asks a peer for Merkle roots.

        `since_epoch` tells the peer how far back this node is willing to
        reconcile, so it can volunteer roots for epochs it holds and we don't --
        those are exactly the epochs we cannot name, and without them a joining
        hub can only ever learn about history it already partly has.
        """
        payload: Dict[int, Any] = {0: [[chan, epoch] for chan, epoch in channel_epochs]}
        if since_epoch is not None:
            payload[1] = int(since_epoch)
        if offset:
            payload[2] = int(offset)
        return WireCodec.pack(Opcode.EPOCH_SYNC_REQ, self.local_hash_bytes, payload)

    def build_epoch_sync_resp(self, channel_epochs: list[tuple[str, int]]) -> list[bytes]:
        frames: list[bytes] = []
        batch: list = []

        def pack(entries) -> bytes:
            return WireCodec.pack(Opcode.EPOCH_SYNC_RESP, self.local_hash_bytes, {0: entries})

        for channel, epoch in dict.fromkeys((str(c), int(e)) for c, e in channel_epochs):
            entry = [channel, epoch, bytes.fromhex(self.db.get_epoch_merkle_root(channel, epoch))]
            if batch and len(pack(batch + [entry])) > MAX_MDU_PAYLOAD:
                frames.append(pack(batch))
                batch = []
            batch.append(entry)

        if batch:
            frames.append(pack(batch))
        return frames

    def sync_targets(self, channels, offset: int = 0) -> list[tuple[str, int]]:
        """
        Channel-epochs worth reconciling: the current epoch for every shared
        channel, plus a window of `MAX_SYNC_EPOCHS` populated epochs starting
        `offset` back from the newest. Empty epochs are skipped, so a quiet week
        costs nothing to sync, and successive rounds raising `offset` walk
        further back through history without ever putting the whole retention
        window on the wire at once.
        """
        shared = [c for c in dict.fromkeys(channels) if c in set(self.db.get_channel_names())]
        if not shared:
            return []
        current = self.db.current_epoch()
        targets = [(c, current) for c in shared]
        populated = self.db.get_populated_epochs(
            shared, self.sync_horizon_epoch(), MAX_SYNC_EPOCHS, offset=offset
        )
        for channel, epoch in populated:
            if (channel, epoch) not in targets:
                targets.append((channel, epoch))
        return targets[:MAX_SYNC_EPOCHS]

    def build_sync_request(self, channels, offset: int = 0) -> Optional[bytes]:
        """One anti-entropy round for `channels`, or None when nothing is shared."""
        targets = self.sync_targets(channels, offset=offset)
        if not targets:
            return None
        return self.build_epoch_sync_req(
            targets, since_epoch=self.sync_horizon_epoch(), offset=offset
        )

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

    def process_inbound_frame(self, raw_bytes: bytes) -> FrameResult:
        """
        Unpacks and applies one frame, reporting what was accepted.

        RNS delivers packets on its own threads, so everything the caller needs
        in order to gossip the result comes back in the FrameResult rather than
        accumulating in engine state.
        """
        try:
            opcode, origin_hash, hop_count, payload = WireCodec.unpack(raw_bytes)
        except Exception as e:
            logger.error(f"Malformed frame: {e}")
            return FrameResult(opcode=None)

        result = FrameResult(opcode=opcode)
        outbound_frames = result.frames

        if opcode == Opcode.FED_HELLO:
            if isinstance(payload, dict):
                raw_channels = payload.get(2) if 2 in payload else payload.get("2", [])
                remote_bucket = payload.get(3) if 3 in payload else payload.get("3")
            elif isinstance(payload, (list, tuple)) and len(payload) > 3:
                raw_channels = payload[2]
                remote_bucket = payload[3]
            else:
                raw_channels = []
                remote_bucket = None
            
            channels = []
            for c in raw_channels if isinstance(raw_channels, list) else []:
                if isinstance(c, dict):
                    name = c.get("name") or c.get("channel")
                    if name:
                        channels.append(str(name))
                elif isinstance(c, str):
                    channels.append(c)
            
            if remote_bucket is not None:
                try:
                    bucket_int = int(remote_bucket)
                    if bucket_int != int(self.db.epoch_bucket_sec):
                        logger.warning(
                            f"Peer {origin_hash.hex()[:10]} uses epoch_bucket_sec={remote_bucket} "
                            f"but this node uses {self.db.epoch_bucket_sec}; epoch roots can never agree. Skipping sync."
                        )
                        return result
                except (ValueError, TypeError):
                    logger.warning(f"Invalid remote epoch bucket payload received: {remote_bucket}")

            result.hello_channels = channels
            sync_frame = self.build_sync_request(channels)
            if sync_frame:
                outbound_frames.append(sync_frame)

        elif opcode == Opcode.CHANNEL_ADD:
            chan_name = payload[0]
            desc = payload[1] or ""
            if isinstance(chan_name, dict):
                chan_name = chan_name.get("name", "unknown")
            chan_name = str(chan_name)
            approver_hash = payload[2].hex() if isinstance(payload.get(2), bytes) else str(payload.get(2) or "")
            created_at = payload.get(3) or 0.0
            signature = payload.get(4) or b""
            approver_key = payload.get(5) or None

            if self._refuse_blackholed(approver_hash, f"channel approval for #{chan_name}"):
                return result

            if approver_key:
                self._learn_identity(payload[2], approver_key, alias="", result=result)

            if chan_name in self.channel_blocklist:
                logger.warning(f"Refused CHANNEL_ADD for #{chan_name} from {origin_hash.hex()[:10]}: "
                               f"channel is blocklisted")
            elif self.db.verify_and_add_channel(name=chan_name, description=str(desc),
                                                approver_hash=approver_hash, created_at=created_at,
                                                signature=signature, public_key=approver_key):
                logger.info(f"Replicated federated channel #{chan_name} "
                            f"(approved by {approver_hash[:10]})")
                result.accepted_channels.append(chan_name)

        elif opcode == Opcode.CHANNEL_REQ:
            if isinstance(payload, dict):
                chan_name = str(payload.get(0) or payload.get("0") or "")
                desc = str(payload.get(1) or payload.get("1") or "")
                requester_payload = payload.get(2) or payload.get("2")
            else:
                chan_name = str(payload[0]) if len(payload) > 0 else ""
                desc = str(payload[1] or "") if len(payload) > 1 else ""
                requester_payload = payload[2] if len(payload) > 2 else None

            requester_hash = origin_hash.hex()
            if isinstance(requester_payload, bytes):
                requester_hash = requester_payload.hex()
            is_federated_nomination = requester_hash != origin_hash.hex()

            if self._refuse_blackholed(origin_hash.hex(), f"channel request #{chan_name}"):
                return result
            if not self.accept_channel_requests:
                logger.info(f"Ignored channel request for #{chan_name}: this node does not take requests")
            elif is_federated_nomination and not self.receive_federated_channel_nominations:
                logger.info(
                    f"Ignored federated channel nomination for #{chan_name}: "
                    f"receive_federated_channel_nominations is disabled"
                )
            elif self.db.get_channel_status(chan_name) == "blocked":
                logger.info(f"Ignored channel request for #{chan_name}: channel is blocked")
            elif chan_name in self.channel_blocklist:
                logger.info(f"Ignored channel request for #{chan_name}: blocklisted")
            elif self.db.add_channel_request(chan_name, desc, requester_hash):
                logger.info(f"Queued channel request for #{chan_name} from {requester_hash[:10]}")
                result.channel_requests.append({
                    "name": chan_name,
                    "description": desc,
                    "requester_hash": requester_hash,
                })

        elif opcode == Opcode.IDENTITY_PUSH:
            if self._refuse_blackholed(payload[0].hex(), "identity push"):
                return result
            self._learn_identity(payload[0], payload[1], alias=str(payload.get(2) or ""), result=result)
            self._retry_deferred(payload[0].hex(), result)

        elif opcode == Opcode.IDENTITY_REQ:
            for wanted in payload[0]:
                frame = self.build_identity_push(wanted.hex())
                if frame:
                    outbound_frames.append(frame)

        elif opcode == Opcode.PROFILE_SYNC:
            handle, status, bio = payload[0], payload[1], payload[2]
            edited_at = payload[3]
            signature = payload[4]
            pub_key = payload.get(5) if len(payload) > 5 else None

            if self._refuse_blackholed(origin_hash.hex(), "profile sync"):
                return result

            if pub_key:
                self._learn_identity(origin_hash, pub_key, alias=str(handle), result=result)

            ok = self.db.verify_and_upsert_profile(
                identity_hash=origin_hash.hex(), handle=handle, status=status, bio=bio,
                edited_at=edited_at, signature=signature
            )
            if ok:
                result.accepted_profiles.append(origin_hash.hex())
                self._retry_deferred(origin_hash.hex(), result)
            else:
                logger.info(f"Discarded profile sync from {origin_hash.hex()[:10]} (failed verification or stale edit)")

        elif opcode == Opcode.BULLETIN_POST:
            bulletin_id = payload[0].hex()
            title, body, ts = payload[1], payload[2], payload[3]
            signature = payload[4]
            if self._refuse_blackholed(origin_hash.hex(), "bulletin"):
                return result
            ok = self.db.verify_and_add_bulletin(
                bulletin_id=bulletin_id, title=title, body=body,
                author_hash=origin_hash.hex(), timestamp=ts, signature=signature
            )
            if not ok:
                logger.info(f"Discarded bulletin '{str(title)[:30]}' from {origin_hash.hex()[:10]} (failed verification)")

        elif opcode == Opcode.EPOCH_SYNC_REQ:
            requested = [(str(c), int(e)) for c, e in payload[0]]
            since_epoch = payload.get(1)
            if since_epoch is not None:
                # Volunteer epochs the requester never mentioned: those are the
                # ones it has no messages for, i.e. precisely the history it is
                # missing. Bounded by the requester's own stated horizon so a
                # peer with a longer retention window cannot force us to
                # re-offer history this node has deliberately aged out.
                horizon = max(int(since_epoch), self.sync_horizon_epoch())
                channels = list(dict.fromkeys(c for c, _ in requested))
                offset = int(payload.get(2) or 0)
                for entry in self.db.get_populated_epochs(channels, horizon, MAX_SYNC_EPOCHS,
                                                          offset=offset):
                    if entry not in requested:
                        requested.append(entry)
            outbound_frames.extend(self.build_epoch_sync_resp(requested[:MAX_SYNC_EPOCHS]))

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
                return result

            unknown_senders = []
            for msg_tuple in payload[0]:
                sender_hash_hex = msg_tuple[2].hex()
                if not self._apply_message(msg_tuple, result) and not self.db.get_public_key(sender_hash_hex):
                    unknown_senders.append(sender_hash_hex)

            if unknown_senders:
                # The record arrived before its author's key. Park it and ask;
                # dropping it outright loses the message for good, since the
                # sender has no reason to ever transmit it again.
                outbound_frames.append(self.build_identity_req(unknown_senders))

        return result

    def _learn_identity(self, identity_hash_bytes: bytes, public_key: bytes, alias: str,
                        result: FrameResult) -> bool:
        """Stores a public key only if it hashes to the identity claiming it."""
        reconstructed = signing.identity_from_public_key(public_key)
        identity_hash_hex = identity_hash_bytes.hex()

        if reconstructed is None:
            logger.warning(f"Ignored unusable public key offered for {identity_hash_hex[:10]}")
            return False
        if reconstructed.hash != identity_hash_bytes:
            logger.warning(f"Ignored public key offered for {identity_hash_hex[:10]}: it belongs to "
                           f"{reconstructed.hash.hex()[:10]}")
            return False

        known = self.db.get_public_key(identity_hash_hex)
        self.db.upsert_identity(identity_hash=identity_hash_hex, alias=alias, public_key=public_key)
        if not known:
            logger.info(f"Learned public key for {identity_hash_hex[:10]}")
            result.learned_identities.append(identity_hash_hex)
        return True

    def _apply_message(self, msg_tuple, result: FrameResult) -> bool:
        msg_id_hex, channel, sender_hash_hex, ts, content = (
            msg_tuple[0].hex(), msg_tuple[1], msg_tuple[2].hex(), msg_tuple[3], msg_tuple[4]
        )
        signature = msg_tuple[5] if len(msg_tuple) > 5 else b""
        if isinstance(channel, dict):
            channel = channel.get("name", "")

        if msg_id_hex in self.seen_msg_ids or self.db.has_message(msg_id_hex):
            return True

        # Blackholed authors are refused before verification: a blocked
        # spammer's records are perfectly well signed, so the signature check
        # would happily let them through.
        if self._refuse_blackholed(sender_hash_hex, f"message in #{channel}"):
            return False

        if not self.channel_permitted(str(channel)):
            logger.info(f"Discarded message {msg_id_hex[:10]}: channel #{channel} not permitted")
            return False

        # Only accepted ids are cached. Marking seen before verifying would let
        # anyone who has observed a public msg_id pre-send a frame with that id
        # and a bogus signature, permanently suppressing the legitimate record.
        ok = self.db.verify_and_add_message(
            msg_id=msg_id_hex, channel=str(channel), sender_hash=sender_hash_hex,
            content=content, timestamp=ts, signature=signature
        )
        if ok:
            self._mark_seen(msg_id_hex)
            result.accepted_msg_ids.append(msg_id_hex)
            return True

        if not self.db.get_public_key(sender_hash_hex):
            self._defer_message(sender_hash_hex, msg_tuple)
        else:
            logger.info(f"Discarded message {msg_id_hex[:10]} in #{channel} "
                        f"from {sender_hash_hex[:10]} (failed verification)")
        return False

    def _retry_deferred(self, sender_hash_hex: str, result: FrameResult):
        """Re-verifies records parked while `sender_hash_hex` was unknown."""
        for msg_tuple in self.deferred_messages.pop(sender_hash_hex, []):
            self._apply_message(msg_tuple, result)
