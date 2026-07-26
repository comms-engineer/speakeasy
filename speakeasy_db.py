import sqlite3
import threading
import time
import hashlib
import logging
from contextlib import contextmanager
from enum import Enum
from typing import Optional, Dict, List, Any, Iterable, Set
import RNS
import signing

logger = logging.getLogger("speakeasy_db")

DEFAULT_EPOCH_BUCKET_SEC = 300
EMPTY_MERKLE_ROOT = "00" * 32

# Records timestamped further ahead than this are rejected outright: a
# far-future timestamp would otherwise pin a message to the top of every
# ordering forever, and win every last-writer-wins profile comparison.
MAX_CLOCK_SKEW_SEC = 300

class BandwidthClass(Enum):
    LOW_MESH = "low_mesh"
    MEDIUM_MESH = "medium_mesh"
    HIGH_SPEED = "high_speed"

def merkle_root(leaf_ids: Iterable[str]) -> str:
    """
    Computes the epoch Merkle root over a set of record ids.

    Both federating peers must derive this identically, so the construction is
    fixed: leaves are sha256(id_bytes) over ids sorted ascending, internal
    nodes are sha256(left + right), a lone trailing node is duplicated, and an
    empty set yields EMPTY_MERKLE_ROOT.
    """
    leaves = []
    for record_id in sorted(leaf_ids):
        try:
            raw = bytes.fromhex(record_id)
        except (ValueError, TypeError):
            raw = str(record_id).encode("utf-8")
        leaves.append(hashlib.sha256(raw).digest())

    if not leaves:
        return EMPTY_MERKLE_ROOT

    level = leaves
    while len(level) > 1:
        if len(level) % 2:
            level.append(level[-1])
        level = [
            hashlib.sha256(level[i] + level[i + 1]).digest()
            for i in range(0, len(level), 2)
        ]
    return level[0].hex()

class SpeakeasyDB:
    def __init__(self, db_path: str = "speakeasy.db",
                 epoch_bucket_sec: int = DEFAULT_EPOCH_BUCKET_SEC,
                 max_message_bytes: int = 0):
        self.db_path = db_path
        self.epoch_bucket_sec = max(1, int(epoch_bucket_sec))
        self.max_message_bytes = int(max_message_bytes)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(db_path, check_same_thread=False, timeout=10.0)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA busy_timeout=10000")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._init_db()

    @contextmanager
    def _tx(self):
        """Yields a cursor inside a locked transaction. RNS delivers packet
        callbacks on its own threads, so every statement is serialized."""
        with self._lock:
            cursor = self._conn.cursor()
            try:
                yield cursor
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise
            finally:
                cursor.close()

    def close(self):
        with self._lock:
            self._conn.close()

    def epoch_for(self, timestamp: float) -> int:
        return int(float(timestamp) // self.epoch_bucket_sec)

    def current_epoch(self) -> int:
        return self.epoch_for(time.time())

    def _epoch_bounds(self, epoch: int) -> tuple:
        start = int(epoch) * self.epoch_bucket_sec
        return start, start + self.epoch_bucket_sec

    def _init_db(self):
        with self._tx() as cursor:
            # Identities table (stores public keys registered via link callbacks or manual inserts)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS identities (
                    identity_hash TEXT PRIMARY KEY,
                    alias TEXT,
                    public_key BLOB,
                    updated_at REAL
                )
            """)

            # Profiles table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS profiles (
                    identity_hash TEXT PRIMARY KEY,
                    handle TEXT,
                    status TEXT,
                    bio TEXT,
                    public_key BLOB,
                    signature BLOB,
                    edited_at REAL
                )
            """)

            # Messages table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS messages (
                    msg_id TEXT PRIMARY KEY,
                    channel TEXT,
                    sender_hash TEXT,
                    content TEXT,
                    timestamp REAL,
                    signature BLOB
                )
            """)

            # Bulletins table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bulletins (
                    bulletin_id TEXT PRIMARY KEY,
                    title TEXT,
                    body TEXT,
                    author_hash TEXT,
                    timestamp REAL,
                    signature BLOB
                )
            """)

            # Channels table. approver_hash/signature are populated for
            # channels approved by a hub operator; locally seeded channels
            # leave them NULL and are never gossiped.
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS channels (
                    name TEXT PRIMARY KEY,
                    description TEXT,
                    created_at REAL,
                    approver_hash TEXT,
                    signature BLOB
                )
            """)
            existing_cols = {row[1] for row in cursor.execute("PRAGMA table_info(channels)")}
            for column, decl in (("approver_hash", "TEXT"), ("signature", "BLOB")):
                if column not in existing_cols:
                    cursor.execute(f"ALTER TABLE channels ADD COLUMN {column} {decl}")

            # Channel creation requests awaiting an operator decision.
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS channel_requests (
                    name TEXT PRIMARY KEY,
                    description TEXT,
                    requester_hash TEXT,
                    requested_at REAL,
                    status TEXT,
                    decided_at REAL
                )
            """)

            # Epoch sync scans messages by (channel, timestamp) range on every
            # anti-entropy round; the bulletin index backs the board view.
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_messages_channel_ts ON messages (channel, timestamp)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_bulletins_ts ON bulletins (timestamp DESC)")

            # Seed default channel if empty
            cursor.execute("SELECT COUNT(*) FROM channels")
            if cursor.fetchone()[0] == 0:
                cursor.execute(
                    "INSERT INTO channels (name, description, created_at) VALUES (?, ?, ?)",
                    ("general", "General discussion channel", time.time())
                )

    # ----------------------------------------------------------------------
    # Identity & Profile Management
    # ----------------------------------------------------------------------

    def upsert_identity(self, identity_hash: str, alias: str = "", public_key: Optional[bytes] = None) -> bool:
        pk_blob = self._as_blob(public_key)
        with self._tx() as cursor:
            cursor.execute("""
                INSERT INTO identities (identity_hash, alias, public_key, updated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(identity_hash) DO UPDATE SET
                    alias = CASE WHEN excluded.alias != '' THEN excluded.alias ELSE identities.alias END,
                    public_key = COALESCE(excluded.public_key, identities.public_key),
                    updated_at = excluded.updated_at
            """, (identity_hash, alias, pk_blob, time.time()))
        return True

    @staticmethod
    def _as_blob(value) -> Optional[bytes]:
        if isinstance(value, bytes):
            return value
        if isinstance(value, str):
            try:
                return bytes.fromhex(value)
            except ValueError:
                return None
        return None

    def get_identity_record(self, identity_hash: str) -> Optional[Dict[str, Any]]:
        """Identity row including public key bytes, or None when unknown."""
        with self._tx() as cursor:
            cursor.execute("SELECT * FROM identities WHERE identity_hash = ?", (identity_hash,))
            row = cursor.fetchone()
        if not row:
            return None
        record = dict(row)
        record["public_key"] = self._as_blob(record.get("public_key")) or record.get("public_key")
        return record

    def get_recent_identity_hashes(self, limit: int = 100) -> List[str]:
        """Most recently seen identities that carry a public key."""
        with self._tx() as cursor:
            cursor.execute("""
                SELECT identity_hash FROM identities
                WHERE public_key IS NOT NULL
                ORDER BY updated_at DESC
                LIMIT ?
            """, (limit,))
            return [row[0] for row in cursor.fetchall()]

    def resolve_identity(self, needle: str) -> Optional[str]:
        """
        Finds an identity hash from a hash prefix or a handle/alias.

        Blocking someone is done from what the user can actually see on screen
        -- a handle, or the short hash shown next to it -- not from a full
        32-character hash they would have to transcribe.
        """
        candidate = (needle or "").strip().lstrip("<").rstrip(">").lower()
        if not candidate:
            return None
        with self._tx() as cursor:
            cursor.execute("""
                SELECT identity_hash FROM identities WHERE identity_hash LIKE ? || '%'
                UNION
                SELECT identity_hash FROM profiles WHERE identity_hash LIKE ? || '%'
            """, (candidate, candidate))
            rows = [row[0] for row in cursor.fetchall()]
            if len(rows) == 1:
                return rows[0]
            if len(rows) > 1:
                return None

            cursor.execute("""
                SELECT identity_hash FROM profiles WHERE LOWER(handle) = ?
                UNION
                SELECT identity_hash FROM identities WHERE LOWER(alias) = ?
            """, (candidate, candidate))
            rows = [row[0] for row in cursor.fetchall()]
        return rows[0] if len(rows) == 1 else None

    def get_public_key(self, identity_hash: str) -> Optional[bytes]:
        """
        Queries the identities table first, then profiles, for stored public key bytes.
        Returns raw bytes or None if not found.
        """
        with self._tx() as cursor:
            for table in ("identities", "profiles"):
                cursor.execute(f"SELECT public_key FROM {table} WHERE identity_hash = ?", (identity_hash,))
                row = cursor.fetchone()
                if row and row[0]:
                    val = row[0]
                    return bytes.fromhex(val) if isinstance(val, str) else bytes(val)
        return None

    def upsert_profile(self, identity_hash: str, handle: str, status: str = "", bio: str = "",
                       public_key: Optional[bytes] = None, signature: Optional[bytes] = None,
                       edited_at: Optional[float] = None) -> bool:
        edited_at = edited_at or time.time()
        pk_blob = self._as_blob(public_key)
        with self._tx() as cursor:
            cursor.execute("""
                INSERT INTO profiles (identity_hash, handle, status, bio, public_key, signature, edited_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(identity_hash) DO UPDATE SET
                    handle = excluded.handle,
                    status = excluded.status,
                    bio = excluded.bio,
                    public_key = COALESCE(excluded.public_key, profiles.public_key),
                    signature = excluded.signature,
                    edited_at = excluded.edited_at
            """, (identity_hash, handle, status, bio, pk_blob, signature, edited_at))
        return True

    def find_profile(self, identity_hash: str) -> Optional[Dict[str, Any]]:
        """Returns the stored profile row, or None when the identity is unknown."""
        with self._tx() as cursor:
            cursor.execute("SELECT * FROM profiles WHERE identity_hash = ?", (identity_hash,))
            row = cursor.fetchone()
        if not row:
            return None
        record = dict(row)
        if isinstance(record.get("public_key"), str):
            record["public_key"] = bytes.fromhex(record["public_key"])
        return record

    def get_profile(self, identity_hash: str) -> Dict[str, Any]:
        """Profile row for `identity_hash`, or a placeholder for unknown identities."""
        return self.find_profile(identity_hash) or {
            "identity_hash": identity_hash,
            "handle": identity_hash[:10],
            "status": "",
            "bio": "",
            "public_key": None,
            "signature": None,
            "edited_at": 0.0
        }

    def sign_and_upsert_profile(self, identity: RNS.Identity, handle: str, status: str = "", bio: str = "") -> Dict[str, Any]:
        identity_hash = identity.hash.hex()
        edited_at = time.time()
        canonical = signing.canonical_profile_bytes(identity_hash, handle, status, bio, edited_at)
        sig = signing.sign_bytes(identity, canonical)
        pub_key = identity.get_public_key()

        self.upsert_profile(
            identity_hash=identity_hash,
            handle=handle,
            status=status,
            bio=bio,
            public_key=pub_key,
            signature=sig,
            edited_at=edited_at
        )
        return {
            "identity_hash": identity_hash,
            "handle": handle,
            "status": status,
            "bio": bio,
            "edited_at": edited_at,
            "signature": sig,
            "public_key": pub_key
        }

    def verify_and_upsert_profile(self, identity_hash: str, handle: str, status: str, bio: str,
                                   edited_at: float, signature: bytes) -> bool:
        pub_key = self.get_public_key(identity_hash)

        try:
            signer_bytes = bytes.fromhex(identity_hash)
        except (ValueError, TypeError):
            return False

        if float(edited_at) > time.time() + MAX_CLOCK_SKEW_SEC:
            logger.warning(f"Rejected profile sync for {identity_hash[:10]}: edited_at too far in the future")
            return False

        canonical = signing.canonical_profile_bytes(identity_hash, handle, status, bio, edited_at)
        if not signing.verify_bytes(signer_bytes, signature, canonical, public_key_bytes=pub_key):
            logger.warning(f"Rejected profile sync for {identity_hash[:10]}: signature invalid")
            return False

        current_edited_at = self.get_profile(identity_hash).get("edited_at") or 0.0
        if edited_at <= current_edited_at:
            logger.info(f"Ignored stale profile sync for {identity_hash[:10]} "
                        f"(incoming edited_at={edited_at} <= stored={current_edited_at})")
            return False

        self.upsert_profile(
            identity_hash=identity_hash,
            handle=handle,
            status=status,
            bio=bio,
            public_key=pub_key,
            signature=signature,
            edited_at=edited_at
        )
        return True

    # ----------------------------------------------------------------------
    # Message Management
    # ----------------------------------------------------------------------

    def add_message(self, msg_id: str, channel: str, sender_hash: str, content: str,
                    timestamp: float, signature: bytes) -> bool:
        try:
            with self._tx() as cursor:
                cursor.execute("""
                    INSERT INTO messages (msg_id, channel, sender_hash, content, timestamp, signature)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (msg_id, channel, sender_hash, content, timestamp, signature))
            return True
        except sqlite3.IntegrityError:
            return False

    def get_message(self, msg_id: str) -> Optional[Dict[str, Any]]:
        with self._tx() as cursor:
            cursor.execute("SELECT * FROM messages WHERE msg_id = ?", (msg_id,))
            row = cursor.fetchone()
        return dict(row) if row else None

    def has_message(self, msg_id: str) -> bool:
        with self._tx() as cursor:
            cursor.execute("SELECT 1 FROM messages WHERE msg_id = ?", (msg_id,))
            return cursor.fetchone() is not None

    def sign_and_insert_message(self, identity: RNS.Identity, channel: str, content: str) -> Optional[Dict[str, Any]]:
        if self.max_message_bytes and len(content.encode("utf-8")) > self.max_message_bytes:
            logger.warning(f"Refused to sign oversized message for #{channel} "
                           f"({len(content.encode('utf-8'))} > {self.max_message_bytes} bytes)")
            return None

        sender_hash = identity.hash.hex()
        ts = time.time()
        raw_id_data = f"{channel}:{sender_hash}:{content}:{ts}:{time.time_ns()}"
        msg_id = hashlib.sha256(raw_id_data.encode("utf-8")).hexdigest()

        canonical = signing.canonical_message_bytes(msg_id, channel, sender_hash, ts, content)
        sig = signing.sign_bytes(identity, canonical)

        ok = self.add_message(msg_id=msg_id, channel=channel, sender_hash=sender_hash,
                               content=content, timestamp=ts, signature=sig)
        if not ok:
            return None
        return {
            "msg_id": msg_id,
            "channel": channel,
            "sender_hash": sender_hash,
            "timestamp": ts,
            "content": content,
            "signature": sig
        }

    def verify_and_add_message(self, msg_id: str, channel: str, sender_hash: str,
                                content: str, timestamp: float, signature: bytes) -> bool:
        try:
            signer_bytes = bytes.fromhex(sender_hash)
        except (ValueError, TypeError):
            return False

        if self.max_message_bytes and len(str(content).encode("utf-8")) > self.max_message_bytes:
            logger.warning(f"Rejected message {msg_id[:10]} in #{channel}: content exceeds max_message_bytes")
            return False

        if float(timestamp) > time.time() + MAX_CLOCK_SKEW_SEC:
            logger.warning(f"Rejected message {msg_id[:10]} in #{channel}: timestamp too far in the future")
            return False

        canonical = signing.canonical_message_bytes(msg_id, channel, sender_hash, timestamp, content)
        if not signing.verify_bytes(signer_bytes, signature, canonical, public_key_bytes=self.get_public_key(sender_hash)):
            logger.warning(f"Rejected message {msg_id[:10]} in #{channel} from {sender_hash[:10]}: signature invalid")
            return False

        return self.add_message(msg_id=msg_id, channel=channel, sender_hash=sender_hash,
                                 content=content, timestamp=timestamp, signature=signature)

    def get_messages(self, channel: str, limit: int = 100, since: float = 0.0) -> List[Dict[str, Any]]:
        with self._tx() as cursor:
            cursor.execute("""
                SELECT * FROM messages
                WHERE channel = ? AND timestamp > ?
                ORDER BY timestamp ASC
                LIMIT ?
            """, (channel, since, limit))
            return [dict(row) for row in cursor.fetchall()]

    # ----------------------------------------------------------------------
    # Epoch Anti-Entropy
    # ----------------------------------------------------------------------

    def get_epoch_message_ids(self, channel: str, epoch: int) -> List[str]:
        """Message ids held locally for `channel` within `epoch`, sorted ascending."""
        start, end = self._epoch_bounds(epoch)
        with self._tx() as cursor:
            cursor.execute("""
                SELECT msg_id FROM messages
                WHERE channel = ? AND timestamp >= ? AND timestamp < ?
                ORDER BY msg_id ASC
            """, (channel, start, end))
            return [row[0] for row in cursor.fetchall()]

    def get_epoch_merkle_root(self, channel: str, epoch: int) -> str:
        """Hex Merkle root summarizing one channel-epoch, for cheap divergence detection."""
        return merkle_root(self.get_epoch_message_ids(channel, epoch))

    def get_missing_messages(self, channel: str, epoch: int, remote_known_ids: Set[str]) -> List[Dict[str, Any]]:
        """Full local rows in `channel`/`epoch` that the requesting peer lacks."""
        known = set(remote_known_ids or ())
        start, end = self._epoch_bounds(epoch)
        with self._tx() as cursor:
            cursor.execute("""
                SELECT * FROM messages
                WHERE channel = ? AND timestamp >= ? AND timestamp < ?
                ORDER BY timestamp ASC
            """, (channel, start, end))
            return [dict(row) for row in cursor.fetchall() if row["msg_id"] not in known]

    def get_populated_epochs(self, channels: Iterable[str], since_epoch: int,
                             limit: int = 48, offset: int = 0) -> List[tuple]:
        """
        (channel, epoch) pairs at or after `since_epoch` that actually hold
        messages, most recent first.

        Anti-entropy walks this rather than every epoch in the retention
        window: at a 300s bucket, two weeks is 4032 epochs, almost all of them
        empty on a quiet mesh, and asking about empty epochs costs a Merkle
        root on the wire for no possible divergence.
        """
        names = list(dict.fromkeys(channels))
        if not names:
            return []
        placeholders = ",".join("?" * len(names))
        with self._tx() as cursor:
            cursor.execute(f"""
                SELECT channel, CAST(timestamp / ? AS INTEGER) AS epoch
                FROM messages
                WHERE channel IN ({placeholders}) AND timestamp >= ?
                GROUP BY channel, epoch
                ORDER BY epoch DESC, channel ASC
                LIMIT ? OFFSET ?
            """, (self.epoch_bucket_sec, *names, int(since_epoch) * self.epoch_bucket_sec,
                  int(limit), int(offset)))
            return [(row[0], int(row[1])) for row in cursor.fetchall()]

    # ----------------------------------------------------------------------
    # Retention
    # ----------------------------------------------------------------------

    def prune_messages(self, ttl_days: float) -> int:
        """Deletes messages older than `ttl_days`; returns the number removed."""
        if not ttl_days or ttl_days <= 0:
            return 0
        cutoff = time.time() - (float(ttl_days) * 86400)
        with self._tx() as cursor:
            cursor.execute("DELETE FROM messages WHERE timestamp < ?", (cutoff,))
            return cursor.rowcount

    def prune_channel_overflow(self, max_per_channel: int) -> int:
        """
        Caps each channel at its `max_per_channel` newest messages.

        A time-based TTL alone does not bound anything: one busy channel can
        fill a Pi's SD card well inside the retention window. Pruning per
        channel rather than globally keeps a quiet channel's history from being
        evicted by a noisy one.
        """
        if not max_per_channel or max_per_channel <= 0:
            return 0
        removed = 0
        with self._tx() as cursor:
            cursor.execute("""
                SELECT channel FROM messages
                GROUP BY channel HAVING COUNT(*) > ?
            """, (int(max_per_channel),))
            channels = [row[0] for row in cursor.fetchall()]
            for channel in channels:
                cursor.execute("""
                    DELETE FROM messages WHERE msg_id IN (
                        SELECT msg_id FROM messages WHERE channel = ?
                        ORDER BY timestamp DESC LIMIT -1 OFFSET ?
                    )
                """, (channel, int(max_per_channel)))
                removed += cursor.rowcount
        return removed

    def db_size_bytes(self) -> int:
        """On-disk size of the database, including as-yet-unreclaimed pages."""
        with self._tx() as cursor:
            page_count = cursor.execute("PRAGMA page_count").fetchone()[0]
            page_size = cursor.execute("PRAGMA page_size").fetchone()[0]
        return int(page_count) * int(page_size)

    def enforce_size_limit(self, max_bytes: int, batch: int = 200) -> int:
        """
        Drops the oldest messages until the database fits `max_bytes`.

        This is the backstop that makes a hub safe to run unattended on a small
        device: whatever the TTL and per-channel caps allow, the node still
        degrades by shedding the oldest history instead of filling the disk.
        Pages freed by a DELETE stay in the file until VACUUM, so each batch is
        vacuumed before the size is re-measured -- otherwise the loop sees no
        progress and deletes far more history than the limit requires.
        """
        if not max_bytes or max_bytes <= 0:
            return 0

        removed = 0
        while self.db_size_bytes() > max_bytes:
            with self._tx() as cursor:
                cursor.execute("""
                    DELETE FROM messages WHERE msg_id IN (
                        SELECT msg_id FROM messages ORDER BY timestamp ASC LIMIT ?
                    )
                """, (int(batch),))
                deleted = cursor.rowcount
            if not deleted:
                # Nothing left to shed: the remaining size is schema, indices
                # and non-message tables, so stop rather than spin.
                logger.warning(
                    f"Database is {self.db_size_bytes()} bytes with no prunable messages left, "
                    f"over the {max_bytes} byte limit."
                )
                break
            removed += deleted
            self.vacuum()
        return removed

    def purge_identity(self, identity_hash: str) -> int:
        """
        Removes everything authored by an identity, for use when a user
        blackholes a spammer: blocking future traffic is not much use if the
        flood they already sent stays on screen. The identity's public key is
        kept, so records that arrive before the block propagates are still
        recognised (and then refused) rather than triggering identity requests.
        """
        with self._tx() as cursor:
            cursor.execute("DELETE FROM messages WHERE sender_hash = ?", (identity_hash,))
            removed = cursor.rowcount
            cursor.execute("DELETE FROM bulletins WHERE author_hash = ?", (identity_hash,))
            removed += cursor.rowcount
            cursor.execute("DELETE FROM profiles WHERE identity_hash = ?", (identity_hash,))
        return removed

    def vacuum(self):
        """Returns free pages to the filesystem. Cannot run inside a transaction."""
        with self._lock:
            self._conn.commit()
            self._conn.execute("VACUUM")

    # ----------------------------------------------------------------------
    # Bulletin Management
    # ----------------------------------------------------------------------

    def add_bulletin(self, title: str, body: str, author_hash: str, timestamp: float,
                     bulletin_id: str, signature: bytes) -> bool:
        try:
            with self._tx() as cursor:
                cursor.execute("""
                    INSERT INTO bulletins (bulletin_id, title, body, author_hash, timestamp, signature)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (bulletin_id, title, body, author_hash, timestamp, signature))
            return True
        except sqlite3.IntegrityError:
            return False

    def sign_and_add_bulletin(self, identity: RNS.Identity, title: str, body: str) -> Optional[Dict[str, Any]]:
        author_hash = identity.hash.hex()
        ts = time.time()
        raw_id_data = f"{title}:{author_hash}:{ts}:{time.time_ns()}"
        bulletin_id = hashlib.sha256(raw_id_data.encode("utf-8")).hexdigest()

        canonical = signing.canonical_bulletin_bytes(bulletin_id, title, body, author_hash, ts)
        sig = signing.sign_bytes(identity, canonical)

        ok = self.add_bulletin(title=title, body=body, author_hash=author_hash,
                                timestamp=ts, bulletin_id=bulletin_id, signature=sig)
        if not ok:
            return None
        return {
            "bulletin_id": bulletin_id,
            "title": title,
            "body": body,
            "author_hash": author_hash,
            "timestamp": ts,
            "signature": sig
        }

    def verify_and_add_bulletin(self, bulletin_id: str, title: str, body: str,
                                 author_hash: str, timestamp: float, signature: bytes) -> bool:
        try:
            signer_bytes = bytes.fromhex(author_hash)
        except (ValueError, TypeError):
            return False

        if float(timestamp) > time.time() + MAX_CLOCK_SKEW_SEC:
            logger.warning(f"Rejected bulletin '{str(title)[:30]}': timestamp too far in the future")
            return False

        canonical = signing.canonical_bulletin_bytes(bulletin_id, title, body, author_hash, timestamp)
        if not signing.verify_bytes(signer_bytes, signature, canonical, public_key_bytes=self.get_public_key(author_hash)):
            logger.warning(f"Rejected bulletin '{str(title)[:30]}' from {author_hash[:10]}: signature invalid")
            return False

        return self.add_bulletin(title=title, body=body, author_hash=author_hash,
                                 timestamp=timestamp, bulletin_id=bulletin_id, signature=signature)

    def get_bulletins(self, limit: int = 50) -> List[Dict[str, Any]]:
        with self._tx() as cursor:
            cursor.execute("""
                SELECT
                    b.*,
                    COALESCE(i.alias, p.handle) AS alias
                FROM bulletins b
                LEFT JOIN identities i ON b.author_hash = i.identity_hash
                LEFT JOIN profiles p ON b.author_hash = p.identity_hash
                ORDER BY b.timestamp DESC
                LIMIT ?
            """, (limit,))
            return [dict(row) for row in cursor.fetchall()]

    # ----------------------------------------------------------------------
    # Channel Management
    # ----------------------------------------------------------------------

    def get_channels(self) -> List[Dict[str, Any]]:
        """Returns all channel records as dictionaries for UI composition."""
        with self._tx() as cursor:
            cursor.execute("SELECT * FROM channels ORDER BY name ASC")
            return [dict(row) for row in cursor.fetchall()]

    def get_channel(self, name: str) -> Optional[Dict[str, Any]]:
        with self._tx() as cursor:
            cursor.execute("SELECT * FROM channels WHERE name = ?", (name,))
            row = cursor.fetchone()
        return dict(row) if row else None

    def get_signed_channels(self) -> List[Dict[str, Any]]:
        """Operator-approved channels, i.e. the ones that can be federated."""
        with self._tx() as cursor:
            cursor.execute("SELECT * FROM channels WHERE signature IS NOT NULL ORDER BY name ASC")
            return [dict(row) for row in cursor.fetchall()]

    def sign_and_add_channel(self, identity: RNS.Identity, name: str,
                             description: str = "") -> Optional[Dict[str, Any]]:
        """Approves a channel under the operator's hub identity, making it federatable."""
        approver_hash = identity.hash.hex()
        created_at = time.time()
        canonical = signing.canonical_channel_bytes(name, description, approver_hash, created_at)
        sig = signing.sign_bytes(identity, canonical)

        # Peers verify the approval against this key, so it has to travel with
        # the channel; a hub that never recorded its own key cannot ship it.
        self.upsert_identity(identity_hash=approver_hash, public_key=identity.get_public_key())

        with self._tx() as cursor:
            cursor.execute("""
                INSERT INTO channels (name, description, created_at, approver_hash, signature)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(name) DO UPDATE SET
                    description = excluded.description,
                    created_at = excluded.created_at,
                    approver_hash = excluded.approver_hash,
                    signature = excluded.signature
            """, (name, description, created_at, approver_hash, sig))

        return {
            "name": name,
            "description": description,
            "created_at": created_at,
            "approver_hash": approver_hash,
            "signature": sig,
        }

    def verify_and_add_channel(self, name: str, description: str, approver_hash: str,
                               created_at: float, signature: bytes,
                               public_key: Optional[bytes] = None) -> bool:
        """
        Stores a channel approved by a remote hub operator, if the approval
        signature checks out. Returns False when the channel is already known
        with an equal-or-newer approval, so replays are idempotent.
        """
        try:
            signer_bytes = bytes.fromhex(approver_hash)
        except (ValueError, TypeError):
            return False

        if float(created_at) > time.time() + MAX_CLOCK_SKEW_SEC:
            logger.warning(f"Rejected channel #{name}: created_at too far in the future")
            return False

        key = public_key or self.get_public_key(approver_hash)
        canonical = signing.canonical_channel_bytes(name, description, approver_hash, created_at)
        if not signing.verify_bytes(signer_bytes, signature, canonical, public_key_bytes=key):
            logger.warning(f"Rejected channel #{name} approved by {approver_hash[:10]}: signature invalid")
            return False

        existing = self.get_channel(name)
        if existing and existing.get("signature") and float(existing.get("created_at") or 0) >= float(created_at):
            return False

        with self._tx() as cursor:
            cursor.execute("""
                INSERT INTO channels (name, description, created_at, approver_hash, signature)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(name) DO UPDATE SET
                    description = excluded.description,
                    created_at = excluded.created_at,
                    approver_hash = excluded.approver_hash,
                    signature = excluded.signature
            """, (name, description, created_at, approver_hash, signature))
        return True

    # ----------------------------------------------------------------------
    # Channel Requests
    # ----------------------------------------------------------------------

    def add_channel_request(self, name: str, description: str, requester_hash: str) -> bool:
        """Queues a channel proposal for the operator. Re-requests are no-ops."""
        if self.get_channel(name):
            return False
        with self._tx() as cursor:
            cursor.execute("SELECT status FROM channel_requests WHERE name = ?", (name,))
            row = cursor.fetchone()
            if row and row[0] == "pending":
                return False
            cursor.execute("""
                INSERT INTO channel_requests (name, description, requester_hash, requested_at, status, decided_at)
                VALUES (?, ?, ?, ?, 'pending', NULL)
                ON CONFLICT(name) DO UPDATE SET
                    description = excluded.description,
                    requester_hash = excluded.requester_hash,
                    requested_at = excluded.requested_at,
                    status = 'pending',
                    decided_at = NULL
            """, (name, description, requester_hash, time.time()))
        return True

    def get_channel_requests(self, status: Optional[str] = "pending") -> List[Dict[str, Any]]:
        with self._tx() as cursor:
            if status:
                cursor.execute("SELECT * FROM channel_requests WHERE status = ? ORDER BY requested_at ASC", (status,))
            else:
                cursor.execute("SELECT * FROM channel_requests ORDER BY requested_at ASC")
            return [dict(row) for row in cursor.fetchall()]

    def set_channel_request_status(self, name: str, status: str) -> bool:
        with self._tx() as cursor:
            cursor.execute(
                "UPDATE channel_requests SET status = ?, decided_at = ? WHERE name = ?",
                (status, time.time(), name)
            )
            return cursor.rowcount > 0

    def get_channel_names(self) -> List[str]:
        """Returns a list of channel name strings for protocol sync frames."""
        return [ch["name"] for ch in self.get_channels()]

    def add_channel(self, name: str, description: str = "") -> bool:
        with self._tx() as cursor:
            try:
                cursor.execute("""
                    INSERT INTO channels (name, description, created_at)
                    VALUES (?, ?, ?)
                """, (name, description, time.time()))
                return True
            except sqlite3.IntegrityError:
                return False

    def get_channel_messages(self, channel_id: str, limit: int = 50) -> List[Dict[str, Any]]:
        """Fetch the most recent messages for a given channel, ordered chronologically with alias resolution."""
        query = """
            SELECT
                m.msg_id,
                m.channel,
                m.sender_hash,
                m.content,
                m.timestamp,
                COALESCE(i.alias, p.handle) AS alias
            FROM messages m
            LEFT JOIN identities i ON m.sender_hash = i.identity_hash
            LEFT JOIN profiles p ON m.sender_hash = p.identity_hash
            WHERE m.channel = ?
            ORDER BY m.timestamp ASC
            LIMIT ?
        """
        with self._tx() as cursor:
            cursor.execute(query, (channel_id, limit))
            return [dict(row) for row in cursor.fetchall()]
