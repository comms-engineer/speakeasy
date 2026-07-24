import sqlite3
import time
import hashlib
import logging
from enum import Enum
from typing import Optional, Dict, List, Any
import RNS
import signing

logger = logging.getLogger("speakeasy_db")

class BandwidthClass(Enum):
    LOW_MESH = "low_mesh"
    MEDIUM_MESH = "medium_mesh"
    HIGH_SPEED = "high_speed"

class SpeakeasyDB:
    def __init__(self, db_path: str = "speakeasy.db"):
        self.db_path = db_path
        self._init_db()

    def _get_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self):
        with self._get_connection() as conn:
            cursor = conn.cursor()

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

            # Channels table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS channels (
                    name TEXT PRIMARY KEY,
                    description TEXT,
                    created_at REAL
                )
            """)

            # Seed default channel if empty
            cursor.execute("SELECT COUNT(*) FROM channels")
            if cursor.fetchone()[0] == 0:
                cursor.execute(
                    "INSERT INTO channels (name, description, created_at) VALUES (?, ?, ?)",
                    ("general", "General discussion channel", time.time())
                )

            conn.commit()

    # ----------------------------------------------------------------------
    # Identity & Profile Management
    # ----------------------------------------------------------------------

    def upsert_identity(self, identity_hash: str, alias: str = "", public_key: Optional[bytes] = None) -> bool:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            pk_blob = (
                public_key if isinstance(public_key, bytes)
                else (bytes.fromhex(public_key) if isinstance(public_key, str) else None)
            )
            cursor.execute("""
                INSERT INTO identities (identity_hash, alias, public_key, updated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(identity_hash) DO UPDATE SET
                    alias = CASE WHEN excluded.alias != '' THEN excluded.alias ELSE identities.alias END,
                    public_key = COALESCE(excluded.public_key, identities.public_key),
                    updated_at = excluded.updated_at
            """, (identity_hash, alias, pk_blob, time.time()))
            conn.commit()
            return True

    def get_public_key(self, identity_hash: str) -> Optional[bytes]:
        """
        Queries the identities table first, then profiles, for stored public key bytes.
        Returns raw bytes or None if not found.
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT public_key FROM identities WHERE identity_hash = ?", (identity_hash,))
            row = cursor.fetchone()
            if row and row[0]:
                val = row[0]
                return bytes.fromhex(val) if isinstance(val, str) else bytes(val)

            cursor.execute("SELECT public_key FROM profiles WHERE identity_hash = ?", (identity_hash,))
            row = cursor.fetchone()
            if row and row[0]:
                val = row[0]
                return bytes.fromhex(val) if isinstance(val, str) else bytes(val)
        return None

    def upsert_profile(self, identity_hash: str, handle: str, status: str = "", bio: str = "",
                       public_key: Optional[bytes] = None, signature: Optional[bytes] = None,
                       edited_at: Optional[float] = None) -> bool:
        edited_at = edited_at or time.time()
        pk_blob = (
            public_key if isinstance(public_key, bytes)
            else (bytes.fromhex(public_key) if isinstance(public_key, str) else None)
        )
        with self._get_connection() as conn:
            cursor = conn.cursor()
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
            conn.commit()
            return True

    def get_profile(self, identity_hash: str) -> Dict[str, Any]:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM profiles WHERE identity_hash = ?", (identity_hash,))
            row = cursor.fetchone()
            if row:
                d = dict(row)
                if d.get("public_key") and isinstance(d["public_key"], str):
                    d["public_key"] = bytes.fromhex(d["public_key"])
                return d
            return {
                "identity_hash": identity_hash,
                "handle": identity_hash[:10],
                "status": "",
                "bio": "",
                "public_key": None,
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

        canonical = signing.canonical_profile_bytes(identity_hash, handle, status, bio, edited_at)
        if not signing.verify_bytes(bytes.fromhex(identity_hash), signature, canonical, public_key_bytes=pub_key):
            logger.warning(f"Rejected profile sync for {identity_hash[:10]}: signature invalid")
            return False

        current = self.get_profile(identity_hash)
        current_edited_at = current.get("edited_at") or 0.0
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
        with self._get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute("""
                    INSERT INTO messages (msg_id, channel, sender_hash, content, timestamp, signature)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (msg_id, channel, sender_hash, content, timestamp, signature))
                conn.commit()
                return True
            except sqlite3.IntegrityError:
                return False

    def sign_and_insert_message(self, identity: RNS.Identity, channel: str, content: str) -> Optional[Dict[str, Any]]:
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
        pub_key = self.get_public_key(sender_hash)

        canonical = signing.canonical_message_bytes(msg_id, channel, sender_hash, timestamp, content)
        if not signing.verify_bytes(bytes.fromhex(sender_hash), signature, canonical, public_key_bytes=pub_key):
            logger.warning(f"Rejected message {msg_id[:10]} in #{channel} from {sender_hash[:10]}: signature invalid")
            return False

        return self.add_message(msg_id=msg_id, channel=channel, sender_hash=sender_hash,
                                 content=content, timestamp=timestamp, signature=signature)

    def get_messages(self, channel: str, limit: int = 100, since: float = 0.0) -> List[Dict[str, Any]]:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT * FROM messages
                WHERE channel = ? AND timestamp > ?
                ORDER BY timestamp ASC
                LIMIT ?
            """, (channel, since, limit))
            return [dict(row) for row in cursor.fetchall()]

    # ----------------------------------------------------------------------
    # Bulletin Management
    # ----------------------------------------------------------------------

    def add_bulletin(self, title: str, body: str, author_hash: str, timestamp: float,
                     bulletin_id: str, signature: bytes) -> bool:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute("""
                    INSERT INTO bulletins (bulletin_id, title, body, author_hash, timestamp, signature)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (bulletin_id, title, body, author_hash, timestamp, signature))
                conn.commit()
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
        pub_key = self.get_public_key(author_hash)

        canonical = signing.canonical_bulletin_bytes(bulletin_id, title, body, author_hash, timestamp)
        if not signing.verify_bytes(bytes.fromhex(author_hash), signature, canonical, public_key_bytes=pub_key):
            logger.warning(f"Rejected bulletin '{title[:30]}' from {author_hash[:10]}: signature invalid")
            return False

        return self.add_bulletin(title=title, body=body, author_hash=author_hash,
                                 timestamp=timestamp, bulletin_id=bulletin_id, signature=signature)

    def get_bulletins(self, limit: int = 50) -> List[Dict[str, Any]]:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT * FROM bulletins
                ORDER BY timestamp DESC
                LIMIT ?
            """, (limit,))
            return [dict(row) for row in cursor.fetchall()]

    # ----------------------------------------------------------------------
    # Channel Management
    # ----------------------------------------------------------------------

    def get_channels(self) -> List[Dict[str, Any]]:
        """Returns all channel records as dictionaries for UI composition."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM channels ORDER BY name ASC")
            return [dict(row) for row in cursor.fetchall()]

    def get_channel_names(self) -> List[str]:
        """Returns a list of channel name strings for protocol sync frames."""
        return [ch["name"] for ch in self.get_channels()]

    def add_channel(self, name: str, description: str = "") -> bool:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute("""
                    INSERT INTO channels (name, description, created_at)
                    VALUES (?, ?, ?)
                """, (name, description, time.time()))
                conn.commit()
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
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, (channel_id, limit))
            return [dict(row) for row in cursor.fetchall()]
