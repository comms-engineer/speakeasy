"""Database helpers for Speakeasy.

This module provides lightweight SQLite-backed calendar support for Phase 1.
The design intentionally includes ownership, channel scoping, and change-log
support so event edits and deletions can flow across federated nodes.
"""

from __future__ import annotations

import hashlib
import json
import logging
import sqlite3
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

import RNS
import signing


@dataclass(frozen=True)
class Calendar:
    """A collection of community events scoped to a chat channel."""

    calendar_id: str
    name: str
    description: Optional[str] = None
    owner_hash: Optional[str] = None
    visibility: str = "public"
    timezone: str = "UTC"
    channel: Optional[str] = None
    created_at: int = 0
    updated_at: int = 0

    def to_db_row(self) -> Tuple[Any, ...]:
        return (
            self.calendar_id,
            self.name,
            self.description,
            self.owner_hash,
            self.visibility,
            self.timezone,
            self.channel,
            self.created_at,
            self.updated_at,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "calendar_id": self.calendar_id,
            "name": self.name,
            "description": self.description,
            "owner_hash": self.owner_hash,
            "visibility": self.visibility,
            "timezone": self.timezone,
            "channel": self.channel,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }

    @classmethod
    def from_db_row(cls, row: Optional[Tuple[Any, ...]]) -> Optional["Calendar"]:
        if row is None:
            return None
        if isinstance(row, dict):
            row = (
                row.get("calendar_id"),
                row.get("name"),
                row.get("description"),
                row.get("owner_hash"),
                row.get("visibility"),
                row.get("timezone"),
                row.get("channel"),
                row.get("created_at"),
                row.get("updated_at"),
            )
        calendar_id, name, description, owner_hash, visibility, timezone, channel, created_at, updated_at = row
        return cls(
            calendar_id=calendar_id,
            name=name,
            description=description,
            owner_hash=owner_hash,
            visibility=visibility or "public",
            timezone=timezone or "UTC",
            channel=channel,
            created_at=created_at or 0,
            updated_at=updated_at or 0,
        )

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "Calendar":
        return cls(
            calendar_id=payload["calendar_id"],
            name=payload["name"],
            description=payload.get("description"),
            owner_hash=payload.get("owner_hash"),
            visibility=payload.get("visibility", "public"),
            timezone=payload.get("timezone", "UTC"),
            channel=payload.get("channel"),
            created_at=int(payload.get("created_at", 0)),
            updated_at=int(payload.get("updated_at", 0)),
        )


@dataclass(frozen=True)
class Event:
    """An individual event belonging to a calendar and channel."""

    event_id: str
    calendar_id: str
    title: str
    description: Optional[str] = None
    location: Optional[str] = None
    start_at: int = 0
    end_at: int = 0
    all_day: bool = False
    status: str = "scheduled"
    channel: Optional[str] = None
    created_by_hash: Optional[str] = None
    created_at: int = 0
    updated_at: int = 0
    deleted_at: Optional[int] = None
    revision: int = 1

    def to_db_row(self) -> Tuple[Any, ...]:
        return (
            self.event_id,
            self.calendar_id,
            self.title,
            self.description,
            self.location,
            self.start_at,
            self.end_at,
            int(self.all_day),
            self.status,
            self.channel,
            self.created_by_hash,
            self.created_at,
            self.updated_at,
            self.deleted_at,
            self.revision,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "calendar_id": self.calendar_id,
            "title": self.title,
            "description": self.description,
            "location": self.location,
            "start_at": self.start_at,
            "end_at": self.end_at,
            "all_day": self.all_day,
            "status": self.status,
            "channel": self.channel,
            "created_by_hash": self.created_by_hash,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "deleted_at": self.deleted_at,
            "revision": self.revision,
        }

    @classmethod
    def from_db_row(cls, row: Optional[Tuple[Any, ...]]) -> Optional["Event"]:
        if row is None:
            return None
        if isinstance(row, dict):
            row = (
                row.get("event_id"),
                row.get("calendar_id"),
                row.get("title"),
                row.get("description"),
                row.get("location"),
                row.get("start_at"),
                row.get("end_at"),
                row.get("all_day"),
                row.get("status"),
                row.get("channel"),
                row.get("created_by_hash"),
                row.get("created_at"),
                row.get("updated_at"),
                row.get("deleted_at"),
                row.get("revision"),
            )
        (
            event_id,
            calendar_id,
            title,
            description,
            location,
            start_at,
            end_at,
            all_day,
            status,
            channel,
            created_by_hash,
            created_at,
            updated_at,
            deleted_at,
            revision,
        ) = row
        return cls(
            event_id=event_id,
            calendar_id=calendar_id,
            title=title,
            description=description,
            location=location,
            start_at=start_at or 0,
            end_at=end_at or 0,
            all_day=bool(all_day),
            status=status or "scheduled",
            channel=channel,
            created_by_hash=created_by_hash,
            created_at=created_at or 0,
            updated_at=updated_at or 0,
            deleted_at=deleted_at,
            revision=revision or 1,
        )

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "Event":
        return cls(
            event_id=payload["event_id"],
            calendar_id=payload["calendar_id"],
            title=payload["title"],
            description=payload.get("description"),
            location=payload.get("location"),
            start_at=int(payload.get("start_at", 0)),
            end_at=int(payload.get("end_at", 0)),
            all_day=bool(payload.get("all_day", False)),
            status=payload.get("status", "scheduled"),
            channel=payload.get("channel"),
            created_by_hash=payload.get("created_by_hash"),
            created_at=int(payload.get("created_at", 0)),
            updated_at=int(payload.get("updated_at", 0)),
            deleted_at=payload.get("deleted_at"),
            revision=int(payload.get("revision", 1)),
        )


@dataclass(frozen=True)
class EventChange:
    """A federated change record for an event."""

    change_id: str
    event_id: str
    calendar_id: str
    operation: str
    revision: int
    payload: str
    created_at: int
    created_by_hash: Optional[str] = None

    def to_db_row(self) -> Tuple[Any, ...]:
        return (
            self.change_id,
            self.event_id,
            self.calendar_id,
            self.operation,
            self.revision,
            self.payload,
            self.created_at,
            self.created_by_hash,
        )

    @classmethod
    def from_db_row(cls, row: Optional[Tuple[Any, ...]]) -> Optional["EventChange"]:
        if row is None:
            return None
        if isinstance(row, dict):
            row = (
                row.get("change_id"),
                row.get("event_id"),
                row.get("calendar_id"),
                row.get("operation"),
                row.get("revision"),
                row.get("payload"),
                row.get("created_at"),
                row.get("created_by_hash"),
            )
        change_id, event_id, calendar_id, operation, revision, payload, created_at, created_by_hash = row
        return cls(
            change_id=change_id,
            event_id=event_id,
            calendar_id=calendar_id,
            operation=operation,
            revision=revision or 0,
            payload=payload or "{}",
            created_at=created_at or 0,
            created_by_hash=created_by_hash,
        )


def create_calendar_tables(connection: sqlite3.Connection) -> None:
    """Create the calendar, event, and event-change tables if they do not exist."""

    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS calendar (
            calendar_id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            description TEXT,
            owner_hash TEXT,
            visibility TEXT NOT NULL DEFAULT 'public',
            timezone TEXT NOT NULL DEFAULT 'UTC',
            channel TEXT,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS event (
            event_id TEXT PRIMARY KEY,
            calendar_id TEXT NOT NULL,
            title TEXT NOT NULL,
            description TEXT,
            location TEXT,
            start_at INTEGER NOT NULL,
            end_at INTEGER NOT NULL,
            all_day INTEGER NOT NULL DEFAULT 0,
            status TEXT NOT NULL DEFAULT 'scheduled',
            channel TEXT,
            created_by_hash TEXT,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL,
            deleted_at INTEGER,
            revision INTEGER NOT NULL DEFAULT 1,
            FOREIGN KEY (calendar_id) REFERENCES calendar(calendar_id) ON DELETE CASCADE
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS event_change (
            change_id TEXT PRIMARY KEY,
            event_id TEXT NOT NULL,
            calendar_id TEXT NOT NULL,
            operation TEXT NOT NULL,
            revision INTEGER NOT NULL,
            payload TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            created_by_hash TEXT,
            FOREIGN KEY (event_id) REFERENCES event(event_id) ON DELETE CASCADE
        )
        """
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_event_calendar_id ON event(calendar_id)"
    )
    connection.execute("CREATE INDEX IF NOT EXISTS idx_event_start_at ON event(start_at)")
    connection.execute("CREATE INDEX IF NOT EXISTS idx_event_channel ON event(channel)")
    connection.execute("CREATE INDEX IF NOT EXISTS idx_event_change_revision ON event_change(revision)")
    connection.commit()


class CalendarStore:
    """Shared calendar persistence helpers for SQLite-backed stores."""

    def __init__(self, db_path: str, *args: Any, **kwargs: Any) -> None:
        self.db_path = db_path
        self.connection = sqlite3.connect(db_path)
        self.connection.row_factory = sqlite3.Row
        create_calendar_tables(self.connection)

    def close(self) -> None:
        self.connection.close()

    def __enter__(self) -> "CalendarStore":
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.close()

    def create_calendar(self, calendar: Calendar) -> None:
        self.connection.execute(
            """
            INSERT INTO calendar (
                calendar_id, name, description, owner_hash, visibility, timezone, channel,
                created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            calendar.to_db_row(),
        )
        self.connection.commit()

    def get_calendar(self, calendar_id: str) -> Optional[Calendar]:
        row = self.connection.execute(
            "SELECT calendar_id, name, description, owner_hash, visibility, timezone, channel, created_at, updated_at FROM calendar WHERE calendar_id = ?",
            (calendar_id,),
        ).fetchone()
        return Calendar.from_db_row(row)

    def create_event(self, event: Event) -> Event:
        self.connection.execute(
            """
            INSERT INTO event (
                event_id, calendar_id, title, description, location, start_at, end_at, all_day,
                status, channel, created_by_hash, created_at, updated_at, deleted_at, revision
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            event.to_db_row(),
        )
        self._append_change(event, operation="create", created_by_hash=event.created_by_hash)
        self.connection.commit()
        return event

    def get_event(self, event_id: str) -> Optional[Event]:
        row = self.connection.execute(
            """
            SELECT event_id, calendar_id, title, description, location, start_at, end_at, all_day,
                   status, channel, created_by_hash, created_at, updated_at, deleted_at, revision
            FROM event WHERE event_id = ?
            """,
            (event_id,),
        ).fetchone()
        return Event.from_db_row(row)

    def list_events_for_channel(self, channel: str, include_deleted: bool = False) -> List[Event]:
        if include_deleted:
            rows = self.connection.execute(
                "SELECT event_id, calendar_id, title, description, location, start_at, end_at, all_day, status, channel, created_by_hash, created_at, updated_at, deleted_at, revision FROM event WHERE channel = ? ORDER BY start_at ASC",
                (channel,),
            ).fetchall()
        else:
            rows = self.connection.execute(
                "SELECT event_id, calendar_id, title, description, location, start_at, end_at, all_day, status, channel, created_by_hash, created_at, updated_at, deleted_at, revision FROM event WHERE channel = ? AND deleted_at IS NULL ORDER BY start_at ASC",
                (channel,),
            ).fetchall()
        events: List[Event] = []
        for row in rows:
            event = Event.from_db_row(row)
            if event is not None:
                events.append(event)
        return events

    def can_modify_event(self, actor_hash: Optional[str], event: Optional[Event]) -> bool:
        if not actor_hash or event is None:
            return False
        if event.created_by_hash == actor_hash:
            return True
        calendar = self.get_calendar(event.calendar_id)
        return bool(calendar and calendar.owner_hash == actor_hash)

    def update_event(self, event: Event, actor_hash: str) -> bool:
        current = self.get_event(event.event_id)
        if current is None or current.deleted_at is not None or not self.can_modify_event(actor_hash, current):
            return False

        updated = Event(
            event_id=current.event_id,
            calendar_id=current.calendar_id,
            title=event.title,
            description=event.description if event.description is not None else current.description,
            location=event.location if event.location is not None else current.location,
            start_at=event.start_at or current.start_at,
            end_at=event.end_at or current.end_at,
            all_day=event.all_day,
            status=event.status or current.status,
            channel=current.channel,
            created_by_hash=current.created_by_hash,
            created_at=current.created_at,
            updated_at=int(event.updated_at or time.time()),
            deleted_at=current.deleted_at,
            revision=current.revision + 1,
        )
        self.connection.execute(
            """
            UPDATE event
            SET title = ?, description = ?, location = ?, start_at = ?, end_at = ?, all_day = ?,
                status = ?, updated_at = ?, deleted_at = ?, revision = ?
            WHERE event_id = ?
            """,
            (
                updated.title,
                updated.description,
                updated.location,
                updated.start_at,
                updated.end_at,
                int(updated.all_day),
                updated.status,
                updated.updated_at,
                updated.deleted_at,
                updated.revision,
                updated.event_id,
            ),
        )
        self._append_change(updated, operation="update", created_by_hash=actor_hash)
        self.connection.commit()
        return True

    def delete_event(self, event_id: str, actor_hash: str) -> bool:
        current = self.get_event(event_id)
        if current is None or current.deleted_at is not None or not self.can_modify_event(actor_hash, current):
            return False

        deleted = Event(
            event_id=current.event_id,
            calendar_id=current.calendar_id,
            title=current.title,
            description=current.description,
            location=current.location,
            start_at=current.start_at,
            end_at=current.end_at,
            all_day=current.all_day,
            status="deleted",
            channel=current.channel,
            created_by_hash=current.created_by_hash,
            created_at=current.created_at,
            updated_at=int(time.time()),
            deleted_at=int(time.time()),
            revision=current.revision + 1,
        )
        self.connection.execute(
            """
            UPDATE event
            SET status = ?, updated_at = ?, deleted_at = ?, revision = ?
            WHERE event_id = ?
            """,
            (deleted.status, deleted.updated_at, deleted.deleted_at, deleted.revision, deleted.event_id),
        )
        self._append_change(deleted, operation="delete", created_by_hash=actor_hash)
        self.connection.commit()
        return True

    def get_event_changes(self, since_revision: Optional[int] = None, limit: int = 100) -> List[EventChange]:
        if since_revision is None:
            rows = self.connection.execute(
                "SELECT change_id, event_id, calendar_id, operation, revision, payload, created_at, created_by_hash FROM event_change ORDER BY created_at ASC, revision ASC LIMIT ?",
                (limit,),
            ).fetchall()
        else:
            rows = self.connection.execute(
                "SELECT change_id, event_id, calendar_id, operation, revision, payload, created_at, created_by_hash FROM event_change WHERE revision >= ? ORDER BY created_at ASC, revision ASC LIMIT ?",
                (since_revision, limit),
            ).fetchall()
        changes: List[EventChange] = []
        for row in rows:
            change = EventChange.from_db_row(row)
            if change is not None:
                changes.append(change)
        return changes

    def apply_event_change(self, change: EventChange) -> None:
        payload = json.loads(change.payload)
        event = Event.from_dict(payload)
        if change.operation == "delete":
            self.connection.execute(
                "UPDATE event SET status = ?, updated_at = ?, deleted_at = ?, revision = ? WHERE event_id = ?",
                (event.status, event.updated_at, event.deleted_at, event.revision, event.event_id),
            )
        elif self.get_event(event.event_id) is None:
            self.connection.execute(
                """
                INSERT INTO event (
                    event_id, calendar_id, title, description, location, start_at, end_at, all_day,
                    status, channel, created_by_hash, created_at, updated_at, deleted_at, revision
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                event.to_db_row(),
            )
        else:
            self.connection.execute(
                """
                UPDATE event
                SET title = ?, description = ?, location = ?, start_at = ?, end_at = ?, all_day = ?,
                    status = ?, channel = ?, created_by_hash = ?, created_at = ?, updated_at = ?,
                    deleted_at = ?, revision = ?
                WHERE event_id = ?
                """,
                (
                    event.title,
                    event.description,
                    event.location,
                    event.start_at,
                    event.end_at,
                    int(event.all_day),
                    event.status,
                    event.channel,
                    event.created_by_hash,
                    event.created_at,
                    event.updated_at,
                    event.deleted_at,
                    event.revision,
                    event.event_id,
                ),
            )
        self.connection.execute(
            "INSERT OR REPLACE INTO event_change (change_id, event_id, calendar_id, operation, revision, payload, created_at, created_by_hash) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            change.to_db_row(),
        )
        self.connection.commit()

    def _append_change(self, event: Event, operation: str, created_by_hash: Optional[str]) -> None:
        change = EventChange(
            change_id=f"{event.event_id}:{event.revision}:{operation}",
            event_id=event.event_id,
            calendar_id=event.calendar_id,
            operation=operation,
            revision=event.revision,
            payload=json.dumps(event.to_dict()),
            created_at=int(time.time()),
            created_by_hash=created_by_hash,
        )
        self.connection.execute(
            "INSERT INTO event_change (change_id, event_id, calendar_id, operation, revision, payload, created_at, created_by_hash) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            change.to_db_row(),
        )


__all__ = [
    "Calendar",
    "CalendarStore",
    "Event",
    "EventChange",
    "create_calendar_tables",
    "SpeakeasyDB",
]

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

class SpeakeasyDB(CalendarStore):
    def __init__(self, db_path: str = "speakeasy.db",
                 epoch_bucket_sec: int = DEFAULT_EPOCH_BUCKET_SEC,
                 max_message_bytes: int = 0):
        self.db_path = db_path
        self.epoch_bucket_sec = max(1, int(epoch_bucket_sec))
        self.max_message_bytes = int(max_message_bytes)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(db_path, check_same_thread=False, timeout=10.0)
        self.connection = self._conn
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
            for column, decl in (("approver_hash", "TEXT"), ("signature", "BLOB"), ("status", "TEXT")):
                if column not in existing_cols:
                    cursor.execute(f"ALTER TABLE channels ADD COLUMN {column} {decl}")
            cursor.execute("UPDATE channels SET status = 'active' WHERE status IS NULL OR status = ''")

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

            # Locally blocked identities. Kept here rather than in RNS's
            # node-wide blackhole table because that table belongs to the master
            # instance: a shared-instance client writing to it changes only its
            # own copy, and the next `rnpath -B` overwrites the shared file and
            # drops the entry. A user's block must survive that.
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS blocked_identities (
                    identity_hash TEXT PRIMARY KEY,
                    reason TEXT,
                    blocked_at REAL
                )
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS known_hosts (
                    hex_hash TEXT PRIMARY KEY,
                    alias TEXT,
                    hops INTEGER,
                    load INTEGER,
                    max_load INTEGER,
                    last_seen REAL,
                    is_manual INTEGER,
                    score REAL
                )
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS operator_actions (
                    action_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp REAL,
                    action TEXT,
                    target TEXT,
                    detail TEXT
                )
            """)

            # Client-side channel visibility preferences scoped to a host.
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS client_channel_prefs (
                    host_hash TEXT,
                    channel_name TEXT,
                    visible INTEGER,
                    updated_at REAL,
                    PRIMARY KEY (host_hash, channel_name)
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

            # Create the calendar tables used by the community calendar feature.
            create_calendar_tables(self._conn)

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

    def upsert_known_host(self, host: Dict[str, Any]) -> bool:
        hex_hash = host.get("hex_hash")
        if not hex_hash:
            return False
        if isinstance(hex_hash, bytes):
            hex_hash = hex_hash.hex()
        alias = host.get("alias") or f"Host-{str(hex_hash)[:6]}"
        hops = int(host.get("hops", 99))
        load = int(host.get("load", 0))
        max_load = int(host.get("max_load", 10))
        last_seen = float(host.get("last_seen", time.time()))
        is_manual = int(bool(host.get("is_manual", False)))
        score = float(host.get("score", 0.0))
        with self._tx() as cursor:
            cursor.execute("""
                INSERT INTO known_hosts (hex_hash, alias, hops, load, max_load, last_seen, is_manual, score)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(hex_hash) DO UPDATE SET
                    alias = excluded.alias,
                    hops = excluded.hops,
                    load = excluded.load,
                    max_load = excluded.max_load,
                    last_seen = excluded.last_seen,
                    is_manual = excluded.is_manual,
                    score = excluded.score
            """, (str(hex_hash), alias, hops, load, max_load, last_seen, is_manual, score))
        return True

    def save_host(self, host: Dict[str, Any]) -> bool:
        return self.upsert_known_host(host)

    def get_host(self, hex_hash: str) -> Optional[Dict[str, Any]]:
        with self._tx() as cursor:
            cursor.execute("SELECT * FROM known_hosts WHERE hex_hash = ?", (str(hex_hash),))
            row = cursor.fetchone()
            if not row:
                return None
            entry = dict(row)
            entry["is_manual"] = bool(entry.get("is_manual", 0))
            return entry

    def load_hosts(self, limit: int = 100) -> List[Dict[str, Any]]:
        with self._tx() as cursor:
            cursor.execute("SELECT * FROM known_hosts ORDER BY score DESC, last_seen DESC LIMIT ?", (int(limit),))
            rows = []
            for row in cursor.fetchall():
                entry = dict(row)
                entry["hex_hash"] = entry.get("hex_hash")
                entry["is_manual"] = bool(entry.get("is_manual", 0))
                rows.append(entry)
            return rows

    def delete_stale_hosts(self, older_than_seconds: float = 7200) -> int:
        cutoff = time.time() - float(older_than_seconds)
        with self._tx() as cursor:
            cursor.execute(
                "DELETE FROM known_hosts WHERE is_manual = 0 AND last_seen < ?",
                (cutoff,),
            )
            return cursor.rowcount

    def get_known_hosts(self) -> List[Dict[str, Any]]:
        return self.load_hosts(limit=500)

    def log_operator_action(self, action: str, target: str = "", detail: str = "") -> bool:
        with self._tx() as cursor:
            cursor.execute(
                """
                INSERT INTO operator_actions (timestamp, action, target, detail)
                VALUES (?, ?, ?, ?)
                """,
                (time.time(), str(action or ""), str(target or ""), str(detail or "")),
            )
        return True

    def get_recent_operator_actions(self, limit: int = 10) -> List[Dict[str, Any]]:
        max_rows = max(1, min(int(limit or 10), 100))
        with self._tx() as cursor:
            cursor.execute(
                """
                SELECT action_id, timestamp, action, target, detail
                FROM operator_actions
                ORDER BY timestamp DESC, action_id DESC
                LIMIT ?
                """,
                (max_rows,),
            )
            return [dict(row) for row in cursor.fetchall()]

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

    def db_payload_bytes(self) -> int:
        """
        Size the database would occupy once free pages are reclaimed.

        Retention decisions must use this rather than the on-disk size. A DELETE
        only moves pages onto SQLite's freelist, so straight after a large prune
        the file still measures its old size -- and a size check reading that
        would conclude the prune achieved nothing and delete the entire
        remaining history while the real payload was a fraction of the budget.
        """
        with self._tx() as cursor:
            page_count = cursor.execute("PRAGMA page_count").fetchone()[0]
            page_size = cursor.execute("PRAGMA page_size").fetchone()[0]
            free_pages = cursor.execute("PRAGMA freelist_count").fetchone()[0]
        return max(0, int(page_count) - int(free_pages)) * int(page_size)

    def enforce_size_limit(self, max_bytes: int, batch: int = 200) -> int:
        """
        Drops the oldest messages until the database fits `max_bytes`.

        This is the backstop that makes a hub safe to run unattended on a small
        device: whatever the TTL and per-channel caps allow, the node still
        degrades by shedding the oldest history instead of filling the disk.

        Measured against `db_payload_bytes()`, discounting pages already on the
        freelist: an earlier prune in the same sweep leaves the file at its old
        size until VACUUM, and measuring that would shed history that is already
        within budget. A single VACUUM at the end returns the pages to the
        filesystem, rather than one per batch -- repeatedly rewriting the whole
        database is exactly what an SD card should not be asked to do.
        """
        if not max_bytes or max_bytes <= 0:
            return 0

        removed = 0
        while self.db_payload_bytes() > max_bytes:
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
                    f"Database is {self.db_payload_bytes()} bytes with no prunable messages left, "
                    f"over the {max_bytes} byte limit."
                )
                break
            removed += deleted

        if removed:
            self.vacuum()
        return removed

    def block_identity(self, identity_hash: str, reason: str = "") -> bool:
        """Blocks an identity for this node only. False if already blocked."""
        if self.is_blocked(identity_hash):
            return False
        with self._tx() as cursor:
            cursor.execute(
                "INSERT OR REPLACE INTO blocked_identities VALUES (?, ?, ?)",
                (identity_hash, reason, time.time()),
            )
        return True

    def unblock_identity(self, identity_hash: str) -> bool:
        with self._tx() as cursor:
            cursor.execute("DELETE FROM blocked_identities WHERE identity_hash = ?",
                           (identity_hash,))
            return cursor.rowcount > 0

    def is_blocked(self, identity_hash: str) -> bool:
        with self._tx() as cursor:
            cursor.execute("SELECT 1 FROM blocked_identities WHERE identity_hash = ?",
                           (identity_hash,))
            return cursor.fetchone() is not None

    def blocked_identities(self) -> List[str]:
        with self._tx() as cursor:
            cursor.execute("SELECT identity_hash FROM blocked_identities ORDER BY blocked_at")
            return [row[0] for row in cursor.fetchall()]

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

    def set_channel_visibility(self, host_hash: str, channel_name: str, visible: bool) -> bool:
        host_key = (host_hash or "").strip().lower()
        chan = str(channel_name or "").lstrip("#").strip()
        if not host_key or not chan:
            return False
        with self._tx() as cursor:
            cursor.execute("""
                INSERT INTO client_channel_prefs (host_hash, channel_name, visible, updated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(host_hash, channel_name) DO UPDATE SET
                    visible = excluded.visible,
                    updated_at = excluded.updated_at
            """, (host_key, chan, int(bool(visible)), time.time()))
        return True

    def get_channel_visibility_map(self, host_hash: str) -> Dict[str, bool]:
        host_key = (host_hash or "").strip().lower()
        if not host_key:
            return {}
        with self._tx() as cursor:
            cursor.execute(
                "SELECT channel_name, visible FROM client_channel_prefs WHERE host_hash = ?",
                (host_key,),
            )
            return {
                str(row[0]).lstrip("#"): bool(row[1])
                for row in cursor.fetchall()
            }

    def get_visible_channels(self, host_hash: str, channel_names: Iterable[str],
                             default_visible: bool = True) -> List[str]:
        prefs = self.get_channel_visibility_map(host_hash)
        visible = []
        for raw in channel_names:
            chan = str(raw or "").lstrip("#").strip()
            if not chan:
                continue
            if prefs.get(chan, default_visible):
                visible.append(chan)
        return visible

    def get_channels(self) -> List[Dict[str, Any]]:
        """Returns all channel records as dictionaries for UI composition."""
        with self._tx() as cursor:
            cursor.execute("SELECT * FROM channels ORDER BY name ASC")
            return [dict(row) for row in cursor.fetchall()]

    def get_active_channel_names(self) -> List[str]:
        """Channel names currently hosted for traffic and federation."""
        with self._tx() as cursor:
            cursor.execute(
                "SELECT name FROM channels WHERE COALESCE(status, 'active') = 'active' ORDER BY name ASC"
            )
            return [row[0] for row in cursor.fetchall()]

    def get_channel(self, name: str) -> Optional[Dict[str, Any]]:
        with self._tx() as cursor:
            cursor.execute("SELECT * FROM channels WHERE name = ?", (name,))
            row = cursor.fetchone()
        return dict(row) if row else None

    def get_channel_status(self, name: str) -> Optional[str]:
        record = self.get_channel(name)
        if not record:
            return None
        return str(record.get("status") or "active").lower()

    def set_channel_status(self, name: str, status: str) -> bool:
        normalized = str(status or "").strip().lower()
        if normalized not in {"active", "paused", "blocked"}:
            return False
        with self._tx() as cursor:
            cursor.execute(
                "UPDATE channels SET status = ? WHERE name = ?",
                (normalized, name),
            )
            return cursor.rowcount > 0

    def get_signed_channels(self) -> List[Dict[str, Any]]:
        """Operator-approved channels, i.e. the ones that can be federated."""
        with self._tx() as cursor:
            cursor.execute(
                "SELECT * FROM channels "
                "WHERE signature IS NOT NULL AND COALESCE(status, 'active') = 'active' "
                "ORDER BY name ASC"
            )
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
                INSERT INTO channels (name, description, created_at, approver_hash, signature, status)
                VALUES (?, ?, ?, ?, ?, 'active')
                ON CONFLICT(name) DO UPDATE SET
                    description = excluded.description,
                    created_at = excluded.created_at,
                    approver_hash = excluded.approver_hash,
                    signature = excluded.signature,
                    status = 'active'
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
                INSERT INTO channels (name, description, created_at, approver_hash, signature, status)
                VALUES (?, ?, ?, ?, ?, 'active')
                ON CONFLICT(name) DO UPDATE SET
                    description = excluded.description,
                    created_at = excluded.created_at,
                    approver_hash = excluded.approver_hash,
                    signature = excluded.signature,
                    status = 'active'
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

    def add_channel(self, name: str, description: str = "", status: str = "active") -> bool:
        normalized = str(status or "active").strip().lower()
        if normalized not in {"active", "paused", "blocked"}:
            normalized = "active"
        with self._tx() as cursor:
            try:
                cursor.execute("""
                    INSERT INTO channels (name, description, created_at, status)
                    VALUES (?, ?, ?, ?)
                """, (name, description, time.time(), normalized))
                return True
            except sqlite3.IntegrityError:
                return False

    def purge_local_channel(self, channel_name: str) -> Dict[str, int]:
        """
        Deletes local records tied to one channel.

        Intended for client-side cleanup of stale channels and their associated
        local history/calendar state. Returns per-table delete counts.
        """
        chan = str(channel_name or "").lstrip("#").strip()
        if not chan:
            return {}

        deleted: Dict[str, int] = {}
        with self._tx() as cursor:
            cursor.execute(
                "DELETE FROM event_change WHERE event_id IN (SELECT event_id FROM event WHERE channel = ?)",
                (chan,),
            )
            deleted["event_change"] = cursor.rowcount

            cursor.execute("DELETE FROM event WHERE channel = ?", (chan,))
            deleted["event"] = cursor.rowcount

            cursor.execute("DELETE FROM calendar WHERE channel = ? OR calendar_id = ?", (chan, chan))
            deleted["calendar"] = cursor.rowcount

            cursor.execute("DELETE FROM messages WHERE channel = ?", (chan,))
            deleted["messages"] = cursor.rowcount

            cursor.execute("DELETE FROM channel_requests WHERE name = ?", (chan,))
            deleted["channel_requests"] = cursor.rowcount

            cursor.execute("DELETE FROM channels WHERE name = ?", (chan,))
            deleted["channels"] = cursor.rowcount

            cursor.execute("DELETE FROM client_channel_prefs WHERE channel_name = ?", (chan,))
            deleted["client_channel_prefs"] = cursor.rowcount

        return deleted

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
