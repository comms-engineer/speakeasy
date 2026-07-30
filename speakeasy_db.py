"""Database helpers for Speakeasy.

This module provides lightweight SQLite-backed calendar support for Phase 1.
The design intentionally includes ownership, channel scoping, and change-log
support so event edits and deletions can flow across federated nodes.
"""

from __future__ import annotations

import json
import sqlite3
import time
from dataclasses import dataclass
from typing import Any, List, Optional, Tuple


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


class SpeakeasyDB:
    """Lightweight SQLite wrapper for calendar persistence and sync."""

    def __init__(self, db_path: str, *args: Any, **kwargs: Any) -> None:
        self.db_path = db_path
        self.connection = sqlite3.connect(db_path)
        self.connection.row_factory = sqlite3.Row
        create_calendar_tables(self.connection)

    def close(self) -> None:
        self.connection.close()

    def __enter__(self) -> "SpeakeasyDB":
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
    "Event",
    "EventChange",
    "create_calendar_tables",
    "SpeakeasyDB",
]
