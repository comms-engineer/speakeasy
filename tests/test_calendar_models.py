import sqlite3
from types import SimpleNamespace
from unittest.mock import patch

from textual.app import App

from reti_speakeasy import RetiSpeakeasyApp
from speakeasy_db import Calendar, Event, SpeakeasyDB, create_calendar_tables


def test_calendar_schema_creates_tables(tmp_path):
    db_path = tmp_path / "calendar.db"
    connection = sqlite3.connect(db_path)
    try:
        create_calendar_tables(connection)
        tables = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name IN ('calendar', 'event', 'event_change')"
            )
        }
        assert tables == {"calendar", "event", "event_change"}
    finally:
        connection.close()


def test_calendar_ui_components_are_present():
    app = RetiSpeakeasyApp()

    async def check_components() -> None:
        async with app.run_test():
            widget_ids = {
                widget.id
                for widget in app.query("*")
                if getattr(widget, "id", None)
            }
            assert "btn-calendar-open" in widget_ids
            assert "tab-calendar" in widget_ids
            assert "calendar-log" in widget_ids

    App.run_test = App.run_test
    import asyncio
    asyncio.run(check_components())


def test_calendar_tui_supports_edit_and_delete_controls():
    app = RetiSpeakeasyApp()

    def fake_engine_init(self, ui_callback=None):
        self.ui_callback = ui_callback
        self.hash_str = "test-hash"
        self.db = SpeakeasyDB(":memory:")
        self.identity = SimpleNamespace(hash=SimpleNamespace(hex=lambda: "test-identity"))
        self.host_manager = None
        self.active_host_link = None
        self.s2s_engine = None
        self.host_channels = set()

    async def check_components() -> None:
        with patch("reti_speakeasy.ReticulumEngine.__init__", fake_engine_init):
            async with app.run_test():
                widget_ids = {
                    widget.id
                    for widget in app.query("*")
                    if getattr(widget, "id", None)
                }
                assert "calendar-table" in widget_ids
                assert "btn-calendar-new" in widget_ids
                assert "btn-calendar-edit" in widget_ids
                assert "btn-calendar-delete" in widget_ids

    import asyncio
    asyncio.run(check_components())


def test_calendar_and_event_models_round_trip():
    calendar = Calendar(
        calendar_id="cal-001",
        name="Community Calendar",
        description="Neighborhood events",
        owner_hash="owner-001",
        visibility="public",
        timezone="UTC",
        channel="parlor",
        created_at=1710000000,
        updated_at=1710000300,
    )
    event = Event(
        event_id="evt-001",
        calendar_id=calendar.calendar_id,
        title="Town Hall",
        description="Discuss the next meetup",
        location="Library",
        start_at=1710003600,
        end_at=1710007200,
        all_day=False,
        status="scheduled",
        channel="parlor",
        created_by_hash="owner-001",
        created_at=1710003600,
        updated_at=1710003700,
        revision=2,
    )

    connection = sqlite3.connect(":memory:")
    try:
        create_calendar_tables(connection)
        connection.execute(
            "INSERT INTO calendar (calendar_id, name, description, owner_hash, visibility, timezone, channel, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            calendar.to_db_row(),
        )
        connection.execute(
            "INSERT INTO event (event_id, calendar_id, title, description, location, start_at, end_at, all_day, status, channel, created_by_hash, created_at, updated_at, deleted_at, revision) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            event.to_db_row(),
        )

        stored_calendar = Calendar.from_db_row(
            connection.execute(
                "SELECT calendar_id, name, description, owner_hash, visibility, timezone, channel, created_at, updated_at FROM calendar WHERE calendar_id = ?",
                (calendar.calendar_id,),
            ).fetchone()
        )
        stored_event = Event.from_db_row(
            connection.execute(
                "SELECT event_id, calendar_id, title, description, location, start_at, end_at, all_day, status, channel, created_by_hash, created_at, updated_at, deleted_at, revision FROM event WHERE event_id = ?",
                (event.event_id,),
            ).fetchone()
        )

        assert stored_calendar == calendar
        assert stored_event == event
    finally:
        connection.close()
