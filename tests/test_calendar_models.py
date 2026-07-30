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
            assert "btn-channel-purge" in widget_ids
            assert any(widget_id.startswith("calendar-log-") for widget_id in widget_ids)
            assert any(widget_id.startswith("calendar-table-") for widget_id in widget_ids)

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
                assert any(widget_id.startswith("calendar-table-") for widget_id in widget_ids)
                assert any(widget_id.startswith("btn-calendar-new-") for widget_id in widget_ids)
                assert any(widget_id.startswith("btn-calendar-edit-") for widget_id in widget_ids)
                assert any(widget_id.startswith("btn-calendar-delete-") for widget_id in widget_ids)

    import asyncio
    asyncio.run(check_components())


def test_channel_tabs_have_their_own_calendar_panels(tmp_path):
    app = RetiSpeakeasyApp()
    db_path = tmp_path / "client.db"

    def fake_engine_init(self, ui_callback=None):
        self.ui_callback = ui_callback
        self.hash_str = "test-hash"
        self.db = SpeakeasyDB(str(db_path))
        self.identity = SimpleNamespace(hash=SimpleNamespace(hex=lambda: "test-identity"))
        self.host_manager = None
        self.active_host_link = None
        self.s2s_engine = None
        self.host_channels = set()

    async def check_components() -> None:
        with patch("reti_speakeasy.ReticulumEngine.__init__", fake_engine_init), \
             patch("reti_speakeasy.client_db_path", return_value=str(db_path)):
            startup_db = SpeakeasyDB(str(db_path))
            startup_db.add_channel("general", "Channel #general")
            startup_db.add_channel("tech", "Channel #tech")
            startup_db.close()
            async with app.run_test():
                widget_ids = {
                    widget.id
                    for widget in app.query("*")
                    if getattr(widget, "id", None)
                }
                assert "calendar-table-general" in widget_ids
                assert "calendar-log-general" in widget_ids
                assert "calendar-table-tech" in widget_ids
                assert "calendar-log-tech" in widget_ids

    import asyncio
    asyncio.run(check_components())


def test_channel_visibility_hides_host_tab(tmp_path):
    app = RetiSpeakeasyApp()
    db_path = tmp_path / "client-visible.db"
    host_hash = "ab" * 16

    def fake_engine_init(self, ui_callback=None):
        self.ui_callback = ui_callback
        self.hash_str = "test-hash"
        self.db = SpeakeasyDB(str(db_path))
        self.identity = SimpleNamespace(hash=SimpleNamespace(hex=lambda: "test-identity"))
        self.host_manager = None
        self.active_host_link = None
        self.s2s_engine = None
        self.host_channels = {"general", "tech"}
        self.current_host_hash = host_hash

    async def check_visibility() -> None:
        with patch("reti_speakeasy.ReticulumEngine.__init__", fake_engine_init), \
             patch("reti_speakeasy.client_db_path", return_value=str(db_path)):
            startup_db = SpeakeasyDB(str(db_path))
            startup_db.add_channel("general", "Channel #general")
            startup_db.add_channel("tech", "Channel #tech")
            startup_db.set_channel_visibility(host_hash, "tech", False)
            startup_db.close()

            async with app.run_test():
                app.sync_channel_tabs(["general", "tech"])
                general_tab = app.query_one("#tab-general")
                tech_tab = app.query_one("#tab-tech")
                assert bool(general_tab.display) is True
                assert bool(tech_tab.display) is False

    import asyncio
    asyncio.run(check_visibility())


def test_purge_channel_removes_tab_from_ui(tmp_path):
    app = RetiSpeakeasyApp()
    db_path = tmp_path / "client-purge.db"
    host_hash = "cd" * 16

    def fake_engine_init(self, ui_callback=None):
        self.ui_callback = ui_callback
        self.hash_str = "test-hash"
        self.db = SpeakeasyDB(str(db_path))
        self.identity = SimpleNamespace(hash=SimpleNamespace(hex=lambda: "test-identity"))
        self.host_manager = None
        self.active_host_link = None
        self.s2s_engine = None
        self.host_channels = {"general", "tech"}
        self.current_host_hash = host_hash

    async def check_purge_removes_tab() -> None:
        with patch("reti_speakeasy.ReticulumEngine.__init__", fake_engine_init), \
             patch("reti_speakeasy.client_db_path", return_value=str(db_path)):
            startup_db = SpeakeasyDB(str(db_path))
            startup_db.add_channel("general", "Channel #general")
            startup_db.add_channel("tech", "Channel #tech")
            startup_db.close()

            async with app.run_test() as pilot:
                app.sync_channel_tabs(["general", "tech"])
                assert len(list(app.query("#tab-tech"))) == 1

                app._purge_local_channel("tech")
                await pilot.pause()

                assert len(list(app.query("#tab-tech"))) == 0

    import asyncio
    asyncio.run(check_purge_removes_tab())


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
