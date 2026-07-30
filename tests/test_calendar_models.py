import sqlite3
from types import SimpleNamespace
from unittest.mock import patch

import RNS
from textual.app import App
from textual.widgets import DataTable, Input, TabbedContent

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
            assert "btn-blocklist-open" in widget_ids
            assert "btn-channel-purge" in widget_ids
            assert "btn-channel-restore" in widget_ids
            assert "tab-bbs" in widget_ids

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
        self.current_host_hash = None

    async def check_components() -> None:
        with patch("reti_speakeasy.ReticulumEngine.__init__", fake_engine_init):
            async with app.run_test():
                app.sync_channel_tabs(["general"])
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
            startup_db.create_calendar(Calendar(
                calendar_id="general",
                name="General",
                description="",
                owner_hash="owner-1",
                visibility="public",
                timezone="UTC",
                channel="general",
                created_at=1710000000,
                updated_at=1710000000,
            ))
            startup_db.create_calendar(Calendar(
                calendar_id="tech",
                name="Tech",
                description="",
                owner_hash="owner-1",
                visibility="public",
                timezone="UTC",
                channel="tech",
                created_at=1710000000,
                updated_at=1710000000,
            ))
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


def test_purged_channel_stays_out_of_reconnect_tabs(tmp_path):
    app = RetiSpeakeasyApp()
    db_path = tmp_path / "client-purged-reconnect.db"
    host_hash = "de" * 16

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

    async def check_reconnect_tabs() -> None:
        with patch("reti_speakeasy.ReticulumEngine.__init__", fake_engine_init), \
             patch("reti_speakeasy.client_db_path", return_value=str(db_path)):
            startup_db = SpeakeasyDB(str(db_path))
            startup_db.add_channel("general", "Channel #general")
            startup_db.add_channel("tech", "Channel #tech")
            startup_db.mark_channel_purged("tech")
            startup_db.close()

            async with app.run_test() as pilot:
                app.sync_channel_tabs(["general", "tech"])
                await pilot.pause()
                assert len(list(app.query("#tab-general"))) == 1
                assert len(list(app.query("#tab-tech"))) == 0

    import asyncio
    asyncio.run(check_reconnect_tabs())


def test_restore_channel_readds_tab_for_current_host(tmp_path):
    app = RetiSpeakeasyApp()
    db_path = tmp_path / "client-restore-current-host.db"
    host_hash = "ac" * 16

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

    async def check_restore_round_trip() -> None:
        with patch("reti_speakeasy.ReticulumEngine.__init__", fake_engine_init), \
             patch("reti_speakeasy.client_db_path", return_value=str(db_path)):
            startup_db = SpeakeasyDB(str(db_path))
            startup_db.add_channel("general", "Channel #general")
            startup_db.add_channel("tech", "Channel #tech")
            startup_db.close()

            async with app.run_test() as pilot:
                app.sync_channel_tabs(["general", "tech"])
                await pilot.pause()
                assert len(list(app.query("#tab-tech"))) == 1

                app._purge_local_channel("tech")
                await pilot.pause()
                assert len(list(app.query("#tab-tech"))) == 0

                app._restore_local_channel("tech")
                await pilot.pause()
                assert len(list(app.query("#tab-tech"))) == 1
                assert "tech" not in app.engine.db.get_purged_channel_names()

    import asyncio
    asyncio.run(check_restore_round_trip())


def test_channel_manager_excludes_purged_channels(tmp_path):
    app = RetiSpeakeasyApp()
    db_path = tmp_path / "client-manager-purged.db"
    host_hash = "fa" * 16

    def fake_engine_init(self, ui_callback=None):
        self.ui_callback = ui_callback
        self.hash_str = "test-hash"
        self.db = SpeakeasyDB(str(db_path))
        self.identity = SimpleNamespace(hash=SimpleNamespace(hex=lambda: "test-identity"))
        self.host_manager = None
        self.active_host_link = None
        self.s2s_engine = None
        self.host_channels = {"general", "tech", "parlor"}
        self.current_host_hash = host_hash

    captured = {}

    async def check_manager_channels() -> None:
        with patch("reti_speakeasy.ReticulumEngine.__init__", fake_engine_init), \
             patch("reti_speakeasy.client_db_path", return_value=str(db_path)):
            startup_db = SpeakeasyDB(str(db_path))
            startup_db.mark_channel_purged("tech")
            startup_db.close()

            async with app.run_test():
                def fake_push_screen(screen, callback=None):
                    captured["screen"] = screen

                app.push_screen = fake_push_screen
                app.action_show_channel_manager_modal()
                assert "screen" in captured
                assert captured["screen"].channels == ["general", "parlor"]

    import asyncio
    asyncio.run(check_manager_channels())


def test_sync_channel_tabs_adds_dynamic_channel_without_widget_errors(tmp_path):
    app = RetiSpeakeasyApp()
    db_path = tmp_path / "client-dynamic-tabs.db"
    host_hash = "ef" * 16

    def fake_engine_init(self, ui_callback=None):
        self.ui_callback = ui_callback
        self.hash_str = "test-hash"
        self.db = SpeakeasyDB(str(db_path))
        self.identity = SimpleNamespace(hash=SimpleNamespace(hex=lambda: "test-identity"))
        self.host_manager = None
        self.active_host_link = None
        self.s2s_engine = None
        self.host_channels = {"general", "test2"}
        self.current_host_hash = host_hash

    async def check_dynamic_add() -> None:
        with patch("reti_speakeasy.ReticulumEngine.__init__", fake_engine_init), \
             patch("reti_speakeasy.client_db_path", return_value=str(db_path)):
            startup_db = SpeakeasyDB(str(db_path))
            startup_db.add_channel("general", "Channel #general")
            startup_db.close()

            async with app.run_test() as pilot:
                app.sync_channel_tabs(["general", "test2"])
                await pilot.pause()
                assert len(list(app.query("#tab-test2"))) == 1
                assert len(list(app.query("#calendar-table-test2"))) == 1

    import asyncio
    asyncio.run(check_dynamic_add())


def test_get_channels_with_local_data_excludes_empty_channels(tmp_path):
    db = SpeakeasyDB(str(tmp_path / "purge-candidates.db"))
    try:
        db.add_channel("withdata", "Channel #withdata")
        db.add_channel("empty", "Channel #empty")
        db.create_calendar(Calendar(
            calendar_id="withdata",
            name="With Data",
            description="",
            owner_hash="owner-1",
            visibility="public",
            timezone="UTC",
            channel="withdata",
            created_at=1710000000,
            updated_at=1710000000,
        ))

        names = [row["name"] for row in db.get_channels_with_local_data()]
        assert "withdata" in names
        assert "empty" not in names
    finally:
        db.close()


def test_startup_only_renders_tabs_for_channels_with_local_data(tmp_path):
    app = RetiSpeakeasyApp()
    db_path = tmp_path / "startup-visible-tabs.db"

    startup_db = SpeakeasyDB(str(db_path))
    startup_db.add_channel("general", "Channel #general")
    startup_db.add_channel("parlor", "Channel #parlor")
    startup_db.create_calendar(Calendar(
        calendar_id="general",
        name="General",
        description="",
        owner_hash="owner-1",
        visibility="public",
        timezone="UTC",
        channel="general",
        created_at=1710000000,
        updated_at=1710000000,
    ))
    startup_db.close()

    def fake_engine_init(self, ui_callback=None):
        self.ui_callback = ui_callback
        self.hash_str = "test-hash"
        self.db = SpeakeasyDB(str(db_path))
        self.identity = SimpleNamespace(hash=SimpleNamespace(hex=lambda: "test-identity"))
        self.host_manager = None
        self.active_host_link = None
        self.s2s_engine = None
        self.host_channels = set()
        self.current_host_hash = None

    async def check_startup_tabs() -> None:
        with patch("reti_speakeasy.ReticulumEngine.__init__", fake_engine_init), \
             patch("reti_speakeasy.client_db_path", return_value=str(db_path)):
            async with app.run_test() as pilot:
                await pilot.pause()
                assert len(list(app.query("#tab-general"))) == 1
                assert len(list(app.query("#tab-parlor"))) == 0

    import asyncio
    asyncio.run(check_startup_tabs())


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


def test_purge_identity_removes_authored_calendar_events(tmp_path):
    db = SpeakeasyDB(str(tmp_path / "blocked-events.db"))
    try:
        db.create_calendar(Calendar(
            calendar_id="parlor",
            name="Parlor",
            description="",
            owner_hash="owner-001",
            visibility="public",
            timezone="UTC",
            channel="parlor",
            created_at=1710000000,
            updated_at=1710000000,
        ))
        db.create_event(Event(
            event_id="evt-spam-1",
            calendar_id="parlor",
            title="Spam Event",
            description="ignore this",
            location="nowhere",
            start_at=1710003600,
            end_at=1710007200,
            all_day=False,
            status="scheduled",
            channel="parlor",
            created_by_hash="spammer-001",
            created_at=1710003600,
            updated_at=1710003600,
            revision=1,
        ))

        removed = db.purge_identity("spammer-001")
        event_changes = db.connection.execute("SELECT COUNT(*) FROM event_change").fetchone()[0]

        assert removed == 1
        assert db.list_events_for_channel("parlor") == []
        assert event_changes == 0
    finally:
        db.close()


def test_blocked_authors_are_hidden_from_bulletins_and_calendar_views(tmp_path):
    app = RetiSpeakeasyApp()
    db_path = tmp_path / "client-blocked-content.db"

    def fake_engine_init(self, ui_callback=None):
        self.ui_callback = ui_callback
        self.hash_str = "test-hash"
        self.db = SpeakeasyDB(str(db_path))
        self.identity = SimpleNamespace(hash=SimpleNamespace(hex=lambda: "test-identity"))
        self.host_manager = None
        self.active_host_link = None
        self.s2s_engine = None
        self.host_channels = {"general"}
        self.current_host_hash = None

    async def check_hidden_content() -> None:
        with patch("reti_speakeasy.ReticulumEngine.__init__", fake_engine_init), \
             patch("reti_speakeasy.client_db_path", return_value=str(db_path)):
            startup_db = SpeakeasyDB(str(db_path))
            startup_db.add_channel("general", "Channel #general")
            startup_db.create_calendar(Calendar(
                calendar_id="general",
                name="General",
                description="",
                owner_hash="owner-1",
                visibility="public",
                timezone="UTC",
                channel="general",
                created_at=1710000000,
                updated_at=1710000000,
            ))
            blocked = RNS.Identity()
            startup_db.upsert_identity(blocked.hash.hex(), "spammer", blocked.get_public_key())
            startup_db.add_bulletin(
                title="Blocked Bulletin",
                body="ignore",
                author_hash=blocked.hash.hex(),
                timestamp=1710007200,
                bulletin_id="blocked-bulletin",
                signature=b"sig",
            )
            startup_db.create_event(Event(
                event_id="evt-blocked",
                calendar_id="general",
                title="Blocked Event",
                description="ignore",
                location="none",
                start_at=1710003600,
                end_at=1710007200,
                all_day=False,
                status="scheduled",
                channel="general",
                created_by_hash=blocked.hash.hex(),
                created_at=1710003600,
                updated_at=1710003600,
                revision=1,
            ))
            startup_db.block_identity(blocked.hash.hex(), reason="Blocked from Speakeasy")
            startup_db.close()

            async with app.run_test() as pilot:
                app.sync_channel_tabs(["general"])
                app.reload_bulletin_board()
                app.reload_calendar_events()
                await pilot.pause()

                bbs_table = app.query_one("#bbs-table", DataTable)
                calendar_table = app.query_one("#calendar-table-general", DataTable)
                assert bbs_table.row_count == 0
                assert calendar_table.row_count == 0

    import asyncio
    asyncio.run(check_hidden_content())


def test_bulletin_archive_view_and_comment_input(tmp_path):
    app = RetiSpeakeasyApp()
    db_path = tmp_path / "client-bulletins.db"
    now = 1710007200.0

    def fake_engine_init(self, ui_callback=None):
        self.ui_callback = ui_callback
        self.hash_str = "test-hash"
        self.db = SpeakeasyDB(str(db_path))
        self.identity = SimpleNamespace(hash=SimpleNamespace(hex=lambda: "test-identity"))
        self.host_manager = None
        self.active_host_link = None
        self.s2s_engine = None
        self.host_channels = {"general"}
        self.current_host_hash = None
        self.bulletin_archive_days = 7.0

    async def check_bulletins() -> None:
        with patch("reti_speakeasy.ReticulumEngine.__init__", fake_engine_init), \
             patch("reti_speakeasy.client_db_path", return_value=str(db_path)), \
             patch("time.time", return_value=now):
            startup_db = SpeakeasyDB(str(db_path))
            startup_db.upsert_identity("test-identity", "operator")
            startup_db.add_bulletin("Old Bulletin", "archive me", "test-identity", now - (9 * 86400), "old-bulletin", b"sig")
            startup_db.add_bulletin("Fresh Bulletin", "keep me", "test-identity", now, "fresh-bulletin", b"sig")
            startup_db.close()

            async with app.run_test() as pilot:
                tabs = app.query_one("#channel-tabs", TabbedContent)
                tabs.active = "tab-bbs"
                app.reload_bulletin_board()
                await pilot.pause()

                bbs_table = app.query_one("#bbs-table", DataTable)
                assert bbs_table.row_count == 1
                assert app.selected_bulletin_id == "fresh-bulletin"

                input_widget = app.query_one("#chat-input", Input)
                input_widget.value = "First comment"
                app.on_input_submitted(SimpleNamespace(input=input_widget, value="First comment"))
                await pilot.pause()

                assert len(app.engine.db.get_bulletin_comments("fresh-bulletin")) == 1

                app._toggle_bulletin_archive_view()
                await pilot.pause()

                assert app.query_one("#bbs-table", DataTable).row_count == 1
                assert app.selected_bulletin_id == "old-bulletin"

    import asyncio
    asyncio.run(check_bulletins())
