import sqlite3
import time
from types import SimpleNamespace

import msgpack
import RNS

from channel_summary import build_channel_summary
from reti_speakeasy import HostSelectorModal, HostManager, RetiSpeakeasyApp, ReticulumEngine


def test_host_selector_populates_table(monkeypatch):
    # Prepare a HostManager with two sample hosts
    hm = HostManager(db=None, probe_interval=300, probes_per_round=1)
    sample1 = {
        "hex_hash": "dd" * 16,
        "alias": "One",
        "hops": 2,
        "load": 0,
        "max_load": 10,
        "last_seen": time.time(),
        "is_manual": False,
        "score": 10.0,
        "hash_bytes": bytes.fromhex("dd" * 16),
    }
    sample2 = {
        "hex_hash": "ee" * 16,
        "alias": "Two",
        "hops": 5,
        "load": 3,
        "max_load": 10,
        "last_seen": time.time(),
        "is_manual": True,
        "score": 5.0,
        "hash_bytes": bytes.fromhex("ee" * 16),
    }
    hm.hosts[sample1["hex_hash"]] = sample1
    hm.hosts[sample2["hex_hash"]] = sample2

    # Create a fake DataTable to capture columns/rows
    class FakeTable:
        def __init__(self):
            self.columns = []
            self.rows = []
            self.cursor_type = None
            self.cursor_row = None
            self.row_count = 0
            self.cursor_coordinate = (0, 0)

        def add_columns(self, *cols):
            self.columns.extend(cols)

        def clear(self):
            self.rows.clear()
            self.row_count = 0

        def add_row(self, *args, key=None):
            self.rows.append((args, key))
            self.row_count += 1

        def coordinate_to_cell_key(self, coord):
            # Simulate returning the key of the first row
            if self.rows:
                return SimpleNamespace(value=self.rows[0][1]), None
            return None, None

    fake_table = FakeTable()

    modal = HostSelectorModal(hm)

    # Monkeypatch modal.query_one to return our fake table when asked for hosts-table
    def fake_query_one(selector, _type=None):
        if selector == "#hosts-table":
            return fake_table
        raise RuntimeError("Unexpected selector")

    monkeypatch.setattr(modal, "query_one", fake_query_one)

    # Run on_mount to add columns and refresh_table to populate rows
    modal.on_mount()
    modal.refresh_table()

    assert "Alias / Hash" in fake_table.columns
    # Should have added two rows (one per host)
    assert fake_table.row_count == 2


def test_host_selector_shows_affinity_count():
    hm = HostManager(
        db=SimpleNamespace(get_active_channel_names=lambda: ["general", "tech"]),
        probe_interval=300,
        probes_per_round=1,
    )
    host = {
        "hex_hash": "11" * 16,
        "alias": "TechHub",
        "metadata": {"channels": ["general", "tech"], "chc": 2},
    }
    hm.hosts[host["hex_hash"]] = host

    ranked = hm.get_ranked_hosts()

    assert ranked[0]["channel_affinity"] == 2


def test_host_manager_tolerates_closed_db_during_ranking():
    class ClosedDB:
        def get_active_channel_names(self):
            raise sqlite3.ProgrammingError("Cannot operate on a closed database")

    hm = HostManager(db=ClosedDB(), probe_interval=300, probes_per_round=1)
    host = {
        "hex_hash": "33" * 16,
        "alias": "ClosedDBHost",
        "metadata": {"channels": ["general"], "chc": 1},
    }
    hm.hosts[host["hex_hash"]] = host

    ranked = hm.get_ranked_hosts()

    assert ranked[0]["channel_affinity"] == 0


def test_action_quit_is_resilient_to_engine_shutdown_errors(monkeypatch):
    app = RetiSpeakeasyApp()
    app.engine = SimpleNamespace(shutdown=lambda: (_ for _ in ()).throw(RuntimeError("boom")))
    monkeypatch.setattr(app, "exit", lambda: None)

    app.action_quit()


def test_host_manager_ignores_ignored_hashes_on_announce():
    local_identity = RNS.Identity()
    hm = HostManager(
        db=None,
        probe_interval=300,
        probes_per_round=1,
        ignored_hashes={local_identity.hash},
    )
    payload = msgpack.packb({"name": "Local Hub", "load": 1, "max_load": 50}, use_bin_type=True)

    hm.update_from_announce(local_identity.hash, local_identity, payload)

    assert hm.hosts == {}


def test_reticulum_engine_emits_diagnostics():
    captured = []

    class FakeUI:
        def __call__(self, event_type, data):
            captured.append((event_type, data))

    engine = ReticulumEngine(ui_callback=FakeUI())
    engine._record_transport_diagnostic("Path request failed")

    assert captured[-1] == ("diagnostic", "Path request failed")


def test_reticulum_engine_suppresses_path_state_unknown_diagnostics():
    captured = []

    class FakeUI:
        def __call__(self, event_type, data):
            captured.append((event_type, data))

    engine = object.__new__(ReticulumEngine)
    engine._diagnostics = []
    engine._stopped = False
    engine.ui_callback = FakeUI()

    engine._record_transport_diagnostic("Path state unknown for [abc]")

    assert captured == []


def test_host_selector_channel_filter_uses_summary():
    hm = HostManager(db=None, probe_interval=300, probes_per_round=1)
    with_tech = {
        "hex_hash": "11" * 16,
        "alias": "TechHub",
        "metadata": {"chs": build_channel_summary(["general", "tech"]), "chc": 2},
    }
    without_tech = {
        "hex_hash": "22" * 16,
        "alias": "QuietHub",
        "metadata": {"chs": build_channel_summary(["general"]), "chc": 1},
    }

    modal = HostSelectorModal(hm)
    modal.channel_filter = "tech"

    assert modal._host_matches_channel_filter(with_tech)
    assert not modal._host_matches_channel_filter(without_tech)
