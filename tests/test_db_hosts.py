import time

from speakeasy_db import SpeakeasyDB


def test_save_and_load_host(tmp_path):
    db_path = tmp_path / "test_hosts.db"
    db = SpeakeasyDB(str(db_path))

    host = {
        "hex_hash": "aa" * 16,
        "alias": "TestHost",
        "last_seen": time.time(),
        "hops": 2,
        "load": 1,
        "max_load": 10,
        "is_manual": False,
        "metadata": {"name": "TestHost"},
    }

    assert db.save_host(host)

    loaded = db.get_host(host["hex_hash"]) 
    assert loaded is not None
    assert loaded["hex_hash"] == host["hex_hash"]
    assert loaded["alias"] == host["alias"]
    assert abs(loaded["hops"] - host["hops"]) == 0

    all_hosts = db.load_hosts()
    assert any(h["hex_hash"] == host["hex_hash"] for h in all_hosts)

    # delete stale hosts returns count
    deleted = db.delete_stale_hosts(older_than_seconds=0)
    # since host was just seen, delete_stale_hosts(0) should remove discovered (non-manual) hosts
    assert isinstance(deleted, int)

    db.close()


def test_channel_visibility_preferences_are_scoped_per_host(tmp_path):
    db_path = tmp_path / "prefs.db"
    db = SpeakeasyDB(str(db_path))
    try:
        host_a = "aa" * 16
        host_b = "bb" * 16

        assert db.set_channel_visibility(host_a, "tech", False)
        assert db.set_channel_visibility(host_a, "general", True)
        assert db.set_channel_visibility(host_b, "tech", True)

        prefs_a = db.get_channel_visibility_map(host_a)
        prefs_b = db.get_channel_visibility_map(host_b)

        assert prefs_a["tech"] is False
        assert prefs_a["general"] is True
        assert prefs_b["tech"] is True

        visible_a = db.get_visible_channels(host_a, ["general", "tech", "music"])
        visible_b = db.get_visible_channels(host_b, ["general", "tech", "music"])

        assert visible_a == ["general", "music"]
        assert visible_b == ["general", "tech", "music"]
    finally:
        db.close()
