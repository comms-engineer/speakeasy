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


def test_channel_status_controls_active_channel_names(tmp_path):
    db_path = tmp_path / "channel-status.db"
    db = SpeakeasyDB(str(db_path))
    try:
        db.add_channel("ops", "operator channel", status="active")
        db.add_channel("quiet", "quiet channel", status="paused")
        db.add_channel("spam", "spam channel", status="blocked")

        assert db.get_channel_status("ops") == "active"
        assert db.get_channel_status("quiet") == "paused"
        assert db.get_channel_status("spam") == "blocked"

        assert db.set_channel_status("quiet", "active")
        assert db.get_channel_status("quiet") == "active"

        active = set(db.get_active_channel_names())
        assert "ops" in active
        assert "quiet" in active
        assert "spam" not in active
    finally:
        db.close()


def test_operator_action_audit_persists_and_orders_recent_first(tmp_path):
    db_path = tmp_path / "operator-actions.db"
    db = SpeakeasyDB(str(db_path))
    try:
        db.log_operator_action("approve_channel", target="lounge", detail="propagated_to=2")
        db.log_operator_action("pause_channel", target="off-topic")

        rows = db.get_recent_operator_actions(limit=5)
        assert len(rows) == 2
        assert rows[0]["action"] == "pause_channel"
        assert rows[1]["action"] == "approve_channel"
    finally:
        db.close()


def test_operator_blacklist_recommendations_dedupe_and_aggregate(tmp_path):
    db_path = tmp_path / "operator-recommendations.db"
    db = SpeakeasyDB(str(db_path))
    try:
        assert db.add_operator_blacklist_recommendation(
            recommended_identity_hash="feedfacefeedface",
            rationale="spam flood",
            source_operator_hash="aa" * 16,
            source_peer_hash="bb" * 16,
            source_node_name="Alpha",
        ) is True
        assert db.add_operator_blacklist_recommendation(
            recommended_identity_hash="feedfacefeedface",
            rationale="spam flood",
            source_operator_hash="aa" * 16,
            source_peer_hash="bb" * 16,
            source_node_name="Alpha",
        ) is False
        assert db.add_operator_blacklist_recommendation(
            recommended_identity_hash="feedfacefeedface",
            rationale="forged identities",
            source_operator_hash="cc" * 16,
            source_peer_hash="dd" * 16,
            source_node_name="Beta",
        ) is True

        rows = db.get_recent_operator_blacklist_recommendations(limit=10)
        summary = db.summarize_operator_blacklist_recommendations(limit=10)

        assert len(rows) == 2
        assert len(summary) == 1
        assert summary[0]["recommended_identity_hash"] == "feedfacefeedface"
        assert summary[0]["recommendation_count"] == 2
        assert summary[0]["source_count"] == 2
        assert "Alpha" in summary[0]["sources"]
        assert "Beta" in summary[0]["sources"]
        assert "spam flood" in summary[0]["rationales"]
        assert "forged identities" in summary[0]["rationales"]
    finally:
        db.close()


def test_bulletin_lifecycle_supports_archival_deletion_and_comments(tmp_path):
    db_path = tmp_path / "bulletins.db"
    db = SpeakeasyDB(str(db_path))
    try:
        db.upsert_identity("author-1", "author")
        db.upsert_identity("author-2", "reply")
        old_ts = time.time() - (9 * 86400)
        db.add_bulletin("Old", "archive me", "author-1", old_ts, "bulletin-old", b"sig")
        db.add_bulletin("New", "keep me", "author-1", time.time(), "bulletin-new", b"sig")

        archived = db.archive_old_bulletins(older_than_days=7)
        comment = db.add_bulletin_comment("bulletin-new", "author-2", "first reply")

        assert archived == 1
        assert comment is not None
        assert db.get_bulletins(archived=False)[0]["bulletin_id"] == "bulletin-new"
        assert db.get_bulletins(archived=True)[0]["bulletin_id"] == "bulletin-old"
        assert len(db.get_bulletin_comments("bulletin-new")) == 1
        deleted = db.delete_bulletin("bulletin-new", "author-1")
        assert deleted is True
        assert db.get_bulletins(archived=False) == []
        assert db.add_bulletin_comment("bulletin-new", "author-2", "after delete") is None
    finally:
        db.close()


def test_purge_local_channel_removes_related_rows(tmp_path):
    db_path = tmp_path / "purge-channel.db"
    db = SpeakeasyDB(str(db_path))
    try:
        db.add_channel("legacy", "old room")
        db.set_channel_visibility("aa" * 16, "legacy", False)

        with db._tx() as cursor:
            cursor.execute(
                "INSERT INTO messages (msg_id, channel, sender_hash, content, timestamp, signature) VALUES (?, ?, ?, ?, ?, ?)",
                ("f" * 64, "legacy", "1" * 32, "hello", 1.0, b""),
            )
            cursor.execute(
                "INSERT INTO channel_requests (name, description, requester_hash, requested_at, status, decided_at) VALUES (?, ?, ?, ?, ?, ?)",
                ("legacy", "old", "2" * 32, 1.0, "pending", None),
            )

        deleted = db.purge_local_channel("legacy")

        assert deleted["channels"] >= 1
        assert deleted["messages"] >= 1
        assert deleted["channel_requests"] >= 1
        assert db.get_channel("legacy") is None
        assert db.get_channel_messages("legacy", limit=10) == []
    finally:
        db.close()
