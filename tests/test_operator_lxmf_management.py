import time
from types import SimpleNamespace

from speakeasy_daemon import SpeakeasyDaemon


class _FakeDB:
    def __init__(self):
        self._channels = [
            {"name": "general", "status": "active"},
            {"name": "tech", "status": "blocked"},
            {"name": "ops", "status": "paused"},
        ]
        self._operator_actions = []
        self._peer_operator_endpoints = []
        self._recommendations = []

    def get_active_channel_names(self):
        return [c["name"] for c in self._channels if c["status"] == "active"]

    def get_channels(self):
        return list(self._channels)

    def get_channel_requests(self, status):
        assert status == "pending"
        return [{"name": "lounge", "description": "chat"}]

    def log_operator_action(self, action, target="", detail=""):
        self._operator_actions.append(
            {
                "timestamp": time.time(),
                "action": str(action),
                "target": str(target),
                "detail": str(detail),
            }
        )
        return True

    def get_recent_operator_actions(self, limit=10):
        return list(reversed(self._operator_actions))[: int(limit)]

    def list_peer_operator_endpoints(self):
        return list(self._peer_operator_endpoints)

    def save_peer_operator_endpoint(self, peer_identity_hash, operator_endpoint_hash):
        self._peer_operator_endpoints = [
            row for row in self._peer_operator_endpoints
            if row["peer_identity_hash"] != str(peer_identity_hash)
        ]
        self._peer_operator_endpoints.append(
            {
                "peer_identity_hash": str(peer_identity_hash),
                "operator_endpoint_hash": str(operator_endpoint_hash),
            }
        )
        return True

    def resolve_identity(self, needle):
        return str(needle or "").strip().lower()

    def is_blocked(self, identity_hash):
        return any(row.get("identity_hash") == str(identity_hash) for row in getattr(self, "_blocked", []))

    def block_identity(self, identity_hash, reason=""):
        if self.is_blocked(identity_hash):
            return False
        blocked = getattr(self, "_blocked", [])
        blocked.append({"identity_hash": str(identity_hash), "reason": str(reason)})
        self._blocked = blocked
        return True

    def unblock_identity(self, identity_hash):
        blocked = getattr(self, "_blocked", [])
        kept = [row for row in blocked if row.get("identity_hash") != str(identity_hash)]
        changed = len(kept) != len(blocked)
        self._blocked = kept
        return changed

    def purge_identity(self, identity_hash):
        return 3 if str(identity_hash) else 0

    def add_operator_blacklist_recommendation(self, recommended_identity_hash, rationale,
                                              source_operator_hash, source_peer_hash="",
                                              source_node_name=""):
        for row in self._recommendations:
            if (
                row["recommended_identity_hash"] == recommended_identity_hash
                and row["rationale"] == rationale
                and row["source_operator_hash"] == source_operator_hash
                and row["source_peer_hash"] == source_peer_hash
                and row["source_node_name"] == source_node_name
            ):
                return False
        self._recommendations.append(
            {
                "recommended_identity_hash": recommended_identity_hash,
                "rationale": rationale,
                "source_operator_hash": source_operator_hash,
                "source_peer_hash": source_peer_hash,
                "source_node_name": source_node_name,
                "received_at": time.time(),
            }
        )
        return True

    def get_recent_operator_blacklist_recommendations(self, limit=10):
        return list(reversed(self._recommendations))[: int(limit)]

    def summarize_operator_blacklist_recommendations(self, limit=10):
        summary = {}
        for row in self._recommendations:
            target = row["recommended_identity_hash"]
            entry = summary.setdefault(
                target,
                {
                    "recommended_identity_hash": target,
                    "recommendation_count": 0,
                    "source_count": 0,
                    "latest_received_at": 0.0,
                    "sources": set(),
                    "rationales": set(),
                },
            )
            entry["recommendation_count"] += 1
            source = row.get("source_node_name") or row.get("source_peer_hash") or row.get("source_operator_hash")
            if source:
                entry["sources"].add(source)
            if row.get("rationale"):
                entry["rationales"].add(row["rationale"])
            entry["latest_received_at"] = max(entry["latest_received_at"], row.get("received_at", 0.0))

        rows = []
        for entry in summary.values():
            rows.append(
                {
                    "recommended_identity_hash": entry["recommended_identity_hash"],
                    "recommendation_count": entry["recommendation_count"],
                    "source_count": len(entry["sources"]),
                    "latest_received_at": entry["latest_received_at"],
                    "sources": ",".join(sorted(entry["sources"])),
                    "rationales": ",".join(sorted(entry["rationales"])),
                }
            )
        rows.sort(key=lambda row: (-float(row["latest_received_at"]), -int(row["recommendation_count"])))
        return rows[: int(limit)]


class _FakeOperator:
    def __init__(self, notify_result=True):
        self.notify_result = notify_result
        self.calls = []
        self.peer_calls = []

    def notify(self, title, body):
        self.calls.append((title, body))
        return self.notify_result

    def notify_peer_operator(self, peer_operator_hash, title, body):
        self.peer_calls.append((peer_operator_hash, title, body))
        return self.notify_result

    def endpoint_hash(self):
        return "aa" * 16

    def format_blacklist_recommendation(self, recommended_identity_hash, rationale, source_peer_hash=""):
        return (
            '{"recommended_identity_hash": "%s", "rationale": "%s", '
            '"source_node_name": "TestNode", "source_peer_hash": "%s", '
            '"source_operator_hash": "%s"}'
        ) % (recommended_identity_hash, rationale, source_peer_hash, self.endpoint_hash())


def _make_daemon_for_unit_tests(notify_result=True):
    daemon = SpeakeasyDaemon.__new__(SpeakeasyDaemon)
    daemon.node_name = "TestNode"
    daemon.identity = SimpleNamespace(hash=bytes.fromhex("11" * 16))
    daemon.active_links = []
    daemon.max_clients = 7
    daemon.db = _FakeDB()
    daemon.operator = _FakeOperator(notify_result=notify_result)
    daemon.operator_bootstrap_pending = False
    daemon.operator_bootstrap_retry_at = 0.0
    daemon.started_at = time.time() - 65
    daemon.discovered_peers = {"peer1": object()}
    return daemon


def test_operator_startup_notice_sent_when_available():
    daemon = _make_daemon_for_unit_tests(notify_result=True)

    daemon._notify_operator_startup()

    assert daemon.operator_bootstrap_pending is False
    assert len(daemon.operator.calls) == 1
    title, body = daemon.operator.calls[0]
    assert title == "Speakeasy online"
    assert "TestNode is alive." in body


def test_operator_startup_notice_retries_when_path_not_ready():
    daemon = _make_daemon_for_unit_tests(notify_result=False)

    daemon._notify_operator_startup()

    assert daemon.operator_bootstrap_pending is True
    assert daemon.operator_bootstrap_retry_at > time.time()


def test_operator_command_help_and_status_aliases():
    daemon = _make_daemon_for_unit_tests(notify_result=True)

    help_text = daemon.handle_operator_command("help", "")
    status_text = daemon.handle_operator_command("status", "")
    status_alias = daemon.handle_operator_command("stats", "")

    assert "Commands:" in help_text
    assert "Node: TestNode" in status_text
    assert status_alias == status_text


def test_operator_requests_alias_matches_pending(monkeypatch):
    daemon = _make_daemon_for_unit_tests(notify_result=True)

    pending = daemon.handle_operator_command("pending", "")
    requests = daemon.handle_operator_command("requests", "")

    assert pending == requests
    assert "#lounge" in pending


def test_operator_recent_command_shows_audit_entries():
    daemon = _make_daemon_for_unit_tests(notify_result=True)
    daemon.db.log_operator_action("approve_channel", target="lounge", detail="propagated_to=2")
    daemon.db.log_operator_action("pause_channel", target="off-topic")

    result = daemon.handle_operator_command("recent", "5")

    assert "Recent operator actions:" in result
    assert "approve_channel" in result
    assert "pause_channel" in result


def test_operator_recent_command_validates_count():
    daemon = _make_daemon_for_unit_tests(notify_result=True)

    result = daemon.handle_operator_command("recent", "many")

    assert result == "Usage: recent [N]"


def test_operator_recommend_command_broadcasts_to_known_peer_operators():
    daemon = _make_daemon_for_unit_tests(notify_result=True)
    daemon.db.save_peer_operator_endpoint("peer-alpha", "bb" * 16)
    daemon.db.save_peer_operator_endpoint("peer-beta", "cc" * 16)

    result = daemon.handle_operator_command("recommend", "deadbeefdeadbeef spamming forged records")

    assert "Broadcast blacklist recommendation" in result
    assert len(daemon.operator.peer_calls) == 2
    assert all(call[1] == "Speakeasy Blacklist Recommendation" for call in daemon.operator.peer_calls)


def test_operator_recommend_command_requires_reason_and_peers():
    daemon = _make_daemon_for_unit_tests(notify_result=True)

    missing_reason = daemon.handle_operator_command("recommend", "deadbeefdeadbeef")
    no_peers = daemon.handle_operator_command("recommend", "deadbeefdeadbeef repeated spam")

    assert missing_reason == "Usage: recommend <identity> <reason>"
    assert no_peers == "No peer operator endpoints known yet."


def test_inbound_peer_blacklist_recommendation_is_stored_and_listed():
    daemon = _make_daemon_for_unit_tests(notify_result=True)
    body = (
        '{"recommended_identity_hash": "feedfacefeedface", '
        '"rationale": "spam flood", '
        '"source_operator_hash": "' + ('bb' * 16) + '", '
        '"source_peer_hash": "' + ('cc' * 16) + '", '
        '"source_node_name": "PeerNode"}'
    )

    daemon.handle_peer_operator_message(
        source_hash="bb" * 16,
        title="Speakeasy Blacklist Recommendation",
        body=body,
    )

    listing = daemon.handle_operator_command("recommendations", "5")

    assert len(daemon.db._recommendations) == 1
    assert "Blacklist recommendation summary:" in listing
    assert "PeerNode" in listing
    assert "spam flood" in listing
    assert daemon.operator.calls[-1][0] == "Blacklist recommendation received"


def test_operator_blockid_blocks_identity_and_purges_local_records():
    daemon = _make_daemon_for_unit_tests(notify_result=True)

    result = daemon.handle_operator_command("blockid", "feedfacefeedface repeated spam")

    assert result == "Blocked identity feedfacefe and purged 3 local record(s)."
    assert daemon.db.is_blocked("feedfacefeedface") is True


def test_operator_unblockid_lifts_local_block():
    daemon = _make_daemon_for_unit_tests(notify_result=True)
    daemon.db.block_identity("feedfacefeedface", reason="spam")

    result = daemon.handle_operator_command("unblockid", "feedfacefeedface")

    assert result == "Unblocked identity feedfacefe."
    assert daemon.db.is_blocked("feedfacefeedface") is False


def test_duplicate_peer_recommendations_are_suppressed_and_aggregated():
    daemon = _make_daemon_for_unit_tests(notify_result=True)
    body = (
        '{"recommended_identity_hash": "feedfacefeedface", '
        '"rationale": "spam flood", '
        '"source_operator_hash": "' + ('bb' * 16) + '", '
        '"source_peer_hash": "' + ('cc' * 16) + '", '
        '"source_node_name": "PeerNode"}'
    )

    daemon.handle_peer_operator_message("bb" * 16, "Speakeasy Blacklist Recommendation", body)
    daemon.handle_peer_operator_message("bb" * 16, "Speakeasy Blacklist Recommendation", body)
    second = (
        '{"recommended_identity_hash": "feedfacefeedface", '
        '"rationale": "forged identities", '
        '"source_operator_hash": "' + ('dd' * 16) + '", '
        '"source_peer_hash": "' + ('ee' * 16) + '", '
        '"source_node_name": "PeerNode2"}'
    )
    daemon.handle_peer_operator_message("dd" * 16, "Speakeasy Blacklist Recommendation", second)

    listing = daemon.handle_operator_command("recommendations", "5")

    assert len(daemon.db._recommendations) == 2
    assert "2 report(s) from 2 source(s)" in listing
    assert "spam flood" in listing
    assert "forged identities" in listing
