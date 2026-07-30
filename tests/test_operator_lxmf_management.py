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


class _FakeOperator:
    def __init__(self, notify_result=True):
        self.notify_result = notify_result
        self.calls = []

    def notify(self, title, body):
        self.calls.append((title, body))
        return self.notify_result

    def endpoint_hash(self):
        return "aa" * 16


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
