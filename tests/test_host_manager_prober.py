import time
import threading
from types import SimpleNamespace
from speakeasy_db import SpeakeasyDB
from reti_speakeasy import HostManager


class DummyTransport:
    def __init__(self):
        self.requested = []

    def has_path(self, dest):
        # Simulate no known path initially
        return False

    def request_path(self, dest):
        self.requested.append(dest)

    def hops_to(self, dest):
        return 3


class DummyIdentity:
    def __init__(self, hash_bytes):
        self.hash = hash_bytes


def test_prober_updates_host(tmp_path, monkeypatch):
    db_path = tmp_path / "prober_hosts.db"
    db = SpeakeasyDB(str(db_path))

    # Create a seed host in DB
    host = {
        "hex_hash": "bb" * 16,
        "alias": "SeedHost",
        "last_seen": time.time() - 3600,
        "hops": 99,
        "load": 0,
        "max_load": 10,
        "is_manual": False,
        "metadata": {},
    }
    assert db.save_host(host)

    # Mock RNS.Transport and RNS.Identity.recall
    dummy_t = DummyTransport()
    monkeypatch.setattr('reti_speakeasy.RNS.Transport', dummy_t)
    monkeypatch.setattr('reti_speakeasy.RNS.Identity.recall', lambda dest: DummyIdentity(dest))

    # Start HostManager with small probe interval
    hm = HostManager(db=db, probe_interval=1, probes_per_round=1)
    hm.start_prober()

    # Wait a short while for prober to run
    time.sleep(2)

    # Stop prober
    hm.stop_prober()

    # Load host back from DB and assert hops updated and last_seen refreshed
    loaded = db.get_host(host["hex_hash"]) 
    assert loaded is not None
    assert loaded["hops"] != host["hops"]
    assert loaded["last_seen"] > host["last_seen"]

    db.close()
