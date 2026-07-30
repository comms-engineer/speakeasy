import msgpack

from reti_speakeasy import HostManager
from speakeasy_db import SpeakeasyDB


def test_host_manager_persists_known_hosts(tmp_path):
    db_path = tmp_path / "client.db"
    db = SpeakeasyDB(str(db_path))

    manager = HostManager(db=db)
    destination_hash = b"\x01" * 32
    payload = msgpack.packb({"name": "Test Hub", "load": 2, "max_load": 10}, use_bin_type=True)
    manager.update_from_announce(destination_hash, None, payload)

    reloaded = HostManager(db=db)
    hosts = reloaded.get_ranked_hosts()

    assert hosts
    assert hosts[0]["hex_hash"] == destination_hash.hex()
    assert hosts[0]["alias"] == "Test Hub"
