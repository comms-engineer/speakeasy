"""
Announce-driven peer discovery.

The daemon is exercised without a live Reticulum stack: `on_peer_announce` and
`connect_discovered_peers` are the whole decision surface, and both are pure
enough to drive directly.
"""

from types import SimpleNamespace

import msgpack
import RNS
import pytest

import speakeasy_daemon
from fed_engine import Opcode, S2SProtocolEngine, WireCodec
from speakeasy_daemon import HubAnnounceHandler, SpeakeasyDaemon
from speakeasy_db import BandwidthClass, SpeakeasyDB


class FakeLink:
    def __init__(self, identity, status=None):
        self.identity = identity
        self.status = RNS.Link.ACTIVE if status is None else status

    def get_remote_identity(self):
        return self.identity


@pytest.fixture
def daemon(tmp_path):
    """A daemon with only the state discovery touches, and no RNS stack."""
    instance = SpeakeasyDaemon.__new__(SpeakeasyDaemon)
    instance.identity = RNS.Identity()
    instance.db = SpeakeasyDB(str(tmp_path / "hub.db"))
    instance.active_links = []
    instance.discovered_peers = {}
    instance.max_clients = 2
    instance.settlement_delay = 0
    instance.auto_discover_peers = True
    instance.linked = []
    instance.channel_presence_cache = {}
    instance.s2s_engine = S2SProtocolEngine(
        db=instance.db,
        local_hash_bytes=instance.identity.hash,
        bandwidth_class=BandwidthClass.HIGH_SPEED,
    )
    instance._link_to = lambda identity, label: instance.linked.append(label)
    yield instance
    instance.db.close()


def announce(daemon, identity=None, name="Peer"):
    identity = identity or RNS.Identity()
    destination = RNS.Destination(identity, RNS.Destination.OUT, RNS.Destination.SINGLE,
                                  speakeasy_daemon.APP_NAME, speakeasy_daemon.ASPECT_HOST)
    daemon.on_peer_announce(destination.hash, identity, msgpack.packb({"name": name}, use_bin_type=True))
    return identity, destination


def test_announce_handler_filters_on_the_host_aspect(daemon):
    handler = HubAnnounceHandler(daemon)

    assert handler.aspect_filter == "speakeasy.host"


def test_announce_registers_peer_and_learns_its_key(daemon):
    identity, destination = announce(daemon, name="Speakeasy-Beta")

    assert destination.hash.hex() in daemon.discovered_peers
    assert daemon.db.get_public_key(identity.hash.hex()) == identity.get_public_key()
    assert daemon.db.get_identity_record(identity.hash.hex())["alias"] == "Speakeasy-Beta"


def test_own_announce_is_ignored(daemon):
    announce(daemon, identity=daemon.identity)

    assert daemon.discovered_peers == {}


def test_repeated_announces_do_not_queue_twice(daemon):
    identity, _ = announce(daemon)
    announce(daemon, identity=identity)

    assert len(daemon.discovered_peers) == 1


def test_discovered_peers_are_linked_once(daemon):
    _, destination = announce(daemon)

    daemon.connect_discovered_peers()
    assert daemon.linked == [destination.hash.hex()]

    # Already linked: no second attempt.
    identity = daemon.discovered_peers[destination.hash.hex()]
    daemon.active_links.append(FakeLink(identity))
    daemon.connect_discovered_peers()
    assert len(daemon.linked) == 1


def test_discovery_respects_capacity(daemon):
    announce(daemon)
    announce(daemon)
    daemon.active_links = [FakeLink(RNS.Identity()), FakeLink(RNS.Identity())]

    daemon.connect_discovered_peers()

    assert daemon.linked == []


def test_discovery_can_be_disabled(daemon):
    announce(daemon)
    daemon.auto_discover_peers = False

    daemon.connect_discovered_peers()

    assert daemon.linked == []


def test_channel_poll_responses_are_cached_for_relay_choices(daemon):
    peer_identity = RNS.Identity()
    link = FakeLink(peer_identity)
    daemon.active_links = [link]

    response = WireCodec.pack(
        Opcode.CHANNEL_POLL_RESP,
        peer_identity.hash,
        {0: "lounge", 1: True},
    )
    daemon._on_packet_received(response, SimpleNamespace(link=link))

    assert daemon.should_relay_channel_to_peer("lounge", link) is False
    assert daemon.should_relay_channel_to_peer("events", link) is True
