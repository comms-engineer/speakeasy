import json
import logging
import os
import signal
import sys
import time
import msgpack
import RNS

from fed_engine import Opcode, S2SProtocolEngine
from speakeasy_db import BandwidthClass, SpeakeasyDB

APP_NAME = "speakeasy"
ASPECT_HOST = "host"
ASPECT_CHAT = "parlor"

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("speakeasy_daemon")


class SpeakeasyDaemon:
    def __init__(self, config_path: str):
        self.config_path = config_path
        self.config = self._load_config()
        self.active_links = []
        self.running = True

        node_cfg = self.config.get("node", {})
        self.node_name = node_cfg.get("name", "speakeasy_host")
        bw_str = node_cfg.get("bandwidth_class", "MEDIUM_MESH")
        self.bandwidth_class = getattr(BandwidthClass, bw_str, BandwidthClass.MEDIUM_MESH)
        self.sync_interval = node_cfg.get("sync_interval_sec", 60)
        self.max_clients = node_cfg.get("max_clients", 10)

        # 1. Initialize DB & Reticulum Stack
        self.db = SpeakeasyDB(f"speakeasy_{self.node_name}.db")
        self.rns = RNS.Reticulum()

        # 2. Identity Management
        identity_path = os.path.expanduser(f"~/.reti_speakeasy/{self.node_name}_identity")
        if os.path.exists(identity_path):
            self.identity = RNS.Identity.from_file(identity_path)
            logger.info(f"Loaded existing identity from {identity_path}")
        else:
            os.makedirs(os.path.dirname(identity_path), exist_ok=True)
            self.identity = RNS.Identity()
            self.identity.to_file(identity_path)
            logger.info(f"Generated new identity saved to {identity_path}")

        # 3. Create Incoming Discovery Destination (Host Aspect)
        self.destination = RNS.Destination(
            self.identity,
            RNS.Destination.IN,
            RNS.Destination.SINGLE,
            APP_NAME,
            ASPECT_HOST,
        )
        self.destination.set_link_established_callback(self._on_link_established)
        logger.info(f"Speakeasy Host Listening on Destination Hash: [{RNS.prettyhexrep(self.destination.hash)}]")

        # 4. Initialize S2S Protocol Engine
        self.s2s_engine = S2SProtocolEngine(
            db=self.db,
            local_hash_bytes=self.identity.hash,
            bandwidth_class=self.bandwidth_class
        )

        self.announce_host()

    def build_announce_payload(self) -> bytes:
        """Serializes host telemetry into a compact msgpack payload (< 128 bytes)."""
        payload = {
            "v": 1,
            "name": self.node_name,
            "load": len(self.active_links),
            "max_load": self.max_clients,
            "flags": 0b00000011  # Supports Bulletin + DM Buffer
        }
        return msgpack.packb(payload, use_bin_type=True)

    def announce_host(self):
        """Broadcasts host destination announce and capacity metadata across Reticulum."""
        payload = self.build_announce_payload()
        self.destination.announce(app_data=payload)
        logger.info(f"Broadcasted host announce for [{RNS.prettyhexrep(self.destination.hash)}] (Load: {len(self.active_links)}/{self.max_clients})")

    def _load_config(self) -> dict:
        if not os.path.exists(self.config_path):
            logger.error(f"Config file not found at {self.config_path}")
            sys.exit(1)
        with open(self.config_path, "r") as f:
            return json.load(f)

    def _on_link_established(self, link):
        link.set_link_closed_callback(self._on_link_closed)
        link.set_packet_callback(self._on_packet_received)
        # Register callback for when client identifies over the link
        link.set_remote_identified_callback(self._on_remote_identified)

        if link not in self.active_links:
            self.active_links.append(link)

        remote_identity = link.get_remote_identity()
        if remote_identity:
            self._register_remote_identity(remote_identity)
            remote_hash = RNS.prettyhexrep(remote_identity.hash)[:10]
        else:
            remote_hash = "Unidentified Client"

        logger.info(f"Inbound client link established with [{remote_hash}]")
        self.announce_host()

        hello_frame = self.s2s_engine.build_hello(self.db.get_channel_names())
        RNS.Packet(link, hello_frame).send()

    def _on_remote_identified(self, link, remote_identity):
        if remote_identity:
            self._register_remote_identity(remote_identity)

    def _register_remote_identity(self, remote_identity):
        remote_hash_hex = remote_identity.hash.hex()
        pub_key = remote_identity.get_public_key()
        logger.info(f"Registered public key for remote identity [{remote_hash_hex[:10]}]")
        self.db.upsert_identity(
            identity_hash=remote_hash_hex,
            alias=f"Anon-{remote_hash_hex[:6]}",
            public_key=pub_key
        )

    def _on_link_closed(self, link):
        if link in self.active_links:
            self.active_links.remove(link)
        logger.info("Client link closed.")
        self.announce_host()

    def _on_packet_received(self, message, packet):
        try:
            opcode, response_frames = self.s2s_engine.process_inbound_frame(message)

            # Send direct responses back to sender
            for resp_bytes in response_frames:
                RNS.Packet(packet.link, resp_bytes).send()

            # HUB RELAY: Fan-out real-time DELTA_PUSH frames to all other connected clients/hosts
            if opcode == Opcode.DELTA_PUSH:
                relayed_count = 0
                for link in self.active_links:
                    if link != packet.link and link.status == RNS.Link.ACTIVE:
                        RNS.Packet(link, message).send()
                        relayed_count += 1
                if relayed_count > 0:
                    logger.info(f"Relayed DELTA_PUSH frame to {relayed_count} peer link(s).")

        except Exception as e:
            logger.error(f"Error processing inbound frame: {e}")

    def maintain_static_peers(self):
        """Attempts connection to configured static peer host nodes."""
        peers = self.config.get("static_peers", [])
        for peer_hex in peers:
            clean_hex = peer_hex.replace("<", "").replace(">", "").replace(" ", "").replace(":", "")
            try:
                dest_bytes = bytes.fromhex(clean_hex)
                if not RNS.Transport.has_path(dest_bytes):
                    logger.info(f"Requesting transport path for static peer [{clean_hex[:10]}]...")
                    RNS.Transport.request_path(dest_bytes)
                    continue

                identity = RNS.Identity.recall(dest_bytes)
                if identity:
                    already_connected = any(
                        l.get_remote_identity() and l.get_remote_identity().hash == identity.hash
                        for l in self.active_links if l.status == RNS.Link.ACTIVE
                    )
                    if not already_connected:
                        logger.info(f"Initiating link to static peer host [{clean_hex[:10]}]...")

                        # Interface Settlement Delay
                        time.sleep(0.35)

                        target_dest = RNS.Destination(identity, RNS.Destination.OUT, RNS.Destination.SINGLE, APP_NAME, ASPECT_HOST)
                        link = RNS.Link(target_dest)
                        link.set_link_established_callback(self._on_link_established)
                        link.set_link_closed_callback(self._on_link_closed)
            except Exception as e:
                logger.error(f"Failed static peer connection to [{clean_hex[:10]}]: {e}")

    def run(self):
        logger.info(f"Daemon successfully started. Service loop active ({self.sync_interval}s interval)...")
        last_sync = 0
        last_announce = time.time()
        while self.running:
            now = time.time()
            if now - last_sync >= self.sync_interval:
                self.maintain_static_peers()
                last_sync = now

            if now - last_announce >= 900:  # Announce every 15 mins
                self.announce_host()
                last_announce = now

            time.sleep(1)

    def stop(self):
        logger.info("Shutting down daemon...")
        self.running = False


if __name__ == "__main__":
    cfg_file = sys.argv[1] if len(sys.argv) > 1 else "speakeasy_config.json"
    daemon = SpeakeasyDaemon(cfg_file)

    def signal_handler(sig, frame):
        daemon.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    daemon.run()
