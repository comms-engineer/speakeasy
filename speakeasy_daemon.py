import json
import logging
import os
import signal
import sys
import time
import msgpack
import RNS

from fed_engine import MAX_MESSAGE_CONTENT_BYTES, Opcode, S2SProtocolEngine
from speakeasy_db import BandwidthClass, SpeakeasyDB, DEFAULT_EPOCH_BUCKET_SEC

APP_NAME = "speakeasy"
ASPECT_HOST = "host"
ASPECT_CHAT = "parlor"

STATE_DIR = os.path.expanduser("~/.reti_speakeasy")

logger = logging.getLogger("speakeasy_daemon")


def configure_logging(log_cfg: dict):
    level = getattr(logging, str(log_cfg.get("level", "INFO")).upper(), logging.INFO)
    handlers = [logging.StreamHandler(sys.stdout)]

    if log_cfg.get("log_to_file"):
        log_path = os.path.expanduser(log_cfg.get("log_file_path") or f"{STATE_DIR}/logs/daemon.log")
        try:
            os.makedirs(os.path.dirname(log_path), exist_ok=True)
            handlers.append(logging.FileHandler(log_path))
        except OSError as e:
            print(f"Could not open log file {log_path}: {e}", file=sys.stderr)

    logging.basicConfig(
        level=level,
        format="[%(asctime)s] [%(levelname)s] %(message)s",
        handlers=handlers,
        force=True,
    )


class SpeakeasyDaemon:
    def __init__(self, config_path: str):
        self.config_path = config_path
        self.config = self._load_config()
        configure_logging(self.config.get("logging", {}))

        self.active_links = []
        self.running = True

        node_cfg = self.config.get("node", {})
        fed_cfg = self.config.get("federation", {})
        chan_cfg = self.config.get("channels", {})
        storage_cfg = self.config.get("storage", {})

        self.node_name = node_cfg.get("name", "speakeasy_host")
        bw_str = node_cfg.get("bandwidth_class", "MEDIUM_MESH")
        self.bandwidth_class = getattr(BandwidthClass, bw_str, BandwidthClass.MEDIUM_MESH)
        self.sync_interval = node_cfg.get("sync_interval_sec", 60)
        self.announce_interval = node_cfg.get("announce_interval_sec", 900)
        self.max_clients = node_cfg.get("max_clients", 10)
        self.settlement_delay = fed_cfg.get("settlement_delay_ms", 350) / 1000.0
        self.allowed_channels = set(chan_cfg.get("allowed_channels") or ())
        self.channel_blocklist = set(chan_cfg.get("channel_blocklist") or ())
        self.message_ttl_days = storage_cfg.get("message_ttl_days", 0)
        self.vacuum_interval = storage_cfg.get("vacuum_interval_hours", 24) * 3600

        # 1. Initialize DB & Reticulum Stack
        os.makedirs(STATE_DIR, exist_ok=True)
        db_filename = storage_cfg.get("db_filename") or f"speakeasy_{self.node_name}.db"
        # Kept inside the state directory: that is the path mounted as a volume
        # by docker-compose, so a CWD-relative database would not survive a
        # container rebuild.
        self.db = SpeakeasyDB(
            db_path=os.path.join(STATE_DIR, db_filename),
            epoch_bucket_sec=fed_cfg.get("epoch_bucket_sec", DEFAULT_EPOCH_BUCKET_SEC),
            max_message_bytes=min(chan_cfg.get("max_message_bytes") or MAX_MESSAGE_CONTENT_BYTES,
                                  MAX_MESSAGE_CONTENT_BYTES),
        )
        for channel in sorted(self.allowed_channels):
            if channel not in self.channel_blocklist:
                self.db.add_channel(channel, f"Configured channel #{channel}")

        self.rns = RNS.Reticulum()

        # 2. Identity Management
        identity_path = os.path.join(STATE_DIR, f"{self.node_name}_identity")
        if os.path.exists(identity_path):
            self.identity = RNS.Identity.from_file(identity_path)
            logger.info(f"Loaded existing identity from {identity_path}")
        else:
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
        self.destination.set_link_established_callback(self._on_inbound_link)
        logger.info(f"Speakeasy Host Listening on Destination Hash: [{RNS.prettyhexrep(self.destination.hash)}]")

        # 4. Initialize S2S Protocol Engine
        self.s2s_engine = S2SProtocolEngine(
            db=self.db,
            local_hash_bytes=self.identity.hash,
            bandwidth_class=self.bandwidth_class,
            allowed_channels=self.allowed_channels or None,
            channel_blocklist=self.channel_blocklist,
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
            print(f"Config file not found at {self.config_path}", file=sys.stderr)
            sys.exit(1)
        with open(self.config_path, "r") as f:
            return json.load(f)

    def _on_inbound_link(self, link):
        if len(self.active_links) >= self.max_clients:
            logger.warning(f"Refused inbound link: at capacity ({self.max_clients} clients).")
            link.teardown()
            return
        self._register_link(link)

    def _on_outbound_link(self, link):
        # Identify to the remote hub, otherwise it can never bind our identity
        # (and public key) to this link, and nothing we sign is verifiable there.
        link.identify(self.identity)
        self._register_link(link)

    def _register_link(self, link):
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

        logger.info(f"Link established with [{remote_hash}]")
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
            accepted_msg_ids = []
            opcode, response_frames = self.s2s_engine.process_inbound_frame(message, accepted_msg_ids)

            # Send direct responses back to sender
            for resp_bytes in response_frames:
                RNS.Packet(packet.link, resp_bytes).send()

            # HUB RELAY: fan out real-time messages that verified locally. The
            # inbound frame itself is never forwarded -- a single frame can mix
            # valid records with forged ones, and relaying raw bytes would turn
            # the hub into an amplifier for unsigned traffic.
            if opcode == Opcode.DELTA_PUSH and accepted_msg_ids:
                relay_frames = self.s2s_engine.build_relay_frames(accepted_msg_ids, hop_count=1)
                peers = [
                    link for link in self.active_links
                    if link != packet.link and link.status == RNS.Link.ACTIVE
                ]
                for link in peers:
                    for frame in relay_frames:
                        RNS.Packet(link, frame).send()
                if peers:
                    logger.info(f"Relayed {len(accepted_msg_ids)} verified message(s) "
                                f"to {len(peers)} peer link(s).")

        except Exception as e:
            logger.error(f"Error processing inbound frame: {e}", exc_info=True)

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
                        link.get_remote_identity() and link.get_remote_identity().hash == identity.hash
                        for link in self.active_links if link.status == RNS.Link.ACTIVE
                    )
                    if not already_connected:
                        logger.info(f"Initiating link to static peer host [{clean_hex[:10]}]...")

                        # Interface Settlement Delay
                        time.sleep(self.settlement_delay)

                        target_dest = RNS.Destination(identity, RNS.Destination.OUT, RNS.Destination.SINGLE, APP_NAME, ASPECT_HOST)
                        link = RNS.Link(target_dest)
                        link.set_link_established_callback(self._on_outbound_link)
                        link.set_link_closed_callback(self._on_link_closed)
            except Exception as e:
                logger.error(f"Failed static peer connection to [{clean_hex[:10]}]: {e}")

    def run(self):
        logger.info(f"Daemon successfully started. Service loop active ({self.sync_interval}s interval)...")
        last_sync = 0
        last_announce = time.time()
        last_vacuum = time.time()
        while self.running:
            now = time.time()
            if now - last_sync >= self.sync_interval:
                self.maintain_static_peers()
                last_sync = now

            if now - last_announce >= self.announce_interval:
                self.announce_host()
                last_announce = now

            if self.vacuum_interval > 0 and now - last_vacuum >= self.vacuum_interval:
                pruned = self.db.prune_messages(self.message_ttl_days)
                logger.info(f"Retention sweep removed {pruned} expired message(s).")
                last_vacuum = now

            time.sleep(1)

    def stop(self):
        logger.info("Shutting down daemon...")
        self.running = False
        for link in list(self.active_links):
            try:
                link.teardown()
            except Exception:
                pass
        self.db.close()


if __name__ == "__main__":
    cfg_file = sys.argv[1] if len(sys.argv) > 1 else "speakeasy_config.json"
    daemon = SpeakeasyDaemon(cfg_file)

    def signal_handler(sig, frame):
        daemon.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    daemon.run()
