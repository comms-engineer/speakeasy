import json
import logging
import os
import signal
import sys
import time
import msgpack
import RNS

import blackhole
from channel_summary import build_channel_summary
from fed_engine import (
    DEFAULT_SYNC_HISTORY_DAYS,
    MAX_MESSAGE_CONTENT_BYTES,
    MAX_SYNC_EPOCHS,
    Opcode,
    S2SProtocolEngine,
    WireCodec,
)
from operator_iface import OperatorInterface
from speakeasy_db import BandwidthClass, SpeakeasyDB, DEFAULT_EPOCH_BUCKET_SEC

APP_NAME = "speakeasy"
ASPECT_HOST = "host"

# Identities pushed to a newly connected peer so it can verify history it is
# about to receive without a round trip per author.
IDENTITY_BOOTSTRAP_LIMIT = 100

STATE_DIR = os.path.expanduser("~/.reti_speakeasy")

logger = logging.getLogger("speakeasy_daemon")


def _env_flag(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


def _reticulum_config_dir() -> str | None:
    override = os.environ.get("SPEAKEASY_RNS_CONFIG_DIR") or os.environ.get("RNS_CONFIG_DIR")
    if override:
        return os.path.expanduser(override)
    return None


def _initialise_reticulum(component: str, require_shared_default: bool = False) -> RNS.Reticulum:
    configdir = _reticulum_config_dir()
    require_shared = _env_flag("SPEAKEASY_REQUIRE_SHARED_INSTANCE", default=require_shared_default)
    rns = RNS.Reticulum(configdir=configdir, require_shared_instance=require_shared)

    if rns.is_connected_to_shared_instance:
        logger.info("%s connected to shared Reticulum instance%s",
                    component,
                    f" via {configdir}" if configdir else "")
    elif rns.is_shared_instance:
        logger.info("%s started a shared Reticulum instance%s",
                    component,
                    f" via {configdir}" if configdir else "")
    else:
        logger.warning("%s is running with a standalone Reticulum instance%s",
                       component,
                       f" via {configdir}" if configdir else "")

    return rns


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


class HubAnnounceHandler:
    """
    Receives Reticulum announces for the Speakeasy host aspect.

    Discovery is what makes the network self-assembling: without it a hub only
    ever talks to the peers hard-coded in `static_peers`.
    """

    def __init__(self, daemon: "SpeakeasyDaemon"):
        self.aspect_filter = f"{APP_NAME}.{ASPECT_HOST}"
        self.daemon = daemon

    def received_announce(self, destination_hash, announced_identity, app_data):
        self.daemon.on_peer_announce(destination_hash, announced_identity, app_data)


class SpeakeasyDaemon:
    def __init__(self, config_path: str):
        self.config_path = config_path
        self.config = self._load_config()
        configure_logging(self.config.get("logging", {}))

        self.active_links = []
        self.running = True
        self.discovered_peers = {}
        self.operator = None
        self.announce_handler = None
        self.propagated_channels = set()
        self.channels_seeded = False
        self.sync_offset = 0
        self.started_at = time.time()
        self.operator_bootstrap_pending = False
        self.operator_bootstrap_retry_at = 0.0
        self.channel_presence_cache: dict[tuple[str, str], bool] = {}

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
        self.max_messages_per_channel = int(storage_cfg.get("max_messages_per_channel", 0) or 0)
        self.max_db_bytes = int(float(storage_cfg.get("max_db_mb", 0) or 0) * 1024 * 1024)
        self.prune_interval = float(storage_cfg.get("prune_interval_hours", 1) or 0) * 3600

        # Never chase history this node would immediately delete: syncing
        # further back than the retention window makes two hubs trade the same
        # messages forever, each pruning what the other just sent.
        history_days = float(fed_cfg.get("max_sync_history_days", DEFAULT_SYNC_HISTORY_DAYS))
        if self.message_ttl_days:
            history_days = min(history_days, float(self.message_ttl_days))
        self.sync_history_days = history_days

        mod_cfg = self.config.get("moderation", {})
        self.operator_lxmf_hash = mod_cfg.get("operator_lxmf_hash") or ""
        self.accept_channel_requests = bool(mod_cfg.get("accept_channel_requests", True))
        self.receive_federated_channel_nominations = bool(
            mod_cfg.get("receive_federated_channel_nominations", True)
        )
        self.auto_discover_peers = bool(fed_cfg.get("auto_discover_peers", True))
        self.include_channel_summary_in_announces = bool(
            fed_cfg.get("include_channel_summary_in_announces", True)
        )

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
            if channel in self.channel_blocklist:
                self.db.add_channel(channel, f"Configured channel #{channel}", status="blocked")
            else:
                self.db.add_channel(channel, f"Configured channel #{channel}", status="active")

        self.allowed_channels = set(self.db.get_active_channel_names())
        self.channel_blocklist.update(
            c["name"] for c in self.db.get_channels()
            if str(c.get("status") or "active").lower() == "blocked"
        )

        self.rns = _initialise_reticulum("Speakeasy daemon")

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
            accept_channel_requests=self.accept_channel_requests,
            receive_federated_channel_nominations=self.receive_federated_channel_nominations,
            sync_history_days=self.sync_history_days,
        )
        self._rebuild_channel_policy()

        # 5. Peer discovery & operator control
        if self.auto_discover_peers:
            self.announce_handler = HubAnnounceHandler(self)
            RNS.Transport.register_announce_handler(self.announce_handler)
            logger.info("Announce-based peer discovery enabled.")
        else:
            self.announce_handler = None

        if self.operator_lxmf_hash:
            try:
                self.operator = OperatorInterface(
                    identity=self.identity,
                    storage_path=os.path.join(STATE_DIR, "lxmf"),
                    operator_hash=self.operator_lxmf_hash,
                    command_handler=self.handle_operator_command,
                    node_name=self.node_name,
                )
            except Exception as e:
                logger.error(f"Could not start the operator LXMF interface: {e}")
        else:
            logger.info("No moderation.operator_lxmf_hash configured; "
                        "use speakeasy_admin.py to review channel requests.")

        self.announce_host()
        self._notify_operator_startup()

    def build_announce_payload(self) -> bytes:
        """Serializes host telemetry into a compact msgpack payload (< 128 bytes)."""
        payload = {
            "v": 1,
            "name": self.node_name,
            "load": len(self.active_links),
            "max_load": self.max_clients,
            "flags": 0b00000011  # Supports Bulletin + DM Buffer
        }
        if self.include_channel_summary_in_announces:
            active_channels = self.db.get_active_channel_names()
            payload["chc"] = len(active_channels)
            payload["chs"] = build_channel_summary(active_channels)
            payload["chv"] = 1
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
        remote = link.get_remote_identity()
        if remote and blackhole.is_blocked(remote.hash, self.db):
            logger.info(f"Refused inbound link from blackholed identity [{remote.hash.hex()[:10]}]")
            link.teardown()
            return
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

        hello_frame = self.s2s_engine.build_hello(self.db.get_active_channel_names())
        RNS.Packet(link, hello_frame).send()
        self._bootstrap_link(link)

    def _rebuild_channel_policy(self):
        """Aligns runtime policy with persisted channel lifecycle state."""
        self.allowed_channels = set(self.db.get_active_channel_names())
        blocked = {
            c["name"] for c in self.db.get_channels()
            if str(c.get("status") or "active").lower() == "blocked"
        }
        self.channel_blocklist.update(blocked)
        if hasattr(self, "s2s_engine") and self.s2s_engine:
            self.s2s_engine.allowed_channels = self.allowed_channels or None
            self.s2s_engine.channel_blocklist = set(self.channel_blocklist)

    def _bootstrap_link(self, link):
        """
        Hands a fresh peer the keys, profiles and approved channels it needs to
        make sense of anything this hub relays to it. A client that has never
        met the other participants cannot verify their signatures, so without
        this every relayed message is silently discarded on arrival.
        """
        identity_hashes = self.db.get_recent_identity_hashes(IDENTITY_BOOTSTRAP_LIMIT)
        frames = self.s2s_engine.build_identity_frames(identity_hashes)
        frames += self.s2s_engine.build_profile_frames(identity_hashes)
        frames += self.s2s_engine.build_channel_frames(
            [c["name"] for c in self.db.get_signed_channels()]
        )
        if self._send_frames(link, frames) and frames:
            logger.info(f"Bootstrapped peer link with {len(frames)} identity/profile/channel frame(s).")

    def _send_frames(self, link, frames) -> bool:
        if link.status != RNS.Link.ACTIVE:
            return False
        for frame in frames:
            RNS.Packet(link, frame).send()
        return True

    def _broadcast(self, frames, exclude_link=None) -> int:
        if not frames:
            return 0
        peers = [
            link for link in self.active_links
            if link is not exclude_link and link.status == RNS.Link.ACTIVE
        ]
        for link in peers:
            self._send_frames(link, frames)
        return len(peers)

    def should_relay_channel_to_peer(self, channel_name: str, link) -> bool:
        if not channel_name:
            return False
        key = (str(channel_name).lstrip("#"), self._peer_key(link))
        if key in self.channel_presence_cache:
            return not self.channel_presence_cache[key]
        return True

    def _peer_key(self, link) -> str:
        remote = getattr(link, "get_remote_identity", lambda: None)()
        if remote is not None:
            return remote.hash.hex()
        return str(id(link))

    def _remember_channel_presence(self, channel_name: str, link, present: bool) -> None:
        if not channel_name:
            return
        self.channel_presence_cache[(str(channel_name).lstrip("#"), self._peer_key(link))] = bool(present)

    def _on_remote_identified(self, link, remote_identity):
        if remote_identity and blackhole.is_blocked(remote_identity.hash, self.db):
            # A link identifies after establishment, so this is the first point
            # at which a blackholed peer can be recognised.
            logger.info(f"Tearing down link from blackholed identity "
                        f"[{remote_identity.hash.hex()[:10]}]")
            link.teardown()
            return
        if remote_identity:
            self._register_remote_identity(remote_identity)
            # Everyone else needs this key to verify what this peer posts.
            self._broadcast(
                self.s2s_engine.build_identity_frames([remote_identity.hash.hex()]),
                exclude_link=link
            )

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
            result = self.s2s_engine.process_inbound_frame(message)

            # Direct responses go back to the sender.
            self._send_frames(packet.link, result.frames)

            # HUB RELAY: fan out records that verified locally. The inbound
            # frame itself is never forwarded -- a single frame can mix valid
            # records with forged ones, and relaying raw bytes would turn the
            # hub into an amplifier for unsigned traffic.
            relay_frames = []
            if result.opcode == Opcode.DELTA_PUSH and result.accepted_msg_ids:
                senders = [
                    row["sender_hash"] for row in
                    (self.db.get_message(m) for m in result.accepted_msg_ids) if row
                ]
                # Keys first: a peer meeting this author for the first time
                # cannot verify the message that follows without them.
                relay_frames += self.s2s_engine.build_identity_frames(senders)
                relay_frames += self.s2s_engine.build_relay_frames(result.accepted_msg_ids, hop_count=1)

            if result.accepted_profiles:
                relay_frames += self.s2s_engine.build_identity_frames(result.accepted_profiles)
                relay_frames += self.s2s_engine.build_profile_frames(result.accepted_profiles)

            if result.accepted_channels:
                relay_frames += self.s2s_engine.build_channel_frames(result.accepted_channels, hop_count=1)

            if result.opcode == Opcode.CHANNEL_POLL_RESP:
                try:
                    _, _, _, payload = WireCodec.unpack(message)
                except Exception:
                    payload = {}
                channel_name = str(payload.get(0) or payload.get("0") or "")
                present = bool(payload.get(1) or payload.get("1") or False)
                self._remember_channel_presence(channel_name, packet.link, present)

            peer_count = self._broadcast(relay_frames, exclude_link=packet.link)
            if peer_count and relay_frames:
                logger.info(f"Relayed {len(relay_frames)} verified frame(s) to {peer_count} peer link(s).")

            for request in result.channel_requests:
                self._notify_operator_of_request(request)
                self._relay_channel_request(request, exclude_link=packet.link)

        except Exception as e:
            logger.error(f"Error processing inbound frame: {e}", exc_info=True)

    # ------------------------------------------------------------------
    # Channel requests & operator control
    # ------------------------------------------------------------------

    def _notify_operator_of_request(self, request: dict):
        profile = self.db.find_profile(request["requester_hash"]) or {}
        requester = profile.get("handle") or request["requester_hash"][:10]
        if self.operator and self.operator.notify_channel_request(
            request["name"], request["description"], str(requester)
        ):
            logger.info(f"Notified operator of channel request #{request['name']}")
        else:
            logger.info(f"Channel request #{request['name']} is pending operator review "
                        f"(no operator notification delivered).")

    def _relay_channel_request(self, request: dict, exclude_link=None):
        frame = self.s2s_engine.build_channel_req(
            request["name"],
            request.get("description") or "",
            requester_hash=request.get("requester_hash"),
        )
        peers = [
            link for link in self.active_links
            if link is not exclude_link and link.status == RNS.Link.ACTIVE
            and self.should_relay_channel_to_peer(request["name"], link)
        ]
        for link in peers:
            self._send_frames(link, [frame])
        if peers:
            logger.info(
                f"Relayed channel nomination #{request['name']} to {len(peers)} peer link(s)."
            )

    def _notify_operator_startup(self):
        if not self.operator:
            return

        active = len(self.db.get_active_channel_names())
        blocked = len([c for c in self.db.get_channels() if (c.get("status") or "active") == "blocked"])
        body = (
            f"{self.node_name} is alive.\n"
            f"Node hash: {self.identity.hash.hex()[:16]}\n"
            f"Active links: {len(self.active_links)}/{self.max_clients}\n"
            f"Channels: {active} active, {blocked} blocked\n\n"
            f"Reply with 'help' for operator commands."
        )

        if self.operator.notify("Speakeasy online", body):
            self.operator_bootstrap_pending = False
            self._log_operator_action("operator_heartbeat_sent", target="startup")
            logger.info("Sent startup heartbeat to operator over LXMF.")
            return

        self.operator_bootstrap_pending = True
        self.operator_bootstrap_retry_at = time.time() + 30.0
        logger.info("Startup heartbeat to operator queued for retry.")

    def _maybe_retry_operator_startup_notice(self):
        if not self.operator or not self.operator_bootstrap_pending:
            return
        if time.time() < self.operator_bootstrap_retry_at:
            return
        self._notify_operator_startup()
        if self.operator_bootstrap_pending:
            self.operator_bootstrap_retry_at = time.time() + 30.0

    def _operator_help_text(self) -> str:
        return (
            "Commands:\n"
            "help\n"
            "status\n"
            "pending (alias: requests)\n"
            "channels\n"
            "recent [N] (alias: audit)\n"
            "approve <channel>\n"
            "deny <channel>\n"
            "add <channel> [description]\n"
            "pause <channel>\n"
            "resume <channel>\n"
            "block <channel>"
        )

    def _log_operator_action(self, action: str, target: str = "", detail: str = ""):
        try:
            self.db.log_operator_action(action=action, target=target, detail=detail)
        except Exception:
            # Operator tooling should keep working even if audit storage fails.
            logger.debug("Could not persist operator action audit record.", exc_info=True)

    def _operator_recent_text(self, argument: str = "") -> str:
        limit = 10
        if argument:
            try:
                limit = max(1, min(int(argument.strip()), 50))
            except ValueError:
                return "Usage: recent [N]"

        rows = self.db.get_recent_operator_actions(limit=limit)
        if not rows:
            return "No operator actions recorded yet."

        lines = ["Recent operator actions:"]
        for row in rows:
            ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(float(row.get("timestamp") or 0)))
            action = str(row.get("action") or "unknown")
            target = str(row.get("target") or "-")
            detail = str(row.get("detail") or "")
            suffix = f" ({detail})" if detail else ""
            lines.append(f"{ts} | {action} | {target}{suffix}")
        return "\n".join(lines)

    def _operator_status_text(self) -> str:
        uptime_sec = int(max(0, time.time() - self.started_at))
        h = uptime_sec // 3600
        m = (uptime_sec % 3600) // 60
        s = uptime_sec % 60
        uptime = f"{h:02d}:{m:02d}:{s:02d}"

        channels = self.db.get_channels()
        active = sum(1 for c in channels if str(c.get("status") or "active").lower() == "active")
        paused = sum(1 for c in channels if str(c.get("status") or "active").lower() == "paused")
        blocked = sum(1 for c in channels if str(c.get("status") or "active").lower() == "blocked")
        pending = len(self.db.get_channel_requests("pending"))
        endpoint = self.operator.endpoint_hash()[:16] if self.operator else "unavailable"

        return (
            f"Node: {self.node_name}\n"
            f"Uptime: {uptime}\n"
            f"LXMF endpoint: {endpoint}\n"
            f"Links: {len(self.active_links)}/{self.max_clients}\n"
            f"Discovered peers: {len(self.discovered_peers)}\n"
            f"Channels: {active} active, {paused} paused, {blocked} blocked\n"
            f"Pending channel requests: {pending}"
        )

    def approve_channel(self, name: str) -> str:
        pending = {r["name"]: r for r in self.db.get_channel_requests("pending")}
        description = (pending.get(name) or {}).get("description") or f"Channel #{name}"

        record = self.db.sign_and_add_channel(self.identity, name, description)
        if not record:
            return f"Could not approve #{name}."

        self.db.set_channel_request_status(name, "approved")
        self.db.set_channel_status(name, "active")
        self.channel_blocklist.discard(name)
        self._rebuild_channel_policy()

        peers = self._broadcast(self.s2s_engine.build_channel_frames([name]))
        self._log_operator_action("approve_channel", target=name,
                      detail=f"propagated_to={peers}")
        logger.info(f"Approved channel #{name}; propagated to {peers} peer link(s).")
        return f"Approved #{name} and propagated it to {peers} connected peer(s)."

    def add_channel(self, name: str, description: str = "") -> str:
        clean = str(name or "").lstrip("#").strip()
        if not clean:
            return "Usage: add <channel> [description]"
        if not self.db.get_channel(clean):
            self.db.add_channel(clean, description or f"Channel #{clean}", status="active")
        record = self.db.sign_and_add_channel(self.identity, clean, description or f"Channel #{clean}")
        if not record:
            return f"Could not add #{clean}."
        self.db.set_channel_status(clean, "active")
        self.channel_blocklist.discard(clean)
        self._rebuild_channel_policy()
        peers = self._broadcast(self.s2s_engine.build_channel_frames([clean]))
        self._log_operator_action("add_channel", target=clean,
                      detail=f"propagated_to={peers}")
        return f"Added #{clean} and propagated it to {peers} connected peer(s)."

    def pause_channel(self, name: str) -> str:
        clean = str(name or "").lstrip("#").strip()
        if not self.db.set_channel_status(clean, "paused"):
            return f"Unknown channel #{clean}."
        self._rebuild_channel_policy()
        self._log_operator_action("pause_channel", target=clean)
        return f"Paused #{clean}; traffic is now refused."

    def resume_channel(self, name: str) -> str:
        clean = str(name or "").lstrip("#").strip()
        if not self.db.set_channel_status(clean, "active"):
            return f"Unknown channel #{clean}."
        self.channel_blocklist.discard(clean)
        self._rebuild_channel_policy()
        if self.db.get_channel(clean).get("signature"):
            self._broadcast(self.s2s_engine.build_channel_frames([clean]))
        self._log_operator_action("resume_channel", target=clean)
        return f"Resumed #{clean}."

    def block_channel(self, name: str) -> str:
        clean = str(name or "").lstrip("#").strip()
        if not self.db.set_channel_status(clean, "blocked"):
            return f"Unknown channel #{clean}."
        self.channel_blocklist.add(clean)
        self._rebuild_channel_policy()
        self._log_operator_action("block_channel", target=clean)
        return f"Blocked #{clean}; requests and traffic are refused."

    def deny_channel(self, name: str) -> str:
        if self.db.set_channel_request_status(name, "denied"):
            self._log_operator_action("deny_channel", target=name)
            logger.info(f"Denied channel request #{name}.")
            return f"Denied #{name}."
        return f"No pending request for #{name}."

    def handle_operator_command(self, command: str, argument: str) -> str:
        if command in {"help", "?"}:
            return self._operator_help_text()
        if command in {"status", "stats"}:
            return self._operator_status_text()
        if command in {"recent", "audit"}:
            return self._operator_recent_text(argument)
        if command == "approve" and argument:
            return self.approve_channel(argument.lstrip("#"))
        if command == "deny" and argument:
            return self.deny_channel(argument.lstrip("#"))
        if command == "add" and argument:
            chan, _, desc = argument.partition(" ")
            return self.add_channel(chan, desc.strip())
        if command == "pause" and argument:
            return self.pause_channel(argument)
        if command == "resume" and argument:
            return self.resume_channel(argument)
        if command == "block" and argument:
            return self.block_channel(argument)
        if command in {"pending", "requests"}:
            requests = self.db.get_channel_requests("pending")
            if not requests:
                return "No pending channel requests."
            return "Pending:\n" + "\n".join(
                f"#{r['name']} - {r['description'] or '(no description)'}" for r in requests
            )
        if command == "channels":
            channels = self.db.get_channels()
            if not channels:
                return "No channels configured."
            return "Channels:\n" + "\n".join(
                f"#{c['name']} [{str(c.get('status') or 'active').lower()}]"
                for c in channels
            )
        return ""

    # ------------------------------------------------------------------
    # Peer discovery
    # ------------------------------------------------------------------

    def on_peer_announce(self, destination_hash, announced_identity, app_data):
        """Queues a discovered hub for connection on the service loop thread."""
        if not announced_identity or announced_identity.hash == self.identity.hash:
            return

        if blackhole.is_blocked(announced_identity.hash, self.db):
            logger.info(f"Ignored announce from blackholed hub [{announced_identity.hash.hex()[:10]}]")
            return

        peer_hex = destination_hash.hex()
        if peer_hex in self.discovered_peers:
            return

        name = peer_hex[:10]
        if app_data:
            try:
                name = str(msgpack.unpackb(app_data, raw=False).get("name", name))
            except Exception:
                pass

        self.discovered_peers[peer_hex] = announced_identity
        self.db.upsert_identity(
            identity_hash=announced_identity.hash.hex(),
            alias=name,
            public_key=announced_identity.get_public_key(),
        )
        logger.info(f"Discovered Speakeasy hub '{name}' at [{peer_hex[:10]}]")

    def _is_connected_to(self, identity) -> bool:
        for link in self.active_links:
            remote = link.get_remote_identity()
            if remote and remote.hash == identity.hash and link.status != RNS.Link.CLOSED:
                return True
        return False

    def propagate_new_channels(self):
        """
        Gossips approved channels this daemon has not announced yet.

        Approvals can also arrive out-of-band (speakeasy_admin.py writing to the
        same database), so the channel table -- not the approval call path -- is
        what drives propagation.
        """
        names = [c["name"] for c in self.db.get_signed_channels()]
        if not self.channels_seeded:
            # Whatever was already approved before startup has had its chance to
            # federate; only re-announce it when a peer links.
            self.propagated_channels.update(names)
            self.channels_seeded = True
            return

        fresh = [name for name in names if name not in self.propagated_channels]
        if not fresh:
            return

        self.propagated_channels.update(fresh)
        self._rebuild_channel_policy()
        peers = self._broadcast(self.s2s_engine.build_channel_frames(fresh))
        logger.info(f"Propagated {len(fresh)} approved channel(s) to {peers} peer link(s).")

    def connect_discovered_peers(self):
        """Links to hubs learned from announces, respecting local capacity."""
        if not self.auto_discover_peers:
            return

        for peer_hex, identity in list(self.discovered_peers.items()):
            if len(self.active_links) >= self.max_clients:
                logger.info("At link capacity; deferring discovered peer connections.")
                return
            if self._is_connected_to(identity) or blackhole.is_blocked(identity.hash, self.db):
                continue
            try:
                self._link_to(identity, peer_hex)
            except Exception as e:
                logger.error(f"Failed to link discovered peer [{peer_hex[:10]}]: {e}")

    def _link_to(self, identity, label: str):
        logger.info(f"Initiating link to hub [{label[:10]}]...")
        # Interface settlement delay.
        time.sleep(self.settlement_delay)
        target = RNS.Destination(identity, RNS.Destination.OUT, RNS.Destination.SINGLE, APP_NAME, ASPECT_HOST)
        link = RNS.Link(target)
        link.set_link_established_callback(self._on_outbound_link)
        link.set_link_closed_callback(self._on_link_closed)

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
                        self._link_to(identity, clean_hex)
            except Exception as e:
                logger.error(f"Failed static peer connection to [{clean_hex[:10]}]: {e}")

    def run(self):
        logger.info(f"Daemon successfully started. Service loop active ({self.sync_interval}s interval)...")
        last_sync = 0
        last_announce = time.time()
        last_vacuum = time.time()
        last_prune = 0.0
        while self.running:
            now = time.time()
            self._maybe_retry_operator_startup_notice()
            if now - last_sync >= self.sync_interval:
                self.maintain_static_peers()
                self.connect_discovered_peers()
                self.propagate_new_channels()
                self.run_sync_round()
                last_sync = now

            if now - last_announce >= self.announce_interval:
                self.announce_host()
                last_announce = now

            if self.prune_interval > 0 and now - last_prune >= self.prune_interval:
                self.enforce_retention()
                last_prune = now

            if self.vacuum_interval > 0 and now - last_vacuum >= self.vacuum_interval:
                self.db.vacuum()
                logger.info(f"Vacuumed database ({self.db.db_size_bytes() // 1024} KiB on disk).")
                last_vacuum = now

            time.sleep(1)

    def run_sync_round(self):
        """
        Asks every live peer to reconcile one window of history.

        Link establishment alone is not enough: it only ever reconciles what
        both sides happen to name at that moment, so a hub that joins an
        existing mesh stays permanently missing everything posted before it
        arrived. Each round advances a cursor further back through the
        retention window and wraps around, so backfill completes over a few
        rounds instead of flooding one link with the entire archive.
        """
        channels = self.db.get_active_channel_names()
        frame = self.s2s_engine.build_sync_request(channels, offset=self.sync_offset)
        if not frame:
            return

        peers = self._broadcast([frame])
        window = self.s2s_engine.sync_targets(channels, offset=self.sync_offset)
        # Short window means the cursor ran past the oldest populated epoch, so
        # start the next sweep from the newest again.
        self.sync_offset = self.sync_offset + MAX_SYNC_EPOCHS if len(window) >= MAX_SYNC_EPOCHS else 0
        if peers:
            logger.info(f"Anti-entropy round over {len(window)} channel-epoch(s) to {peers} peer link(s).")

    def enforce_retention(self):
        """
        Keeps the database inside its configured budget.

        Runs in three escalating steps -- age, per-channel depth, then a hard
        size ceiling -- because a hub on a Raspberry Pi or a solar node has a
        disk budget that history growth must not silently exceed.
        """
        expired = self.db.prune_messages(self.message_ttl_days)
        overflow = self.db.prune_channel_overflow(self.max_messages_per_channel)
        oversize = self.db.enforce_size_limit(self.max_db_bytes)

        if (expired or overflow) and not oversize:
            # The size stage reclaims its own pages; when it had nothing to do,
            # the pages the first two stages freed still need returning to the
            # filesystem, which is the whole point on a fixed disk budget.
            self.db.vacuum()

        if expired or overflow or oversize:
            logger.info(
                f"Retention sweep removed {expired} expired, {overflow} over-cap and "
                f"{oversize} over-budget message(s); database now "
                f"{self.db.db_size_bytes() // 1024} KiB."
            )

    def stop(self):
        logger.info("Shutting down daemon...")
        self.running = False
        if self.announce_handler:
            RNS.Transport.deregister_announce_handler(self.announce_handler)
        if self.operator:
            self.operator.stop()
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
