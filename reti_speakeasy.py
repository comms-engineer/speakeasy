import datetime
import math
import os
import sys
import threading
import time
import msgpack
import RNS

from rich.markup import escape
from textual.app import App, ComposeResult
from textual.containers import Horizontal, Vertical
from textual.screen import ModalScreen
from textual.widgets import Button, DataTable, Footer, Header, Input, Label, RichLog, TabbedContent, TabPane, TextArea

import blackhole
from fed_engine import MAX_MESSAGE_CONTENT_BYTES, Opcode, S2SProtocolEngine
from speakeasy_db import BandwidthClass, SpeakeasyDB

APP_NAME = "speakeasy"
ASPECT_HOST = "host"

STATE_DIR = os.path.expanduser("~/.reti_speakeasy")


def instance_name() -> str:
    return sys.argv[1] if len(sys.argv) > 1 else "client_default"


def client_db_path() -> str:
    """Client state lives alongside the identity file so it is independent of CWD."""
    os.makedirs(STATE_DIR, exist_ok=True)
    return os.path.join(STATE_DIR, f"speakeasy_{instance_name()}.db")

# -----------------------------------------------------------------------------
# Host Discovery & Ranking Manager
# -----------------------------------------------------------------------------

def calculate_host_score(hops: int, load: int, max_load: int, last_seen: float) -> float:
    LAMBDA = 0.00077  # Decay factor (~15m half-life)
    elapsed_seconds = max(0, time.time() - last_seen)

    hop_score = 100.0 / (hops + 1)
    load_ratio = min(load / max(max_load, 1), 1.0)
    capacity_score = 1.0 - (0.8 * load_ratio)
    decay = math.exp(-LAMBDA * elapsed_seconds)

    return round(hop_score * capacity_score * decay, 2)

class HostManager:
    def __init__(self):
        self.hosts = {}

    def update_from_announce(self, destination_hash: bytes, announced_identity: RNS.Identity, app_data: bytes):
        hex_hash = destination_hash.hex()

        metadata = {}
        if app_data:
            try:
                metadata = msgpack.unpackb(app_data, raw=False)
            except Exception:
                pass

        self.hosts[hex_hash] = {
            "hash_bytes": destination_hash,
            "hex_hash": hex_hash,
            "identity": announced_identity,
            "alias": metadata.get("name", f"Host-{hex_hash[:6]}"),
            # hops_to() returns None while no path is known yet.
            "hops": RNS.Transport.hops_to(destination_hash) or 99,
            "load": metadata.get("load", 0),
            "max_load": metadata.get("max_load", 10),
            "last_seen": time.time(),
            "is_manual": False,
            "score": 0.0
        }

    def add_manual_host(self, hex_hash_str: str) -> bool:
        clean_hex = hex_hash_str.replace("<", "").replace(">", "").replace(" ", "").replace(":", "")
        try:
            dest_bytes = bytes.fromhex(clean_hex)
            expected_bytes = RNS.Reticulum.TRUNCATED_HASHLENGTH // 8
            if len(dest_bytes) != expected_bytes:
                return False
        except ValueError:
            return False

        if not RNS.Transport.has_path(dest_bytes):
            RNS.Transport.request_path(dest_bytes)

        self.hosts[clean_hex] = {
            "hash_bytes": dest_bytes,
            "hex_hash": clean_hex,
            "identity": RNS.Identity.recall(dest_bytes),
            "alias": f"Manual ({clean_hex[:8]})",
            "hops": RNS.Transport.hops_to(dest_bytes) if RNS.Transport.has_path(dest_bytes) else 99,
            "load": 0,
            "max_load": 10,
            "last_seen": time.time(),
            "is_manual": True,
            "score": 0.0
        }
        return True

    def get_ranked_hosts(self) -> list:
        now = time.time()
        ranked = []
        for hex_hash, host in list(self.hosts.items()):
            if not host["is_manual"] and (now - host["last_seen"] > 7200):
                del self.hosts[hex_hash]
                continue

            host["score"] = calculate_host_score(
                host["hops"],
                host["load"],
                host["max_load"],
                host["last_seen"]
            )
            ranked.append(host)

        return sorted(ranked, key=lambda x: x["score"], reverse=True)

class SpeakeasyHostDiscoveryHandler:
    def __init__(self, host_manager: HostManager):
        self.host_manager = host_manager
        self.aspect_filter = f"{APP_NAME}.{ASPECT_HOST}"

    def received_announce(self, destination_hash, announced_identity, app_data):
        self.host_manager.update_from_announce(destination_hash, announced_identity, app_data)

# -----------------------------------------------------------------------------
# Modals
# -----------------------------------------------------------------------------

class HostSelectorModal(ModalScreen[str]):
    BINDINGS = [
        ("escape", "dismiss_modal", "Cancel / Close")
    ]

    CSS = """
    HostSelectorModal {
        align: center middle;
        background: rgba(0, 0, 0, 0.85);
    }
    #dialog {
        padding: 1 2;
        width: 86;
        height: auto;
        max-height: 90%;
        border: thick $accent;
        background: $surface;
        layout: vertical;
    }
    #modal-title {
        text-style: bold;
        content-align: center middle;
        margin-bottom: 1;
    }
    #hosts-table {
        height: 7;
        margin-bottom: 1;
    }
    #manual-input {
        margin-bottom: 1;
    }
    #button-row {
        height: 3;
        align: center middle;
    }
    Button {
        margin: 0 1;
    }
    """

    def __init__(self, host_manager: HostManager):
        super().__init__()
        self.host_manager = host_manager

    def compose(self) -> ComposeResult:
        with Vertical(id="dialog"):
            yield Label(" Select Host Entrypoint", id="modal-title")
            yield DataTable(id="hosts-table")
            yield Input(placeholder="Or enter manual 32-char hex hash...", id="manual-input")
            with Horizontal(id="button-row"):
                yield Button("Connect Selected", variant="success", id="btn-connect-sel")
                yield Button("Connect Manual", variant="primary", id="btn-connect-manual")
                yield Button("Cancel [Esc]", variant="error", id="btn-cancel")

    def action_dismiss_modal(self) -> None:
        self.dismiss(None)

    def on_mount(self) -> None:
        table = self.query_one("#hosts-table", DataTable)
        table.cursor_type = "row"
        table.add_columns("Alias / Hash", "Hops", "Load", "Type", "Score")
        self.refresh_table()

    def refresh_table(self) -> None:
        table = self.query_one("#hosts-table", DataTable)
        table.clear()
        ranked = self.host_manager.get_ranked_hosts()
        for h in ranked:
            h_type = "Manual" if h["is_manual"] else "Discovered"
            load_str = f"{h['load']}/{h['max_load']}" if not h["is_manual"] else "N/A"
            table.add_row(h["alias"], str(h["hops"]), load_str, h_type, str(h["score"]), key=h["hex_hash"])

    def on_input_submitted(self, event: Input.Submitted) -> None:
        event.stop()
        val = event.value.strip()
        if val:
            self.host_manager.add_manual_host(val)
            self.dismiss(val)
        else:
            self.dismiss(None)

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "btn-connect-sel":
            table = self.query_one("#hosts-table", DataTable)
            if table.cursor_row is not None and table.row_count > 0:
                row_key, _ = table.coordinate_to_cell_key(table.cursor_coordinate)
                self.dismiss(row_key.value)
            else:
                self.dismiss(None)
        elif event.button.id == "btn-connect-manual":
            val = self.query_one("#manual-input", Input).value.strip()
            if val:
                self.host_manager.add_manual_host(val)
                self.dismiss(val)
            else:
                self.dismiss(None)
        else:
            self.dismiss(None)


class ProfileModal(ModalScreen[dict]):
    BINDINGS = [
        ("escape", "dismiss_modal", "Cancel / Close")
    ]

    CSS = """
    ProfileModal { align: center middle; background: rgba(0, 0, 0, 0.75); }
    #dialog {
        padding: 1 2;
        width: 62;
        height: auto;
        max-height: 90%;
        border: thick $secondary;
        background: $surface;
        layout: vertical;
    }
    #modal-title { text-style: bold; content-align: center middle; margin-bottom: 1; }
    .field-label { margin-top: 1; text-style: bold; }
    #button-row { height: 3; align: center middle; margin-top: 1; }
    Button { margin: 0 1; }
    """

    def __init__(self, current_profile: dict = None):
        super().__init__()
        self.current_profile = current_profile or {}

    def compose(self) -> ComposeResult:
        with Vertical(id="dialog"):
            yield Label(" Edit Profile Identity", id="modal-title")
            yield Label("Handle / Callsign:", classes="field-label")
            yield Input(value=self.current_profile.get("handle", ""), id="input-handle")
            yield Label("Status:", classes="field-label")
            yield Input(value=self.current_profile.get("status", ""), id="input-status")
            with Horizontal(id="button-row"):
                yield Button("Save & Sync", variant="success", id="btn-save")
                yield Button("Cancel [Esc]", variant="error", id="btn-cancel")

    def action_dismiss_modal(self) -> None:
        self.dismiss(None)

    def on_input_submitted(self, event: Input.Submitted) -> None:
        event.stop()
        self.save_and_dismiss()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "btn-save":
            self.save_and_dismiss()
        else:
            self.dismiss(None)

    def save_and_dismiss(self) -> None:
        handle = self.query_one("#input-handle", Input).value.strip()
        status = self.query_one("#input-status", Input).value.strip()
        self.dismiss({"handle": handle, "status": status})


class BulletinPostModal(ModalScreen[dict]):
    BINDINGS = [
        ("escape", "dismiss_modal", "Cancel / Close")
    ]

    CSS = """
    BulletinPostModal { align: center middle; background: rgba(0, 0, 0, 0.75); }
    #dialog {
        padding: 1 2;
        width: 72;
        height: auto;
        max-height: 90%;
        border: thick $accent;
        background: $surface;
        layout: vertical;
    }
    #modal-title { text-style: bold; content-align: center middle; margin-bottom: 1; }
    .field-label { margin-top: 1; text-style: bold; }
    #post-body { height: 8; margin-bottom: 1; }
    #button-row { height: 3; align: center middle; margin-top: 1; }
    Button { margin: 0 1; }
    """

    def compose(self) -> ComposeResult:
        with Vertical(id="dialog"):
            yield Label(" Create Bulletin Post", id="modal-title")
            yield Label("Title / Subject:", classes="field-label")
            yield Input(placeholder="Post Subject...", id="input-title")
            yield Label("Body:", classes="field-label")
            yield TextArea(id="post-body")
            with Horizontal(id="button-row"):
                yield Button("Post Bulletin", variant="success", id="btn-post")
                yield Button("Cancel [Esc]", variant="error", id="btn-cancel")

    def action_dismiss_modal(self) -> None:
        self.dismiss(None)

    def on_input_submitted(self, event: Input.Submitted) -> None:
        event.stop()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "btn-post":
            title = self.query_one("#input-title", Input).value.strip()
            body = self.query_one("#post-body", TextArea).text.strip()
            if title and body:
                self.dismiss({"title": title, "body": body})
            else:
                self.notify("Both Title and Body are required.", severity="error")
        else:
            self.dismiss(None)

class ChannelRequestModal(ModalScreen[dict]):
    BINDINGS = [
        ("escape", "dismiss_modal", "Cancel / Close")
    ]

    CSS = """
    ChannelRequestModal { align: center middle; }
    #dialog { width: 60; height: auto; border: thick $accent; background: $surface; padding: 1 2; }
    #modal-title { width: 100%; text-align: center; text-style: bold; color: $accent; margin-bottom: 1; }
    .field-label { margin-top: 1; color: $text-muted; }
    Input { margin-bottom: 1; }
    #modal-buttons { height: 3; align: center middle; margin-top: 1; }
    Button { margin: 0 1; }
    """

    def compose(self) -> ComposeResult:
        with Vertical(id="dialog"):
            yield Label(" Request New Channel", id="modal-title")
            yield Label("Channel name (no #):", classes="field-label")
            yield Input(placeholder="lounge", id="input-channel")
            yield Label("What is it for?", classes="field-label")
            yield Input(placeholder="Off-topic chatter", id="input-purpose")
            yield Label("The hub operator has to approve this.", classes="field-label")
            with Horizontal(id="modal-buttons"):
                yield Button("Send Request", variant="success", id="btn-request")
                yield Button("Cancel [Esc]", variant="error", id="btn-cancel")

    def action_dismiss_modal(self) -> None:
        self.dismiss(None)

    def on_input_submitted(self, event: Input.Submitted) -> None:
        self._submit()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "btn-request":
            self._submit()
        else:
            self.dismiss(None)

    def _submit(self) -> None:
        name = self.query_one("#input-channel", Input).value.strip().lstrip("#")
        purpose = self.query_one("#input-purpose", Input).value.strip()
        if not name or not name.replace("-", "").replace("_", "").isalnum():
            self.notify("Channel names must be alphanumeric (dashes and underscores allowed).",
                        severity="error")
            return
        self.dismiss({"name": name.lower(), "description": purpose})

# -----------------------------------------------------------------------------
# Reticulum Engine
# -----------------------------------------------------------------------------

class ReticulumEngine:
    def __init__(self, ui_callback):
        self.ui_callback = ui_callback
        self.active_host_link = None
        self.current_host_hash = None
        self.auto_failover_enabled = True
        # Channels the connected host advertised in its FED_HELLO. Anything
        # outside this set is dropped by the hub, so the client must not
        # pretend a message to it was delivered.
        self.host_channels = set()

        self.db = SpeakeasyDB(client_db_path(), max_message_bytes=MAX_MESSAGE_CONTENT_BYTES)
        self.rns = RNS.Reticulum()

        identity_path = os.path.join(STATE_DIR, f"{instance_name()}_identity")
        if os.path.exists(identity_path):
            self.identity = RNS.Identity.from_file(identity_path)
        else:
            self.identity = RNS.Identity()
            self.identity.to_file(identity_path)

        self.destination = RNS.Destination(
            self.identity,
            RNS.Destination.IN,
            RNS.Destination.SINGLE,
            APP_NAME,
            "client",
        )
        self.hash_str = RNS.prettyhexrep(self.destination.hash)

        self.host_manager = HostManager()
        self.discovery_handler = SpeakeasyHostDiscoveryHandler(self.host_manager)
        RNS.Transport.register_announce_handler(self.discovery_handler)

        self.s2s_engine = S2SProtocolEngine(
            db=self.db,
            local_hash_bytes=self.identity.hash,
            bandwidth_class=BandwidthClass.MEDIUM_MESH
        )

    def _notify_ui(self, event_type: str, data=None):
        if self.ui_callback:
            self.ui_callback(event_type, data)

    def connect_to_host(self, hash_hex: str, is_failover: bool = False):
        def _connect_worker():
            clean_hex = hash_hex.replace("<", "").replace(">", "").replace(" ", "").replace(":", "")
            try:
                dest_bytes = bytes.fromhex(clean_hex)

                if not RNS.Transport.has_path(dest_bytes):
                    self._notify_ui("system", f"Requesting path for host [{clean_hex[:10]}]...")
                    RNS.Transport.request_path(dest_bytes)
                    time.sleep(0.5)

                identity = RNS.Identity.recall(dest_bytes)
                if not identity:
                    self.host_manager.add_manual_host(clean_hex)
                    identity = RNS.Identity.recall(dest_bytes)

                if identity:
                    prefix = "Failover" if is_failover else "Connecting"
                    self._notify_ui("system", f"{prefix}: Establishing link to host [{clean_hex[:10]}]...")
                    time.sleep(0.35)

                    target_dest = RNS.Destination(identity, RNS.Destination.OUT, RNS.Destination.SINGLE, APP_NAME, ASPECT_HOST)
                    link = RNS.Link(target_dest)
                    link.set_link_established_callback(self._on_link_established)
                    link.set_link_closed_callback(self._on_link_closed)
                    link.set_packet_callback(self._on_packet_received)

                    self.current_host_hash = clean_hex
                else:
                    self._notify_ui("system", f"[bold red]Error:[/] Identity unknown for host [{clean_hex[:10]}].")
                    if is_failover:
                        self.trigger_auto_failover(exclude_hash=clean_hex)

            except Exception as e:
                self._notify_ui("system", f"[bold red]Connection Error:[/] {e}")
                if is_failover:
                    self.trigger_auto_failover(exclude_hash=clean_hex)

        threading.Thread(target=_connect_worker, daemon=True).start()

    def _on_link_established(self, link):
        self.active_host_link = link

        # 1. Identify local identity to host over the active link
        link.identify(self.identity)

        remote_identity = link.get_remote_identity()
        if remote_identity:
            self.db.upsert_identity(
                identity_hash=remote_identity.hash.hex(),
                alias=f"Host-{remote_identity.hash.hex()[:6]}",
                public_key=remote_identity.get_public_key()
            )
            remote_hash = RNS.prettyhexrep(remote_identity.hash)[:10]
        else:
            remote_hash = "Host"

        self._notify_ui("system", f"[bold green]Connected:[/] Session active with host [{remote_hash}].")
        self._notify_ui("host_updated", remote_hash)

        # 2. Sync Hello
        hello_frame = self.s2s_engine.build_hello(self.db.get_channel_names())
        RNS.Packet(link, hello_frame).send()

        # 3. Transmit signed profile record (delivers handle and public key to host DB)
        profile = self.db.get_profile(self.identity.hash.hex())
        profile_record = self.db.sign_and_upsert_profile(
            identity=self.identity,
            handle=profile.get("handle", ""),
            status=profile.get("status", ""),
            bio=profile.get("bio", "")
        )
        sync_frame = self.s2s_engine.build_profile_sync(profile_record)
        RNS.Packet(link, sync_frame).send()

    def _on_link_closed(self, link):
        self.active_host_link = None
        self.host_channels = set()
        self._notify_ui("system", "[bold red]Disconnected:[/] Host link dropped.")
        self._notify_ui("host_updated", "None")

        if self.auto_failover_enabled and self.current_host_hash:
            self._notify_ui("system", "[bold yellow]Auto-Failover:[/] Searching for next highest-ranked host...")
            self.trigger_auto_failover(exclude_hash=self.current_host_hash)

    def trigger_auto_failover(self, exclude_hash: str):
        ranked_hosts = self.host_manager.get_ranked_hosts()
        candidates = [h for h in ranked_hosts if h["hex_hash"] != exclude_hash]

        if candidates:
            best_candidate = candidates[0]
            self._notify_ui("system", f"[bold yellow]Failover Pivot:[/] Connecting to host '{best_candidate['alias']}' (Score: {best_candidate['score']})...")
            self.connect_to_host(best_candidate["hex_hash"], is_failover=True)
        else:
            self._notify_ui("system", "[bold red]Failover Failed:[/] No alternate reachable hosts found in catalog.")

    def _on_packet_received(self, message, packet):
        try:
            result = self.s2s_engine.process_inbound_frame(message)
            for resp_bytes in result.frames:
                RNS.Packet(packet.link, resp_bytes).send()

            if result.hello_channels:
                self.host_channels = set(result.hello_channels)
                self._notify_ui("channels_updated", sorted(self.host_channels))

            for name in result.accepted_channels:
                self.host_channels.add(name)
                self._notify_ui("system", f"[bold green]New channel:[/] #{name} was approved by an operator.")
                self._notify_ui("channels_updated", sorted(self.host_channels))

            if result.opcode == Opcode.BULLETIN_POST:
                self._notify_ui("refresh_bbs", None)
            else:
                self._notify_ui("refresh_chat", None)
        except Exception as e:
            self._notify_ui("system", f"Failed to process packet: {e}")

    def _host_link_active(self) -> bool:
        return bool(self.active_host_link and self.active_host_link.status == RNS.Link.ACTIVE)

    def broadcast_chat_message(self, channel: str, text: str) -> bool:
        if not self._host_link_active():
            self._notify_ui("system", "[bold red]Not sent:[/] no active host link. "
                                      "Press H to connect to a hub.")
            return False

        # The hub silently discards channels it does not carry, so refuse here
        # rather than storing a message locally that nobody else will ever see.
        if self.host_channels and channel not in self.host_channels:
            self._notify_ui("system", f"[bold red]Not sent:[/] the host does not carry #{channel}. "
                                      f"Press N to request it from the operator.")
            return False

        msg_record = self.db.sign_and_insert_message(self.identity, channel, text)
        if not msg_record:
            self._notify_ui("system", f"[bold red]Not sent:[/] messages are limited to "
                                      f"{MAX_MESSAGE_CONTENT_BYTES} bytes.")
            return False

        for frame in self.s2s_engine.build_delta_push_chunks([msg_record]):
            RNS.Packet(self.active_host_link, frame).send()
        return True

    def block_identity(self, needle: str) -> bool:
        """
        Blocks an identity for this client and forgets what it already posted.

        The block is stored in the client's own database, not in Reticulum's
        node-wide blackhole list: that list belongs to the master RNS instance,
        so a shared-instance client writing to it would change only its own copy
        -- invisible to `rnpath -b` -- and the next `rnpath -B` would overwrite
        the shared file and drop the entry. Blackholes set by the node's operator
        with `rnpath -B` are still honoured on top of this.
        """
        identity_hash = self.db.resolve_identity(needle)
        if not identity_hash:
            self._notify_ui("system", f"[bold red]Unknown identity:[/] {escape(needle)}. "
                                      f"Use a handle or a hash prefix.")
            return False
        if identity_hash == self.identity.hash.hex():
            self._notify_ui("system", "[bold red]Refused:[/] you cannot block yourself.")
            return False

        if not self.db.block_identity(identity_hash, reason="Blocked from Speakeasy"):
            self._notify_ui("system", f"[bold yellow]Already blocked:[/] {identity_hash[:10]}.")
            return False

        removed = self.db.purge_identity(identity_hash)
        self._notify_ui("system", f"[bold green]Blocked:[/] {identity_hash[:10]}; "
                                  f"purged {removed} record(s).")
        self._notify_ui("refresh_chat", None)
        self._notify_ui("refresh_bbs", None)
        return True

    def unblock_identity(self, needle: str) -> bool:
        identity_hash = self.db.resolve_identity(needle) or (needle or "").strip().lower()
        if not self.db.unblock_identity(identity_hash):
            if blackhole.is_blackholed(identity_hash):
                self._notify_ui("system", f"[bold yellow]Blackholed node-wide:[/] lift it with "
                                          f"rnpath -U {identity_hash[:16]}.")
            else:
                self._notify_ui("system", f"[bold yellow]Not blocked:[/] {escape(needle)}.")
            return False
        self._notify_ui("system", f"[bold green]Unblocked:[/] {identity_hash[:10]}.")
        self._notify_ui("refresh_chat", None)
        return True

    def list_blocked(self):
        local = set(self.db.blocked_identities())
        node_wide = blackhole.blackholed_hashes()
        if not local and not node_wide:
            self._notify_ui("system", "No blocked identities.")
            return

        for identity_hash in sorted(local | node_wide):
            # The alias survives a block; the profile does not, having been
            # purged, so identities is the reliable place to name them.
            record = self.db.get_identity_record(identity_hash) or {}
            alias = record.get("alias") or "unknown"
            scope = "node-wide blackhole" if identity_hash in node_wide else "blocked"
            self._notify_ui("system", f"{scope}: {identity_hash[:10]} ({escape(str(alias))})")

    def request_channel(self, name: str, description: str) -> bool:
        if not self._host_link_active():
            self._notify_ui("system", "[bold red]Request failed:[/] no active host link.")
            return False

        RNS.Packet(self.active_host_link, self.s2s_engine.build_channel_req(name, description)).send()
        self._notify_ui("system", f"[bold yellow]Requested:[/] #{name} is awaiting operator approval.")
        return True

    def broadcast_bulletin(self, title: str, body: str) -> str:
        bulletin_record = self.db.sign_and_add_bulletin(self.identity, title, body)
        if bulletin_record and bulletin_record.get("bulletin_id"):
            bulletin_frame = self.s2s_engine.build_bulletin_post(bulletin_record)
            if self.active_host_link and self.active_host_link.status == RNS.Link.ACTIVE:
                RNS.Packet(self.active_host_link, bulletin_frame).send()
            return bulletin_record["bulletin_id"]
        return ""

# -----------------------------------------------------------------------------
# Main Application
# -----------------------------------------------------------------------------

class RetiSpeakeasyApp(App):
    CSS = """
    Screen { layout: horizontal; background: $surface; }
    #sidebar { width: 38; height: 100%; background: $panel; border-right: heavy $accent; padding: 0 1; layout: vertical; }
    #main-area { width: 1fr; height: 100%; layout: vertical; }
    #channel-tabs { height: 1fr; }
    .widget-header { background: $accent; color: $text; text-style: bold; padding: 0 1; width: 100%; margin-top: 1; }
    #chat-input { width: 100%; dock: bottom; }
    .sidebar-btn { width: 100%; margin-top: 1; }
    .chat-log { height: 1fr; border: solid $secondary; background: $surface-darken-1; }
    #system-log { height: 1fr; border: solid $secondary; background: $surface-darken-1; min-height: 8; }
    #bbs-container { height: 1fr; layout: vertical; padding: 1; }
    #bbs-table { height: 10; margin-bottom: 1; }
    #bbs-viewer { height: 1fr; border: solid $accent; background: $surface-darken-1; }
    #bbs-top-bar { height: 3; align: right middle; margin-bottom: 1; }
    """

    BINDINGS = [
        ("q", "quit", "Quit"),
        ("h", "show_host_modal", "Select Host"),
        ("p", "show_profile_modal", "Edit Profile"),
        ("b", "show_bulletin_modal", "New Bulletin"),
        ("n", "show_channel_request_modal", "Request Channel"),
    ]

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)

        startup_db = SpeakeasyDB(client_db_path())
        db_chans = startup_db.get_channels()
        startup_db.close()

        with Horizontal():
            with Vertical(id="sidebar"):
                yield Label(" Node Identity", classes="widget-header")
                yield Label("Hash:", id="hash-title")
                yield Label("Initializing...", id="hash-label")
                yield Label(" Connected Host", classes="widget-header")
                yield Label("None", id="host-label")
                yield Button(" Select Host (H)", id="btn-host-open", classes="sidebar-btn", variant="primary")
                yield Button(" Edit Profile (P)", id="btn-profile-open", classes="sidebar-btn", variant="default")
                yield Button(" New Bulletin (B)", id="btn-bulletin-open", classes="sidebar-btn", variant="success")
                yield Button(" Request Channel (N)", id="btn-channel-open", classes="sidebar-btn", variant="warning")
                yield Label(" System Log", classes="widget-header")
                yield RichLog(id="system-log", classes="chat-log", highlight=True, markup=True)
            with Vertical(id="main-area"):
                with TabbedContent(id="channel-tabs"):
                    for chan in db_chans:
                        chan_name = chan["name"] if isinstance(chan, dict) else str(chan)
                        clean_id = chan_name.lstrip('#')
                        with TabPane(f"#{clean_id}", id=f"tab-{clean_id}"):
                            yield RichLog(id=f"log-{clean_id}", classes="chat-log", highlight=True, markup=True)
                    with TabPane(" Bulletin Board", id="tab-bbs"):
                        with Vertical(id="bbs-container"):
                            with Horizontal(id="bbs-top-bar"):
                                yield Button(" Post New Bulletin", id="btn-bbs-post", variant="success")
                            yield DataTable(id="bbs-table")
                            yield RichLog(id="bbs-viewer", classes="chat-log", highlight=True, markup=True)
                yield Input(placeholder="Message, or /block <handle> /unblock <handle> /blocked",
                            id="chat-input")
        yield Footer()

    def on_mount(self) -> None:
        self.engine = ReticulumEngine(ui_callback=self.handle_engine_event)
        self.query_one("#hash-label", Label).update(f"[bold gold1]{self.engine.hash_str}[/]")

        bbs_table = self.query_one("#bbs-table", DataTable)
        bbs_table.cursor_type = "row"
        bbs_table.add_columns("Title", "Author", "Date")

        self.reload_all_chat_logs()
        self.reload_bulletin_board()

    def get_current_channel(self) -> str:
        tabs = self.query_one("#channel-tabs", TabbedContent)
        active_tab = tabs.active
        if not active_tab:
            return "general"
        return active_tab.replace("tab-", "").lstrip('#')

    def reload_all_chat_logs(self) -> None:
        db_chans = self.engine.db.get_channels()
        for chan in db_chans:
            chan_name = chan["name"] if isinstance(chan, dict) else str(chan)
            clean_chan_id = chan_name.lstrip('#')

            try:
                log_widget = self.query_one(f"#log-{clean_chan_id}", RichLog)
            except Exception:
                continue

            log_widget.clear()
            history = self.engine.db.get_channel_messages(clean_chan_id, limit=50)
            blocked = blackhole.blocked_hashes(self.engine.db)
            for msg in history:
                # Records already stored when the user blocked the author, or
                # relayed by a hub that does not share the block.
                if msg.get("sender_hash") in blocked:
                    continue
                sender = msg.get("alias") or (msg["sender_hash"][:10] if msg.get("sender_hash") else "Unknown")
                log_widget.write(f"[bold cyan]\\[{escape(sender)}]:[/] {escape(msg['content'])}")

    def reload_bulletin_board(self) -> None:
        table = self.query_one("#bbs-table", DataTable)
        table.clear()
        bulletins = self.engine.db.get_bulletins()
        for b in bulletins:
            author = b.get("alias") or b["author_hash"][:10]
            date_str = datetime.datetime.fromtimestamp(b["timestamp"]).strftime("%Y-%m-%d %H:%M")
            table.add_row(b["title"], author, date_str, key=b["bulletin_id"])

    def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
        if event.data_table.id == "bbs-table":
            bulletin_id = event.row_key.value
            bulletins = self.engine.db.get_bulletins()
            selected = next((b for b in bulletins if b["bulletin_id"] == bulletin_id), None)
            viewer = self.query_one("#bbs-viewer", RichLog)
            viewer.clear()
            if selected:
                author = selected.get("alias") or selected["author_hash"][:10]
                date_str = datetime.datetime.fromtimestamp(selected["timestamp"]).strftime("%Y-%m-%d %H:%M:%S")
                viewer.write(f"[bold gold1]Title:[/] {escape(selected['title'])}\n"
                             f"[bold cyan]Author:[/] {escape(author)} ({selected['author_hash'][:10]})\n"
                             f"[bold yellow]Date:[/] {date_str}\n"
                             f"[dim]{'-'*50}[/dim]\n"
                             f"{escape(selected['body'])}")

    def action_show_host_modal(self) -> None:
        def handle_host_selection(selected_hash: str | None) -> None:
            if selected_hash:
                self.engine.connect_to_host(selected_hash)

        self.push_screen(HostSelectorModal(self.engine.host_manager), handle_host_selection)

    def action_show_profile_modal(self) -> None:
        # Profiles are keyed on the identity hash, not the destination hash.
        profile = self.engine.db.get_profile(self.engine.identity.hash.hex())

        def handle_profile(data: dict | None) -> None:
            if data:
                # Sign and upsert via DB helper to generate proper signature and edited_at timestamp
                profile_record = self.engine.db.sign_and_upsert_profile(
                    identity=self.engine.identity,
                    handle=data["handle"],
                    status=data["status"],
                    bio=profile.get("bio", "")
                )
                self.notify(f"Profile saved: {data['handle']}")

                sync_frame = self.engine.s2s_engine.build_profile_sync(profile_record)
                if self.engine.active_host_link and self.engine.active_host_link.status == RNS.Link.ACTIVE:
                    RNS.Packet(self.engine.active_host_link, sync_frame).send()

                self.reload_all_chat_logs()

        self.push_screen(ProfileModal(current_profile=profile), handle_profile)

    def sync_channel_tabs(self, channel_names) -> None:
        """Adds a tab for every channel the host carries that is not shown yet."""
        tabs = self.query_one("#channel-tabs", TabbedContent)
        for name in channel_names:
            clean_id = str(name).lstrip("#")
            if not clean_id:
                continue
            try:
                self.query_one(f"#log-{clean_id}", RichLog)
                continue
            except Exception:
                pass
            self.engine.db.add_channel(clean_id, f"Channel #{clean_id}")
            tabs.add_pane(TabPane(
                f"#{clean_id}",
                RichLog(id=f"log-{clean_id}", classes="chat-log", highlight=True, markup=True),
                id=f"tab-{clean_id}",
            ))
        self.reload_all_chat_logs()

    def action_show_channel_request_modal(self) -> None:
        def handle_request(data: dict | None) -> None:
            if data:
                self.engine.request_channel(data["name"], data["description"])

        self.push_screen(ChannelRequestModal(), handle_request)

    def action_show_bulletin_modal(self) -> None:
        def handle_bulletin(data: dict | None) -> None:
            if data:
                self.engine.broadcast_bulletin(data["title"], data["body"])
                self.notify("Bulletin posted and synchronized.")
                self.reload_bulletin_board()

        self.push_screen(BulletinPostModal(), handle_bulletin)

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "btn-host-open":
            self.action_show_host_modal()
        elif event.button.id == "btn-profile-open":
            self.action_show_profile_modal()
        elif event.button.id in ("btn-bulletin-open", "btn-bbs-post"):
            self.action_show_bulletin_modal()
        elif event.button.id == "btn-channel-open":
            self.action_show_channel_request_modal()

    def handle_command(self, text: str) -> None:
        command, _, argument = text[1:].partition(" ")
        argument = argument.strip()
        command = command.lower()

        if command == "block" and argument:
            self.engine.block_identity(argument)
        elif command == "unblock" and argument:
            self.engine.unblock_identity(argument)
        elif command == "blocked":
            self.engine.list_blocked()
        else:
            self.handle_engine_event(
                "system",
                "[bold yellow]Commands:[/] /block <handle|hash>, /unblock <handle|hash>, /blocked"
            )

    def handle_engine_event(self, event_type: str, data) -> None:
        def update_ui() -> None:
            if event_type == "system":
                try:
                    sys_log = self.query_one("#system-log", RichLog)
                    # Engine messages carry their own Rich markup.
                    sys_log.write(f"[bold yellow]Sys:[/] {data}")
                except Exception:
                    pass
            elif event_type == "refresh_chat":
                self.reload_all_chat_logs()
            elif event_type == "refresh_bbs":
                self.reload_bulletin_board()
            elif event_type == "channels_updated":
                self.sync_channel_tabs(data or [])
            elif event_type == "host_updated":
                self.query_one("#host-label", Label).update(f"[bold green]{escape(str(data))}[/]")

        if threading.current_thread() is threading.main_thread():
            update_ui()
        else:
            self.call_from_thread(update_ui)

    def on_input_submitted(self, event: Input.Submitted) -> None:
        # Strict input routing guard
        if event.input.id != "chat-input":
            return

        text = event.value.strip()
        if text.startswith("/"):
            self.handle_command(text)
            event.input.value = ""
            return

        if text:
            curr_chan = self.get_current_channel()
            if curr_chan != "bbs" and self.engine.broadcast_chat_message(curr_chan, text):
                self.reload_all_chat_logs()
            event.input.value = ""

if __name__ == "__main__":
    app = RetiSpeakeasyApp()
    app.run()
