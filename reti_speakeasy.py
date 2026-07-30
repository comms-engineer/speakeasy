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
from speakeasy_db import BandwidthClass, Calendar, Event, SpeakeasyDB

APP_NAME = "speakeasy"
ASPECT_HOST = "host"

STATE_DIR = os.path.expanduser("~/.reti_speakeasy")


def instance_name() -> str:
    return sys.argv[1] if len(sys.argv) > 1 else "client_default"


def client_db_path() -> str:
    """Client state lives alongside the identity file so it is independent of CWD."""
    os.makedirs(STATE_DIR, exist_ok=True)
    return os.path.join(STATE_DIR, f"speakeasy_{instance_name()}.db")


# small helper to present ages
def human_age(seconds: float) -> str:
    seconds = int(max(0, seconds))
    if seconds < 10:
        return "just now"
    if seconds < 60:
        return f"{seconds}s"
    minutes = seconds // 60
    if minutes < 60:
        return f"{minutes}m"
    hours = minutes // 60
    if hours < 24:
        return f"{hours}h"
    days = hours // 24
    return f"{days}d"


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
    def __init__(self, db: Optional[SpeakeasyDB] = None, probe_interval: int = 30, probes_per_round: int = 6):
        self.hosts = {}
        self.db = db
        self.probe_interval = int(probe_interval)
        self.probes_per_round = int(probes_per_round)
        self._prober_thread = None
        self._stop_prober = threading.Event()

        # Load cached hosts from DB if available
        if self.db:
            try:
                for h in self.db.load_hosts(limit=500):
                    # ensure hash_bytes present
                    if not h.get("hash_bytes") and h.get("hex_hash"):
                        try:
                            h["hash_bytes"] = bytes.fromhex(h["hex_hash"]) if h.get("hex_hash") else None
                        except Exception:
                            h["hash_bytes"] = None
                    self.hosts[h["hex_hash"]] = h
            except Exception:
                # don't fail initialization on DB errors
                pass

    def start_prober(self):
        if self._prober_thread and self._prober_thread.is_alive():
            return
        self._stop_prober.clear()
        self._prober_thread = threading.Thread(target=self._prober_loop, daemon=True)
        self._prober_thread.start()

    def stop_prober(self):
        self._stop_prober.set()
        if self._prober_thread:
            self._prober_thread.join(timeout=1.0)

    def _prober_loop(self):
        # Periodically probe top-ranked hosts to refresh hops/last_seen
        while not self._stop_prober.is_set():
            try:
                ranked = self.get_ranked_hosts()
                now = time.time()
                for h in ranked[: self.probes_per_round]:
                    dest = h.get("hash_bytes")
                    if not dest:
                        # attempt to decode hex_hash
                        try:
                            dest = bytes.fromhex(h.get("hex_hash"))
                            h["hash_bytes"] = dest
                        except Exception:
                            continue

                    # Request a path if none known
                    if not RNS.Transport.has_path(dest):
                        try:
                            RNS.Transport.request_path(dest)
                        except Exception:
                            pass

                    # Allow a short window for RNS to populate hops
                    time.sleep(0.18)

                    try:
                        h["hops"] = RNS.Transport.hops_to(dest) or 99
                    except Exception:
                        h["hops"] = h.get("hops", 99)

                    # If identity is known locally, mark as seen
                    try:
                        identity = RNS.Identity.recall(dest)
                        if identity:
                            h["identity"] = identity
                            h["alias"] = getattr(identity, "alias", h.get("alias") or f"Host-{h['hex_hash'][:6]}")
                            h["last_seen"] = time.time()
                    except Exception:
                        pass

                    # Recompute score and persist
                    h["score"] = calculate_host_score(h.get("hops", 99), h.get("load", 0), h.get("max_load", 10), h.get("last_seen", now))
                    if self.db:
                        try:
                            self.db.save_host(h)
                        except Exception:
                            pass

                # Sleep until next round
                for _ in range(max(1, int(self.probe_interval))):
                    if self._stop_prober.is_set():
                        break
                    time.sleep(1)
            except Exception:
                # Don't crash the prober thread on unexpected errors
                time.sleep(max(1, self.probe_interval))

    def update_from_announce(self, destination_hash: bytes, announced_identity: RNS.Identity, app_data: bytes):
        hex_hash = destination_hash.hex()

        metadata = {}
        if app_data:
            try:
                metadata = msgpack.unpackb(app_data, raw=False)
            except Exception:
                pass

        host = {
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
            "metadata": metadata,
            "score": 0.0,
        }

        self.hosts[hex_hash] = host
        if self.db:
            try:
                self.db.save_host(host)
            except Exception:
                pass

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
            try:
                RNS.Transport.request_path(dest_bytes)
            except Exception:
                pass

        host = {
            "hash_bytes": dest_bytes,
            "hex_hash": clean_hex,
            "identity": RNS.Identity.recall(dest_bytes),
            "alias": f"Manual ({clean_hex[:8]})",
            "hops": RNS.Transport.hops_to(dest_bytes) if RNS.Transport.has_path(dest_bytes) else 99,
            "load": 0,
            "max_load": 10,
            "last_seen": time.time(),
            "is_manual": True,
            "metadata": {},
            "score": 0.0,
        }

        self.hosts[clean_hex] = host
        if self.db:
            try:
                self.db.save_host(host)
            except Exception:
                pass
        return True

    def get_ranked_hosts(self) -> list:
        now = time.time()
        ranked = []
        for hex_hash, host in list(self.hosts.items()):
            if not host.get("is_manual") and (now - host.get("last_seen", 0) > 7200):
                # keep DB entry but drop from in-memory catalog to avoid noise
                del self.hosts[hex_hash]
                continue

            host["score"] = calculate_host_score(
                host.get("hops", 99),
                host.get("load", 0),
                host.get("max_load", 10),
                host.get("last_seen", now),
            )
            ranked.append(host)

        return sorted(ranked, key=lambda x: x.get("score", 0.0), reverse=True)


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
        table.add_columns("Alias / Hash", "Hops", "Load", "Type", "Score", "Age", "Reachable")
        self.refresh_table()

    def refresh_table(self) -> None:
        table = self.query_one("#hosts-table", DataTable)
        table.clear()
        ranked = self.host_manager.get_ranked_hosts()
        now = time.time()
        for h in ranked:
            h_type = "Manual" if h.get("is_manual") else "Discovered"
            load_str = f"{h.get('load')}/{h.get('max_load')}" if not h.get("is_manual") else "N/A"
            age_str = human_age(now - float(h.get("last_seen", 0)))
            try:
                reachable = "Yes" if (RNS.Transport.hops_to(h.get("hash_bytes")) is not None or RNS.Transport.has_path(h.get("hash_bytes"))) else "No"
            except Exception:
                reachable = "?"
            table.add_row(h.get("alias"), str(h.get("hops")), load_str, h_type, str(h.get("score")), age_str, reachable, key=h.get("hex_hash"))

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


# (rest of file unchanged beyond modals)

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


# The rest of the file remains unchanged; we'll patch in HostManager usage below where ReticulumEngine is defined.
