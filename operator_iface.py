"""
LXMF control channel between a headless hub and its human operator.

The daemon has no console, so channel requests are delivered to the operator's
regular LXMF client (Sideband, Nomad Network, ...) as a normal message, and the
operator replies with `approve <channel>` / `deny <channel>`. Commands are only
honoured from the exact source hash configured as the operator, so approval
authority is bound to a key the operator already controls.
"""

import json
import logging
import os
from typing import Callable, List, Optional

import LXMF
import RNS

logger = logging.getLogger("speakeasy_daemon.operator")

DISPLAY_NAME = "Speakeasy Hub"
USAGE = (
    "Commands: help | status | pending | channels | requests | recent [N] | "
    "approve <channel> | deny <channel> | add <channel> [description] | "
    "pause <channel> | resume <channel> | block <channel> | "
    "recommend <identity> <reason> | recommendations [N]"
)

RECOMMENDATION_TITLE = "Speakeasy Blacklist Recommendation"


class OperatorInterface:
    """
    :param command_handler: Called with the parsed command and argument for
        every authenticated operator message; returns the reply text.
    """

    def __init__(self, identity: RNS.Identity, storage_path: str, operator_hash: str,
                 command_handler: Callable[[str, str], str], node_name: str = "Speakeasy",
                 peer_message_handler: Optional[Callable[[str, str, str], None]] = None):
        self.command_handler = command_handler
        self.node_name = node_name
        self.operator_hash = self._parse_hash(operator_hash)
        self.peer_message_handler = peer_message_handler

        os.makedirs(storage_path, exist_ok=True)
        self.router = LXMF.LXMRouter(identity=identity, storagepath=storage_path)
        self.local_destination = self.router.register_delivery_identity(
            identity, display_name=f"{DISPLAY_NAME} ({node_name})"
        )
        self.router.register_delivery_callback(self._on_lxmf_delivery)
        self.local_destination.announce()
        logger.info(f"Operator LXMF endpoint announced at "
                    f"[{RNS.prettyhexrep(self.local_destination.hash)}]; "
                    f"accepting commands from [{self.operator_hash.hex()[:10]}]")

    @staticmethod
    def _parse_hash(value: str) -> Optional[bytes]:
        cleaned = str(value or "").replace("<", "").replace(">", "").replace(":", "").replace(" ", "")
        try:
            return bytes.fromhex(cleaned)
        except ValueError:
            logger.error(f"Invalid operator LXMF hash '{value}'; operator control disabled")
            return None

    def _notify_hash(self, target_hash: bytes, title: str, body: str) -> bool:
        if not target_hash:
            return False

        if not RNS.Transport.has_path(target_hash):
            RNS.Transport.request_path(target_hash)
            logger.info("Requested path to operator; notification will be retried on the next event")
            return False

        operator_identity = RNS.Identity.recall(target_hash)
        if not operator_identity:
            logger.warning("Operator identity not yet known; cannot deliver notification")
            return False

        destination = RNS.Destination(
            operator_identity, RNS.Destination.OUT, RNS.Destination.SINGLE, "lxmf", "delivery"
        )
        message = LXMF.LXMessage(
            destination, self.local_destination, body, title,
            desired_method=LXMF.LXMessage.DIRECT
        )
        self.router.handle_outbound(message)
        return True

    def notify(self, title: str, body: str) -> bool:
        """Sends a message to the operator. Returns False if it could not be dispatched."""
        if not self.operator_hash:
            return False
        return self._notify_hash(self.operator_hash, title, body)

    def notify_peer_operator(self, peer_operator_hash: str, title: str, body: str) -> bool:
        target_hash = self._parse_hash(peer_operator_hash)
        if not target_hash:
            return False
        return self._notify_hash(target_hash, title, body)

    def notify_channel_request(self, name: str, description: str, requester: str) -> bool:
        body = (f"#{name} requested on {self.node_name}\n"
                f"By: {requester}\n"
                f"Description: {description or '(none)'}\n\n"
            f"Reply 'approve {name}', 'deny {name}', or pre-create with 'add {name}'.")
        return self.notify("Channel request", body)

    def endpoint_hash(self) -> str:
        """LXMF endpoint hash used by this node for operator control."""
        try:
            return self.local_destination.hash.hex()
        except Exception:
            return ""

    def format_blacklist_recommendation(self, recommended_identity_hash: str, rationale: str,
                                        source_peer_hash: str = "") -> str:
        payload = {
            "v": 1,
            "recommended_identity_hash": str(recommended_identity_hash or "").strip().lower(),
            "rationale": str(rationale or "").strip(),
            "source_node_name": self.node_name,
            "source_peer_hash": str(source_peer_hash or "").strip().lower(),
            "source_operator_hash": self.endpoint_hash(),
        }
        return json.dumps(payload, sort_keys=True)

    def _on_lxmf_delivery(self, message):
        source_hash = message.source_hash
        content = message.content.decode("utf-8", errors="replace") if isinstance(message.content, bytes) \
            else str(message.content)

        if message.title == RECOMMENDATION_TITLE and self.peer_message_handler:
            try:
                self.peer_message_handler(source_hash.hex(), str(message.title), content)
            except Exception:
                logger.exception("Failed to process peer operator message")
            return

        if not self.operator_hash or source_hash != self.operator_hash:
            logger.warning(f"Ignored LXMF command from non-operator [{source_hash.hex()[:10]}]")
            return

        reply = self._dispatch(content)
        if reply:
            self.notify("Speakeasy", reply)

    def _dispatch(self, content: str) -> str:
        replies: List[str] = []
        for line in content.strip().splitlines():
            parts = line.strip().split(None, 1)
            if not parts:
                continue
            command = parts[0].lower()
            argument = parts[1].strip() if len(parts) > 1 else ""
            replies.append(self.command_handler(command, argument))
        return "\n".join(r for r in replies if r) or USAGE

    def stop(self):
        try:
            self.router.exit_handler()
        except Exception as e:
            logger.debug(f"LXMF router shutdown: {e}")
