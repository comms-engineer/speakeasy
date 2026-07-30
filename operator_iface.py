"""
LXMF control channel between a headless hub and its human operator.

The daemon has no console, so channel requests are delivered to the operator's
regular LXMF client (Sideband, Nomad Network, ...) as a normal message, and the
operator replies with `approve <channel>` / `deny <channel>`. Commands are only
honoured from the exact source hash configured as the operator, so approval
authority is bound to a key the operator already controls.
"""

import logging
import os
from typing import Callable, List, Optional

import LXMF
import RNS

logger = logging.getLogger("speakeasy_daemon.operator")

DISPLAY_NAME = "Speakeasy Hub"
USAGE = (
    "Commands: approve <channel> | deny <channel> | add <channel> [description] | "
    "pause <channel> | resume <channel> | block <channel> | pending | channels"
)


class OperatorInterface:
    """
    :param command_handler: Called with the parsed command and argument for
        every authenticated operator message; returns the reply text.
    """

    def __init__(self, identity: RNS.Identity, storage_path: str, operator_hash: str,
                 command_handler: Callable[[str, str], str], node_name: str = "Speakeasy"):
        self.command_handler = command_handler
        self.node_name = node_name
        self.operator_hash = self._parse_hash(operator_hash)

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

    def notify(self, title: str, body: str) -> bool:
        """Sends a message to the operator. Returns False if it could not be dispatched."""
        if not self.operator_hash:
            return False

        if not RNS.Transport.has_path(self.operator_hash):
            RNS.Transport.request_path(self.operator_hash)
            logger.info("Requested path to operator; notification will be retried on the next event")
            return False

        operator_identity = RNS.Identity.recall(self.operator_hash)
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

    def notify_channel_request(self, name: str, description: str, requester: str) -> bool:
        body = (f"#{name} requested on {self.node_name}\n"
                f"By: {requester}\n"
                f"Description: {description or '(none)'}\n\n"
            f"Reply 'approve {name}', 'deny {name}', or pre-create with 'add {name}'.")
        return self.notify("Channel request", body)

    def _on_lxmf_delivery(self, message):
        source_hash = message.source_hash
        if not self.operator_hash or source_hash != self.operator_hash:
            logger.warning(f"Ignored LXMF command from non-operator [{source_hash.hex()[:10]}]")
            return

        content = message.content.decode("utf-8", errors="replace") if isinstance(message.content, bytes) \
            else str(message.content)
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
