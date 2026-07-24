#!/usr/bin/env python3
"""
Operator CLI for a Speakeasy hub.

Fallback for operators who have not configured `moderation.operator_lxmf_hash`.
It writes directly to the hub database; a running daemon picks approvals up on
its next sync tick and propagates them to federated hubs, so the hub does not
need to be restarted (or stopped) to approve a channel.

    python speakeasy_admin.py pending
    python speakeasy_admin.py approve lounge
    python speakeasy_admin.py deny spam
"""

import argparse
import json
import os
import sys

import RNS

from fed_engine import MAX_MESSAGE_CONTENT_BYTES
from speakeasy_db import SpeakeasyDB, DEFAULT_EPOCH_BUCKET_SEC

STATE_DIR = os.path.expanduser("~/.reti_speakeasy")


def load_hub(config_path: str):
    with open(config_path, "r") as handle:
        config = json.load(handle)

    node_name = config.get("node", {}).get("name", "speakeasy_host")
    storage_cfg = config.get("storage", {})
    fed_cfg = config.get("federation", {})

    db_filename = storage_cfg.get("db_filename") or f"speakeasy_{node_name}.db"
    db = SpeakeasyDB(
        db_path=os.path.join(STATE_DIR, db_filename),
        epoch_bucket_sec=fed_cfg.get("epoch_bucket_sec", DEFAULT_EPOCH_BUCKET_SEC),
        max_message_bytes=MAX_MESSAGE_CONTENT_BYTES,
    )

    identity_path = os.path.join(STATE_DIR, f"{node_name}_identity")
    if not os.path.exists(identity_path):
        print(f"No hub identity at {identity_path}; start the daemon once first.", file=sys.stderr)
        sys.exit(1)

    return db, RNS.Identity.from_file(identity_path)


def main():
    parser = argparse.ArgumentParser(description="Speakeasy hub operator CLI")
    parser.add_argument("--config", default="speakeasy_config.json", help="hub config file")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("pending", help="list channel requests awaiting a decision")
    sub.add_parser("channels", help="list known channels")
    approve = sub.add_parser("approve", help="approve a requested channel")
    approve.add_argument("name")
    approve.add_argument("--description", default="")
    deny = sub.add_parser("deny", help="deny a requested channel")
    deny.add_argument("name")

    args = parser.parse_args()
    db, identity = load_hub(args.config)

    if args.command == "pending":
        requests = db.get_channel_requests("pending")
        if not requests:
            print("No pending channel requests.")
        for request in requests:
            print(f"#{request['name']:<20} by {request['requester_hash'][:10]}  "
                  f"{request['description'] or '(no description)'}")

    elif args.command == "channels":
        for channel in db.get_channels():
            approver = channel.get("approver_hash")
            origin = f"approved by {approver[:10]}" if approver else "local"
            print(f"#{channel['name']:<20} {origin}")

    elif args.command == "approve":
        name = args.name.lstrip("#")
        pending = {r["name"]: r for r in db.get_channel_requests("pending")}
        description = args.description or (pending.get(name) or {}).get("description") or f"Channel #{name}"
        db.sign_and_add_channel(identity, name, description)
        db.set_channel_request_status(name, "approved")
        print(f"Approved #{name}. A running daemon will propagate it on its next sync tick.")

    elif args.command == "deny":
        name = args.name.lstrip("#")
        if db.set_channel_request_status(name, "denied"):
            print(f"Denied #{name}.")
        else:
            print(f"No request found for #{name}.")

    db.close()


if __name__ == "__main__":
    main()
