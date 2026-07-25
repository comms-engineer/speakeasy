---
name: testing-speakeasy
description: How to run and end-to-end test the Speakeasy RNS hub daemon and Textual TUI clients on one machine (process ordering, announce timing, channel policy, DB inspection).
---

# Testing Speakeasy (hub daemon + TUI clients)

## Devin Secrets Needed
None. Everything is local: no credentials, no external network, no services.

## Setup
- `pip install -r requirements.txt` (rns, msgpack, textual, rich).
- Wipe `~/.reti_speakeasy` for a clean run (identities, hub DB, per-instance client DBs, logs live there).
- Hub: `python3 speakeasy_daemon.py <config>`; clients: `python3 reti_speakeasy.py <instance_name>`.
- Clients are full-screen Textual apps: launch each in its own terminal window, e.g.
  `setsid x-terminal-emulator -e bash -c "python3 reti_speakeasy.py alice; read" &`, then place them
  with `wmctrl -i -r <winid> -b remove,maximized_vert,maximized_horz` followed by
  `wmctrl -i -r <winid> -e 0,<x>,0,800,1200` (remove the maximized state first or the move is ignored).
- Launch each background process in its own `exec` call with a short `sleep`; a call that hits the
  tool timeout can kill the process group it just spawned.

## Ordering pitfalls that will otherwise waste a run
1. **Start the hub first.** The first RNS process becomes the shared-instance master. If you kill/restart
   the daemon while clients are running, the clients' RNS stack is orphaned and discovery silently stops —
   restart every process in order instead.
2. **Announce timing.** The hub announces only at startup and every `announce_interval_sec` (900 by
   default), so a client started afterwards sees an empty host table on `h`. Copy the config and set
   `node.announce_interval_sec` to ~20 for testing rather than restarting the daemon.
3. **Channel policy.** Defaults are now unified to `general/parlor/tech`, and a connected client mirrors
   the hub's FED_HELLO channel list (extra tabs appear seconds after connecting), so posting on
   `#general` works out of the box. If you need a channel the hub does *not* carry (to test the
   "Not sent" refusal), add it to that client's DB **before** launching it — tabs for local-only
   channels are built at startup: `SpeakeasyDB(path).add_channel('ghost','x')`.
4. **Fresh instance names beat wiping** when prior runs left state: `python3 reti_speakeasy.py ann`
   creates a brand-new identity + DB. For hubs, copy the config and change both `node.name` and
   `storage.db_filename`.

## Testing federation, discovery and operator approval
- Two hubs peer with **no `static_peers`** when `federation.auto_discover_peers` is true: start hub A,
  then hub B from a second config copy; each logs `Discovered Speakeasy hub '<name>' at [...]`, then
  `Initiating link to hub [...]` / `Link established` / `Bootstrapped peer link with N ... frame(s).`
  Linking happens on the service tick, so lower `node.sync_interval_sec` (as well as
  `announce_interval_sec`) to ~20 in the copies. Peers must share `federation.epoch_bucket_sec`.
- Hub B only federates messages posted **after** the peer link comes up; do not expect historical
  backfill in a short window.
- Operator channel approval without LXMF: `python3 speakeasy_admin.py --config <cfg> pending |
  approve <chan> | deny <chan> | channels`. A properly signed channel shows as
  `#name  approved by <hub hash>`; `local` means unsigned/seeded. The daemon propagates approvals on its
  next sync tick, and connected clients grow the new tab live.
- The client's system log now renders Rich markup correctly (the old literal `[bold red]` bug is gone).

## Verifying behaviour (don't trust the TUI alone)
- Hub truth is SQLite: `~/.reti_speakeasy/speakeasy_daemon.db`, tables `messages`, `profiles`,
  `identities`, `bulletins`. Client truth: `~/.reti_speakeasy/speakeasy_<instance>.db`.
- Hub log strings worth asserting on: `Speakeasy Host Listening on Destination Hash`,
  `Link established with`, `Registered public key for remote identity`,
  `Relayed N verified message(s) to M peer link(s).`
- **A hub "Relayed ..." log line does NOT mean the peer client accepted it.** Receiving clients verify
  signatures against their own `identities` table and drop unverifiable records, logging only via
  `logging` (invisible in the TUI). Key gossip (IDENTITY_PUSH 0x09 / IDENTITY_REQ 0x0A) now propagates
  sender keys, so the correct assertion is: the receiving client's DB has both the `messages` row **and**
  an `identities` row for the sender that you never inserted by hand. If a relayed message is missing,
  check that `identities` row first — a missing key means gossip regressed, not that the relay failed.
- Textual `Input` quirks: `ctrl+a` moves to line start (it is not select-all). Clear a field with
  `End`, `shift+Home`, `BackSpace`. After clicking a modal's Save button, wait for the toast before
  typing elsewhere, or keystrokes land back in the modal field.
- The sidebar system log is 38 columns wide, so long lines are truncated on screen; zoom the region and
  corroborate with the DB/log.
