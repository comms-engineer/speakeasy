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
3. **Channel policy vs seeded channel.** Clients seed only `general`, while the shipped config allows
   `parlor/backroom/broadsheet`, so messages typed on the default tab are discarded by the hub
   (`Discarded message ...: channel #general not permitted`) with no client-side feedback. To exercise
   the store/relay path, add an allowed channel to each client DB before launching:
   `SpeakeasyDB(path).add_channel('parlor','Parlor')`. Channel tabs are built at startup, so add the
   channel before starting the client.

## Verifying behaviour (don't trust the TUI alone)
- Hub truth is SQLite: `~/.reti_speakeasy/speakeasy_daemon.db`, tables `messages`, `profiles`,
  `identities`, `bulletins`. Client truth: `~/.reti_speakeasy/speakeasy_<instance>.db`.
- Hub log strings worth asserting on: `Speakeasy Host Listening on Destination Hash`,
  `Link established with`, `Registered public key for remote identity`,
  `Relayed N verified message(s) to M peer link(s).`
- **A hub "Relayed ..." log line does NOT mean the peer client accepted it.** Receiving clients verify
  signatures against their own `identities` table and drop unverifiable records, logging only via
  `logging` (invisible in the TUI). If a relayed message never appears in the peer, check whether that
  client has an `identities` row for the sender; nothing currently propagates client public keys between
  clients. A useful diagnostic (not a fix) is copying the sender's `public_key` from the hub DB into the
  receiver's `identities` table and resending — if it then arrives, key distribution is the gap.
- Textual `Input` quirks: `ctrl+a` moves to line start (it is not select-all). Clear a field with
  `End`, `shift+Home`, `BackSpace`. After clicking a modal's Save button, wait for the toast before
  typing elsewhere, or keystrokes land back in the modal field.
- The sidebar system log is 38 columns wide, so long lines are truncated on screen; zoom the region and
  corroborate with the DB/log. Note the log currently escapes Rich markup, so entries display literally
  as `[bold red]Not sent:[/]`.
