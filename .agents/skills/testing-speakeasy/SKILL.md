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
- **Historical backfill works but takes several rounds.** A hub started with an empty DB and empty
  `static_peers` pulls history posted before it existed, via anti-entropy rather than the initial HELLO.
  Assert on `Anti-entropy round over N channel-epoch(s) to M peer link(s).` and allow ~4+
  `node.sync_interval_sec` ticks (≈80 s at 20 s) before judging. Each round covers ≤48 channel-epochs
  and a cursor walks backwards, so long histories need proportionally more rounds. To test it, post the
  messages **and verify the joiner's DB file does not exist yet** before starting the joiner.
- Operator channel approval without LXMF: `python3 speakeasy_admin.py --config <cfg> pending |
  approve <chan> | deny <chan> | channels`. A properly signed channel shows as
  `#name  approved by <hub hash>`; `local` means unsigned/seeded. The daemon propagates approvals on its
  next sync tick, and connected clients grow the new tab live.
- The client's system log now renders Rich markup correctly (the old literal `[bold red]` bug is gone).

## Testing retention (`storage` block)
- Use a config copy with a tiny budget to make the sweep observable, e.g.
  `max_messages_per_channel: 5`, `max_db_mb: 1`, `prune_interval_hours: 0.001` (≈3.6 s).
- Assert on `Retention sweep removed X expired, Y over-cap and Z over-budget message(s); database now
  N KiB.` A hub that peers with an existing hub will backfill enough messages to breach a small cap on
  its own — no manual traffic needed for the cap stage.
- Only `messages` are pruned: `identities`, `channels` and `profiles` must survive, and the hub must
  still verify a *new* post afterwards. Assert that explicitly — it is the real risk of pruning.
- The TTL stage will not fire in a short run (`message_ttl_days` is 30). To exercise the byte ceiling
  you must bulk-insert rows directly into the hub DB; state clearly in the report that generation was
  scripted while the sweep itself ran live in the daemon.
- The size stage measures `db_payload_bytes()` = `(page_count - freelist_count) * page_size`, **not** the
  on-disk size, and vacuums once at the end. An older build measured the raw `page_count`, so after a big
  cap prune the inflated free pages made it shed the entire remaining history. If you ever see a non-zero
  `over-budget` count together with a resulting file far *inside* the budget (e.g. `5 over-budget;
  database now 60 KiB` against a 1 MB limit), that regression is back — it is not your setup.
- Test the two stages **separately**, with two config copies; a single config cannot distinguish them:
  - cap stage / no over-deletion: `max_messages_per_channel: 5` + `max_db_mb: 1`, flood ~9,000 rows.
    Expect `... over-cap and 0 over-budget ...`, the channel count still exactly 5, and the file
    shrinking (record `page_count * page_size` right after the insert to prove it).
  - genuine shedding: same but `max_messages_per_channel: 100000`, so the cap cannot help and
    `enforce_size_limit` must act. Expect a non-zero over-budget count, final payload ≤ `max_db_mb`, and
    **oldest-first** eviction — give bulk rows ordered `msg_id`s (`bulk000000`…) and ascending timestamps
    so you can assert the low-numbered ones are gone while the highest survive.

## Testing blocking / RNS blackhole
- Requires an RNS with the blackhole API — verify first:
  `python3 -c "import RNS; print(hasattr(RNS.Transport,'blackhole_identity'))"` and `rnpath -h | grep -i blackhole`.
  Present in RNS 1.4.0 (`rnpath -b` list, `-B` add, `-U` lift).
- Clear `~/.reticulum/storage/blackhole` (a **directory** containing a msgpack `local` file) between
  scenarios, alongside `~/.reti_speakeasy`.
- `/block <handle|hash-prefix>`, `/unblock`, `/blocked` are typed into the **chat input**, not keybindings.
  Resolution matches a unique hash prefix, then handle/alias, so the target's profile must have gossiped
  first. Blocking purges the author's messages/bulletins/profile locally but keeps the key, so `/blocked`
  afterwards shows a hash prefix instead of the handle.
- **Client blocks and operator blackholes are two separate stores; keep them straight.**
  `/block` writes the client DB's `blocked_identities` table (`blackhole.py` only *reads* RNS's table).
  So `rnpath -b` **correctly does not list a client `/block`** — do not report that as a bug. Assert a
  client block on: the System Log line, `select * from blocked_identities` in
  `speakeasy_<instance>.db`, the DB purge, and the author's next post failing to appear.
- Worthwhile assertions for a client block, all cheap:
  - **durability**: quit the client with `q`, post from the author meanwhile, relaunch with the *same*
    instance name and reconnect — the block must still hide both old and new records.
  - **not clobbered**: `rnpath -B <unrelated hash>` rewrites the shared blackhole file; the client's
    `blocked_identities` row must survive (an older build lost it here).
  - **scope**: the hub DB and the *other* client must still contain the blocked author's messages.
  - `/blocked` prints `blocked: <hash10> (<alias>)` for local and `node-wide blackhole: <hash10>` for RNS
    entries; `/unblock` on an RNS entry must say `Blackholed node-wide: lift it with rnpath -U ...`.
- Two client-side gotchas when checking those labels:
  - RNS loads the blackhole file at **process start** and a shared-instance client never reloads it, so an
    operator `rnpath -B` issued *after* the client launched will not show in that client's `/blocked`.
    Restart the client (or set the blackhole before launching it) to test the `node-wide` label.
  - `/unblock` matches RNS entries by **full 32-char hash only**; a hash prefix falls through to
    `Not blocked: ...`. Use the full hash when testing the `rnpath -U` hint.
- `rnpath -B <hash>` *does* reach the master, so it blocks at the **hub** for everyone: assert
  `Dropped message in #<chan> from blackholed identity <hash10>` in the daemon log. Note the message is
  not permanently refused — once the block is lifted, anti-entropy refetches and stores it.

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
- Keep terminal windows ~1080px tall, not full 1200: at full height the chat input sits under the
  floating taskbar, so clicks meant for the input hit the panel (raising Chrome) and leave the TUI
  unfocused. Unfocused typing is destructive — `/block ...` sends `b`, which opens the bulletin modal.
  Click the chat input, screenshot to confirm the text is staged, *then* press Return.
- The profile modal opens pre-populated with the current handle (often the hash prefix), so typing
  appends. Always `End`, `shift+Home`, `BackSpace` first or you get handles like `198a2eef9fDebRadio`.
- Use `xdotool windowactivate <winid>` to focus a specific client window instead of clicking near screen
  edges, and `xdotool windowminimize` to dismiss a browser window that stole focus.
