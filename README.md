# Speakeasy

Federated social hubs for Reticulum.

Speakeasy is a decentralized, topic-based discussion network that runs entirely over the
[Reticulum Network Stack](https://reticulum.network/). Hubs federate with each other,
clients can attach to any hub, and every record — chat message, bulletin, profile — is
signed by its author's RNS identity, so authorship survives relaying across untrusted hops.

## Components

| File | Role |
| --- | --- |
| `speakeasy_daemon.py` | Hub daemon: announces itself, accepts client/peer links, relays and stores verified records |
| `reti_speakeasy.py` | Textual TUI client: host discovery and ranking, channels, bulletin board, profile |
| `fed_engine.py` | S2S wire protocol (msgpack frames) and the epoch anti-entropy state machine |
| `speakeasy_db.py` | SQLite storage, verification choke point, epoch Merkle roots |
| `signing.py` | Canonical byte encodings, signing, and verification |
| `operator_iface.py` | LXMF endpoint that pages the hub operator and takes `approve` / `deny` replies |
| `speakeasy_admin.py` | Offline CLI for reviewing the channel-request queue |

## Trust model

There is no server-side account system; identity is the RNS keypair.

- Every message, bulletin, and profile carries an Ed25519 signature over a canonical
  encoding (`signing.py`) of its fields.
- Nothing is stored or re-relayed until `SpeakeasyDB.verify_and_*` validates that
  signature against the claimed identity hash. That is the single choke point.
- A public key is only accepted for an identity if it hashes to that identity's hash, so a
  peer cannot present its own key alongside somebody else's hash.
- Hubs re-pack records from their own storage when relaying, so a hub never forwards
  unverified bytes on a peer's behalf.
- Keys reach the people who need them by gossip, not by configuration: hubs push the
  public keys and signed profiles they hold, so a client that has never met the author of
  a relayed message can still verify it. A record whose author is still unknown is parked
  (bounded) and an `IDENTITY_REQ` is sent, rather than silently discarded — mesh links do
  not guarantee that a key arrives before the message it signs.

## Federation

Hubs reconcile history with epoch-bucketed anti-entropy:

```
FED_HELLO          -> advertise channels + epoch bucket size
EPOCH_SYNC_REQ     -> "what is your Merkle root for (channel, epoch)?"
EPOCH_SYNC_RESP    -> roots; a mismatch means divergence
DELTA_REQ          -> "here are the ids I hold; send what I lack"
DELTA_PUSH         -> the missing signed records (MDU-sized batches)
```

`DELTA_REQ` doubles as an advertisement of what the requester holds, so reconciliation is
symmetric: both sides end an exchange with the same records. Peers must agree on
`federation.epoch_bucket_sec`, otherwise epoch roots can never match — mismatches are
detected at `FED_HELLO` and logged.

### Historical backfill

A hub joining an existing mesh inherits history, not just new traffic. Two details make
that work:

- **The responder volunteers epochs.** A hub with an empty database cannot *name* a single
  historical epoch to ask about, so `EPOCH_SYNC_REQ` also carries how far back the requester
  is willing to reconcile, and the peer offers roots for populated epochs inside that
  horizon. Without this, two hubs only ever compare the epoch they are both currently in —
  where they trivially agree — and history predating the link is never transferred.
- **Backfill is bounded, not a bulk transfer.** Only epochs that actually hold messages are
  compared, so a quiet fortnight costs nothing (at a 300 s bucket, two weeks is over 4000
  epochs, nearly all empty). Each round covers at most `MAX_SYNC_EPOCHS` channel-epochs and
  successive rounds walk the window further back, wrapping around when they reach the
  oldest history. Merkle responses are chunked to fit the MDU.

The horizon is `federation.max_sync_history_days`, clamped to `storage.message_ttl_days`:
chasing history the node would delete on its next retention sweep would make two hubs trade
the same records forever.

### Peer discovery

With `federation.auto_discover_peers` enabled (the default) a hub registers an RNS announce
handler filtered to the `speakeasy.host` aspect, so hubs find each other simply by being on
the same Reticulum network — `static_peers` is a fallback, not a requirement. Announces from
the hub itself are ignored, discovered peers are linked at most once, and discovery stops at
`node.max_clients`.

For client-side host filtering, announces can optionally include a tiny fixed-size channel
summary (plus channel count) so the client can pre-filter likely hubs by channel before
connecting. This is a probabilistic prefilter, not an authority source: exact channel lists
still come from `FED_HELLO` after link establishment.

### Channels

Channels are not purely local configuration. A client can request one (`n` in the TUI),
which reaches the hub as a `CHANNEL_REQ` and lands in a pending queue; nothing is created
yet. When the operator approves it, the hub signs the channel under its own identity and
federates it as a signed `CHANNEL_ADD`. Receiving hubs verify the approver's signature and
key-to-hash binding before adding it, accept it even if their own `allowed_channels`
predates the approval, and ignore replays, so the same approval circulating around a mesh
converges instead of looping.

Channel nominations are federated to peer hubs by default: when one hub accepts a
`CHANNEL_REQ`, it relays the nomination (including the original requester hash) so other
operators can review the same request independently for their own hub. Set
`moderation.receive_federated_channel_nominations` to `false` to opt out of receiving
federated nominations while still handling local client requests.

Approval happens over LXMF when `moderation.operator_lxmf_hash` is set: the hub messages the
operator on each request and accepts `approve <channel>` / `deny <channel>` replies **only**
from that exact source hash. Otherwise use the CLI, which drives the same queue:

```bash
python speakeasy_admin.py pending
python speakeasy_admin.py approve lounge
python speakeasy_admin.py deny spam
python speakeasy_admin.py add events --description "Community events"
python speakeasy_admin.py pause lounge
python speakeasy_admin.py resume lounge
python speakeasy_admin.py block spam
```

`pause` keeps a channel defined but refuses traffic, `resume` re-enables it, and
`block` refuses both traffic and future requests for that channel.

### Operator Management Over LXMF

When `moderation.operator_lxmf_hash` is configured, the daemon starts an LXMF control endpoint
and sends an automatic startup heartbeat (`Speakeasy online`) to the configured operator hash.
If the route to the operator is not ready yet, the node retries delivery every 30 seconds until
it succeeds. This creates or refreshes a message thread in your LXMF client so operator actions
can stay in one place.

#### LXMF command reference

Send one command per line in the LXMF thread; multiple lines are processed in order.

| Command | Effect |
| --- | --- |
| `help` or `?` | Show full command help |
| `status` or `stats` | Node status (uptime, links, channel state counts, pending requests) |
| `pending` or `requests` | List pending channel nominations |
| `recent [N]` or `audit [N]` | Show most recent operator actions (default 10, max 50) |
| `channels` | List channels and status (`active`, `paused`, `blocked`) |
| `approve <channel>` | Approve queued request and propagate signed channel add |
| `deny <channel>` | Deny queued request |
| `add <channel> [description]` | Create + approve channel immediately |
| `pause <channel>` | Keep channel but refuse traffic |
| `resume <channel>` | Re-enable channel traffic |
| `block <channel>` | Refuse channel traffic and future channel requests |

#### Example LXMF session

```text
status
pending
approve lounge
recent 5
pause off-topic
channels
```

Operator actions are stored locally in the node database as an audit trail
(`approve`, `deny`, `add`, `pause`, `resume`, `block`, startup heartbeat), and
can be reviewed at any time from the same LXMF thread with `recent`.

Command authorization is strict: only messages received from the exact configured
`moderation.operator_lxmf_hash` are accepted. Messages from any other LXMF identity are ignored.

> **Not yet verified on real hardware:** the LXMF path has only been smoke-tested — the
> endpoint starts and announces, and the source-hash authorisation is in place, but issuing
> `approve <channel>` from a second live LXMF peer (Sideband, MeshChat) has not been
> exercised end to end. Confirm it when running a hub on your own hardware with
> `moderation.operator_lxmf_hash` pointed at your own address; the CLI above drives the same
> queue in the meantime.

## Storage on small hardware

Much of Reticulum runs on a Pi or a similar SBC with an SD card, so an unbounded message
table is a real failure mode — the node dies of a full disk instead of degrading by
forgetting old chatter. Retention is applied in three escalating steps on a timer
(`storage.prune_interval_hours`):

1. **Age** — messages older than `storage.message_ttl_days` are dropped.
2. **Depth per channel** — each channel keeps at most `storage.max_messages_per_channel`
   of its newest messages. The cap is per channel rather than global so one flooded channel
   cannot evict a quiet one's entire history.
3. **Hard ceiling** — if the database still exceeds `storage.max_db_mb`, the oldest
   messages are shed until it fits. This is the backstop that makes a hub safe to run
   unattended.

The ceiling is measured against the size the database will occupy once free pages are
reclaimed, not its current size on disk. A `DELETE` only moves pages onto SQLite's freelist,
so straight after steps 1 and 2 the file still measures its old size — and step 3 reading
that would conclude the earlier pruning achieved nothing and shed the entire remaining
history while the real payload sat at a fraction of the budget.

Only message history is ever pruned. If the database is over budget with no messages left
to drop — the schema, indexes, identities and profiles alone exceeding the ceiling — the hub
logs a warning and stops, rather than deleting the keys it needs to verify anyone. Space
freed by deletes is returned to the filesystem by `VACUUM` on
`storage.vacuum_interval_hours`; SQLite is in WAL mode, so expect a `-wal` file alongside
the database in addition to the configured budget. Set any of these limits to `0` to
disable that step.

## Blocking identities

Blocking works at two distinct scopes: the **node operator** blackholes an identity for the
whole node with Reticulum's own tooling, and a **user** blocks someone for their own client
only. Speakeasy honours both.

The operator scope is RNS's blackhole list, which Speakeasy reads rather than duplicating:

```bash
rnpath -B <identity_hash>                    # blackhole an identity
rnpath -B <identity_hash> --duration 24      # ...for 24 hours
rnpath -b                                    # list blackholed identities
rnpath -U <identity_hash>                    # lift it
```

RNS applies the list to *pathing*: announces and paths for a blackholed identity are
dropped. That alone does nothing about a spammer whose records arrive relayed inside a
hub's frames, carrying no path of their own — so Speakeasy also applies it at the record
layer. A blackholed identity's messages, profiles, bulletins, channel requests and channel
approvals are refused before verification (their records are perfectly well signed, so the
signature check would happily let them through), their keys are neither learned nor
gossiped onward, records already stored from them are no longer relayed, and inbound links
from them are torn down. The list is consulted per record, so a block applied with `rnpath`
against a running node takes effect on the next frame rather than the next restart.

From the client, `/block <handle|hash>` blocks an identity **for that client only** and
purges what they already posted from its database, `/unblock` lifts it and `/blocked` lists
both scopes, marking operator blackholes as such. Blocked authors are also filtered out of
already-stored history when a channel is rendered.

A client block is stored in the client's own database, *not* in RNS's blackhole list, and so
will not appear in `rnpath -b`. This is deliberate: that list is node-wide state owned by
the master RNS instance, and Speakeasy processes are normally shared-instance clients, so
writing to it from a client changes only that process's in-memory copy — and the next
`rnpath -B` rewrites the shared file wholesale and silently drops the entry. It is also the
wrong scope: one user muting a bore should not blackhole them for every application on the
machine. Use `rnpath -B` when you do want a block to apply node-wide, including to a hub's
federated records.

## Running a hub

```bash
pip install -r requirements.txt
python speakeasy_daemon.py speakeasy_config.json
```

Or with Docker (state, including the database and identity, persists in `./data`):

```bash
docker compose up --build
```

## Running the client

```bash
pip install -r requirements.txt
python reti_speakeasy.py [instance_name]
```

`instance_name` selects an identity and database under `~/.reti_speakeasy`, so several
clients can share one machine. Press `h` to pick a discovered host, `p` to edit your
profile, `b` to post a bulletin, `n` to request a new channel. The client mirrors the
channel list its hub advertises, and refuses to "send" to a channel the hub does not carry
instead of storing a message nobody will ever receive.

## Configuration

`speakeasy_config.json` drives the daemon:

| Key | Effect |
| --- | --- |
| `node.name` | Hub name, announced to clients and used for the identity filename |
| `node.bandwidth_class` | `LOW_MESH`, `MEDIUM_MESH`, or `HIGH_SPEED` |
| `node.sync_interval_sec` | How often static peers are (re)connected |
| `node.announce_interval_sec` | Host announce cadence |
| `node.max_clients` | Inbound link ceiling; further links are torn down |
| `static_peers` | Destination hashes of hubs to federate with |
| `federation.epoch_bucket_sec` | Anti-entropy epoch width; must match peers |
| `federation.settlement_delay_ms` | Pause before initiating a peer link |
| `federation.auto_discover_peers` | Federate with any hub heard announcing, not just `static_peers` |
| `federation.include_channel_summary_in_announces` | Includes a compact channel summary in announces for low-overhead client host prefiltering |
| `channels.allowed_channels` | Channels this hub hosts; operator-approved channels are accepted on top of this |
| `moderation.accept_channel_requests` | Whether clients may queue channel requests |
| `moderation.receive_federated_channel_nominations` | Whether this hub accepts channel nominations relayed from federated peers |
| `moderation.operator_lxmf_hash` | LXMF address for operator control; the only source accepted for LXMF management commands |
| `channels.channel_blocklist` | Channels rejected outright |
| `channels.max_message_bytes` | Maximum message size accepted or signed (capped at `MAX_MESSAGE_CONTENT_BYTES` so a record always fits one frame) |
| `storage.db_filename` | Database name inside `~/.reti_speakeasy` |
| `federation.max_sync_history_days` | How far back backfill reconciles; clamped to `storage.message_ttl_days` |
| `storage.message_ttl_days` | Retention window for the periodic prune sweep |
| `storage.max_messages_per_channel` | Per-channel message cap; `0` disables |
| `storage.max_db_mb` | Hard database size ceiling, enforced by shedding oldest history; `0` disables |
| `storage.prune_interval_hours` | Retention sweep cadence |
| `storage.vacuum_interval_hours` | How often freed pages are returned to the filesystem |
| `logging.*` | Log level and optional log file |

## Development

```bash
pip install -r requirements.txt
pip install pytest ruff
ruff check .
pytest -q
```

Tests cover wire-frame round trips, signature tampering and impersonation rejection, two
in-process hubs converging over a simulated link, key/profile gossip and out-of-order key
recovery, signed channel approval and its propagation, and announce-driven peer discovery.
