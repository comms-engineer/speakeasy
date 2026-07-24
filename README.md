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
profile, `b` to post a bulletin.

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
| `channels.allowed_channels` | Channels this hub hosts; also the policy for peer-proposed channels |
| `channels.channel_blocklist` | Channels rejected outright |
| `channels.max_message_bytes` | Maximum message size accepted or signed (capped at `MAX_MESSAGE_CONTENT_BYTES` so a record always fits one frame) |
| `storage.db_filename` | Database name inside `~/.reti_speakeasy` |
| `storage.message_ttl_days` | Retention window for the periodic prune sweep |
| `logging.*` | Log level and optional log file |

## Development

```bash
pip install -r requirements.txt
pip install pytest ruff
ruff check .
pytest -q
```

Tests cover wire-frame round trips, signature tampering and impersonation rejection, and
two in-process hubs converging over a simulated link.
