# Architecture

This document describes the current architecture of the Meshtastic Discord bridge
bot in `/home/neonx/bot2`, with enough detail to support future upgrades.

## Overview

The bot bridges a Meshtastic mesh network and a Discord channel. It listens to
mesh packets (text, telemetry, position, routing) via the Meshtastic Python API,
persists node/telemetry/position data in SQLite, and exposes commands and status
views in Discord. It sends Discord messages to the mesh, performs periodic
maintenance (cache cleanup, presence evaluation, telemetry summaries, alerts),
and is now Docker-first (compose stack).

Key traits:
- Single Python process with async event loop (discord.py) and background tasks.
- Meshtastic interface uses pubsub callbacks for packet reception.
- SQLite persistence with WAL mode, connection pooling, and indexes.
- Queues between Discord and mesh to decouple IO and rate control.
- Pagination for heavy listings; geocoding optional and rate-limited.

## Components

### Entry Point
- `bot.py` is the main module and entry point (`main()`).
- Loads environment variables via `python-dotenv`.
- Builds a `Config` object from environment and `config.py`.
- Initializes `MeshtasticDatabase`, `MeshtasticInterface`, and `DiscordBot`.

### DiscordBot (discord.Client)
File: `bot.py`
- Owns the Discord connection and main async tasks.
- Maintains queues:
  - `mesh_to_discord`: mesh -> Discord messages (max 1000).
  - `discord_to_mesh`: Discord -> mesh messages (max 100).
- Sets up `CommandHandler` and binds its queues.
- Background tasks:
  - `background_task()`:
    - Subscribes to Meshtastic pubsub topics.
    - Processes queues and node refresh.
    - Presence evaluation and cleanup.
    - Watchdog reconnect if no mesh packets arrive.
  - `telemetry_update_task()`:
    - Periodic summary updates (see `bot.py` for schedule).
  - `high_altitude_task()`:
    - Periodic high altitude alerts with DB deduping.
- Event handlers:
  - `on_ready()` connects to Meshtastic and starts background tasks.
  - `on_message()` routes commands and handles ping.
- Graceful shutdown cancels tasks and closes Discord.

### MeshtasticInterface
File: `bot.py`
- Connects to Meshtastic via:
  - TCP (`MESHTASTIC_HOSTNAME`), or
  - Serial (`MESHTASTIC_SERIAL_PORT` or default serial device).
- Provides:
  - `connect()` to establish interface.
  - `send_text()` to send to a node or primary channel.
  - `process_nodes()` to read `iface.nodes` and persist to DB.
  - `get_nodes_from_db()` to retrieve stored nodes.

### CommandHandler
File: `bot.py`
- Handles Discord commands and responses.
- Key features:
  - Command routing with aliases from `config.py`.
  - Per-user cooldown (2s).
  - Async DB query helper with timeout, retries, and circuit breaker.
  - Caching of DB responses (1 minute TTL).
  - Live packet buffer for monitoring commands.
- Supported command set is defined in `self.commands` and includes:
  - `$help`, `$txt`, `$send`, `$activenodes`, `$nodes`, `$telem`,
    `$status`, `$topo`, `$where`, `$nearest`, `$uptime`.

### Database Layer
File: `database.py`
- SQLite database with WAL mode, connection pooling, and indexes.
- Handles schema setup and lightweight migrations on startup.
- Provides methods for node management, telemetry, positions, messages,
  topology data, presence state, DB health (`get_db_health`), and metric history
  (`get_metric_history`).
- Optimized latest telemetry/position snapshot using max-timestamp joins.
- Background DB maintenance thread runs cleanup based on retention.

### Maintenance and Utilities
- `maintain_db.py`: manual stats, cleanup, and node inspection.
- `fix_database.py`: repair script for corrupted indexes and WAL files.
- Docker-first; legacy local scripts removed from repo.

## Data Flows

### Mesh -> Discord
1. Meshtastic library emits pubsub event `meshtastic.receive`.
2. `DiscordBot.on_mesh_receive()` parses packet and updates:
   - `nodes.last_heard` immediately.
   - `messages`, `telemetry`, and `positions` tables depending on packet.
3. For text packets:
   - Message payload enqueued to `mesh_to_discord`.
4. `background_task()` drains `mesh_to_discord` and posts to Discord.

### Discord -> Mesh
1. User sends a command or message in Discord.
2. `on_message()` routes `$` commands to `CommandHandler`.
3. Commands that send text enqueue work in `discord_to_mesh`.
4. `background_task()` drains `discord_to_mesh` and calls
   `MeshtasticInterface.send_text()` (to primary or destination node).

### Node Refresh and Telemetry
1. `background_task()` periodically calls `MeshtasticInterface.process_nodes()`.
2. Interface nodes are translated into DB updates:
   - `nodes` upsert.
   - `telemetry` and `positions` when data is present.
3. `telemetry_update_task()` composes summary data from DB and posts to Discord.
4. Telemetry snapshot queries use max-timestamp joins; per-node tables are paged.

### Presence and Alerts
- Presence transitions are computed with hysteresis.
- New node announcements are deduped with `nodes.announced_at`.
- High altitude alerts are deduped via `alerts` table.
- (New) Optional geocoding for nearest city; configurable and rate-limited.

## Database Schema (SQLite)

The database file defaults to `meshtastic.db` unless overridden by
`DATABASE_PATH`.

Tables (high level):
- `nodes`: node identity and status (id, names, last_seen, last_heard,
  hops, router/client, presence_state, announced_at, labels).
- `telemetry`: device and environment metrics plus radio and position data (lat/lon/alt fields can be stored when present).
- `positions`: latitude/longitude updates with timestamp and source.
- `messages`: mesh packet metadata (text, hops, snr, rssi, port).
- `alerts`: dedupe table for alert types.

Indexes:
- Per-table timestamp indexes and composite lookup indexes for fast latest
  record access (telemetry, positions, messages). Latest snapshot queries now use max-timestamp joins to reduce scan cost.

## Configuration

Sources:
- `.env` (runtime, secrets): `DISCORD_TOKEN`, `DISCORD_CHANNEL_ID`,
  `MESHTASTIC_HOSTNAME`, `MESHTASTIC_SERIAL_PORT`, `ALERT_CHANNEL_ID`,
  `DATABASE_PATH`.
- `config.py`: behavior defaults and feature flags.

Notable settings:
- `BOT_CONFIG.active_node_threshold`, presence thresholds and hysteresis.
- `BOT_CONFIG.rate_limits` for per-minute Discord message categories.
- `LOGGING_CONFIG` for log file and rotation.
- `HIGH_ALTITUDE` thresholds and cooldown.

## Deployment

### Docker
Files: `Dockerfile`, `docker-compose.yml`
- Dockerfile sets `DATABASE_PATH=/data/meshtastic.db` and runs `bot.py`.
- Compose runs with `network_mode: host`, `privileged: true`, and mounts
  serial devices and volumes.
- Volume mapping uses `./data:/data` and `./logs:/app/logs` for persistence.

### Local
- Docker-first; legacy local scripts removed from repo.

## Observability

Logging:
- Rotating file handler (`LOGGING_CONFIG.file`, default `bot.log`) and stdout.
- Extra logs in `start_bot.sh` use `meshtastic_bot.log`.

Metrics:
- No dedicated metrics stack; status is reported via Discord commands
  (for example, `$status`, `$uptime`) and DB stats via `maintain_db.py`.

## Upgrade Considerations

Areas most likely to change safely:
- Discord command set and presentation (isolated in `CommandHandler`).
- Storage schema (migrations live in `MeshtasticDatabase.init_database()`).
- Meshtastic connectivity and packet parsing (isolated in `MeshtasticInterface`
  and `DiscordBot.on_mesh_receive()`).

Risks and coupling points:
- Pubsub topics (`meshtastic.receive`, `meshtastic.connection.established`)
  are library-dependent. Validate when upgrading `meshtastic`.
- Discord intents and API changes can break message handling.
- Database paths in Docker compose should be consistent with `DATABASE_PATH`.
- Long-running background tasks share event loop; avoid blocking calls or
  use `asyncio.to_thread()` as current code does for DB IO.

Suggested future improvements (optional):
- Separate configuration class from runtime environment for testability.
- Provide migration scripts for schema changes beyond lightweight alters.
- Add structured logs and more explicit health checks.
