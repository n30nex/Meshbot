# Big Phat Update v2.6 Plan

## Goals
- Ship the requested v2.6 feature set with minimal disruption.
- Keep changes incremental and testable.
- Prefer DB-backed data for analytics and summaries.

## Phased Implementation

### Phase 1: Foundations + Schema Versioning
- Add a schema version table and a migration runner in `database.py`.
- Define a migration list and bump current schema version.
- Add DB helpers for:
  - last message per node
  - telemetry history slices for trend calculations
  - position history for movement trails
  - per-node message counts for rate/leaderboards

### Phase 2: Command UX + Help
- Expand `$help` to include all existing commands.
- Add `$help <command>` detail view (usage + examples).
- Wire hidden commands into the command registry.
- Add `$node <name>` profile card (latest telemetry, last message, trends).
- Add `$history <node> <metric> --spark`.
- Add `$leaderboards` for node health score rankings.
- Add `$trace <node>` to the command registry.
- Add `$otm <node>` for movement/trail summaries.

### Phase 3: Map Snapshots
- Add static map snapshot support using a provider URL template.
- Default to OSM static map endpoint with configurable base URL.
- Add a caching option (simple file cache with TTL).

### Phase 4: Queue Persistence
- Persist `discord_to_mesh` and `mesh_to_discord` queues on shutdown.
- Restore queues on startup with age-based drop policy.
- Add config for max age and retain/drop behavior.

### Phase 5: Logs + Maintenance
- Add optional structured logging (JSON) formatter.
- Add runtime log-level command (admin-only).
- Add scheduled DB backup and WAL checkpoint/cleanup.

## Notes
- Map snapshots can be implemented as static-image URLs (no API key required for OSM static map), with optional caching to reduce repeated fetches.
- Health score: combine battery slope, uptime, last-heard age, and message rate into a 0–100 score with tunable weights in config.
