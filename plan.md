# Big Phat Update v2.7 Brainstorm

## Goals
- Focus on stability, observability, and operator control.
- Reduce noisy alerts and improve movement signal quality.
- Make scaling to larger meshes smoother.
- Keep changes incremental and reversible.

## Brainstorm Themes (not committed)

### Reliability and Resilience
- Connection watchdog with exponential backoff and max retries before full reset.
- Serial path auto-detect + validation on startup, log chosen device.
- Graceful shutdown: flush queues, checkpoint WAL, record final stats.
- Startup health check: verify env vars + permissions with actionable errors.
- Enforce configured channel for commands with explicit logging.

### Movement and Location Quality
- Per-node movement profiles (static vs mobile).
- Accuracy-aware gating using precision or HDOP when available.
- Sliding window to reduce GPS jitter; require N consistent moves.
- Separate "moving" state from "moved" alert notifications.
- Configurable alert cooldowns per node + quiet hours.

### Database and Performance
- Paginated DB queries for node listings (LIMIT/OFFSET) instead of full scans.
- Add indexes for position history and topology queries.
- WAL checkpoint based on size threshold, not only time.
- Per-table retention policies (telemetry, positions, messages, alerts).
- DB health command to show WAL size and last checkpoint age.

### Command UX
- Configurable command prefix at runtime.
- Rate limits per command category and per user.
- Read-only `$config` or `$settings` view.
- Filters for `$nodes`/`$activenodes` (routers, recent, active).
- Export commands for nodes/telemetry in CSV or JSON.

### Observability
- Optional structured logs via config flag.
- Log rotation and retention reporting.
- Lightweight metrics summary (queue depth, drops, reconnects).
- Track dropped message counts over time.

### Security and Privacy
- Optional markdown and mention sanitization toggles (default on).
- Location privacy mode to hide coordinates for non-admins.
- Per-command permissions (admin-only list).
- Env sanity checks + redaction in logs.

## Proposed Phases
- Phase 1: Reliability + guardrails.
- Phase 2: Movement quality + noise reduction.
- Phase 3: DB performance + exports.
- Phase 4: UX polish + observability.

## Open Questions
- Should we support multiple Discord channels for read-only commands?
- Do we want per-node labels or roles for filtering?
- Preferred export format: CSV, JSON, or both?

## Risks
- Feature creep vs stability.
- Increased DB load from richer queries.
- Location privacy expectations for mixed audiences.

## Validation
- Run high-traffic mesh simulation or replay logs.
- Compare queue drop stats before and after changes.
- Validate movement alerts with real mobile nodes.
