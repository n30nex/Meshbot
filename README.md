# Meshtastic Discord Bridge Bot – v2.7

Docker-first bridge between Meshtastic and Discord, with telemetry, locations, and topology views.

## What’s new in 2.7
- ADSB flight matching for airborne mesh nodes: looks up aircraft from the combined feed and posts a probable match with confidence.
- Flight-number prompts when a high-altitude node reports position or mentions being airborne without a callsign (rate-limited).
- Flight match embeds now link to the ADSB WebUI for the matched ICAO.
- Flight tracking: sends “Safe Flight <node>” on first detection, plus Discord notices when the mesh node goes out of range or the ADSB target drops.

## What’s new in 2.6
- Docker workflow by default; local scripts removed.
- Paginated `$nodes` / `$activenodes` with optional nearest-city lookup (configurable).
- Rich telemetry: `$telem` shows per-node table (battery, voltage, temp, humidity, pressure, channel/air util, uptime, seen) plus summary.
- History and health: `$history <node> <metric>` for recent values; `$dbhealth` for DB counts/last timestamps/size.
- Topology: `$topo` shows hop-aware ASCII routes plus full edge list.
- Position ingest: accepts both integer and float lat/lon from position or telemetry packets; optional reverse geocode caching.

## Install & Run (Docker)
```bash
docker compose up --build -d
```

Environment (`.env`):
```
DISCORD_TOKEN=your_discord_token
DISCORD_CHANNEL_ID=your_channel_id
MESHTASTIC_HOSTNAME=optional_tcp_host   # or use serial via docker-compose devices
ALERT_CHANNEL_ID=optional_alert_channel
```

## Key Commands
- `$help` – list commands
- `$txt <message>` / `$send <longname> <message>` – send to mesh / specific node
- `$nodes`, `$activenodes` – paginated node lists with SNR/battery/last seen (and city when GPS available)
- `$telem` – telemetry summary + per-node table
- `$history <node> <metric>` – latest metric values
- `$dbhealth` – DB stats and last timestamps
- `$topo` – network topology with hops/edges
- `$where <name>`, `$nearest <name>` – location utilities
- `$status`, `$uptime` – bridge status and uptime

Topology and tables:
- `$topo` now anchors to the bot radio (when known), grouping nodes by hop and showing busiest links.
- `$nodes` / `$activenodes` use a telemetry-style grid (Bat/Volt/Ch/Air/Uptime/Seen/City); full environmental metrics remain in `$telem`.
- `$telem` pulls the latest non-null metrics per field so environment readings don’t disappear when later packets omit them.

## Configuration (config.py)
- `BOT_CONFIG`: thresholds, rate limits, watchdog, geocode toggles (`geocode_enabled`, `geocode_max_per_run`, `geocode_timeout`), pagination size (`node_page_size`).
- `HIGH_ALTITUDE`: thresholds/cooldowns for altitude alerts.
- `ADSB_LOOKUP`: ADSB endpoint (`endpoint_url`), WebUI base link (`webui_base_url`), timeouts, 10s prompt cooldown, altitude threshold, match scoring tunables, and out-of-range timers.
- `LOGGING_CONFIG`: level/format/file.

## Behavior Notes
- Reverse geocoding is optional and rate-limited; disable with `geocode_enabled=False` if undesired.
- Data persistence is under `./data` (mounted to `/data` in Docker).
- Presence/new-node announcements remain conservative to avoid noise.
- ADSB matching uses the combined feed by default (`http://127.0.0.1:8090/data/aircraft.json`) and links to `https://adsb.canadaverse.org/?icao=<hex>`.

## Development
- Dependencies: see `requirements.txt`.
- Telemetry/position storage uses WAL and indexed lookups; queries are paged to avoid slow scans.

## License
Same license as the original Meshtastic Discord bridge bot.
