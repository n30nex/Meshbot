"""
Configuration file for Meshtastic Discord Bridge Bot
Modify these settings to customize bot behavior
"""

# Bot Configuration
BOT_CONFIG = {
    # Message settings
    'message_max_length': 225,
    'command_prefix': '$',

    # Node management
    'node_refresh_interval': 60,  # seconds
    # The threshold (in minutes) to consider a node active. Previously this was
    # set to 15 minutes, which could miss devices that only check in hourly.
    # Increase to 60 minutes by default to align with hourly summary expectations.
    'active_node_threshold': 60,  # minutes
    
    # Discord settings
    'embed_color': 0x00ff00,  # Green color for embeds
    'message_timeout': 30,  # seconds for message deletion
    
    # Meshtastic settings
    'connection_timeout': 10,  # seconds
    'retry_attempts': 3,
    'retry_delay': 5,  # seconds
    
    # Presence detection and boot behavior
    'presence_threshold_min': 60,            # minutes for ONLINE
    'presence_hysteresis_factor': 2.0,       # offline threshold = threshold * factor
    'boot_announce_suppress_seconds': 30,    # suppress new-node announces right after boot
    'presence_announcements_enabled': False, # disable presence spam; keep new-node only
    'connection_watchdog_minutes': 10,       # reconnect if no packets arrive for this many minutes
    'meshtastic_serial_port': None,          # optional explicit serial port (e.g., COM5 or /dev/ttyUSB0)
    'geocode_enabled': True,                 # allow reverse geocoding for city lookup
    'geocode_max_per_run': 20,               # soft cap per command run
    'geocode_timeout': 3,                    # seconds for HTTP reverse geocode
    
    # Optional alert channel (defaults to main channel if None/0)
    'alert_channel_id': 0,
    
    # Discord rate limits (per category)
    'rate_limits': {
        'announcement_per_min': 12,
        'alert_per_min': 12,
        'command_reply_per_min': 30
    }
}

# Logging Configuration
LOGGING_CONFIG = {
    'level': 'INFO',
    'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    'file': 'logs/bot.log',
    'max_size': 10 * 1024 * 1024,  # 10MB
    'backup_count': 5,
    'structured': False,
}

# Command Aliases (for user convenience)
COMMAND_ALIASES = {
    '$telem': '$telemetry',
    '$list': '$activenodes',
    '$info': '$status',
    '$leaderboards': '$leaderboard',
    '$lb': '$leaderboard',
    '$stats': '$msgstats',
    '$msgstat': '$msgstats',
    '$networkart': '$netart',
    '$art': '$netart',
    '$OTM': '$otm',
}

# Node Display Settings
NODE_DISPLAY = {
    'show_unknown_fields': False,
    'time_format': '%H:%M:%S',
    'date_format': '%Y-%m-%d',
    'max_nodes_per_message': 20,
}

# Health score tuning for leaderboards/profile cards
HEALTH_SCORE = {
    'weights': {
        'battery_slope': 0.25,
        'uptime': 0.25,
        'last_heard': 0.30,
        'packet_rate': 0.20,
    },
    'battery_slope_floor': -5.0,        # % per hour: <= floor -> 0 score
    'battery_slope_ceiling': 0.0,       # % per hour: >= ceiling -> 1 score
    'uptime_hours_target': 24.0,        # hours to reach full uptime score
    'last_heard_good_minutes': 10,      # <= this -> full score
    'last_heard_bad_minutes': 120,      # >= this -> zero score
    'packet_rate_good_per_hour': 10.0,  # >= this -> full score
    'packet_rate_bad_per_hour': 0.0,    # <= this -> zero score
    'packet_rate_window_hours': 24,     # hours for packet rate window
}

# Static map snapshot settings
MAP_SNAPSHOT = {
    'enabled': True,
    'base_url': 'https://staticmap.openstreetmap.de/staticmap.php',
    'zoom': 13,
    'size': '600x400',
    'marker_color': 'red',
    'cache_enabled': True,
    'cache_dir': 'data/map_cache',
    'cache_ttl_hours': 24,
    'timeout_seconds': 5,
}

# Queue persistence across restarts
QUEUE_PERSISTENCE = {
    'enabled': True,
    'path': 'data/queue_state.json',
    'max_age_seconds': 3600,
    'policy': {
        'discord_to_mesh': 'retain',
        'mesh_to_discord': 'drop_old',
    },
}

# Automated DB maintenance and backups
DB_MAINTENANCE = {
    'enabled': True,
    'backup_dir': 'data/backups',
    'backup_interval_hours': 24,
    'backup_retention_days': 7,
    'wal_checkpoint_interval_hours': 6,
    'cleanup_retention_days': 30,
}

# Message Templates
MESSAGE_TEMPLATES = {
    'mesh_message': "\U0001f4e1 **Mesh Message:** {message}",
    'message_sent': "\u2705 Message sent successfully",
    'error_generic': "\u26a0\ufe0f An error occurred: {error}",
    'no_nodes': "\u2139\ufe0f No nodes available",
    'connection_status': "\U0001f4f6 **Connection Status:**\nDiscord: {discord_status}\nMeshtastic: {mesh_status}",
}

# High altitude detection
HIGH_ALTITUDE = {
    'high_altitude_threshold_m': 1500,
    'high_altitude_cooldown_minutes': 180,
}

# Movement detection
MOVEMENT_DETECTION = {
    'distance_threshold_m': 100.0,
    'speed_threshold_mps': 3.0,
    'speed_distance_floor_m': 25.0,
    'jitter_floor_m': 15.0,
    'min_interval_seconds': 15,
    'alert_cooldown_seconds': 300,
}
