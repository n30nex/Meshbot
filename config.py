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
    'file': 'bot.log',
    'max_size': 10 * 1024 * 1024,  # 10MB
    'backup_count': 5,
}

# Command Aliases (for user convenience)
COMMAND_ALIASES = {
    '$telem': '$telemetry',
    '$list': '$activenodes',
    '$info': '$status',
}

# Node Display Settings
NODE_DISPLAY = {
    'show_unknown_fields': False,
    'time_format': '%H:%M:%S',
    'date_format': '%Y-%m-%d',
    'max_nodes_per_message': 20,
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
