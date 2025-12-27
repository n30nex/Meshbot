import discord
import asyncio
import os
import sys
import io
import json
import hashlib
import math
import re
import logging
from logging.handlers import RotatingFileHandler
import threading
from typing import Optional, Dict, Any, List, Tuple
from dataclasses import dataclass
from dotenv import load_dotenv
from pubsub import pub
import meshtastic
import meshtastic.tcp_interface
import meshtastic.serial_interface
import queue
import time
import requests
from urllib.parse import urlencode
from datetime import datetime, timedelta, timezone
from database import MeshtasticDatabase
import functools
import weakref
from config import (
    BOT_CONFIG,
    LOGGING_CONFIG,
    HEALTH_SCORE,
    MAP_SNAPSHOT,
    QUEUE_PERSISTENCE,
    DB_MAINTENANCE,
    ADSB_LOOKUP,
)

class JsonFormatter(logging.Formatter):
    """Simple JSON formatter for structured logs."""

    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "line": record.lineno,
        }
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=True)

def _configure_logging() -> logging.Logger:
    """Configure application logging with rotation."""
    try:
        level_name = str(LOGGING_CONFIG.get("level", "INFO")).upper()
        log_level = getattr(logging, level_name, logging.INFO)
        log_format = LOGGING_CONFIG.get(
            "format", "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        log_file = LOGGING_CONFIG.get("file", "bot.log")
        log_dir = os.path.dirname(log_file)
        if log_dir:
            os.makedirs(log_dir, exist_ok=True)
        max_bytes = int(LOGGING_CONFIG.get("max_size", 10 * 1024 * 1024))
        backup_count = int(LOGGING_CONFIG.get("backup_count", 5))
    except Exception:
        log_level = logging.INFO
        log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        log_file = "bot.log"
        max_bytes = 10 * 1024 * 1024
        backup_count = 5

    handlers = [
        RotatingFileHandler(
            log_file, maxBytes=max_bytes, backupCount=backup_count, encoding="utf-8"
        ),
        logging.StreamHandler(sys.stdout),
    ]

    structured = bool(LOGGING_CONFIG.get("structured", False))
    if structured:
        formatter = JsonFormatter()
    else:
        formatter = logging.Formatter(log_format)
    for handler in handlers:
        handler.setFormatter(formatter)

    logging.basicConfig(level=log_level, handlers=handlers, force=True)
    logging.captureWarnings(True)
    return logging.getLogger(__name__)


# Configure logging
logger = _configure_logging()

# Load environment variables
load_dotenv()


def get_utc_time():
    """Get current time in UTC (timezone-aware)"""
    return datetime.now(timezone.utc)


def format_utc_time(dt=None, format_str="%Y-%m-%d %H:%M:%S UTC"):
    """Format datetime in UTC"""
    if dt is None:
        dt = get_utc_time()
    return dt.strftime(format_str)

def normalize_node_id(val: Any) -> Optional[str]:
    """Normalize numeric or string node IDs to the !xxxxxxxx format."""
    if val is None:
        return None
    if isinstance(val, str):
        if val.startswith("!"):
            return val
        try:
            num = int(val, 0)
        except Exception:
            return val
    else:
        try:
            num = int(val)
        except Exception:
            return None
    if num <= 0:
        return None
    return f"!{num:08x}"


# Cache decorator for expensive operations
def cache_result(ttl_seconds=300):
    """Cache function results for a specified time"""

    def decorator(func):
        cache = {}
        cache_times = {}

        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            # Create cache key
            key = str(args) + str(sorted(kwargs.items()))
            now = time.time()

            # Check if cached result is still valid
            if key in cache and now - cache_times.get(key, 0) < ttl_seconds:
                return cache[key]

            # Execute function and cache result
            try:
                result = await func(*args, **kwargs)
                cache[key] = result
                cache_times[key] = now
                return result
            except Exception as e:
                logger.error(f"Error in cached function {func.__name__}: {e}")
                raise

        # Add cache cleanup method
        def clear_cache():
            cache.clear()
            cache_times.clear()

        wrapper.clear_cache = clear_cache
        return wrapper

    return decorator


@dataclass
class Config:
    """Configuration class for bot settings"""

    discord_token: str
    channel_id: int
    meshtastic_hostname: Optional[str]
    meshtastic_serial_port: Optional[str] = BOT_CONFIG.get("meshtastic_serial_port")
    message_max_length: int = BOT_CONFIG["message_max_length"]
    node_refresh_interval: int = BOT_CONFIG["node_refresh_interval"]
    active_node_threshold: int = BOT_CONFIG[
        "active_node_threshold"
    ]  # minutes (now using config.py value)
    telemetry_update_interval: int = 3600  # 1 hour in seconds
    alert_channel_id: int = BOT_CONFIG.get("alert_channel_id", 0)
    presence_threshold_min: int = BOT_CONFIG.get("presence_threshold_min", 60)
    presence_hysteresis_factor: float = BOT_CONFIG.get(
        "presence_hysteresis_factor", 2.0
    )
    presence_announcements_enabled: bool = BOT_CONFIG.get(
        "presence_announcements_enabled", True
    )
    connection_watchdog_minutes: int = BOT_CONFIG.get(
        "connection_watchdog_minutes", 10
    )


class MeshtasticInterface:
    """Handles Meshtastic radio communication"""

    def __init__(
        self,
        hostname: Optional[str] = None,
        database: Optional[MeshtasticDatabase] = None,
        serial_port: Optional[str] = None,
    ):
        self.hostname = hostname
        self.iface = None  # Changed to match reference implementation
        self.database = database
        self.last_node_refresh = 0
        self.serial_port = serial_port

    async def connect(self) -> bool:
        """Connect to Meshtastic radio"""
        try:
            if self.hostname and len(self.hostname) > 1:
                logger.info(f"Connecting to Meshtastic via TCP: {self.hostname}")
                self.iface = meshtastic.tcp_interface.TCPInterface(self.hostname)
            else:
                if self.serial_port:
                    serial_path = self.serial_port
                    if os.path.isdir(serial_path):
                        try:
                            entries = sorted(
                                e
                                for e in os.listdir(serial_path)
                                if not e.startswith(".")
                            )
                            if entries:
                                serial_path = os.path.join(serial_path, entries[0])
                                logger.info(
                                    f"Connecting to Meshtastic via Serial device {serial_path}"
                                )
                            else:
                                logger.warning(
                                    f"Serial port directory {serial_path} is empty; falling back to default"
                                )
                                serial_path = None
                        except Exception as list_err:
                            logger.warning(
                                f"Failed to read serial port directory {serial_path}: {list_err}"
                            )
                            serial_path = None
                    elif not os.path.exists(serial_path):
                        logger.warning(
                            f"Serial port {serial_path} not found; falling back to default"
                        )
                        serial_path = None
                    if serial_path:
                        self.iface = meshtastic.serial_interface.SerialInterface(
                            devPath=serial_path
                        )
                    else:
                        logger.info("Connecting to Meshtastic via Serial")
                        self.iface = meshtastic.serial_interface.SerialInterface()
                else:
                    logger.info("Connecting to Meshtastic via Serial")
                    self.iface = meshtastic.serial_interface.SerialInterface()

            # Wait for connection
            await asyncio.sleep(2)

            # Check connection status more safely
            try:
                if hasattr(self.iface, "isConnected") and callable(
                    self.iface.isConnected
                ):
                    if self.iface.isConnected():
                        logger.info("Successfully connected to Meshtastic")
                        return True
                    else:
                        logger.error("Failed to connect to Meshtastic")
                        return False
                else:
                    # If isConnected method doesn't exist, assume connection is successful
                    logger.info("Connected to Meshtastic (connection status unknown)")
                    return True

            except Exception as conn_check_error:
                logger.warning(f"Could not check connection status: {conn_check_error}")
                logger.info("Assuming connection is successful")
                return True

        except Exception as e:
            logger.error(f"Error connecting to Meshtastic: {e}")
            return False

    def send_text(self, message: str, destination_id: Optional[str] = None) -> bool:
        """Send text message via Meshtastic"""
        try:
            if not self.iface:
                logger.error("No Meshtastic interface available")
                return False

            if destination_id:
                logger.info(
                    f"Attempting to send message to node {destination_id} (type: {type(destination_id)})"
                )
                # Use the correct Meshtastic API based on the reference implementation
                try:
                    # Try the standard Meshtastic API
                    self.iface.sendText(message, destinationId=destination_id)
                    logger.info(
                        f"Sent message to node {destination_id}: {message[:50]}..."
                    )
                except Exception as e:
                    logger.error(
                        f"Error sending to specific node {destination_id}: {e}"
                    )
                    # Fallback to primary channel
                    logger.warning(f"Falling back to primary channel")
                    self.iface.sendText(message)
                    return True
            else:
                self.iface.sendText(message)
                logger.info(f"Sent message to primary channel: {message[:50]}...")
            return True
        except Exception as e:
            logger.error(f"Error sending message: {e}")
            return False

    def process_nodes(self) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """Process and store nodes in database"""
        if not self.iface or not self.database:
            return [], []

        try:
            if not hasattr(self.iface, "nodes"):
                logger.debug("Interface has no nodes attribute")
                return [], []

            nodes = self.iface.nodes
            if not nodes:
                logger.debug("No nodes available to process")
                return [], []

            processed_nodes = []
            new_nodes = []

            logger.info(f"Processing {len(nodes)} nodes from Meshtastic interface")

            for node_id, node_data in nodes.items():
                try:
                    # Extract node information with better error handling
                    # Extract last_heard from node_data.  Use UTC and avoid
                    # defaulting to current time if lastHeard is missing, so that
                    # nodes are not incorrectly marked active.  The timestamp
                    # format should be 'YYYY-MM-DD HH:MM:SS' to align with
                    # SQLite datetime functions.  If lastHeard is not available
                    # the field is left as None and will not override the
                    # existing value in the database.
                    last_heard_val = node_data.get("lastHeard")
                    if last_heard_val is not None:
                        try:
                            last_heard_str = datetime.utcfromtimestamp(
                                last_heard_val
                            ).strftime("%Y-%m-%d %H:%M:%S")
                        except Exception:
                            last_heard_str = None
                    else:
                        last_heard_str = None

                    next_hop_raw = (
                        node_data.get("nextHop")
                        or node_data.get("next_hop")
                        or node_data.get("nextHopId")
                    )
                    next_hop_id = normalize_node_id(next_hop_raw)

                    node_info = {
                        "node_id": str(node_id),
                        "node_num": node_data.get("num"),
                        "long_name": str(
                            node_data.get("user", {}).get("longName", "Unknown")
                        ),
                        "short_name": str(
                            node_data.get("user", {}).get("shortName", "")
                        ),
                        "macaddr": node_data.get("macaddr"),
                        "hw_model": node_data.get("hwModel"),
                        "firmware_version": node_data.get("firmwareVersion"),
                        "last_heard": last_heard_str,
                        "hops_away": node_data.get("hopsAway", 0),
                        "is_router": node_data.get("isRouter", False),
                        "is_client": node_data.get("isClient", True),
                        "next_hop_node_id": next_hop_id,
                    }

                    # Store in database
                    try:
                        success, is_new = self.database.add_or_update_node(node_info)
                        if success:
                            processed_nodes.append(node_info)
                            if is_new:
                                new_nodes.append(node_info)
                                logger.info(
                                    f"New node added: {node_info['long_name']} ({node_info['node_id']})"
                                )
                    except Exception as db_error:
                        logger.error(f"Database error for node {node_id}: {db_error}")
                        continue

                    # Store telemetry if available - check for actual values
                    telemetry_data = {}
                    # Radio metrics
                    if node_data.get("snr") is not None:
                        telemetry_data["snr"] = node_data.get("snr")
                    if node_data.get("rssi") is not None:
                        telemetry_data["rssi"] = node_data.get("rssi")
                    if node_data.get("frequency") is not None:
                        telemetry_data["frequency"] = node_data.get("frequency")
                    # Position and movement metrics
                    if node_data.get("latitude") is not None:
                        telemetry_data["latitude"] = node_data.get("latitude")
                    if node_data.get("longitude") is not None:
                        telemetry_data["longitude"] = node_data.get("longitude")
                    if node_data.get("altitude") is not None:
                        telemetry_data["altitude"] = node_data.get("altitude")
                    if node_data.get("speed") is not None:
                        telemetry_data["speed"] = node_data.get("speed")
                    if node_data.get("heading") is not None:
                        telemetry_data["heading"] = node_data.get("heading")
                    if node_data.get("accuracy") is not None:
                        telemetry_data["accuracy"] = node_data.get("accuracy")
                    # Additional metrics if present on node_data
                    # Some devices may expose battery, voltage or temperature values at the node level
                    for field, key in [
                        ("battery_level", "batteryLevel"),
                        ("voltage", "voltage"),
                        ("temperature", "temperature"),
                        ("humidity", "relativeHumidity"),
                        ("pressure", "barometricPressure"),
                        ("gas_resistance", "gasResistance"),
                    ]:
                        value = node_data.get(key)
                        if value is not None:
                            telemetry_data[field] = value

                    # Only store telemetry if we have actual data
                    if telemetry_data:
                        try:
                            self.database.add_telemetry(
                                node_info["node_id"], telemetry_data
                            )
                            logger.debug(
                                f"Stored telemetry for {node_info['long_name']}: {telemetry_data}"
                            )
                        except Exception as telemetry_error:
                            logger.error(
                                f"Error storing telemetry for node {node_id}: {telemetry_error}"
                            )

                    # Store position if available
                    if (
                        node_data.get("latitude") is not None
                        and node_data.get("longitude") is not None
                    ):
                        try:
                            position_data = {
                                "latitude": node_data.get("latitude"),
                                "longitude": node_data.get("longitude"),
                                "altitude": node_data.get("altitude"),
                                "speed": node_data.get("speed"),
                                "heading": node_data.get("heading"),
                                "accuracy": node_data.get("accuracy"),
                                "source": "meshtastic",
                            }
                            self.database.add_position(
                                node_info["node_id"], position_data
                            )
                            logger.debug(
                                f"Stored position for {node_info['long_name']}"
                            )
                        except Exception as position_error:
                            logger.error(
                                f"Error storing position for node {node_id}: {position_error}"
                            )

                except Exception as e:
                    logger.error(f"Error processing node {node_id}: {e}")
                    continue

            self.last_node_refresh = time.time()
            logger.info(f"Processed {len(processed_nodes)} nodes, {len(new_nodes)} new")
            return processed_nodes, new_nodes

        except Exception as e:
            logger.error(f"Error processing nodes: {e}")
            return [], []

    def get_nodes_from_db(self) -> List[Dict[str, Any]]:
        """Get nodes from database"""
        if not self.database:
            return []
        try:
            nodes = self.database.get_all_nodes()
            logger.debug(f"Retrieved {len(nodes)} nodes from database")
            return nodes
        except Exception as e:
            logger.error(f"Error getting nodes from database: {e}")
            return []


class CommandHandler:
    """Handles Discord bot commands with caching and performance optimizations"""

    def __init__(
        self,
        meshtastic: MeshtasticInterface,
        discord_to_mesh: queue.Queue,
        database: MeshtasticDatabase,
        event_loop=None,
    ):
        self.meshtastic = meshtastic
        self.discord_to_mesh = discord_to_mesh
        self.database = database
        self.event_loop = event_loop
        self.commands = {
            "$help": self.cmd_help,
            "$txt": self.cmd_send_primary,  # Changed from $sendprimary
            "$send": self.cmd_send_node,  # Changed to use fuzzy name matching
            "$activenodes": self.cmd_active_nodes,
            "$nodes": self.cmd_all_nodes,
            "$node": self.cmd_node,
            "$telem": self.cmd_telemetry,
            "$history": self.cmd_history,
            "$dbhealth": self.cmd_dbhealth,
            "$status": self.cmd_status,
            "$topo": self.cmd_topology_tree,
            "$trace": self.cmd_trace_route,
            "$leaderboard": self.cmd_leaderboard,
            "$msgstats": self.cmd_message_statistics,
            "$netart": self.cmd_network_art,
            "$live": self.cmd_live_monitor,
            "$where": self.cmd_where,
            "$nearest": self.cmd_nearest,
            "$otm": self.cmd_otm,
            "$loglevel": self.cmd_loglevel,
            "$uptime": self.cmd_uptime,
        }

        # Apply command aliases defined in config.  The config.COMMAND_ALIASES
        # mapping defines alternate command names that should invoke the same
        # handler as an existing canonical command.  To make the mapping
        # robust, we register aliases in both directions: if the canonical
        # command exists in the `commands` dict, map the alias to it; if
        # instead the alias exists but the canonical does not, map the
        # canonical to the alias handler.  This ensures that either form
        # (`$telem` or `$telemetry`, `$nodes` or `$activenodes`) works
        # transparently.
        try:
            from config import COMMAND_ALIASES

            for alias, canonical in COMMAND_ALIASES.items():
                if canonical in self.commands:
                    # canonical already registered: assign alias
                    self.commands[alias] = self.commands[canonical]
                elif alias in self.commands:
                    # alias registered but canonical missing: assign canonical
                    self.commands[canonical] = self.commands[alias]
        except Exception as alias_error:
            logger.debug(f"Failed to apply command aliases: {alias_error}")

        self.command_help = {
            "$help": {
                "summary": "Show help or command details",
                "usage": "$help [command]",
                "details": "Use without args to list commands, or pass a command for details.",
                "examples": ["$help", "$help $node"],
                "category": "Core",
            },
            "$txt": {
                "summary": "Send a message to the mesh",
                "usage": "$txt <message>",
                "details": f"Max length {BOT_CONFIG.get('message_max_length', 225)} chars.",
                "examples": ["$txt Hello mesh!"],
                "category": "Core",
            },
            "$send": {
                "summary": "Send a message to a specific node",
                "usage": "$send <longname> <message>",
                "details": "Fuzzy-matches node names.",
                "examples": ["$send Rover-1 Telemetry check"],
                "category": "Core",
            },
            "$status": {
                "summary": "Bridge status overview",
                "usage": "$status",
                "details": "Shows Discord + Meshtastic connectivity and node counts.",
                "examples": ["$status"],
                "category": "Core",
            },
            "$uptime": {
                "summary": "Uptime + queue health",
                "usage": "$uptime",
                "details": "Shows uptime and queue backlog.",
                "examples": ["$uptime"],
                "category": "Core",
            },
            "$nodes": {
                "summary": "List all known nodes",
                "usage": "$nodes",
                "details": "Paged node list with latest telemetry.",
                "examples": ["$nodes"],
                "category": "Nodes",
            },
            "$activenodes": {
                "summary": "List recently active nodes",
                "usage": "$activenodes",
                "details": f"Active threshold: {BOT_CONFIG.get('active_node_threshold', 60)} minutes.",
                "examples": ["$activenodes"],
                "category": "Nodes",
            },
            "$node": {
                "summary": "Node profile card",
                "usage": "$node <name>",
                "details": "Shows latest telemetry, last message, and trend snippets.",
                "examples": ["$node rover"],
                "category": "Nodes",
            },
            "$where": {
                "summary": "Latest location for a node",
                "usage": "$where <name>",
                "details": "Uses the most recent position data.",
                "examples": ["$where base"],
                "category": "Nodes",
            },
            "$nearest": {
                "summary": "Nearest nodes to a node",
                "usage": "$nearest <name>",
                "details": "Ranks nodes by geographic distance.",
                "examples": ["$nearest rover"],
                "category": "Nodes",
            },
            "$telem": {
                "summary": "Telemetry summary",
                "usage": "$telem",
                "details": "Latest telemetry per node with summary stats.",
                "examples": ["$telem"],
                "category": "Telemetry",
            },
            "$history": {
                "summary": "Metric history for a node",
                "usage": "$history <node> <metric> [--spark]",
                "details": "Use --spark for a compact sparkline.",
                "examples": ["$history rover battery_level --spark"],
                "category": "Telemetry",
            },
            "$dbhealth": {
                "summary": "Database health snapshot",
                "usage": "$dbhealth",
                "details": "Counts and last timestamps per table.",
                "examples": ["$dbhealth"],
                "category": "Telemetry",
            },
            "$topo": {
                "summary": "Network topology tree",
                "usage": "$topo",
                "details": "Builds a hop-aware ASCII topology map.",
                "examples": ["$topo"],
                "category": "Network",
            },
            "$trace": {
                "summary": "Trace route to a node",
                "usage": "$trace <name>",
                "details": "Uses recent message routing data to infer hops.",
                "examples": ["$trace rover"],
                "category": "Network",
            },
            "$leaderboard": {
                "summary": "Health score leaderboards",
                "usage": "$leaderboard",
                "details": "Ranks nodes by health score and activity.",
                "examples": ["$leaderboard"],
                "category": "Analytics",
            },
            "$msgstats": {
                "summary": "Message statistics",
                "usage": "$msgstats",
                "details": "24h message stats and activity pattern.",
                "examples": ["$msgstats"],
                "category": "Analytics",
            },
            "$live": {
                "summary": "Live packet monitor",
                "usage": "$live",
                "details": "Toggles live mesh packet updates for 1 minute.",
                "examples": ["$live"],
                "category": "Live",
            },
            "$netart": {
                "summary": "ASCII network art",
                "usage": "$netart",
                "details": "Fun ASCII snapshot of the mesh.",
                "examples": ["$netart"],
                "category": "Live",
            },
            "$otm": {
                "summary": "Movement trail summary",
                "usage": "$otm <name>",
                "details": "Summarizes recent movement and trail points.",
                "examples": ["$otm rover"],
                "category": "Live",
            },
            "$loglevel": {
                "summary": "Change log verbosity at runtime",
                "usage": "$loglevel <level>",
                "details": "Levels: DEBUG, INFO, WARNING, ERROR, CRITICAL.",
                "examples": ["$loglevel INFO"],
                "category": "Admin",
            },
        }

        self._help_categories = {
            "Core": ["$help", "$txt", "$send", "$status", "$uptime"],
            "Nodes": ["$nodes", "$activenodes", "$node", "$where", "$nearest"],
            "Telemetry": ["$telem", "$history", "$dbhealth"],
            "Network": ["$topo", "$trace"],
            "Analytics": ["$leaderboard", "$msgstats"],
            "Live": ["$live", "$netart", "$otm"],
            "Admin": ["$loglevel"],
        }

        self._help_lookup = {}
        for cmd in self.command_help.keys():
            self._help_lookup[cmd] = cmd
            self._help_lookup[cmd.lower()] = cmd
        try:
            from config import COMMAND_ALIASES

            for alias, canonical in COMMAND_ALIASES.items():
                if canonical in self.command_help:
                    self._help_lookup[alias] = canonical
                    self._help_lookup[alias.lower()] = canonical
                elif alias in self.command_help:
                    self._help_lookup[canonical] = alias
                    self._help_lookup[canonical.lower()] = alias
        except Exception as alias_error:
            logger.debug(f"Failed to build help aliases: {alias_error}")

        # Cache for frequently accessed data
        self._node_cache = {}
        self._cache_timestamps = {}
        self._cache_ttl = 60  # 1 minute cache TTL
        self._cache_lock = asyncio.Lock()  # Thread safety for cache operations

        # Rate limiting
        self._command_cooldowns = {}
        self._cooldown_duration = 2  # 2 seconds between commands per user

        # Live monitor state
        self._live_monitors = {}  # user_id -> {'active': bool, 'task': asyncio.Task}
        self._packet_buffer = []  # Store recent packets for live display
        self._max_packet_buffer = 50  # Keep last 50 packets
        self._packet_buffer_lock = threading.Lock()  # Thread safety for packet buffer
        self.mesh_to_discord_queue: Optional[queue.Queue] = None
        self._start_time = time.time()
        self._geo_cache: Dict[str, str] = {}

        # Circuit breaker for database operations
        self._circuit_breaker = {
            "failures": 0,
            "last_failure_time": 0,
            "state": "closed",  # closed, open, half_open
            "failure_threshold": 5,
            "timeout": 60,  # seconds
            "half_open_time": 30,  # seconds to wait before trying again
        }

    async def handle_command(self, message: discord.Message) -> bool:
        """Route command to appropriate handler with rate limiting"""
        content = message.content.strip()

        # No alias normalization here: alias expansion is handled in
        # `__init__` by registering both alias and canonical names in
        # `self.commands`.  This ensures that users can invoke either
        # form without requiring additional string manipulation at
        # runtime.

        # Rate limiting check
        user_id = message.author.id
        now = time.time()

        if user_id in self._command_cooldowns:
            if now - self._command_cooldowns[user_id] < self._cooldown_duration:
                await self._safe_send(
                    message.channel,
                    "⏰ Please wait a moment before using another command.",
                )
                return True

        # Update cooldown (only after successful command execution)
        # We'll move this to after the command is executed

        for cmd, handler in sorted(
            self.commands.items(), key=lambda item: len(item[0]), reverse=True
        ):
            if content.startswith(cmd):
                try:
                    await handler(message)
                    # Update cooldown only after successful command execution
                    self._command_cooldowns[user_id] = now
                    return True
                except Exception as e:
                    logger.error(f"Error handling command {cmd}: {e}")
                    await self._safe_send(
                        message.channel, f"❌ Error executing command: {e}"
                    )
                    return True
        # If the content appears to be a command (starts with the command
        # prefix) but did not match any known command, inform the user.
        try:
            from config import BOT_CONFIG

            prefix = BOT_CONFIG.get("command_prefix", "$")
        except Exception:
            prefix = "$"
        if content.startswith(prefix):
            # Extract the attempted command from the original message
            attempted = message.content.strip().split()[0]
            await self._safe_send(
                message.channel,
                f"❓ Unknown command '{attempted}'. Try {prefix}help for a list of commands.",
            )
            # Update cooldown to prevent repeated unknown commands spamming
            self._command_cooldowns[user_id] = now
            return True

        return False

    async def _get_cached_data(self, key: str, fetch_func, *args, timeout=10, **kwargs):
        """Get data from cache or fetch if not available (thread-safe, async)"""
        now = time.time()

        async with self._cache_lock:
            # Check cache first
            if (
                key in self._node_cache
                and key in self._cache_timestamps
                and now - self._cache_timestamps[key] < self._cache_ttl
            ):
                return self._node_cache[key]

        # Fetch fresh data asynchronously with timeout
        data = await self._run_db_query(fetch_func, *args, timeout=timeout, **kwargs)

        if data is not None:
            # Update cache with lock
            async with self._cache_lock:
                self._node_cache[key] = data
                self._cache_timestamps[key] = now
            return data
        else:
            # Return cached data if available, even if stale
            logger.warning(f"Database query failed for {key}, returning stale cache")
            async with self._cache_lock:
                return self._node_cache.get(key, [])

    async def clear_cache(self):
        """Clear all cached data (thread-safe)"""
        async with self._cache_lock:
            self._node_cache.clear()
            self._cache_timestamps.clear()
        logger.info("Command handler cache cleared")

    def _haversine_km(self, lat1, lon1, lat2, lon2) -> float:
        """Compute great-circle distance in kilometers."""
        try:
            from math import radians, sin, cos, asin, sqrt

            R = 6371.0
            dlat = radians(lat2 - lat1)
            dlon = radians(lon2 - lon1)
            a = (
                sin(dlat / 2) ** 2
                + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2) ** 2
            )
            c = 2 * asin(sqrt(a))
            return R * c
        except Exception:
            return 0.0

    async def cmd_where(self, message: discord.Message):
        """Show latest known location for a node: $where <name>"""
        parts = message.content.strip().split(maxsplit=1)
        if len(parts) < 2:
            await self._safe_send(message.channel, "❌ Use: `$where <node_name>`")
            return
        name = parts[1]
        node = await self._run_db_query(
            self.database.find_node_by_name, name, timeout=5
        )
        if not node:
            await self._safe_send(
                message.channel, f"❌ No node found matching '{name}'."
            )
            return
        pos = await self._run_db_query(
            self.database.get_last_position, node["node_id"], timeout=5
        )
        if not pos:
            await self._safe_send(
                message.channel,
                f"📍 No position data for **{node.get('long_name') or name}**.",
            )
            return
        lat = pos.get("latitude")
        lon = pos.get("longitude")
        alt = pos.get("altitude")
        ts = pos.get("timestamp")
        desc = f"**{node.get('long_name') or node['node_id']}**"
        embed = discord.Embed(
            title="📍 Node Location",
            description=desc,
            color=0x00FF00,
            timestamp=get_utc_time(),
        )
        if lat is not None and lon is not None:
            try:
                embed.add_field(
                    name="Lat, Lon",
                    value=f"{float(lat):.5f}, {float(lon):.5f}",
                    inline=True,
                )
            except Exception:
                embed.add_field(name="Lat, Lon", value=f"{lat}, {lon}", inline=True)
        if alt is not None:
            try:
                embed.add_field(
                    name="Altitude (m)", value=f"{float(alt):.0f}", inline=True
                )
            except Exception:
                pass
        if ts:
            embed.add_field(name="Last Update", value=str(ts), inline=True)
        map_payload = None
        if lat is not None and lon is not None:
            try:
                lat_f = float(lat)
                lon_f = float(lon)
                map_payload = await self._get_map_snapshot(lat_f, lon_f)
                if map_payload and map_payload.get("url"):
                    embed.set_image(url=map_payload["url"])
            except Exception:
                map_payload = None
        if map_payload and map_payload.get("path"):
            filename = os.path.basename(map_payload["path"])
            try:
                file_obj = discord.File(map_payload["path"], filename=filename)
                embed.set_image(url=f"attachment://{filename}")
                await message.channel.send(embed=embed, file=file_obj)
                return
            except Exception:
                pass
        await message.channel.send(embed=embed)

    async def cmd_nearest(self, message: discord.Message):
        """Show nearest nodes to target: $nearest <name>"""
        parts = message.content.strip().split(maxsplit=1)
        if len(parts) < 2:
            await self._safe_send(message.channel, "❌ Use: `$nearest <node_name>`")
            return
        name = parts[1]
        target = await self._run_db_query(
            self.database.find_node_by_name, name, timeout=5
        )
        if not target:
            await self._safe_send(
                message.channel, f"❌ No node found matching '{name}'."
            )
            return
        tpos = await self._run_db_query(
            self.database.get_last_position, target["node_id"], timeout=5
        )
        if not tpos or tpos.get("latitude") is None or tpos.get("longitude") is None:
            await self._safe_send(
                message.channel,
                f"📍 No position data for **{target.get('long_name') or name}**.",
            )
            return
        nodes = await self._run_db_query(
            self.database.get_all_nodes, None, timeout=10
        )
        results = []
        for n in nodes:
            if n.get("node_id") == target["node_id"]:
                continue
            lat = n.get("latitude")
            lon = n.get("longitude")
            if lat is None or lon is None:
                continue
            try:
                d = self._haversine_km(
                    float(tpos["latitude"]),
                    float(tpos["longitude"]),
                    float(lat),
                    float(lon),
                )
            except Exception:
                continue
            results.append((d, n))
        results.sort(key=lambda x: x[0])
        top = results[:5]
        if not top:
            await self._safe_send(
                message.channel, "No nearby nodes with known positions."
            )
            return
        lines = []
        for d, n in top:
            nm = n.get("long_name") or n.get("short_name") or n.get("node_id")
            lines.append(f"- {nm}: {d:.1f} km away")
        await self._send_long_message(
            message.channel, "📍 Nearest nodes:\n" + "\n".join(lines)
        )

    async def cmd_otm(self, message: discord.Message):
        """Movement trail summary: $otm <name>"""
        parts = message.content.strip().split(maxsplit=1)
        if len(parts) < 2:
            await self._safe_send(message.channel, "Use: `$otm <node_name>`")
            return
        name = parts[1].strip()
        node = await self._run_db_query(
            self.database.find_node_by_name, name, timeout=5
        )
        if not node:
            await self._safe_send(
                message.channel, f"❌ No node found matching '{name}'."
            )
            return
        node_id = node.get("node_id")
        history = await self._run_db_query(
            self.database.get_position_history, node_id, 24, 200, timeout=10
        )
        if not history or len(history) < 2:
            await self._safe_send(
                message.channel,
                f"📍 Not enough position history for **{node.get('long_name') or node_id}**.",
            )
            return

        points = []
        for entry in history:
            lat = entry.get("latitude")
            lon = entry.get("longitude")
            ts = entry.get("timestamp")
            dt = self._parse_timestamp(ts) if isinstance(ts, str) else None
            if lat is None or lon is None or dt is None:
                continue
            try:
                points.append((dt, float(lat), float(lon), entry))
            except Exception:
                continue
        if len(points) < 2:
            await self._safe_send(
                message.channel,
                f"📍 Not enough valid trail points for **{node.get('long_name') or node_id}**.",
            )
            return

        points.sort(key=lambda item: item[0])
        total_distance = 0.0
        for idx in range(1, len(points)):
            total_distance += self._haversine_m(
                points[idx - 1][1],
                points[idx - 1][2],
                points[idx][1],
                points[idx][2],
            )

        straight_line = self._haversine_m(
            points[0][1], points[0][2], points[-1][1], points[-1][2]
        )
        duration_seconds = (points[-1][0] - points[0][0]).total_seconds()
        avg_speed = (
            total_distance / duration_seconds if duration_seconds > 0 else 0.0
        )

        embed = discord.Embed(
            title="🧭 On The Move",
            description=f"**{node.get('long_name') or node_id}** trail summary",
            color=0x2ECC71,
            timestamp=get_utc_time(),
        )
        embed.add_field(
            name="Trail Summary",
            value=(
                f"Samples: {len(points)}\n"
                f"Window: {points[0][0].strftime('%Y-%m-%d %H:%M')} → {points[-1][0].strftime('%Y-%m-%d %H:%M')}\n"
                f"Total distance: {total_distance/1000:.2f} km\n"
                f"Straight-line: {straight_line/1000:.2f} km\n"
                f"Avg speed: {avg_speed*3.6:.2f} km/h"
            ),
            inline=False,
        )

        tail_lines = []
        for dt, lat, lon, entry in points[-5:]:
            alt = entry.get("altitude")
            alt_text = ""
            if alt is not None:
                try:
                    alt_text = f", {float(alt):.0f}m"
                except Exception:
                    alt_text = ""
            tail_lines.append(
                f"{dt.strftime('%H:%M')} - {lat:.5f}, {lon:.5f}{alt_text}"
            )
        embed.add_field(
            name="Recent Trail Points",
            value="\n".join(tail_lines),
            inline=False,
        )

        await message.channel.send(embed=embed)

    def _check_circuit_breaker(self):
        """Check if circuit breaker allows database operations"""
        now = time.time()
        breaker = self._circuit_breaker

        if breaker["state"] == "open":
            # Check if enough time has passed to try half-open
            if now - breaker["last_failure_time"] > breaker["half_open_time"]:
                breaker["state"] = "half_open"
                logger.info("Circuit breaker entering half-open state")
                return True
            return False

        return True

    def _record_db_success(self):
        """Record successful database operation"""
        breaker = self._circuit_breaker
        if breaker["state"] == "half_open":
            breaker["state"] = "closed"
            breaker["failures"] = 0
            logger.info("Circuit breaker closed - database healthy")
        elif breaker["state"] == "closed":
            # Reset failure count on success
            breaker["failures"] = max(0, breaker["failures"] - 1)

    def _record_db_failure(self):
        """Record failed database operation"""
        breaker = self._circuit_breaker
        breaker["failures"] += 1
        breaker["last_failure_time"] = time.time()

        if breaker["failures"] >= breaker["failure_threshold"]:
            if breaker["state"] != "open":
                breaker["state"] = "open"
                logger.error(
                    f"⚠️ Circuit breaker OPEN - database experiencing issues ({breaker['failures']} failures)"
                )

    async def _run_db_query(self, func, *args, timeout=10, retry_count=3, **kwargs):
        """
        Run database query asynchronously with timeout, retry, and circuit breaker.

        Args:
            func: Database function to call
            *args: Arguments to pass to function
            timeout: Maximum time to wait for query (seconds)
            retry_count: Number of retries on failure
            **kwargs: Keyword arguments to pass to function

        Returns:
            Query result or None on failure
        """
        # Check circuit breaker
        if not self._check_circuit_breaker():
            logger.warning("Circuit breaker is OPEN - returning cached data")
            return None

        last_exception = None

        for attempt in range(retry_count):
            try:
                # Run database operation in thread pool with timeout
                start_time = time.monotonic()
                result = await asyncio.wait_for(
                    asyncio.to_thread(func, *args, **kwargs), timeout=timeout
                )
                elapsed = time.monotonic() - start_time

                # Record success
                self._record_db_success()

                # Log slow queries
                if elapsed >= 5:
                    logger.warning(
                        "Slow query completed: %s took %.2fs",
                        func.__name__,
                        elapsed,
                    )

                return result

            except asyncio.TimeoutError:
                last_exception = asyncio.TimeoutError(
                    f"Query {func.__name__} timed out after {timeout}s"
                )
                logger.warning(
                    f"Database query timeout (attempt {attempt + 1}/{retry_count}): {func.__name__}"
                )

                # Record failure
                self._record_db_failure()

                # Exponential backoff
                if attempt < retry_count - 1:
                    wait_time = 0.5 * (2**attempt)  # 0.5s, 1s, 2s
                    await asyncio.sleep(wait_time)

            except Exception as e:
                last_exception = e
                logger.error(
                    f"Database error (attempt {attempt + 1}/{retry_count}): {func.__name__} - {e}"
                )

                # Record failure
                self._record_db_failure()

                # Exponential backoff
                if attempt < retry_count - 1:
                    wait_time = 0.5 * (2**attempt)
                    await asyncio.sleep(wait_time)

        # All retries failed
        logger.error(f"All retries exhausted for {func.__name__}: {last_exception}")
        return None

    def _enqueue_discord_message(self, payload: str) -> bool:
        """Enqueue a Discord-originated payload to mesh with drop-oldest policy."""
        try:
            self.discord_to_mesh.put_nowait(payload)
            return True
        except queue.Full:
            try:
                dropped = self.discord_to_mesh.get_nowait()
                preview = (
                    f"{dropped[:50]}..."
                    if isinstance(dropped, str) and len(dropped) > 50
                    else str(dropped)
                )
                logger.warning(
                    f"Discord→mesh queue full, dropped oldest entry: {preview}"
                )
                self.discord_to_mesh.put_nowait(payload)
                return True
            except queue.Empty:
                logger.warning("Discord→mesh queue full and no entries to drop")
                return False
        except Exception as e:
            logger.error(f"Error enqueuing Discord→mesh payload: {e}")
            return False

    def add_packet_to_buffer(self, packet_info: dict):
        """Add packet information to the live monitor buffer (thread-safe)"""
        try:
            # Add timestamp
            packet_info["timestamp"] = datetime.utcnow().isoformat()

            # Thread-safe buffer update using regular lock
            with self._packet_buffer_lock:
                # Add to buffer
                self._packet_buffer.append(packet_info)

                # Keep only the last N packets
                if len(self._packet_buffer) > self._max_packet_buffer:
                    self._packet_buffer.pop(0)

        except Exception as e:
            logger.error(f"Error adding packet to buffer: {e}")

    def calculate_distance(
        self, lat1: float, lon1: float, lat2: float, lon2: float
    ) -> float:
        """Calculate distance between two coordinates in meters using Haversine formula"""
        try:
            import math

            # Convert to radians
            lat1_rad = math.radians(lat1)
            lon1_rad = math.radians(lon1)
            lat2_rad = math.radians(lat2)
            lon2_rad = math.radians(lon2)

            # Haversine formula
            dlat = lat2_rad - lat1_rad
            dlon = lon2_rad - lon1_rad
            a = (
                math.sin(dlat / 2) ** 2
                + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon / 2) ** 2
            )
            c = 2 * math.asin(math.sqrt(a))

            # Earth's radius in meters
            earth_radius = 6371000
            distance = earth_radius * c

            return distance
        except Exception as e:
            logger.error(f"Error calculating distance: {e}")
            return 0.0

    def _render_unicode_graph(
        self, values: list, width: int = 20, height: int = 8
    ) -> str:
        """Render a sparkline/bar graph using Unicode block characters"""
        if not values or len(values) == 0:
            return "No data"

        try:
            # Filter out None values
            clean_values = [v for v in values if v is not None]
            if not clean_values:
                return "No data"

            # Normalize values to 0-height range
            min_val = min(clean_values)
            max_val = max(clean_values)

            if max_val == min_val:
                # All values are the same
                return "▄" * min(len(values), width)

            # Block characters from empty to full
            blocks = [" ", "▁", "▂", "▃", "▄", "▅", "▆", "▇", "█"]

            # Sample values if too many
            if len(clean_values) > width:
                step = len(clean_values) / width
                sampled = [clean_values[int(i * step)] for i in range(width)]
            else:
                sampled = clean_values

            # Convert to block characters
            graph = ""
            for val in sampled:
                if val is None:
                    graph += " "
                else:
                    normalized = (val - min_val) / (max_val - min_val)
                    block_index = int(normalized * (len(blocks) - 1))
                    graph += blocks[block_index]

            return graph
        except Exception as e:
            logger.error(f"Error rendering graph: {e}")
            return "Error rendering graph"

    def _render_battery_bar(self, battery_level: float) -> str:
        """Render battery level as colored emoji bars"""
        if battery_level is None:
            return "🔋 ?"

        try:
            level = int(battery_level)
            if level >= 90:
                return "🔋 " + "🟩" * 5
            elif level >= 70:
                return "🔋 " + "🟩" * 4 + "⬜"
            elif level >= 50:
                return "🔋 " + "🟩" * 3 + "⬜" * 2
            elif level >= 30:
                return "🔋 " + "🟩" * 2 + "🟨" + "⬜" * 2
            elif level >= 15:
                return "🔋 " + "🟩" + "🟨" * 2 + "⬜" * 2
            else:
                return "🔋 " + "🟥" * 2 + "⬜" * 3
        except Exception as e:
            logger.error(f"Error rendering battery bar: {e}")
            return "🔋 ?"

    def _render_signal_bar(self, snr: float) -> str:
        """Render signal quality as colored bars"""
        if snr is None:
            return "📡 ?"

        try:
            if snr >= 10:
                return "📡 " + "🟩" * 5 + " Excellent"
            elif snr >= 5:
                return "📡 " + "🟩" * 4 + "⬜" + " Good"
            elif snr >= 0:
                return "📡 " + "🟩" * 3 + "⬜" * 2 + " Fair"
            elif snr >= -5:
                return "📡 " + "🟨" * 2 + "⬜" * 3 + " Weak"
            else:
                return "📡 " + "🟥" + "⬜" * 4 + " Poor"
        except Exception as e:
            logger.error(f"Error rendering signal bar: {e}")
            return "📡 ?"

    async def _create_paginated_embed(
        self, items: list, page: int, items_per_page: int, title: str, format_func
    ) -> discord.Embed:
        """Create a paginated embed from a list of items"""
        total_pages = (len(items) + items_per_page - 1) // items_per_page
        page = max(1, min(page, total_pages))  # Clamp page number

        start_idx = (page - 1) * items_per_page
        end_idx = min(start_idx + items_per_page, len(items))
        page_items = items[start_idx:end_idx]

        embed = discord.Embed(
            title=f"{title} (Page {page}/{total_pages})",
            description=f"Showing items {start_idx + 1}-{end_idx} of {len(items)}",
            color=0x00BFFF,
            timestamp=get_utc_time(),
        )

        # Format items using the provided function
        content = format_func(page_items)
        embed.add_field(
            name="Items", value=content[:1024], inline=False
        )  # Discord field limit

        embed.set_footer(
            text=f"Page {page}/{total_pages} | Use reactions to navigate: ◀️ Previous | ⏹️ Stop | ▶️ Next"
        )

        return embed

    async def _add_reaction_navigation(
        self, message: discord.Message, total_pages: int
    ):
        """Add reaction buttons for pagination"""
        if total_pages > 1:
            await message.add_reaction("◀️")
            await message.add_reaction("⏹️")
            await message.add_reaction("▶️")

    async def cmd_help(self, message: discord.Message):
        """Show help information"""
        parts = message.content.strip().split(maxsplit=1)
        if len(parts) > 1:
            query = parts[1].strip()
            if not query.startswith("$"):
                query = f"${query}"
            canonical = self._help_lookup.get(query) or self._help_lookup.get(
                query.lower()
            )
            if not canonical or canonical not in self.command_help:
                await self._safe_send(
                    message.channel,
                    f"❌ Unknown command `{query}`. Try `$help` to list commands.",
                )
                return
            meta = self.command_help[canonical]
            embed = discord.Embed(
                title=f"🤖 Help: {canonical}",
                description=meta.get("summary", "Command help"),
                color=0x00FF00,
                timestamp=get_utc_time(),
            )
            embed.add_field(name="Usage", value=f"`{meta.get('usage', canonical)}`")
            details = meta.get("details")
            if details:
                embed.add_field(name="Details", value=details, inline=False)
            examples = meta.get("examples", [])
            if examples:
                example_text = "\n".join(f"`{ex}`" for ex in examples[:5])
                embed.add_field(name="Examples", value=example_text, inline=False)
            embed.set_footer(text="🌍 UTC Time")
            await message.channel.send(embed=embed)
            return

        embed = discord.Embed(
            title="🤖 Meshtastic Discord Bridge Commands",
            description="Complete command reference for the mesh network bridge",
            color=0x00FF00,
            timestamp=get_utc_time(),
        )
        embed.set_thumbnail(
            url="https://raw.githubusercontent.com/meshtastic/firmware/master/docs/assets/logo/meshtastic-logo.png"
        )
        embed.set_footer(text="🌍 UTC Time | Use $help <command> for detailed info")

        for category, commands in self._help_categories.items():
            lines = []
            for cmd in commands:
                meta = self.command_help.get(cmd)
                if not meta:
                    continue
                summary = meta.get("summary", "")
                lines.append(f"`{cmd}` - {summary}")
            if lines:
                embed.add_field(
                    name=f"{category} Commands",
                    value="\n".join(lines)[:1024],
                    inline=False,
                )

        await message.channel.send(embed=embed)

    async def cmd_uptime(self, message: discord.Message):
        """Show bot uptime and queue health."""
        uptime_seconds = max(0, time.time() - self._start_time)
        uptime_text = str(timedelta(seconds=int(uptime_seconds)))

        outbound_size = self.discord_to_mesh.qsize()
        outbound_capacity = self.discord_to_mesh.maxsize or "∞"

        if self.mesh_to_discord_queue:
            inbound_size = self.mesh_to_discord_queue.qsize()
            inbound_capacity = self.mesh_to_discord_queue.maxsize or "∞"
            inbound_value = f"{inbound_size}/{inbound_capacity}"
        else:
            inbound_value = "N/A"

        last_refresh = getattr(self.meshtastic, "last_node_refresh", 0) or 0
        if last_refresh:
            refresh_age = max(0, time.time() - last_refresh)
            refresh_text = f"{int(refresh_age)}s ago"
        else:
            refresh_text = "Not yet"

        try:
            iface_ok = (
                self.meshtastic.iface.isConnected()
                if self.meshtastic.iface and hasattr(self.meshtastic.iface, "isConnected")
                else False
            )
            iface_status = "Connected" if iface_ok else "Disconnected"
        except Exception:
            iface_status = "Unknown"

        embed = discord.Embed(
            title="\u23f1\ufe0f Bridge Uptime & Queues",
            color=0x00BFFF,
            timestamp=get_utc_time(),
        )
        embed.add_field(name="Uptime", value=uptime_text, inline=True)
        embed.add_field(
            name="Discord \u2192 Mesh", value=f"{outbound_size}/{outbound_capacity}", inline=True
        )
        embed.add_field(name="Mesh \u2192 Discord", value=inbound_value, inline=True)
        embed.add_field(name="Last Node Refresh", value=refresh_text, inline=True)
        embed.add_field(name="Interface", value=iface_status, inline=True)

        await message.channel.send(embed=embed)

    async def cmd_loglevel(self, message: discord.Message):
        """Change runtime log level."""
        parts = message.content.strip().split(maxsplit=1)
        if not self._is_admin(message):
            await self._safe_send(
                message.channel,
                "❌ You don't have permission to change log levels.",
            )
            return
        if len(parts) < 2:
            current = logging.getLogger().getEffectiveLevel()
            await self._safe_send(
                message.channel,
                f"Use: `$loglevel <level>` (current: {logging.getLevelName(current)})",
            )
            return
        level_name = parts[1].strip().upper()
        if not hasattr(logging, level_name):
            await self._safe_send(
                message.channel,
                "❌ Invalid level. Use DEBUG, INFO, WARNING, ERROR, or CRITICAL.",
            )
            return
        level = getattr(logging, level_name)
        logging.getLogger().setLevel(level)
        for handler in logging.getLogger().handlers:
            handler.setLevel(level)
        await self._safe_send(
            message.channel, f"✅ Log level set to {level_name}."
        )

    async def cmd_send_primary(self, message: discord.Message):
        """Send message to primary channel (renamed from $sendprimary to $txt)"""
        content = message.content
        if " " not in content:
            await self._safe_send(
                message.channel, "❌ Please provide a message to send."
            )
            return

        # Respect the configured maximum message length from BOT_CONFIG
        try:
            from config import BOT_CONFIG

            max_len = BOT_CONFIG.get("message_max_length", 225)
        except Exception:
            max_len = 225
        message_text = content[content.find(" ") + 1 :][:max_len]
        if not message_text.strip():
            await self._safe_send(message.channel, "❌ Message cannot be empty.")
            return

        await self._safe_send(
            message.channel, f"📤 Sending to primary channel:\n```{message_text}```"
        )
        if not self._enqueue_discord_message(message_text):
            await self._safe_send(
                message.channel,
                "⚠️ Message queue is full, please try again in a moment.",
            )
            return False

    async def cmd_send_node(self, message: discord.Message):
        """Send message to specific node using fuzzy name matching"""
        content = message.content

        if not content.startswith("$send "):
            await self._safe_send(
                message.channel, "❌ Use format: `$send <longname> <message>`"
            )
            return

        try:
            # Extract node name and message
            parts = content.split(" ", 2)
            if len(parts) < 3:
                await self._safe_send(
                    message.channel, "❌ Use format: `$send <longname> <message>`"
                )
                return

            node_name = parts[1]
            # Respect the configured maximum message length from BOT_CONFIG
            try:
                from config import BOT_CONFIG

                max_len = BOT_CONFIG.get("message_max_length", 225)
            except Exception:
                max_len = 225
            message_text = parts[2][:max_len]

            if not message_text.strip():
                await self._safe_send(message.channel, "❌ Message cannot be empty.")
                return

            # Find node by name using fuzzy matching
            try:
                logger.info(f"Searching for node with name: '{node_name}'")
                node = await self._run_db_query(
                    self.database.find_node_by_name, node_name, timeout=5
                )
                if not node:
                    await self._safe_send(
                        message.channel,
                        f"❌ No node found with name '{node_name}'. Try using `$nodes` to see available nodes.",
                    )
                    return

                logger.info(
                    f"Found node: {node['long_name']} with ID: {node['node_id']}"
                )
            except Exception as db_error:
                logger.error(f"Database error finding node by name: {db_error}")
                await self._safe_send(
                    message.channel, "❌ Error searching for node in database."
                )
                return

            # Clean the node ID (remove any prefixes like '!' that Meshtastic doesn't expect)
            clean_node_id = node["node_id"].lstrip("!")

            # Log the node data for debugging
            logger.info(f"Node data: {node}")
            logger.info(
                f"Original node_id: '{node['node_id']}', Cleaned: '{clean_node_id}'"
            )

            # Try to convert to integer format that Meshtastic expects
            try:
                # Convert hex string to integer (this is what Meshtastic typically expects)
                node_id_int = int(clean_node_id, 16)
                logger.info(f"Converted to integer: {node_id_int}")
                final_node_id = node_id_int
            except ValueError:
                # If conversion fails, use the cleaned string
                logger.info(
                    f"Could not convert '{clean_node_id}' to integer, using string"
                )
                final_node_id = clean_node_id

            await self._safe_send(
                message.channel,
                f"📤 Sending to node **{node['long_name']}** (ID: {final_node_id}):\n```{message_text}```",
            )
            if not self._enqueue_discord_message(
                f"nodenum={final_node_id} {message_text}"
            ):
                await self._safe_send(
                    message.channel, "⚠️ Message queue is full, please try again later."
                )
                return
            logger.info(f"Sent message with node ID: {final_node_id}")

        except Exception as e:
            logger.error(f"Error parsing send command: {e}")
            await self._safe_send(
                message.channel,
                "❌ Error parsing command. Use format: `$send <longname> <message>`",
            )

    async def cmd_active_nodes(self, message: discord.Message):
        """Show active nodes from last {} minutes""".format(
            BOT_CONFIG["active_node_threshold"]
        )
        try:
            # Use caching for better performance
            nodes = await self._get_cached_data(
                f"active_nodes_{BOT_CONFIG['active_node_threshold']}",
                self.database.get_active_nodes,
                BOT_CONFIG["active_node_threshold"],
            )
            if not nodes:
                embed = discord.Embed(
                    title="📡 Active Nodes",
                    description=f"No active nodes in the last {BOT_CONFIG['active_node_threshold']} minutes",
                    color=0xFF6B6B,
                    timestamp=get_utc_time(),
                )
                embed.set_thumbnail(
                    url="https://raw.githubusercontent.com/meshtastic/firmware/master/docs/assets/logo/meshtastic-logo.png"
                )
                embed.set_footer(text="🌍 UTC Time | Check back later for activity")
                await message.channel.send(embed=embed)
                return
        except Exception as db_error:
            logger.error(f"Database error getting active nodes: {db_error}")
            await self._safe_send(
                message.channel, "❌ Error retrieving node data from database."
            )
            return

        await self._attach_locations(nodes)

        threshold = BOT_CONFIG["active_node_threshold"]
        summary_lines = [
            f"Active nodes: {len(nodes)} (last {threshold} minutes)",
        ]
        try:
            embed_color = BOT_CONFIG.get("embed_color", 0x00FF00)
        except Exception:
            embed_color = 0x00FF00

        try:
            page_size = BOT_CONFIG.get("node_page_size", 40) if isinstance(BOT_CONFIG, dict) else 40
            total_pages = max(1, (len(nodes) + page_size - 1) // page_size)
            for page in range(total_pages):
                slice_nodes = nodes[page * page_size : (page + 1) * page_size]
                title = f"Active Nodes (last {threshold} min)"
                if total_pages > 1:
                    title = f"{title} ({page+1}/{total_pages})"
                if page == 0 and summary_lines:
                    summary_embed = discord.Embed(
                        title=title,
                        description="\n".join(summary_lines),
                        color=embed_color,
                        timestamp=get_utc_time(),
                    )
                    summary_embed.set_footer(text="UTC time | ID column shows last 4 chars")
                    await message.channel.send(embed=summary_embed)

                tables = self._build_nodes_tables(title, slice_nodes, page + 1, total_pages)
                for table in tables:
                    await message.channel.send(table)
        except Exception as send_error:
            logger.error(f"Error sending active nodes embed: {send_error}")
            await self._safe_send(
                message.channel, "❌ Error sending message to channel."
            )

    async def cmd_all_nodes(self, message: discord.Message):
        """Show all known nodes"""
        try:
            # Use caching for better performance
            nodes = await self._get_cached_data(
                "all_nodes", self.database.get_all_nodes
            )
            if not nodes:
                await message.channel.send("📡 No nodes available.")
                return
        except Exception as db_error:
            logger.error(f"Database error getting all nodes: {db_error}")
            await message.channel.send("❌ Error retrieving node data from database.")
            return

        await self._attach_locations(nodes)

        try:
            threshold = BOT_CONFIG["active_node_threshold"]
        except Exception:
            threshold = 60
        try:
            now = datetime.utcnow()
            active_cutoff = now - timedelta(minutes=threshold)
            active_count = 0
            for node in nodes:
                last_heard = node.get("last_heard")
                if not last_heard:
                    continue
                try:
                    if "T" in last_heard:
                        dt = datetime.fromisoformat(last_heard.replace("Z", "+00:00"))
                        if dt.tzinfo is not None:
                            dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
                    else:
                        dt = datetime.strptime(last_heard, "%Y-%m-%d %H:%M:%S")
                    if dt > active_cutoff:
                        active_count += 1
                except Exception:
                    continue
        except Exception:
            active_count = 0

        routers = sum(1 for node in nodes if node.get("is_router"))
        summary_lines = [
            f"Total nodes: {len(nodes)}",
            f"Active (last {threshold} min): {active_count}",
            f"Routers: {routers}",
        ]
        try:
            embed_color = BOT_CONFIG.get("embed_color", 0x00FF00)
        except Exception:
            embed_color = 0x00FF00

        try:
            page_size = BOT_CONFIG.get("node_page_size", 40) if isinstance(BOT_CONFIG, dict) else 40
            total_pages = max(1, (len(nodes) + page_size - 1) // page_size)
            for page in range(total_pages):
                slice_nodes = nodes[page * page_size : (page + 1) * page_size]
                title = "All Known Nodes"
                if total_pages > 1:
                    title = f"{title} ({page+1}/{total_pages})"
                if page == 0 and summary_lines:
                    summary_embed = discord.Embed(
                        title=title,
                        description="\n".join(summary_lines),
                        color=embed_color,
                        timestamp=get_utc_time(),
                    )
                    summary_embed.set_footer(text="UTC time | ID column shows last 4 chars")
                    await message.channel.send(embed=summary_embed)

                tables = self._build_nodes_tables(title, slice_nodes, page + 1, total_pages)
                for table in tables:
                    await message.channel.send(table)
        except Exception as send_error:
            logger.error(f"Error sending all nodes embed: {send_error}")
            await self._safe_send(
                message.channel, "❌ Error sending message to channel."
            )

    async def cmd_node(self, message: discord.Message):
        """Show a node profile card"""
        try:
            parts = message.content.strip().split(maxsplit=1)
            if len(parts) < 2:
                await self._safe_send(message.channel, "Use: `$node <name>`")
                return
            name = parts[1].strip()
            if not name:
                await self._safe_send(message.channel, "Use: `$node <name>`")
                return

            node = await self._run_db_query(
                self.database.find_node_by_name, name, timeout=5
            )
            if not node:
                await self._safe_send(
                    message.channel, f"No node found matching '{name}'."
                )
                return

            node_id = node.get("node_id")
            snapshot = await self._run_db_query(
                self.database.get_node_snapshot, node_id, timeout=5
            )
            if not snapshot:
                snapshot = node

            last_msg = await self._run_db_query(
                self.database.get_last_message_for_node, node_id, timeout=5
            )
            packet_window = int(HEALTH_SCORE.get("packet_rate_window_hours", 24))
            msg_counts = await self._run_db_query(
                self.database.get_message_counts_by_node, packet_window, timeout=5
            )
            message_rate = None
            if msg_counts:
                for row in msg_counts:
                    if row.get("node_id") == node_id:
                        message_rate = (row.get("message_count") or 0) / max(
                            1, packet_window
                        )
                        break

            battery_hist = await self._run_db_query(
                self.database.get_metric_history,
                node_id,
                "battery_level",
                6,
                timeout=5,
            )
            temp_hist = await self._run_db_query(
                self.database.get_metric_history,
                node_id,
                "temperature",
                6,
                timeout=5,
            )
            snr_hist = await self._run_db_query(
                self.database.get_metric_history,
                node_id,
                "snr",
                6,
                timeout=5,
            )
            battery_slope = self._metric_slope_per_hour(battery_hist or [])
            temp_slope = self._metric_slope_per_hour(temp_hist or [])
            snr_slope = self._metric_slope_per_hour(snr_hist or [])

            health_score, _ = self._compute_health_score(
                snapshot, message_rate, battery_slope
            )

            embed = discord.Embed(
                title=f"📟 Node Profile: {snapshot.get('long_name')}",
                description=f"ID: `{node_id}`",
                color=0x00BFFF,
                timestamp=get_utc_time(),
            )

            short_name = snapshot.get("short_name") or "n/a"
            model = snapshot.get("hw_model") or "n/a"
            firmware = snapshot.get("firmware_version") or "n/a"
            hops = snapshot.get("hops_away")
            last_heard = snapshot.get("last_heard")
            last_heard_age = self._format_age(last_heard)
            presence = snapshot.get("presence_state") or "unknown"

            embed.add_field(
                name="Identity",
                value=(
                    f"Short: {short_name}\n"
                    f"Model: {model}\n"
                    f"FW: {firmware}"
                ),
                inline=True,
            )
            embed.add_field(
                name="Status",
                value=(
                    f"Last heard: {last_heard_age}\n"
                    f"Hops: {hops if hops is not None else 'n/a'}\n"
                    f"Presence: {presence}"
                ),
                inline=True,
            )
            if health_score is not None:
                embed.add_field(
                    name="Health Score",
                    value=f"{health_score:.1f}/100",
                    inline=True,
                )

            def fmt(val, suffix=""):
                if val is None:
                    return "n/a"
                try:
                    return f"{float(val):.2f}{suffix}"
                except Exception:
                    return f"{val}{suffix}"

            telemetry_lines = [
                f"Battery: {fmt(snapshot.get('battery_level'), '%')}",
                f"Voltage: {fmt(snapshot.get('voltage'), 'V')}",
                f"Temp: {fmt(snapshot.get('temperature'), '°C')}",
                f"Humidity: {fmt(snapshot.get('humidity'), '%')}",
                f"Pressure: {fmt(snapshot.get('pressure'), ' hPa')}",
                f"SNR: {fmt(snapshot.get('snr'), ' dB')}",
                f"RSSI: {fmt(snapshot.get('rssi'), ' dBm')}",
            ]
            uptime_seconds = snapshot.get("uptime_seconds")
            if uptime_seconds is not None:
                try:
                    uptime_text = str(timedelta(seconds=int(float(uptime_seconds))))
                except Exception:
                    uptime_text = "n/a"
                telemetry_lines.append(f"Uptime: {uptime_text}")

            embed.add_field(
                name="Telemetry",
                value="\n".join(telemetry_lines),
                inline=False,
            )

            trend_lines = []
            if battery_slope is not None:
                trend_lines.append(f"Battery slope: {battery_slope:+.2f}%/hr")
            if temp_slope is not None:
                trend_lines.append(f"Temp slope: {temp_slope:+.2f}°C/hr")
            if snr_slope is not None:
                trend_lines.append(f"SNR slope: {snr_slope:+.2f} dB/hr")
            if message_rate is not None:
                trend_lines.append(f"Packet rate: {message_rate:.2f}/hr")
            if trend_lines:
                embed.add_field(
                    name="Trends",
                    value="\n".join(trend_lines),
                    inline=False,
                )

            if last_msg:
                from_id = last_msg.get("from_node_id")
                to_id = last_msg.get("to_node_id")
                direction = "received" if to_id == node_id else "sent"
                other_id = from_id if to_id == node_id else to_id
                other_name = (
                    await self._run_db_query(
                        self.database.get_node_display_name, other_id, timeout=2
                    )
                    if other_id
                    else "unknown"
                )
                msg_text = last_msg.get("message_text") or "<non-text payload>"
                msg_text = msg_text.replace("\n", " ").strip()
                if len(msg_text) > 120:
                    msg_text = msg_text[:117] + "..."
                ts = last_msg.get("timestamp") or "unknown"
                embed.add_field(
                    name="Last Message",
                    value=f"{direction} {self._format_age(ts)} ago\nWith: {other_name}\n“{msg_text}”",
                    inline=False,
                )

            lat = snapshot.get("latitude")
            lon = snapshot.get("longitude")
            map_payload = None
            if lat is not None and lon is not None:
                try:
                    lat_f = float(lat)
                    lon_f = float(lon)
                    embed.add_field(
                        name="Location",
                        value=f"{lat_f:.5f}, {lon_f:.5f}\nhttps://maps.google.com/?q={lat_f},{lon_f}",
                        inline=False,
                    )
                    map_payload = await self._get_map_snapshot(lat_f, lon_f)
                    if map_payload and map_payload.get("url"):
                        embed.set_image(url=map_payload["url"])
                except Exception:
                    pass
            if map_payload and map_payload.get("path"):
                filename = os.path.basename(map_payload["path"])
                try:
                    file_obj = discord.File(map_payload["path"], filename=filename)
                    embed.set_image(url=f"attachment://{filename}")
                    await message.channel.send(embed=embed, file=file_obj)
                    return
                except Exception:
                    pass

            await message.channel.send(embed=embed)

        except Exception as e:
            logger.error(f"Error building node profile: {e}")
            await self._safe_send(
                message.channel, "❌ Error building node profile."
            )

    async def cmd_telemetry(self, message: discord.Message):
        """Show telemetry information"""
        try:
            summary = await self._run_db_query(
                self.database.get_telemetry_summary, 60, timeout=5
            )
            if not summary:
                embed = discord.Embed(
                    title="📊 Telemetry Summary",
                    description="No telemetry data available in the last 60 minutes",
                    color=0xFF6B6B,
                    timestamp=get_utc_time(),
                )
                embed.set_thumbnail(
                    url="https://raw.githubusercontent.com/meshtastic/firmware/master/docs/assets/logo/meshtastic-logo.png"
                )
                embed.set_footer(text="🌍 UTC Time | Data collection in progress...")
                await message.channel.send(embed=embed)
                return
        except Exception as db_error:
            logger.error(f"Database error getting telemetry summary: {db_error}")
            await self._safe_send(
                message.channel, "❌ Error retrieving telemetry data from database."
            )
            return

        embed = discord.Embed(
            title="📊 Telemetry Summary",
            description="Last 60 minutes of network telemetry data",
            color=0x00FF00,
            timestamp=get_utc_time(),
        )

        # Node statistics
        embed.add_field(
            name="📡 **Network Status**",
            value=f"""Total Nodes: {summary.get("total_nodes", 0)}
Active Nodes: {summary.get("active_nodes", 0)}""",
            inline=True,
        )

        # Environmental data
        env_data = ""
        if summary.get("avg_battery") is not None:
            env_data += f"🔋 Battery: {summary['avg_battery']:.1f}%\n"
        else:
            env_data += "🔋 Battery: N/A\n"

        if summary.get("avg_temperature") is not None:
            env_data += f"🌡️ Temperature: {summary['avg_temperature']:.1f}°C\n"
        else:
            env_data += "🌡️ Temperature: N/A\n"

        if summary.get("avg_humidity") is not None:
            env_data += f"💧 Humidity: {summary['avg_humidity']:.1f}%\n"
        else:
            env_data += "💧 Humidity: N/A\n"

        embed.add_field(name="🌍 **Environmental**", value=env_data, inline=True)

        # Signal quality
        signal_data = ""
        if summary.get("avg_snr") is not None:
            signal_data += f"📶 SNR: {summary['avg_snr']:.1f} dB\n"
        else:
            signal_data += "📶 SNR: N/A\n"

        if summary.get("avg_rssi") is not None:
            signal_data += f"📡 RSSI: {summary['avg_rssi']:.1f} dBm\n"
        else:
            signal_data += "📡 RSSI: N/A\n"

        embed.add_field(name="📶 **Signal Quality**", value=signal_data, inline=True)

        await message.channel.send(embed=embed)

        # Latest per-node telemetry snapshot (paged later)
        try:
            nodes = await self._run_db_query(
                self.database.get_latest_telemetry_snapshot, limit=200, timeout=10
            )
        except Exception as node_err:
            logger.error(f"Error loading nodes for telemetry details: {node_err}")
            nodes = []

        def _fmt_pct(val):
            if val is None:
                return None
            try:
                return f"{int(val)}%"
            except Exception:
                return None

        def _fmt_num(val, decimals=1, suffix=""):
            if val is None:
                return None
            try:
                return f"{float(val):.{decimals}f}{suffix}"
            except Exception:
                return None

        def _fmt_uptime(seconds):
            if not seconds:
                return "--"
            try:
                seconds = float(seconds)
            except Exception:
                return "--"
            if seconds <= 0:
                return "0s"
            minutes = seconds / 60
            hours = minutes / 60
            days = hours / 24
            if days >= 1:
                return f"{days:.1f}d"
            if hours >= 1:
                return f"{hours:.1f}h"
            return f"{minutes:.0f}m"

        def _fmt_seen(last_heard):
            age = self._format_age_short(last_heard)
            return age if age else "--"

        rows = []
        for node in nodes or []:
            battery = node.get("battery_level")
            voltage = node.get("voltage")
            chan_util = node.get("channel_utilization")
            air_util = node.get("air_util_tx")
            uptime = node.get("uptime_seconds")
            temp = node.get("temperature")
            humid = node.get("humidity")
            pressure = node.get("pressure")
            snr = node.get("snr")
            rssi = node.get("rssi")

            if all(
                v is None
                for v in [
                    battery,
                    voltage,
                    chan_util,
                    air_util,
                    uptime,
                    temp,
                    humid,
                    pressure,
                    snr,
                    rssi,
                ]
            ):
                continue

            name = (
                node.get("long_name")
                or node.get("short_name")
                or node.get("node_id")
                or "Unknown"
            )
            name = self._truncate_text(str(name), 18)
            node_id = str(node.get("node_id") or "")
            node_id_short = node_id[-4:] if len(node_id) >= 4 else node_id or "--"
            age = _fmt_seen(node.get("last_heard"))

            bat_txt = _fmt_pct(battery) or "--"
            volt_txt = _fmt_num(voltage, decimals=3, suffix="") or "--"
            temp_txt = _fmt_num(temp, decimals=1, suffix="") or "--"
            humid_txt = _fmt_num(humid, decimals=0, suffix="") or "--"
            press_txt = _fmt_num(pressure, decimals=0, suffix="") or "--"
            chan_txt = _fmt_num(chan_util, decimals=1, suffix="") or "--"
            air_txt = _fmt_num(air_util, decimals=1, suffix="") or "--"
            up_txt = _fmt_uptime(uptime)
            seen_txt = age or "--"
            pos_ts = node.get("position_ts") or node.get("last_heard")

            rows.append(
                {
                    "id": node_id_short,
                    "name": name,
                    "bat": bat_txt,
                    "volt": volt_txt,
                    "temp": temp_txt,
                    "humid": humid_txt,
                    "press": press_txt,
                    "chan": chan_txt,
                    "air": air_txt,
                    "uptime": up_txt,
                    "seen": seen_txt,
                    "pos_ts": pos_ts,
                }
            )

        if rows:
            header = (
                f"{'ID':<6} | {'Name':<20} | {'Bat%':>5} | {'Volt':>6} | "
                f"{'Temp°C':>6} | {'Hum%':>5} | {'hPa':>6} | "
                f"{'Ch%':>5} | {'Air%':>5} | {'Uptime':>8} | {'Seen':>6}"
            )
            divider = "-" * len(header)
            prefix = "```text\n"
            suffix = "\n```"
            max_chars = 1900

            def block_len(lines):
                return len(prefix) + len(suffix) + sum(len(line) + 1 for line in lines)

            table_lines = [header, divider]
            current_len = block_len(table_lines)

            for r in rows:
                line = (
                    f"{r['id']:<6} | "
                    f"{r['name']:<20} | "
                    f"{r['bat']:>5} | "
                    f"{r['volt']:>6} | "
                    f"{r['temp']:>6} | "
                    f"{r['humid']:>5} | "
                    f"{r['press']:>6} | "
                    f"{r['chan']:>5} | "
                    f"{r['air']:>5} | "
                    f"{r['uptime']:>8} | "
                    f"{r['seen']:>6}"
                )
                if (
                    current_len + len(line) + 1 > max_chars
                    and len(table_lines) > 2
                ):
                    block = prefix + "\n".join(table_lines) + suffix
                    await message.channel.send(block)
                    table_lines = [header, divider]
                    current_len = block_len(table_lines)

                table_lines.append(line)
                current_len += len(line) + 1

            if len(table_lines) > 2:
                block = prefix + "\n".join(table_lines) + suffix
                await message.channel.send(block)

    async def cmd_history(self, message: discord.Message):
        """Show recent metric history: $history <node> <metric>"""
        try:
            parts = message.content.strip().split()
            flags = [p for p in parts if p.startswith("--")]
            args = [p for p in parts if not p.startswith("--")]
            show_spark = "--spark" in flags or "--sparkline" in flags
            if len(args) < 3:
                await self._safe_send(
                    message.channel,
                    "Use: `$history <node> <metric> [--spark]` (metric: battery_level, voltage, temperature, humidity, pressure, snr, rssi, channel_utilization, air_util_tx)",
                )
                return
            name = args[1]
            metric = args[2]
            node = await self._run_db_query(
                self.database.find_node_by_name, name, timeout=5
            )
            if not node:
                await self._safe_send(
                    message.channel, f"No node found matching '{name}'."
                )
                return
            hist = await self._run_db_query(
                self.database.get_metric_history,
                node.get("node_id"),
                metric,
                20,
                timeout=5,
            )
            if not hist:
                await self._safe_send(
                    message.channel, f"No data for {metric} on {node.get('long_name')}"
                )
                return
            lines = [f"{node.get('long_name')} [{metric}] last {len(hist)}:"]
            if show_spark:
                values = [h.get("value") for h in reversed(hist) if h.get("value") is not None]
                spark = self._build_sparkline(values)
                if spark:
                    lines.append(f"Spark: {spark}")
                if values:
                    try:
                        vmin = min(values)
                        vmax = max(values)
                        delta = values[-1] - values[0] if len(values) > 1 else 0
                        lines.append(
                            f"Range: {vmin:.2f} → {vmax:.2f} | Δ {delta:+.2f}"
                        )
                    except Exception:
                        pass
            for entry in hist:
                val = entry.get("value")
                ts = entry.get("timestamp")
                lines.append(f"- {ts}: {val}")
            await self._safe_send(message.channel, "\n".join(lines))
        except Exception as e:
            logger.error(f"Error in history command: {e}")
            await self._safe_send(message.channel, "Error fetching history.")

    async def cmd_dbhealth(self, message: discord.Message):
        """Show database health and last timestamps"""
        try:
            stats = await self._run_db_query(self.database.get_db_health, timeout=5)
            if not stats:
                await self._safe_send(message.channel, "DB stats unavailable.")
                return
            embed = discord.Embed(
                title="🗄️ DB Health",
                color=0x3498DB,
                timestamp=get_utc_time(),
            )
            embed.add_field(
                name="Counts",
                value=(
                    f"Nodes: {stats.get('nodes',0)}\n"
                    f"Telemetry: {stats.get('telemetry',0)}\n"
                    f"Positions: {stats.get('positions',0)}\n"
                    f"Messages: {stats.get('messages',0)}\n"
                ),
                inline=True,
            )
            embed.add_field(
                name="Last seen",
                value=(
                    f"Telemetry: {stats.get('last_telem') or 'n/a'}\n"
                    f"Position: {stats.get('last_pos') or 'n/a'}\n"
                    f"Message: {stats.get('last_msg') or 'n/a'}\n"
                ),
                inline=True,
            )
            embed.add_field(
                name="Size",
                value=f"{stats.get('db_size_mb','?')} MB",
                inline=True,
            )
            await message.channel.send(embed=embed)
        except Exception as e:
            logger.error(f"Error in dbhealth command: {e}")
            await self._safe_send(message.channel, "Error fetching DB health.")

    async def cmd_status(self, message: discord.Message):
        """Show bridge status"""
        # Check Meshtastic connection status safely
        meshtastic_status = "❌ Disconnected"
        last_rx_age = None
        mesh_connected = False
        if self.meshtastic and self.meshtastic.iface:
            connected_flag = None
            try:
                if hasattr(self.meshtastic.iface, "isConnected") and callable(
                    self.meshtastic.iface.isConnected
                ):
                    connected_flag = bool(self.meshtastic.iface.isConnected())
            except Exception:
                connected_flag = None

            try:
                last_rx_age = time.time() - float(self._last_mesh_rx_time)
            except Exception:
                last_rx_age = None

            if connected_flag is True:
                mesh_connected = True
            elif getattr(self.meshtastic.iface, "myInfo", None) is not None:
                mesh_connected = True
            elif last_rx_age is not None:
                try:
                    threshold = max(120, int(getattr(self, "_watchdog_threshold", 0) or 0))
                except Exception:
                    threshold = 120
                if last_rx_age <= threshold:
                    mesh_connected = True

            if mesh_connected:
                meshtastic_status = "✅ Connected"
            else:
                meshtastic_status = "⚠️ No recent packets"

        # Get database statistics
        try:
            db_stats = await self._run_db_query(
                self.database.get_telemetry_summary, 60, timeout=5
            )
            node_count = db_stats.get("total_nodes", 0)
            active_count = db_stats.get("active_nodes", 0)
            presence = await self._run_db_query(
                self.database.get_presence_counts, timeout=5
            )
        except Exception:
            node_count = 0
            active_count = 0
            presence = {"online": 0, "offline": 0, "unknown": 0, "total": 0}

        # Determine status color and emoji
        if meshtastic_status == "✅ Connected":
            status_color = 0x00FF00
            status_emoji = "🟢"
            status_text = "All Systems Operational"
        else:
            status_color = 0xFF6B6B
            status_emoji = "🔴"
            status_text = "Service Issues Detected"

        embed = discord.Embed(
            title=f"{status_emoji} Bridge Status",
            description=f"**{status_text}**\n*Real-time system monitoring*",
            color=status_color,
            timestamp=get_utc_time(),
        )
        embed.set_thumbnail(
            url="https://raw.githubusercontent.com/meshtastic/firmware/master/docs/assets/logo/meshtastic-logo.png"
        )
        embed.set_footer(text="🌍 UTC Time | Last updated")

        # Service status
        embed.add_field(
            name="🖥️ **Services**",
            value=f"""Discord: ✅ Connected
Meshtastic: {meshtastic_status}
Database: ✅ Connected""",
            inline=True,
        )

        # Network status
        embed.add_field(
            name="📡 **Network**",
            value=f"""Total Nodes: {node_count}
Active Nodes: {active_count}
Current Time: {format_utc_time()}""",
            inline=True,
        )
        if last_rx_age is not None:
            try:
                minutes = int(last_rx_age // 60)
                if minutes < 60:
                    age_text = f"{minutes}m"
                else:
                    hours = minutes // 60
                    age_text = f"{hours}h"
                embed.add_field(
                    name="📶 **Mesh Activity**",
                    value=f"Last Packet: {age_text} ago",
                    inline=True,
                )
            except Exception:
                pass
        # Presence summary
        embed.add_field(
            name="👥 **Presence**",
            value=f"Online: {presence.get('online', 0)} | Offline: {presence.get('offline', 0)} | Unknown: {presence.get('unknown', 0)}",
            inline=True,
        )

        # System health
        health_score = 0
        if mesh_connected:
            health_score += 50
        if node_count > 0:
            health_score += 30
        if active_count > 0:
            health_score += 20

        if health_score >= 80:
            health_status = "🟢 Excellent"
        elif health_score >= 60:
            health_status = "🟡 Good"
        elif health_score >= 40:
            health_status = "🟠 Fair"
        else:
            health_status = "🔴 Poor"

        embed.add_field(
            name="💚 **System Health**",
            value=f"Status: {health_status}\nScore: {health_score}/100",
            inline=True,
        )

        await message.channel.send(embed=embed)

    async def cmd_network_topology(self, message: discord.Message):
        """Show network topology and connections with ASCII network diagram"""
        try:
            topology = await self._run_db_query(
                self.database.get_network_topology, timeout=10
            )
            nodes = await self._get_cached_data(
                "all_nodes", self.database.get_all_nodes
            )

            embed = discord.Embed(
                title="🌐 Network Topology",
                description="**Mesh Network Structure & Connections**\n*Real-time network visualization*",
                color=0x0099FF,
                timestamp=get_utc_time(),
            )
            embed.set_thumbnail(
                url="https://raw.githubusercontent.com/meshtastic/firmware/master/docs/assets/logo/meshtastic-logo.png"
            )
            embed.set_footer(text="🌍 UTC Time | Network analysis")

            # Create ASCII network diagram
            ascii_network = self._create_network_diagram(nodes, topology["connections"])

            embed.add_field(
                name="🌳 **Network Tree Diagram**",
                value=f"```\n{ascii_network}\n```",
                inline=False,
            )

            # Network statistics
            embed.add_field(
                name="📊 **Network Stats**",
                value=f"""Total Nodes: {topology["total_nodes"]}
Active Nodes: {topology["active_nodes"]}
Router Nodes: {topology["router_nodes"]}
Avg Hops: {topology["avg_hops"]:.1f}""",
                inline=True,
            )

            # Top connections
            if topology["connections"]:
                top_connections = topology["connections"][:5]  # Top 5 connections
                connections_text = ""
                for conn in top_connections:
                    from_name = (
                        await self._run_db_query(
                            self.database.get_node_display_name,
                            conn["from_node"],
                            timeout=2,
                        )
                        or conn["from_node"]
                    )
                    to_name = (
                        await self._run_db_query(
                            self.database.get_node_display_name,
                            conn["to_node"],
                            timeout=2,
                        )
                        or conn["to_node"]
                    )
                    connections_text += f"**{from_name}** → **{to_name}**\n"
                    connections_text += f"Messages: {conn['message_count']}, Hops: {conn['avg_hops']:.1f}\n\n"

                embed.add_field(
                    name="🔗 **Top Connections**",
                    value=connections_text[:1024],  # Discord field limit
                    inline=True,
                )
            else:
                embed.add_field(
                    name="🔗 **Connections**",
                    value="No recent connections found",
                    inline=True,
                )

            await message.channel.send(embed=embed)

        except Exception as e:
            logger.error(f"Error getting network topology: {e}")
            await self._safe_send(
                message.channel, "❌ Error retrieving network topology."
            )

    async def cmd_topology_tree(self, message: discord.Message):
        """Show visual tree of all radio connections"""
        try:
            # Send immediate feedback
            status_msg = await message.channel.send(
                "🔄 Generating network topology map, please wait..."
            )

            # Get nodes with async timeout
            nodes = await self._get_cached_data(
                "all_nodes", self.database.get_all_nodes, timeout=10
            )

            if not nodes:
                await status_msg.edit(
                    content="📡 **No nodes available for topology analysis**"
                )
                return

            # Get topology with async timeout
            topology = await self._run_db_query(
                self.database.get_network_topology, timeout=10
            )

            if topology is None:
                await status_msg.edit(
                    content="❌ Couldn't load topology (database timeout). Try `$nodes` for a simpler view."
                )
                return

            # Delete status message
            await status_msg.delete()

            connection_lines = self._build_topology_ascii(
                nodes, topology.get("connections", [])
            )

            total_nodes = len(nodes)
            active_connections = len(topology.get("connections", []))
            avg_hops = topology.get("avg_hops", 0) or 0
            summary = (
                f"Network Topology (last 24h)\n"
                f"Radios: {total_nodes} | Connections: {active_connections} | Avg hops: {avg_hops:.1f}"
            )
            await message.channel.send(summary)

            chunks = self._chunk_lines(connection_lines, 1900)
            for chunk in chunks:
                block = "```text\n" + "\n".join(chunk) + "\n```"
                await message.channel.send(block)

        except Exception as e:
            logger.error(f"Error creating topology tree: {e}")
            await self._safe_send(
                message.channel,
                f"❌ Couldn't create topology tree: {str(e)[:100]}. Try `$nodes` instead.",
            )

    async def cmd_message_statistics(self, message: discord.Message):
        """Show message statistics and network activity"""
        try:
            stats = await self._run_db_query(
                self.database.get_message_statistics, hours=24, timeout=10
            )

            embed = discord.Embed(
                title="📊 Message Statistics",
                description="24-hour network activity summary",
                color=0x9B59B6,
                timestamp=get_utc_time(),
            )

            # Basic statistics
            embed.add_field(
                name="📈 **Activity**",
                value=f"""Total Messages: {stats.get("total_messages", 0)}
Unique Senders: {stats.get("unique_senders", 0)}
Unique Recipients: {stats.get("unique_recipients", 0)}""",
                inline=True,
            )

            # Signal quality - safe formatting
            avg_hops = stats.get("avg_hops", 0) or 0
            avg_snr = stats.get("avg_snr", 0) or 0
            avg_rssi = stats.get("avg_rssi", 0) or 0

            embed.add_field(
                name="📶 **Signal Quality**",
                value=f"""Avg Hops: {avg_hops:.1f}
Avg SNR: {avg_snr:.1f} dB
Avg RSSI: {avg_rssi:.1f} dBm""",
                inline=True,
            )

            # Hourly distribution
            hourly_dist = stats.get("hourly_distribution", {})
            if hourly_dist:
                # Find peak hours
                peak_hour = (
                    max(hourly_dist.items(), key=lambda x: x[1])
                    if hourly_dist
                    else ("N/A", 0)
                )
                quiet_hour = (
                    min(hourly_dist.items(), key=lambda x: x[1])
                    if hourly_dist
                    else ("N/A", 0)
                )

                embed.add_field(
                    name="⏰ **Activity Pattern**",
                    value=f"""Peak Hour: {peak_hour[0]}:00 ({peak_hour[1]} msgs)
Quiet Hour: {quiet_hour[0]}:00 ({quiet_hour[1]} msgs)
Active Hours: {len(hourly_dist)}""",
                    inline=True,
                )

            await message.channel.send(embed=embed)

        except Exception as e:
            logger.error(f"Error getting message statistics: {e}")
            await self._safe_send(
                message.channel, "❌ Error retrieving message statistics."
            )

    async def cmd_trace_route(self, message: discord.Message):
        """Trace route to a specific node with visual hop-by-hop path"""
        content = message.content.strip()

        if not content.startswith("$trace "):
            await self._safe_send(
                message.channel, "❌ Use format: `$trace <node_name>`"
            )
            return

        try:
            node_name = content[7:].strip()  # Remove '$trace '
            if not node_name:
                await self._safe_send(message.channel, "❌ Please specify a node name.")
                return

            # Find the target node
            target_node = await self._run_db_query(
                self.database.find_node_by_name, node_name, timeout=5
            )
            if not target_node:
                await self._safe_send(
                    message.channel,
                    f"❌ No node found with name '{node_name}'. Try using `$nodes` to see available nodes.",
                )
                return

            # Get network topology and analyze routing
            topology = await self._run_db_query(
                self.database.get_network_topology, timeout=10
            )
            route_path = await self._analyze_route_to_node(
                target_node["node_id"], topology
            )

            embed = discord.Embed(
                title=f"🛣️ Trace Route to {target_node['long_name']}",
                description=f"Analyzing network path to **{target_node['node_id']}**",
                color=0x00BFFF,
                timestamp=get_utc_time(),
            )

            # Target node info
            embed.add_field(
                name="🎯 **Target Node**",
                value=f"""**Name:** {target_node["long_name"]}
**ID:** `{target_node["node_id"]}`
**Hops Away:** {target_node.get("hops_away", "Unknown")}
**Last Heard:** {target_node.get("last_heard", "Unknown")}""",
                inline=True,
            )

            # Route path visualization
            if route_path:
                route_text = self._format_route_path(route_path)
                embed.add_field(name="🛤️ **Route Path**", value=route_text, inline=False)

                # Route statistics
                total_hops = len(route_path) - 1  # -1 because we don't count the source
                avg_snr = sum(hop.get("snr", 0) for hop in route_path[1:]) / max(
                    1, len(route_path) - 1
                )
                avg_rssi = sum(hop.get("rssi", 0) for hop in route_path[1:]) / max(
                    1, len(route_path) - 1
                )

                embed.add_field(
                    name="📊 **Route Statistics**",
                    value=f"""**Total Hops:** {total_hops}
**Avg SNR:** {avg_snr:.1f} dB
**Avg RSSI:** {avg_rssi:.1f} dBm
**Path Quality:** {self._assess_route_quality(avg_snr, total_hops)}""",
                    inline=True,
                )
            else:
                embed.add_field(
                    name="🛤️ **Route Path**",
                    value="❌ **No route found** - Node may be unreachable or no recent communication data available",
                    inline=False,
                )

            # Network overview
            embed.add_field(
                name="🌐 **Network Overview**",
                value=f"""**Active Nodes:** {topology["active_nodes"]}
**Total Connections:** {len(topology["connections"])}
**Network Avg Hops:** {topology["avg_hops"]:.1f}""",
                inline=True,
            )

            # Connection quality to target
            connections_to_target = [
                conn
                for conn in topology["connections"]
                if conn["to_node"] == target_node["node_id"]
            ]
            if connections_to_target:
                best_connection = max(
                    connections_to_target, key=lambda x: x["message_count"]
                )
                from_name = (
                    await self._run_db_query(
                        self.database.get_node_display_name,
                        best_connection["from_node"],
                        timeout=2,
                    )
                    or best_connection["from_node"]
                )

                embed.add_field(
                    name="🔗 **Best Connection**",
                    value=f"""**From:** {from_name}
**Messages:** {best_connection["message_count"]}
**Avg Hops:** {best_connection["avg_hops"]:.1f}
**Avg SNR:** {best_connection["avg_snr"]:.1f} dB""",
                    inline=True,
                )

            embed.set_footer(text=f"Route analysis completed at")
            await message.channel.send(embed=embed)

        except Exception as e:
            logger.error(f"Error tracing route: {e}")
            await self._safe_send(message.channel, "❌ Error tracing route to node.")

    async def _analyze_route_to_node(self, target_node_id: str, topology: dict) -> list:
        """Analyze the route to a specific node based on message data"""
        try:
            # Get all messages to the target node using async wrapper
            def get_messages():
                with self.database._get_connection() as conn:
                    cursor = conn.cursor()
                    cursor.execute(
                        """
                        SELECT from_node_id, to_node_id, hops_away, snr, rssi, timestamp
                        FROM messages 
                        WHERE to_node_id = ? 
                        ORDER BY timestamp DESC 
                        LIMIT 100
                    """,
                        (target_node_id,),
                    )
                    return cursor.fetchall()

            messages = await self._run_db_query(get_messages, timeout=5) or []

            if not messages:
                return []

            # Find the most common path by analyzing message patterns
            # Group messages by hops_away to understand the path
            hop_groups = {}
            for msg in messages:
                hops = msg[2]  # hops_away
                if hops not in hop_groups:
                    hop_groups[hops] = []
                hop_groups[hops].append(msg)

            # Build route path from hop groups
            route_path = []
            sorted_hops = sorted(hop_groups.keys())

            for hop_count in sorted_hops:
                # Get the most recent message for this hop count
                recent_msg = max(hop_groups[hop_count], key=lambda x: x[5])  # timestamp
                from_node_id = recent_msg[0]
                snr = recent_msg[3]
                rssi = recent_msg[4]

                # Get node display name
                from_name = (
                    await self._run_db_query(
                        self.database.get_node_display_name, from_node_id, timeout=2
                    )
                    or from_node_id
                )

                route_path.append(
                    {
                        "node_id": from_node_id,
                        "node_name": from_name,
                        "hops_away": hop_count,
                        "snr": snr,
                        "rssi": rssi,
                    }
                )

            # Add target node at the end
            target_name = (
                await self._run_db_query(
                    self.database.get_node_display_name, target_node_id, timeout=2
                )
                or target_node_id
            )
            route_path.append(
                {
                    "node_id": target_node_id,
                    "node_name": target_name,
                    "hops_away": 0,
                    "snr": None,
                    "rssi": None,
                }
            )

            return route_path

        except Exception as e:
            logger.error(f"Error analyzing route to node {target_node_id}: {e}")
            return []

    def _format_route_path(self, route_path: list) -> str:
        """Format the route path for display with visual indicators"""
        if not route_path:
            return "No route data available"

        path_lines = []

        for i, hop in enumerate(route_path):
            node_name = hop["node_name"]
            node_id = hop["node_id"]
            hops_away = hop["hops_away"]
            snr = hop.get("snr")
            rssi = hop.get("rssi")

            # Determine hop indicator
            if i == 0:
                # Source node
                hop_indicator = "🏠"
                hop_text = "SOURCE"
            elif i == len(route_path) - 1:
                # Target node
                hop_indicator = "🎯"
                hop_text = "TARGET"
            else:
                # Intermediate hop
                hop_indicator = f"🔄 {i}"
                hop_text = f"HOP {i}"

            # Format signal quality
            signal_info = ""
            if snr is not None and rssi is not None:
                signal_quality = self._get_signal_quality_icon(snr)
                signal_info = f" {signal_quality} SNR:{snr:.1f}dB RSSI:{rssi:.1f}dBm"
            elif snr is not None:
                signal_quality = self._get_signal_quality_icon(snr)
                signal_info = f" {signal_quality} SNR:{snr:.1f}dB"
            elif rssi is not None:
                signal_info = f" 📶 RSSI:{rssi:.1f}dBm"

            # Format the hop line
            hop_line = f"{hop_indicator} **{hop_text}:** {node_name}"
            if len(node_id) > 8:  # Only show short ID if it's long
                hop_line += f" (`{node_id[:8]}...`)"
            else:
                hop_line += f" (`{node_id}`)"

            hop_line += signal_info

            path_lines.append(hop_line)

            # Add connection line (except for the last hop)
            if i < len(route_path) - 1:
                path_lines.append("    ⬇️")

        return "\n".join(path_lines)

    def _get_signal_quality_icon(self, snr: float) -> str:
        """Get signal quality icon based on SNR"""
        if snr > 10:
            return "🟢"  # Excellent
        elif snr > 5:
            return "🟡"  # Good
        elif snr > 0:
            return "🟠"  # Fair
        else:
            return "🔴"  # Poor

    def _assess_route_quality(self, avg_snr: float, total_hops: int) -> str:
        """Assess overall route quality"""
        if avg_snr > 10 and total_hops <= 2:
            return "🟢 Excellent"
        elif avg_snr > 5 and total_hops <= 4:
            return "🟡 Good"
        elif avg_snr > 0 and total_hops <= 6:
            return "🟠 Fair"
        else:
            return "🔴 Poor"

    async def cmd_leaderboard(self, message: discord.Message):
        """Show node health leaderboards"""
        try:
            nodes = await self._get_cached_data(
                "all_nodes_full", self.database.get_all_nodes, None
            )
            if not nodes:
                await self._safe_send(
                    message.channel, "📡 No nodes available for leaderboard."
                )
                return

            packet_window = int(HEALTH_SCORE.get("packet_rate_window_hours", 24))
            msg_counts = await self._run_db_query(
                self.database.get_message_counts_by_node, packet_window, timeout=10
            )
            msg_map = {row["node_id"]: row.get("message_count", 0) for row in msg_counts}

            scores = []
            for node in nodes:
                node_id = node.get("node_id")
                if not node_id:
                    continue
                battery_hist = await self._run_db_query(
                    self.database.get_metric_history,
                    node_id,
                    "battery_level",
                    6,
                    timeout=5,
                )
                battery_slope = self._metric_slope_per_hour(battery_hist or [])
                message_rate = msg_map.get(node_id, 0) / max(1, packet_window)
                score, _ = self._compute_health_score(
                    node, message_rate, battery_slope
                )
                if score is None:
                    continue
                scores.append(
                    {
                        "node": node,
                        "score": score,
                        "message_rate": message_rate,
                        "battery_slope": battery_slope,
                    }
                )

            if not scores:
                await self._safe_send(
                    message.channel,
                    "📡 Not enough data to compute health scores yet.",
                )
                return

            top = sorted(scores, key=lambda x: x["score"], reverse=True)[:5]
            bottom = sorted(scores, key=lambda x: x["score"])[:5]
            rate_top = sorted(
                scores, key=lambda x: x["message_rate"], reverse=True
            )[:5]

            embed = discord.Embed(
                title="🏆 Node Health Leaderboards",
                description=f"Scores based on battery slope, uptime, last-heard age, and packet rate ({packet_window}h window).",
                color=0xFFD700,
                timestamp=get_utc_time(),
            )

            def format_line(idx, entry):
                node = entry["node"]
                name = node.get("long_name") or node.get("short_name") or node.get("node_id")
                age = self._format_age(node.get("last_heard"))
                rate = entry.get("message_rate", 0)
                medal = "🥇" if idx == 0 else "🥈" if idx == 1 else "🥉" if idx == 2 else "🏅"
                return f"{medal} {name} — {entry['score']:.1f} | last heard {age} | {rate:.2f}/hr"

            top_lines = [format_line(i, entry) for i, entry in enumerate(top)]
            bottom_lines = [format_line(i, entry) for i, entry in enumerate(bottom)]

            embed.add_field(
                name="Top Health",
                value="\n".join(top_lines)[:1024],
                inline=False,
            )
            embed.add_field(
                name="Needs Attention",
                value="\n".join(bottom_lines)[:1024],
                inline=False,
            )

            rate_lines = []
            for i, entry in enumerate(rate_top):
                node = entry["node"]
                name = node.get("long_name") or node.get("short_name") or node.get("node_id")
                medal = "🥇" if i == 0 else "🥈" if i == 1 else "🥉" if i == 2 else "🏅"
                rate_lines.append(f"{medal} {name} — {entry['message_rate']:.2f}/hr")
            if rate_lines:
                embed.add_field(
                    name="Packet Rate Leaders",
                    value="\n".join(rate_lines)[:1024],
                    inline=False,
                )

            await message.channel.send(embed=embed)

        except Exception as e:
            logger.error(f"Error creating leaderboard: {e}")
            await self._safe_send(message.channel, "❌ Error creating leaderboard.")

    async def cmd_network_art(self, message: discord.Message):
        """Create ASCII network art"""
        try:
            nodes = await self._get_cached_data(
                "all_nodes", self.database.get_all_nodes
            )
            topology = await self._run_db_query(
                self.database.get_network_topology, timeout=10
            )

            if not nodes:
                await self._safe_send(
                    message.channel, "📡 No nodes available for network art."
                )
                return

            embed = discord.Embed(
                title="🎨 Network Art",
                description="ASCII art representation of your mesh network",
                color=0xFF69B4,
                timestamp=get_utc_time(),
            )

            # Create simple ASCII network diagram
            art_lines = []
            art_lines.append("```")
            art_lines.append("🌐 MESHTASTIC NETWORK ART 🌐")
            art_lines.append("=" * 40)
            art_lines.append("")

            # Show active nodes as a simple diagram
            active_nodes = []
            for n in nodes:
                if n.get("last_heard"):
                    try:
                        last_heard = datetime.fromisoformat(
                            n["last_heard"].replace("Z", "+00:00")
                        )
                        if last_heard > datetime.now() - timedelta(hours=1):
                            active_nodes.append(n)
                    except (ValueError, TypeError) as e:
                        logger.warning(
                            f"Error parsing last_heard for node {n.get('long_name', 'Unknown')}: {e}"
                        )
                        continue

            if active_nodes:
                art_lines.append("🟢 ACTIVE NODES:")
                for i, node in enumerate(active_nodes[:8]):  # Limit to 8 for ASCII art
                    snr = node.get("snr")
                    if snr is not None:
                        if snr > 5:
                            status_icon = "🟢"
                        elif snr > 0:
                            status_icon = "🟡"
                        else:
                            status_icon = "🔴"
                    else:
                        status_icon = "⚪"
                    art_lines.append(f"  {status_icon} {node['long_name'][:15]}")

                if len(active_nodes) > 8:
                    art_lines.append(f"  ... and {len(active_nodes) - 8} more")
            else:
                art_lines.append("⚪ No active nodes")

            art_lines.append("")

            # Show connections as lines
            if topology.get("connections"):
                art_lines.append("🔗 CONNECTIONS:")
                for i, conn in enumerate(topology["connections"][:5]):
                    from_name = self.database.get_node_display_name(conn["from_node"])[
                        :10
                    ]
                    to_name = self.database.get_node_display_name(conn["to_node"])[:10]
                    art_lines.append(f"  {from_name} ─── {to_name}")

                if len(topology["connections"]) > 5:
                    art_lines.append(
                        f"  ... and {len(topology['connections']) - 5} more"
                    )
            else:
                art_lines.append("🔗 No connections detected")

            art_lines.append("")
            art_lines.append("=" * 40)
            art_lines.append("```")

            # Create the art
            art_text = "\n".join(art_lines)

            embed.add_field(name="🎨 **Network Diagram**", value=art_text, inline=False)

            # Network stats for the art
            total_nodes = len(nodes)
            active_count = len(active_nodes)
            connection_count = len(topology.get("connections", []))

            embed.add_field(
                name="📊 **Art Stats**",
                value=f"""Total Nodes: {total_nodes}
Active Nodes: {active_count}
Connections: {connection_count}
Art Quality: {"🎨" * min(5, total_nodes // 2)}""",
                inline=True,
            )

            await message.channel.send(embed=embed)

        except Exception as e:
            logger.error(f"Error creating network art: {e}")
            await self._safe_send(message.channel, "❌ Error creating network art.")

    async def cmd_live_monitor(self, message: discord.Message):
        """Real-time network monitor showing live packet activity"""
        user_id = message.author.id

        # Check if user already has a live monitor running
        if user_id in self._live_monitors and self._live_monitors[user_id]["active"]:
            # Stop the existing monitor
            try:
                logger.debug(f"Stopping live monitor for user {user_id}")
                self._live_monitors[user_id]["active"] = False

                if "task" in self._live_monitors[user_id]:
                    task = self._live_monitors[user_id]["task"]
                    try:
                        if not task.done():
                            task.cancel()
                            logger.debug(
                                f"Cancelled live monitor task for user {user_id}"
                            )
                        else:
                            logger.debug(
                                f"Live monitor task for user {user_id} was already done"
                            )
                    except Exception as task_error:
                        logger.error(
                            f"Error cancelling task for user {user_id}: {task_error}"
                        )
                        # Continue with cleanup even if task cancellation fails

                await message.channel.send("🛑 **Live monitor stopped**")
                del self._live_monitors[user_id]
                logger.debug(f"Successfully stopped live monitor for user {user_id}")
                return
            except Exception as e:
                logger.error(
                    f"Error stopping live monitor for user {user_id}: {type(e).__name__}: {str(e)}"
                )
                logger.error(f"Exception details: {repr(e)}")
                await message.channel.send("🛑 **Live monitor stopped** (with errors)")
                # Clean up even if there was an error
                if user_id in self._live_monitors:
                    del self._live_monitors[user_id]
                return

        # Start live monitor (cooldown is handled globally in handle_command)

        embed = discord.Embed(
            title="📡 Live Network Monitor",
            description="**Starting live packet monitoring...**\n\n*Monitoring will run for 1 minute or until you type `$live` again*",
            color=0x00FF00,
            timestamp=get_utc_time(),
        )
        embed.add_field(
            name="📊 **What you'll see:**",
            value="• Packet types and sources\n• Message content previews\n• Telemetry data summaries\n• Traceroute information\n• Signal quality metrics",
            inline=False,
        )
        embed.set_footer(text=f"Requested by {message.author.display_name}")

        status_message = await message.channel.send(embed=embed)

        # Start the live monitoring task
        try:
            task = asyncio.create_task(
                self._run_live_monitor(message.channel, user_id, status_message)
            )
            self._live_monitors[user_id] = {"active": True, "task": task}
            logger.info(
                f"Started live monitor for user {message.author.display_name} ({user_id})"
            )
        except Exception as e:
            logger.error(f"Error starting live monitor for user {user_id}: {e}")
            await message.channel.send(f"❌ **Error starting live monitor:** {str(e)}")
            return

    async def _run_live_monitor(self, channel, user_id, status_message):
        """Run the live monitor for 10 seconds"""
        try:
            start_time = time.time()
            last_update = start_time
            packet_count = 0

            while time.time() - start_time < 60:  # 1 minute timeout
                if (
                    user_id not in self._live_monitors
                    or not self._live_monitors[user_id]["active"]
                ):
                    break

                # Check for new packets in buffer (thread-safe)
                with self._packet_buffer_lock:
                    current_packets = len(self._packet_buffer)
                    if current_packets > packet_count:
                        # New packets available, copy them
                        new_packets = self._packet_buffer[packet_count:].copy()
                        packet_count = current_packets
                    else:
                        new_packets = []

                if new_packets:
                    # Update status message with new packets
                    await self._update_live_display(
                        channel, status_message, new_packets, time.time() - start_time
                    )
                    last_update = time.time()

                # Check every 0.5 seconds
                await asyncio.sleep(0.5)

            # Final update
            if (
                user_id in self._live_monitors
                and self._live_monitors[user_id]["active"]
            ):
                await self._finalize_live_monitor(
                    channel, status_message, packet_count, time.time() - start_time
                )

        except asyncio.CancelledError:
            logger.info(f"Live monitor cancelled for user {user_id}")
        except Exception as e:
            logger.error(
                f"Error in live monitor for user {user_id}: {type(e).__name__}: {str(e)}"
            )
            logger.error(f"Exception details: {repr(e)}")
            try:
                await channel.send(f"❌ **Live monitor error:** {str(e)}")
            except Exception as send_error:
                logger.error(f"Error sending error message: {send_error}")
        finally:
            # Clean up
            if user_id in self._live_monitors:
                del self._live_monitors[user_id]

    async def _update_live_display(
        self, channel, status_message, new_packets, elapsed_time
    ):
        """Update the live monitor display with new packets"""
        try:
            if not new_packets:
                return

            # Create embed for new packets
            embed = discord.Embed(
                title="📡 Live Network Monitor",
                description=f"**Live packet monitoring** - {elapsed_time:.1f}s elapsed",
                color=0x00BFFF,
                timestamp=datetime.utcnow(),
            )

            # Add packet information
            packet_text = ""
            for packet in new_packets[-10:]:  # Show last 10 packets
                packet_type = packet.get("type", "UNKNOWN")
                from_name = packet.get("from_name", "Unknown")
                portnum = packet.get("portnum", "UNKNOWN")
                hops = packet.get("hops", 0)
                snr = packet.get("snr", "N/A")
                rssi = packet.get("rssi", "N/A")

                # Format packet info
                if packet_type == "text":
                    text_preview = packet.get("text", "")[:30]
                    if len(packet.get("text", "")) > 30:
                        text_preview += "..."
                    packet_text += (
                        f"💬 **{from_name}** ({portnum}) - `{text_preview}`\n"
                    )
                elif packet_type == "telemetry":
                    sensor_data = packet.get("sensor_data", [])
                    sensor_summary = (
                        ", ".join(sensor_data[:3]) if sensor_data else "No data"
                    )
                    packet_text += (
                        f"📊 **{from_name}** ({portnum}) - {sensor_summary}\n"
                    )
                elif packet_type == "traceroute":
                    to_name = packet.get("to_name", "Unknown")
                    hops_count = packet.get("hops_count", 0)
                    packet_text += (
                        f"🛣️ **{from_name}** → **{to_name}** ({hops_count} hops)\n"
                    )
                elif packet_type == "movement":
                    distance_moved = packet.get("distance_moved")
                    speed_mps = packet.get("speed_mps")
                    parts = []
                    if distance_moved is not None:
                        try:
                            parts.append(f"moved {float(distance_moved):.1f}m")
                        except Exception:
                            pass
                    if speed_mps is not None:
                        try:
                            parts.append(f"{float(speed_mps):.1f} m/s")
                        except Exception:
                            pass
                    details = f" ({', '.join(parts)})" if parts else ""
                    packet_text += f"🚶 **{from_name}** on the move{details}\n"
                else:
                    packet_text += f"📦 **{from_name}** ({portnum}) - {packet_type}\n"

                # Add signal info
                packet_text += f"   └─ Hops: {hops} | SNR: {snr} | RSSI: {rssi}\n\n"

            if packet_text:
                embed.add_field(
                    name="📦 **Recent Packets:**",
                    value=packet_text[:1024],  # Discord field limit
                    inline=False,
                )

            embed.set_footer(text="Type `$live` again to stop monitoring")

            await status_message.edit(embed=embed)

        except Exception as e:
            logger.error(f"Error updating live display: {e}")

    async def _finalize_live_monitor(
        self, channel, status_message, total_packets, elapsed_time
    ):
        """Finalize the live monitor with summary"""
        try:
            embed = discord.Embed(
                title="📡 Live Network Monitor - Complete",
                description=f"**Monitoring completed** - {elapsed_time:.1f}s total",
                color=0x00FF00,
                timestamp=datetime.utcnow(),
            )

            embed.add_field(
                name="📊 **Summary:**",
                value=f"Total Packets: {total_packets}\nDuration: {elapsed_time:.1f}s\nAverage Rate: {total_packets / elapsed_time:.1f} packets/sec",
                inline=False,
            )

            embed.set_footer(text="Live monitoring session ended")

            await status_message.edit(embed=embed)

        except Exception as e:
            logger.error(f"Error finalizing live monitor: {e}")

    async def cmd_clear_database(self, message: discord.Message):
        """Clear database and force fresh start"""
        try:
            # Clear all data from database
            with self.database._get_connection() as conn:
                cursor = conn.cursor()

                # Clear all tables
                cursor.execute("DELETE FROM telemetry")
                cursor.execute("DELETE FROM positions")
                cursor.execute("DELETE FROM messages")
                cursor.execute("DELETE FROM nodes")

                # Reset auto-increment counters
                cursor.execute(
                    "DELETE FROM sqlite_sequence WHERE name IN ('telemetry', 'positions', 'messages')"
                )

                conn.commit()

                logger.info("Database cleared by user command")

            # Clear command handler cache
            await self.clear_cache()

            embed = discord.Embed(
                title="🗑️ Database Cleared",
                description="All data has been cleared from the database",
                color=0xFF6B6B,
                timestamp=get_utc_time(),
            )

            embed.add_field(
                name="✅ **Cleared Tables**",
                value="• Nodes\n• Telemetry\n• Positions\n• Messages\n• Cache",
                inline=True,
            )

            embed.add_field(
                name="🔄 **Next Steps**",
                value="The bot will now collect fresh data from the mesh network. Use `$nodes` to see new data as it's collected.",
                inline=True,
            )

            await message.channel.send(embed=embed)

        except Exception as e:
            logger.error(f"Error clearing database: {e}")
            await self._safe_send(message.channel, f"❌ Error clearing database: {e}")

    async def cmd_debug_info(self, message: discord.Message):
        """Show debug information about database and data storage"""
        try:
            # Get database counts using async wrapper
            def get_db_counts():
                with self.database._get_connection() as conn:
                    cursor = conn.cursor()
                    cursor.execute("SELECT COUNT(*) FROM nodes")
                    node_count = cursor.fetchone()[0]
                    cursor.execute("SELECT COUNT(*) FROM telemetry")
                    telemetry_count = cursor.fetchone()[0]
                    cursor.execute("SELECT COUNT(*) FROM positions")
                    position_count = cursor.fetchone()[0]
                    cursor.execute("SELECT COUNT(*) FROM messages")
                    message_count = cursor.fetchone()[0]
                    return node_count, telemetry_count, position_count, message_count

            counts = await self._run_db_query(get_db_counts, timeout=5)
            if counts:
                node_count, telemetry_count, position_count, message_count = counts
            else:
                node_count = telemetry_count = position_count = message_count = 0

            # Get some sample data
            nodes = await self._run_db_query(self.database.get_all_nodes, timeout=5)
            recent_telemetry = []
            if nodes and len(nodes) > 0:
                # Get recent telemetry for first node
                recent_telemetry = (
                    await self._run_db_query(
                        self.database.get_telemetry_history,
                        nodes[0]["node_id"],
                        hours=1,
                        limit=5,
                        timeout=5,
                    )
                    or []
                )

            embed = discord.Embed(
                title="🔍 Debug Information",
                description="Database and data storage status",
                color=0x00BFFF,
                timestamp=get_utc_time(),
            )

            embed.add_field(
                name="📊 **Database Counts**",
                value=f"""Nodes: {node_count}
Telemetry: {telemetry_count}
Positions: {position_count}
Messages: {message_count}""",
                inline=True,
            )

            embed.add_field(
                name="🔄 **Cache Status**",
                value=f"""Cache Entries: {len(self._node_cache)}
Cache TTL: {self._cache_ttl}s
Last Refresh: {time.time() - self.meshtastic.last_node_refresh:.1f}s ago""",
                inline=True,
            )

            if nodes:
                embed.add_field(
                    name="📡 **Sample Node Data**",
                    value=f"""Total Nodes: {len(nodes)}
First Node: {nodes[0]["long_name"]}
Has SNR: {nodes[0].get("snr") is not None}
Has Battery: {nodes[0].get("battery_level") is not None}
Last Heard: {nodes[0].get("last_heard", "Unknown")}""",
                    inline=False,
                )

            if recent_telemetry:
                embed.add_field(
                    name="📈 **Recent Telemetry**",
                    value=f"Found {len(recent_telemetry)} recent telemetry records for {nodes[0]['long_name']}",
                    inline=False,
                )
            else:
                embed.add_field(
                    name="📈 **Recent Telemetry**",
                    value="No recent telemetry data found",
                    inline=False,
                )

            await message.channel.send(embed=embed)

        except Exception as e:
            logger.error(f"Error getting debug info: {e}")
            await self._safe_send(message.channel, f"❌ Error getting debug info: {e}")

    def _create_signal_tree(
        self, excellent_nodes, good_nodes, poor_nodes, unknown_nodes
    ):
        """Create ASCII tree for signal strength visualization"""
        tree_lines = []
        tree_lines.append("🔥 SIGNAL STRENGTH TREE")
        tree_lines.append("=" * 50)
        tree_lines.append("")

        # Root of tree
        tree_lines.append("🌐 Mesh Network")
        tree_lines.append("│")

        # Excellent signal branch
        if excellent_nodes:
            tree_lines.append("├─ 🟢 Excellent Signal")
            for i, node in enumerate(excellent_nodes[:8]):  # Limit to 8 for space
                if i == len(excellent_nodes[:8]) - 1 and len(excellent_nodes) > 8:
                    tree_lines.append(
                        f"│  └─ {node['long_name'][:20]} ({node.get('snr', 0):.1f}dB) +{len(excellent_nodes) - 8} more"
                    )
                else:
                    tree_lines.append(
                        f"│  ├─ {node['long_name'][:20]} ({node.get('snr', 0):.1f}dB)"
                    )
        else:
            tree_lines.append("├─ 🟢 Excellent Signal (None)")

        # Good signal branch
        if good_nodes:
            tree_lines.append("├─ 🟡 Good Signal")
            for i, node in enumerate(good_nodes[:6]):
                if i == len(good_nodes[:6]) - 1 and len(good_nodes) > 6:
                    tree_lines.append(
                        f"│  └─ {node['long_name'][:20]} ({node.get('snr', 0):.1f}dB) +{len(good_nodes) - 6} more"
                    )
                else:
                    tree_lines.append(
                        f"│  ├─ {node['long_name'][:20]} ({node.get('snr', 0):.1f}dB)"
                    )
        else:
            tree_lines.append("├─ 🟡 Good Signal (None)")

        # Poor signal branch
        if poor_nodes:
            tree_lines.append("├─ 🔴 Poor Signal")
            for i, node in enumerate(poor_nodes[:4]):
                if i == len(poor_nodes[:4]) - 1 and len(poor_nodes) > 4:
                    tree_lines.append(
                        f"│  └─ {node['long_name'][:20]} ({node.get('snr', 0):.1f}dB) +{len(poor_nodes) - 4} more"
                    )
                else:
                    tree_lines.append(
                        f"│  ├─ {node['long_name'][:20]} ({node.get('snr', 0):.1f}dB)"
                    )
        else:
            tree_lines.append("├─ 🔴 Poor Signal (None)")

        # Unknown signal branch
        if unknown_nodes:
            tree_lines.append("└─ ⚪ Unknown Signal")
            for i, node in enumerate(unknown_nodes[:4]):
                if i == len(unknown_nodes[:4]) - 1 and len(unknown_nodes) > 4:
                    tree_lines.append(
                        f"   └─ {node['long_name'][:20]} +{len(unknown_nodes) - 4} more"
                    )
                else:
                    tree_lines.append(f"   ├─ {node['long_name'][:20]}")
        else:
            tree_lines.append("└─ ⚪ Unknown Signal (None)")

        tree_lines.append("")
        tree_lines.append("Legend: 🟢 >10dB  🟡 5-10dB  🔴 <5dB  ⚪ Unknown")

        return "\n".join(tree_lines)

    def _create_network_diagram(self, nodes, connections):
        """Create ASCII network diagram for topology visualization"""
        diagram_lines = []
        diagram_lines.append("🌐 NETWORK TOPOLOGY DIAGRAM")
        diagram_lines.append("=" * 50)
        diagram_lines.append("")

        # Group nodes by activity and hops
        active_nodes = []
        for n in nodes:
            if n.get("last_heard"):
                try:
                    last_heard = datetime.fromisoformat(
                        n["last_heard"].replace("Z", "+00:00")
                    )
                    if last_heard > datetime.now() - timedelta(hours=1):
                        active_nodes.append(n)
                except (ValueError, TypeError) as e:
                    logger.warning(
                        f"Error parsing last_heard for node {n.get('long_name', 'Unknown')}: {e}"
                    )
                    continue

        # Sort by hops away
        active_nodes.sort(key=lambda x: x.get("hops_away", 0))

        if not active_nodes:
            diagram_lines.append("⚪ No active nodes detected")
            return "\n".join(diagram_lines)

        # Create hierarchical diagram
        diagram_lines.append("📡 Active Network Nodes:")
        diagram_lines.append("")

        # Group by hops
        hop_groups = {}
        for node in active_nodes:
            hops = node.get("hops_away", 0)
            if hops not in hop_groups:
                hop_groups[hops] = []
            hop_groups[hops].append(node)

        # Draw the network tree
        for hops in sorted(hop_groups.keys()):
            nodes_at_hop = hop_groups[hops]

            if hops == 0:
                diagram_lines.append("🏠 DIRECT CONNECTIONS (0 hops)")
            else:
                diagram_lines.append(f"🔗 HOP {hops} NODES")

            for i, node in enumerate(nodes_at_hop[:6]):  # Limit to 6 per hop
                # Get signal quality indicator
                snr = node.get("snr")
                if snr is not None:
                    if snr > 10:
                        signal_icon = "🟢"
                    elif snr > 5:
                        signal_icon = "🟡"
                    else:
                        signal_icon = "🔴"
                else:
                    signal_icon = "⚪"

                # Get battery indicator
                battery = node.get("battery_level")
                if battery is not None:
                    if battery > 80:
                        battery_icon = "🔋"
                    elif battery > 40:
                        battery_icon = "🪫"
                    else:
                        battery_icon = "🔴"
                else:
                    battery_icon = "❓"

                # Format node name
                node_name = node["long_name"][:15]
                if i == len(nodes_at_hop) - 1 and len(nodes_at_hop) > 6:
                    diagram_lines.append(
                        f"   └─ {signal_icon}{battery_icon} {node_name} +{len(nodes_at_hop) - 6} more"
                    )
                else:
                    diagram_lines.append(
                        f"   ├─ {signal_icon}{battery_icon} {node_name}"
                    )

            diagram_lines.append("")

        # Show connections if available
        if connections:
            diagram_lines.append("🔗 TOP CONNECTIONS:")
            for i, conn in enumerate(connections[:5]):
                from_name = self.database.get_node_display_name(conn["from_node"])[:12]
                to_name = self.database.get_node_display_name(conn["to_node"])[:12]
                msg_count = conn["message_count"]
                avg_hops = conn["avg_hops"]

                if i == len(connections[:5]) - 1 and len(connections) > 5:
                    diagram_lines.append(
                        f"   {from_name} ──→ {to_name} ({msg_count}msgs) +{len(connections) - 5} more"
                    )
                else:
                    diagram_lines.append(
                        f"   {from_name} ──→ {to_name} ({msg_count}msgs)"
                    )

        diagram_lines.append("")
        diagram_lines.append(
            "Legend: 🟢🟡🔴 Signal Quality | 🔋🪫🔴 Battery | ⚪❓ Unknown"
        )

        return "\n".join(diagram_lines)

    def _format_node_info(self, node: Dict[str, Any]) -> str:
        """Format node information for display"""
        try:
            long_name = str(node.get("long_name", "Unknown"))
            node_id = str(node.get("node_id", "Unknown"))
            node_num = str(node.get("node_num", "Unknown"))
            hops_away = str(node.get("hops_away", "0"))
            snr = str(node.get("snr", "?"))
            battery = (
                f"{node.get('battery_level', 'N/A')}%"
                if node.get("battery_level") is not None
                else "N/A"
            )
            temperature = (
                f"{node.get('temperature', 'N/A'):.1f}°C"
                if node.get("temperature") is not None
                else "N/A"
            )

            if node.get("last_heard"):
                try:
                    last_heard = datetime.fromisoformat(node["last_heard"])
                    time_str = last_heard.strftime("%H:%M:%S")
                except (ValueError, TypeError, KeyError) as e:
                    logger.debug(f"Error parsing last_heard timestamp: {e}")
                    time_str = "Unknown"
            else:
                time_str = "Unknown"

            return f"**{long_name}** (ID: {node_id}, Num: {node_num}) - Hops: {hops_away}, SNR: {snr}, Battery: {battery}, Temp: {temperature}, Last: {time_str}"

        except Exception as e:
            logger.error(f"Error formatting node info: {e}")
            return f"**Node {node.get('node_id', 'Unknown')}** - Error formatting data"

    def _truncate_text(self, text: str, width: int) -> str:
        """Truncate text to a fixed width using ASCII ellipsis."""
        if width <= 0:
            return ""
        if text is None:
            text = ""
        text = str(text)
        if len(text) <= width:
            return text
        if width <= 3:
            return text[:width]
        return text[: width - 3] + "..."

    def _format_age_short(self, last_heard: Optional[str]) -> str:
        """Return age as short text like 5m, 2h, 1d."""
        if not last_heard:
            return "--"
        dt = None
        try:
            if isinstance(last_heard, str):
                if "T" in last_heard:
                    dt = datetime.fromisoformat(last_heard.replace("Z", "+00:00"))
                else:
                    dt = datetime.strptime(last_heard, "%Y-%m-%d %H:%M:%S")
            elif isinstance(last_heard, datetime):
                dt = last_heard
        except Exception:
            dt = None
        if not dt:
            return "--"
        now = datetime.utcnow()
        if dt.tzinfo is not None:
            now = datetime.now(tz=dt.tzinfo)
        delta = now - dt
        minutes = int(delta.total_seconds() // 60)
        if minutes < 0:
            minutes = 0
        if minutes < 60:
            return f"{minutes}m"
        hours = minutes // 60
        if hours < 24:
            return f"{hours}h"
        days = hours // 24
        return f"{days}d"

    def _format_node_row(self, node: Dict[str, Any]) -> str:
        """Format a node row for compact, human-readable tables."""
        name = (
            node.get("long_name")
            or node.get("short_name")
            or node.get("node_id")
            or "Unknown"
        )
        name = self._truncate_text(str(name), 20)
        node_id = str(node.get("node_id") or "")
        node_id_short = node_id[-4:] if len(node_id) >= 4 else node_id or "--"
        batt = node.get("battery_level")
        volt = node.get("voltage")
        chan = node.get("channel_utilization")
        air = node.get("air_util_tx")
        uptime = node.get("uptime_seconds")
        last = self._format_age_short(node.get("last_heard"))

        def fmt_pct(val):
            try:
                return f"{int(val)}%"
            except Exception:
                return None

        def fmt_num(val, decimals=1):
            try:
                return f"{float(val):.{decimals}f}"
            except Exception:
                return None

        def fmt_uptime(seconds):
            if not seconds:
                return None
            try:
                seconds = float(seconds)
            except Exception:
                return None
            if seconds <= 0:
                return "0s"
            minutes = seconds / 60
            hours = minutes / 60
            days = hours / 24
            if days >= 1:
                return f"{days:.1f}d"
            if hours >= 1:
                return f"{hours:.1f}h"
            return f"{minutes:.0f}m"

        bat_txt = fmt_pct(batt) or "--"
        volt_txt = fmt_num(volt, decimals=3) or "--"
        chan_txt = fmt_num(chan, decimals=1) or "--"
        air_txt = fmt_num(air, decimals=1) or "--"
        up_txt = fmt_uptime(uptime) or "--"
        seen_txt = last or "--"
        city = node.get("approx_location") or ""
        city_txt = self._truncate_text(str(city), 16) if city else ""

        return (
            f"{node_id_short:<6} | {name:<20} | "
            f"{bat_txt:>5} | {volt_txt:>6} | "
            f"{chan_txt:>5} | {air_txt:>5} | {up_txt:>8} | {seen_txt:>6} | {city_txt:<16}"
        )

    def _chunk_lines(self, lines: List[str], max_chars: int) -> List[List[str]]:
        """Chunk lines to fit within Discord embed description limits."""
        chunks = []
        current = []
        current_len = 0
        for line in lines:
            line_len = len(line) + 1
            if current and current_len + line_len > max_chars:
                chunks.append(current)
                current = []
                current_len = 0
            current.append(line)
            current_len += line_len
        if current:
            chunks.append(current)
        return chunks

    async def _reverse_geocode(self, lat: float, lon: float) -> Optional[str]:
        """Lookup nearest locality using Nominatim (cached, best-effort)."""
        try:
            from config import BOT_CONFIG
            if not BOT_CONFIG.get("geocode_enabled", True):
                return None
            timeout = BOT_CONFIG.get("geocode_timeout", 3)
        except Exception:
            timeout = 3
        try:
            lat = float(lat)
            lon = float(lon)
        except Exception:
            return None

        key = f"{round(lat, 2)},{round(lon, 2)}"
        if key in self._geo_cache:
            return self._geo_cache[key]

        def _call():
            try:
                resp = requests.get(
                    "https://nominatim.openstreetmap.org/reverse",
                    params={"format": "json", "lat": lat, "lon": lon, "zoom": 10},
                    headers={"User-Agent": "meshtastic-discord-bot/1.0"},
                    timeout=timeout,
                )
                if resp.status_code != 200:
                    return None
                data = resp.json()
                addr = data.get("address", {})
                for field in ["city", "town", "village", "hamlet", "county"]:
                    if addr.get(field):
                        return addr[field]
                if data.get("display_name"):
                    return data["display_name"].split(",")[0]
            except Exception:
                return None
            return None

        city = await asyncio.to_thread(_call)
        if city:
            self._geo_cache[key] = city
        return city

    async def _attach_locations(self, nodes: List[Dict[str, Any]]):
        """Add approx_location to nodes when lat/lon available."""
        try:
            from config import BOT_CONFIG
            max_lookups = BOT_CONFIG.get("geocode_max_per_run", 20)
        except Exception:
            max_lookups = 20

        tasks = []
        for node in nodes or []:
            if node.get("approx_location"):
                continue
            lat = node.get("latitude")
            lon = node.get("longitude")
            if lat is None or lon is None:
                continue
            if len(tasks) >= max_lookups:
                break

            async def _set_loc(n: Dict[str, Any], la, lo):
                city = await self._reverse_geocode(la, lo)
                if city:
                    n["approx_location"] = city

            tasks.append(_set_loc(node, lat, lon))

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    def _build_nodes_tables(
        self,
        title: str,
        nodes: List[Dict[str, Any]],
        page_number: int,
        total_pages: int,
    ) -> List[str]:
        """Create plain code-block tables for node listings (telemetry-style)."""
        header = (
            f"{'ID':<6} | {'Name':<20} | {'Bat%':>5} | {'Volt':>6} | "
            f"{'Ch%':>5} | {'Air%':>5} | {'Uptime':>8} | {'Seen':>6} | {'City':<16}"
        )
        divider = "-" * len(header)
        rows = [self._format_node_row(node) for node in nodes]
        lines = [header, divider] + rows
        block_lines = lines
        chunks = self._chunk_lines(block_lines, 1800)
        return ["```text\n" + "\n".join(chunk) + "\n```" for chunk in chunks]

    def _build_topology_ascii(
        self, nodes: List[Dict[str, Any]], connections: List[Dict[str, Any]]
    ) -> List[str]:
        """Build a multiline ASCII topology view using hops and all known connections."""
        if not nodes:
            return ["No nodes available."]

        name_by_id: Dict[str, str] = {}
        hops_by_id: Dict[str, Optional[int]] = {}
        last_heard_by_id: Dict[str, Optional[str]] = {}
        via_by_id: Dict[str, Optional[str]] = {}
        for node in nodes:
            node_id = str(node.get("node_id") or "")
            name = (
                node.get("long_name")
                or node.get("short_name")
                or node_id
                or "Unknown"
            )
            name_by_id[node_id] = self._truncate_text(str(name), 20)
            hop_val = node.get("hops_away")
            try:
                hops_by_id[node_id] = int(hop_val) if hop_val is not None else None
            except Exception:
                hops_by_id[node_id] = None
            last_heard_by_id[node_id] = node.get("last_heard")
            relay_id = normalize_node_id(node.get("last_relay_node_id"))
            next_hop_id = normalize_node_id(node.get("next_hop_node_id"))
            via_id = relay_id or next_hop_id
            via_by_id[node_id] = via_id if via_id != node_id else None

        # Build adjacency from message-derived connections. Prefer to drop broadcast edges,
        # but if that leaves us empty, fall back to all edges so the view is never blank.
        def _build_adj(edge_list):
            adj: Dict[str, List[Dict[str, Any]]] = {}
            edges_out = []
            for conn in edge_list or []:
                from_id = str(conn.get("from_node") or "")
                to_id = str(conn.get("to_node") or "")
                if not from_id or not to_id:
                    continue
                msg_count = conn.get("message_count", 0) or 0
                avg_hops = conn.get("avg_hops", 0) or 0
                entry = {"node": to_id, "message_count": msg_count, "avg_hops": avg_hops}
                adj.setdefault(from_id, []).append(entry)
                entry_rev = {"node": from_id, "message_count": msg_count, "avg_hops": avg_hops}
                adj.setdefault(to_id, []).append(entry_rev)
                edges_out.append(
                    {"from": from_id, "to": to_id, "msg": msg_count, "avg_hops": avg_hops}
                )
            return adj, edges_out

        connections_no_broadcast = [
            c
            for c in connections or []
            if not str(c.get("from_node") or "").startswith("^")
            and not str(c.get("to_node") or "").startswith("^")
        ]
        adjacency, filtered_edges = _build_adj(connections_no_broadcast)
        if not adjacency and connections:
            adjacency, filtered_edges = _build_adj(connections)

        if not adjacency:
            return ["No recent connections found."]

        def _degree_score(node_id: str) -> int:
            return sum(n.get("message_count", 0) or 0 for n in adjacency.get(node_id, []))

        # Roots: prefer the bot's own node_id if known, else any hop 0, else top-degree.
        roots = []
        if hasattr(self, "my_node_id") and self.my_node_id:
            if self.my_node_id in adjacency:
                roots = [self.my_node_id]
        if not roots:
            roots = [nid for nid, hop in hops_by_id.items() if hop == 0 and nid in adjacency]
        if not roots:
            roots = sorted(adjacency.keys(), key=_degree_score, reverse=True)[:5]

        # Build probable parent mapping based on hops and message volume
        parent_for: Dict[str, str] = {}
        nodes_sorted = sorted(
            hops_by_id.keys(),
            key=lambda nid: (
                hops_by_id.get(nid) if hops_by_id.get(nid) is not None else 999,
                -_degree_score(nid),
            ),
        )
        for nid in nodes_sorted:
            if nid in roots:
                continue
            best_parent = None
            best_score = None
            for edge in adjacency.get(nid, []):
                neighbor = edge["node"]
                neighbor_hop = hops_by_id.get(neighbor)
                node_hop = hops_by_id.get(nid)
                msg = edge.get("message_count", 0) or 0
                avg_h = edge.get("avg_hops", 0) or 0
                # Prefer parents that are closer to root (lower hop), then higher traffic, then lower avg hops
                hop_rank = 0
                if neighbor_hop is None or node_hop is None:
                    hop_rank = 1
                elif neighbor_hop > node_hop:
                    hop_rank = 2
                score = (
                    hop_rank,
                    neighbor_hop if neighbor_hop is not None else 99,
                    -msg,
                    avg_h,
                )
                if best_score is None or score < best_score:
                    best_score = score
                    best_parent = neighbor
            if best_parent:
                parent_for[nid] = best_parent

        children: Dict[str, List[str]] = {}
        for child, parent in parent_for.items():
            children.setdefault(parent, []).append(child)

        def _edge_info(a: str, b: str) -> Dict[str, Any]:
            for edge in adjacency.get(a, []):
                if edge["node"] == b:
                    return edge
            return {}

        lines = ["MESH TOPOLOGY (last 24h)"]

        # Group by hop for clearer Meshtastic-style view
        hop_groups: Dict[str, List[str]] = {}
        for nid, hop in hops_by_id.items():
            key = str(hop) if hop is not None else "?"
            hop_groups.setdefault(key, []).append(nid)

        for hop in sorted(
            hop_groups.keys(),
            key=lambda h: (h == "?", int(h) if h.isdigit() else 999),
        ):
            lines.append("")
            lines.append(f"HOP {hop}")
            header = (
                f"{'ID':<5} {'Name':<24} {'Seen':>5} {'Via':<14} {'Msgs':>5} {'AvgH':>5}"
            )
            divider = "-" * len(header)
            lines.append(header)
            lines.append(divider)

            hop_nodes = sorted(
                hop_groups[hop],
                key=lambda nid: (-_degree_score(nid), name_by_id.get(nid, nid)),
            )
            for nid in hop_nodes:
                name = name_by_id.get(nid, nid) or nid
                short_id = nid[-4:] if len(nid) >= 4 else nid or "--"
                parent = via_by_id.get(nid) or parent_for.get(nid)
                edge = _edge_info(nid, parent) if parent else {}
                msg = edge.get("message_count")
                avg_h = edge.get("avg_hops")

                via_label = "--"
                if parent:
                    via_name = name_by_id.get(parent, parent) or parent
                    via_label = self._truncate_text(via_name, 14)
                elif str(hop) == "0":
                    via_label = "Direct"

                msg_txt = f"{int(msg):>5d}" if msg is not None else "   --"
                avg_txt = f"{avg_h:>5.1f}" if avg_h is not None else "   --"
                seen_txt = self._format_age_short(last_heard_by_id.get(nid))

                lines.append(
                    f"{short_id:<5} {name:<24} {seen_txt:>5} {via_label:<14} {msg_txt} {avg_txt}"
                )

        # Top links
        lines.append("")
        lines.append("")
        lines.append("Top links (by messages)")
        top_edges = sorted(filtered_edges, key=lambda e: e.get("msg", 0) or 0, reverse=True)[
            :12
        ]
        if not top_edges:
            lines.append("None")
        else:
            link_header = f"{'From':<24} {'To':<24} {'Msgs':>5} {'AvgH':>5}"
            lines.append(link_header)
            lines.append("-" * len(link_header))
            for edge in top_edges:
                f_id = edge["from"]
                t_id = edge["to"]
                msgs = edge.get("msg", 0) or 0
                avg_h = edge.get("avg_hops", 0) or 0
                f_name = name_by_id.get(f_id, f_id) or f_id
                t_name = name_by_id.get(t_id, t_id) or t_id
                f_label = self._truncate_text(f"{f_name}({f_id[-4:] or '--'})", 24)
                t_label = self._truncate_text(f"{t_name}({t_id[-4:] or '--'})", 24)
                lines.append(
                    f"{f_label:<24} {t_label:<24} {msgs:>5d} {avg_h:>5.1f}"
                )

        return lines

    def _create_connection_tree(self, nodes, connections):
        """Create readable ASCII tree for Discord showing network topology"""
        tree_lines = []
        tree_lines.append("🌐 MESH NETWORK TOPOLOGY")
        tree_lines.append("=" * 50)

        # Get active nodes (last 2 hours)
        active_nodes = []
        for n in nodes:
            if n.get("last_heard"):
                try:
                    last_heard = datetime.fromisoformat(
                        n["last_heard"].replace("Z", "+00:00")
                    )
                    if last_heard > datetime.now() - timedelta(hours=2):
                        active_nodes.append(n)
                except (ValueError, TypeError):
                    continue

        if not active_nodes:
            tree_lines.append("📡 No active nodes found")
            return "\n".join(tree_lines)

        # Build routing map
        routing_map = {}
        for conn in connections:
            from_node = conn["from_node"]
            to_node = conn["to_node"]
            if from_node not in routing_map:
                routing_map[from_node] = []
            if to_node not in routing_map:
                routing_map[to_node] = []
            routing_map[from_node].append(
                {"node": to_node, "msgs": conn["message_count"]}
            )
            routing_map[to_node].append(
                {"node": from_node, "msgs": conn["message_count"]}
            )

        # Sort and group by hops
        active_nodes.sort(key=lambda x: x.get("hops_away") or 0)
        hop_groups = {}
        for node in active_nodes:
            hops = node.get("hops_away") or 0
            if hops not in hop_groups:
                hop_groups[hops] = []
            hop_groups[hops].append(node)

        # Build readable tree
        for hops in sorted(hop_groups.keys()):
            nodes_at_hop = hop_groups[hops]

            # Hop header
            if hops == 0:
                tree_lines.append(f"\n📡 DIRECT CONNECTIONS (0 hops):")
            else:
                tree_lines.append(f"\n🔗 {hops} HOP{'S' if hops > 1 else ''} AWAY:")

            # Show nodes with better formatting
            for i, node in enumerate(nodes_at_hop):
                snr = node.get("snr")
                battery = node.get("battery_level")
                node_id = node.get("node_id")
                long_name = node.get("long_name", "Unknown")

                # Signal quality indicators
                if snr is not None:
                    if snr > 10:
                        sig_icon = "🟢"  # Good
                        sig_text = "Good"
                    elif snr > 5:
                        sig_icon = "🟡"  # OK
                        sig_text = "OK"
                    else:
                        sig_icon = "🔴"  # Poor
                        sig_text = "Poor"
                else:
                    sig_icon = "⚪"  # Unknown
                    sig_text = "Unknown"

                # Battery level
                if battery is not None:
                    if battery > 80:
                        bat_icon = "🔋"  # Full
                        bat_text = "Full"
                    elif battery > 40:
                        bat_icon = "🪫"  # Low
                        bat_text = "Low"
                    else:
                        bat_icon = "🔋"  # Empty
                        bat_text = "Empty"
                else:
                    bat_icon = "❓"  # Unknown
                    bat_text = "Unknown"

                # Node type
                node_type = "Router" if node.get("is_router") else "Client"
                type_icon = "📡" if node.get("is_router") else "📱"

                # Find routing parent
                via_text = ""
                if hops > 0 and node_id in routing_map:
                    for parent_hops in range(hops):
                        parent_nodes = hop_groups.get(parent_hops, [])
                        for parent in parent_nodes:
                            parent_id = parent.get("node_id")
                            if parent_id and parent_id in routing_map:
                                for route in routing_map[parent_id]:
                                    if route["node"] == node_id:
                                        via_text = f" via {parent.get('long_name', 'Unknown')[:15]}"
                                        break
                                if via_text:
                                    break
                        if via_text:
                            break

                # Format node line
                node_line = f"  {type_icon} {long_name[:20]:<20} | {sig_icon} {sig_text:<6} | {bat_icon} {bat_text:<6} | ID: {node_id}{via_text}"
                tree_lines.append(node_line)

        # Top connections
        if connections:
            tree_lines.append(f"\n🔗 TOP CONNECTIONS:")
            sorted_conns = sorted(
                connections, key=lambda x: x["message_count"], reverse=True
            )
            for i, conn in enumerate(sorted_conns[:5]):  # Top 5 connections
                from_name = self.database.get_node_display_name(conn["from_node"])[:15]
                to_name = self.database.get_node_display_name(conn["to_node"])[:15]
                msgs = conn["message_count"]
                avg_hops = conn.get("avg_hops", 0)

                tree_lines.append(
                    f"  {from_name} ↔ {to_name} ({msgs} msgs, {avg_hops:.1f} avg hops)"
                )

        return "\n".join(tree_lines)

    def _calculate_tree_depth(self, connections):
        """Calculate the maximum depth of the connection tree"""
        if not connections:
            return 0

        # Simple heuristic: max hops in connections + 1
        max_hops = 0
        for conn in connections:
            hops = conn.get("avg_hops", 0)
            if hops is not None and hops > max_hops:
                max_hops = hops

        return int(max_hops) + 1

    def _analyze_connection_quality(self, nodes, connections):
        """Analyze and summarize connection quality"""
        if not nodes or not connections:
            return "No connection data available"

        # Analyze signal quality
        excellent_signal = 0
        good_signal = 0
        poor_signal = 0

        for node in nodes:
            snr = node.get("snr")
            if snr is not None:
                if snr > 10:
                    excellent_signal += 1
                elif snr > 5:
                    good_signal += 1
                else:
                    poor_signal += 1

        total_with_signal = excellent_signal + good_signal + poor_signal

        if total_with_signal == 0:
            signal_quality = "No signal data"
        else:
            excellent_pct = (excellent_signal / total_with_signal) * 100
            good_pct = (good_signal / total_with_signal) * 100
            poor_pct = (poor_signal / total_with_signal) * 100

            if excellent_pct > 60:
                signal_quality = f"🟢 Excellent ({excellent_pct:.0f}%)"
            elif good_pct > 40:
                signal_quality = f"🟡 Good ({good_pct:.0f}%)"
            else:
                signal_quality = f"🔴 Poor ({poor_pct:.0f}%)"

        # Analyze battery health
        high_battery = sum(
            1
            for n in nodes
            if n.get("battery_level") is not None and n.get("battery_level") > 80
        )
        low_battery = sum(
            1
            for n in nodes
            if n.get("battery_level") is not None and n.get("battery_level") < 40
        )

        if high_battery > low_battery:
            battery_health = f"🔋 Good ({high_battery} high)"
        elif low_battery > 0:
            battery_health = f"🪫 Low ({low_battery} critical)"
        else:
            battery_health = "❓ Unknown"

        return f"""Signal: {signal_quality}
Battery: {battery_health}
Connections: {len(connections)} active"""

    async def _send_long_message(self, channel, message: str):
        """Send long messages by splitting if needed"""
        try:
            if len(message) <= 2000:
                await channel.send(message)
            else:
                # Split into chunks
                chunks = [message[i : i + 1900] for i in range(0, len(message), 1900)]
                for chunk in chunks:
                    await channel.send(chunk)
        except Exception as e:
            logger.error(f"Error sending long message: {e}")
            # Try to send a simple error message
            try:
                await channel.send("❌ Error sending message to channel.")
            except discord.HTTPException as e:
                logger.error(f"Failed to send error message to channel: {e}")
            except Exception as e:
                logger.error(f"Unexpected error sending error message: {e}")

    async def cmd_graph(self, message: discord.Message):
        """Show Unicode graph of telemetry history for a node"""
        content = message.content.strip()

        # Parse command: $graph <node> <metric>
        parts = content.split(maxsplit=2)
        if len(parts) < 3:
            await self._safe_send(
                message.channel,
                "❌ Use format: `$graph <node_name> <metric>`\n"
                "**Metrics:** battery, voltage, temperature, snr, rssi",
            )
            return

        try:
            node_name = parts[1]
            metric = parts[2].lower()

            # Find the node
            node = await self._run_db_query(
                self.database.find_node_by_name, node_name, timeout=5
            )
            if not node:
                await self._safe_send(
                    message.channel,
                    f"❌ No node found with name '{node_name}'. Try `$nodes` to see available nodes.",
                )
                return

            # Get telemetry history
            history = await self._run_db_query(
                self.database.get_telemetry_history,
                node["node_id"],
                hours=24,
                limit=100,
                timeout=5,
            )

            if not history or len(history) == 0:
                await self._safe_send(
                    message.channel,
                    f"📊 No telemetry history found for {node['long_name']}",
                )
                return

            # Extract metric values
            metric_map = {
                "battery": "battery_level",
                "voltage": "voltage",
                "temperature": "temperature",
                "temp": "temperature",
                "snr": "snr",
                "rssi": "rssi",
                "humidity": "humidity",
            }

            if metric not in metric_map:
                await self._safe_send(
                    message.channel,
                    f"❌ Unknown metric '{metric}'. Use: battery, voltage, temperature, snr, rssi, humidity",
                )
                return

            metric_field = metric_map[metric]
            values = [h.get(metric_field) for h in history]
            values = [v for v in values if v is not None]  # Filter None values

            if not values:
                await self._safe_send(
                    message.channel,
                    f"📊 No {metric} data found for {node['long_name']}",
                )
                return

            # Create graph
            graph = self._render_unicode_graph(values, width=40)

            # Calculate stats
            avg_val = sum(values) / len(values)
            min_val = min(values)
            max_val = max(values)
            current_val = values[-1] if values else None

            # Create embed
            embed = discord.Embed(
                title=f"📊 {metric.title()} Graph - {node['long_name']}",
                description=f"24-hour history ({len(values)} data points)",
                color=0x9B59B6,
                timestamp=get_utc_time(),
            )

            # Add graph
            embed.add_field(name=f"📈 Trend", value=f"```{graph}```", inline=False)

            # Add stats
            unit = ""
            if metric in ["battery"]:
                unit = "%"
            elif metric in ["voltage"]:
                unit = "V"
            elif metric in ["temperature"]:
                unit = "°C"
            elif metric in ["snr", "rssi"]:
                unit = "dB"

            embed.add_field(
                name="📊 Statistics",
                value=f"""**Current:** {current_val:.1f}{unit}
**Average:** {avg_val:.1f}{unit}
**Min:** {min_val:.1f}{unit}
**Max:** {max_val:.1f}{unit}
**Range:** {max_val - min_val:.1f}{unit}""",
                inline=True,
            )

            # Add trend indicator
            if len(values) >= 2:
                recent_avg = sum(values[-5:]) / min(5, len(values))
                older_avg = sum(values[:5]) / min(5, len(values))

                if recent_avg > older_avg * 1.1:
                    trend = "📈 Increasing"
                elif recent_avg < older_avg * 0.9:
                    trend = "📉 Decreasing"
                else:
                    trend = "➡️ Stable"

                embed.add_field(name="🎯 Trend", value=trend, inline=True)

            embed.set_footer(text=f"Node ID: {node['node_id']}")
            await message.channel.send(embed=embed)

        except Exception as e:
            logger.error(f"Error creating graph: {e}")
            await self._safe_send(
                message.channel, f"❌ Error creating graph: {str(e)[:100]}"
            )

    async def cmd_predict(self, message: discord.Message):
        """Predict battery life for a node"""
        content = message.content.strip()
        parts = content.split(maxsplit=1)

        if len(parts) < 2:
            await self._safe_send(
                message.channel, "❌ Use format: `$predict <node_name>`"
            )
            return

        try:
            node_name = parts[1]

            # Find the node
            node = await self._run_db_query(
                self.database.find_node_by_name, node_name, timeout=5
            )
            if not node:
                await self._safe_send(
                    message.channel, f"❌ No node found with name '{node_name}'"
                )
                return

            # Get battery history (last 7 days)
            history = await self._run_db_query(
                self.database.get_telemetry_history,
                node["node_id"],
                hours=168,
                limit=500,
                timeout=5,
            )

            if not history or len(history) < 2:
                await self._safe_send(
                    message.channel,
                    f"📊 Not enough battery data for {node['long_name']} to make predictions",
                )
                return

            # Extract battery values with timestamps
            battery_data = [
                (h.get("timestamp"), h.get("battery_level")) for h in history
            ]
            battery_data = [(t, b) for t, b in battery_data if t and b is not None]

            if len(battery_data) < 2:
                await self._safe_send(
                    message.channel, f"📊 Not enough battery data for predictions"
                )
                return

            # Sort by timestamp
            battery_data.sort(key=lambda x: x[0])

            # Calculate drain rate (% per hour)
            from datetime import datetime

            first_time = datetime.fromisoformat(
                battery_data[0][0].replace("Z", "+00:00")
            )
            last_time = datetime.fromisoformat(
                battery_data[-1][0].replace("Z", "+00:00")
            )
            first_battery = battery_data[0][1]
            last_battery = battery_data[-1][1]

            time_diff_hours = (last_time - first_time).total_seconds() / 3600
            if time_diff_hours == 0:
                await self._safe_send(
                    message.channel, "📊 Not enough time range for prediction"
                )
                return

            drain_rate = (first_battery - last_battery) / time_diff_hours

            # Current battery level
            current_battery = last_battery

            # Predict time to critical (10%)
            if drain_rate <= 0:
                prediction = "🔋 Battery is charging or stable!"
                color = 0x00FF00
            else:
                hours_remaining = (current_battery - 10) / drain_rate
                days_remaining = hours_remaining / 24

                if hours_remaining < 0:
                    prediction = "🔴 Battery CRITICAL - Already below 10%!"
                    color = 0xFF0000
                elif hours_remaining < 24:
                    prediction = (
                        f"🟡 ~{hours_remaining:.1f} hours remaining until critical"
                    )
                    color = 0xFFAA00
                else:
                    prediction = (
                        f"🟢 ~{days_remaining:.1f} days remaining until critical"
                    )
                    color = 0x00FF00

            # Create embed
            embed = discord.Embed(
                title=f"🔋 Battery Prediction - {node['long_name']}",
                description=f"Analysis based on {len(battery_data)} data points over {time_diff_hours / 24:.1f} days",
                color=color,
                timestamp=get_utc_time(),
            )

            embed.add_field(
                name="📊 Current Status",
                value=f"""**Battery:** {current_battery:.1f}%
**Drain Rate:** {drain_rate:.2f}% per hour
**Daily Drain:** {drain_rate * 24:.1f}% per day""",
                inline=True,
            )

            embed.add_field(name="🔮 Prediction", value=prediction, inline=True)

            # Battery trend graph
            battery_values = [b for _, b in battery_data[-40:]]  # Last 40 readings
            graph = self._render_unicode_graph(battery_values, width=30)
            embed.add_field(
                name="📈 Battery Trend (recent)", value=f"```{graph}```", inline=False
            )

            embed.set_footer(text=f"Node ID: {node['node_id']}")
            await message.channel.send(embed=embed)

        except Exception as e:
            logger.error(f"Error predicting battery: {e}")
            await self._safe_send(
                message.channel, f"❌ Error predicting battery: {str(e)[:100]}"
            )

    async def cmd_coverage(self, message: discord.Message):
        """Show network coverage analysis"""
        try:
            # Get all nodes
            nodes = await self._get_cached_data(
                "all_nodes", self.database.get_all_nodes, timeout=10
            )

            if not nodes:
                await self._safe_send(
                    message.channel, "📡 No nodes available for coverage analysis"
                )
                return

            # Categorize nodes by last heard time
            from datetime import datetime, timedelta, timezone

            now = datetime.now(timezone.utc)

            active_now = 0  # Last 5 minutes
            active_hour = 0  # Last hour
            active_day = 0  # Last 24 hours
            offline_24h = 0  # Not heard in 24+ hours

            for node in nodes:
                last_heard = node.get("last_heard")
                if not last_heard:
                    offline_24h += 1
                    continue

                try:
                    last_heard_dt = datetime.fromisoformat(
                        last_heard.replace("Z", "+00:00")
                    )
                    time_diff = (now - last_heard_dt).total_seconds() / 60  # minutes

                    if time_diff <= 5:
                        active_now += 1
                    if time_diff <= 60:
                        active_hour += 1
                    if time_diff <= 1440:  # 24 hours
                        active_day += 1
                    else:
                        offline_24h += 1
                except:
                    offline_24h += 1

            total_nodes = len(nodes)
            coverage_pct = (active_day / total_nodes * 100) if total_nodes > 0 else 0

            # Determine color
            if coverage_pct >= 80:
                color = 0x00FF00
            elif coverage_pct >= 60:
                color = 0xFFAA00
            else:
                color = 0xFF0000

            embed = discord.Embed(
                title="📡 Network Coverage Analysis",
                description=f"Coverage: {coverage_pct:.0f}% ({active_day}/{total_nodes} nodes)",
                color=color,
                timestamp=get_utc_time(),
            )

            embed.add_field(
                name="⏱️ Activity Timeline",
                value=f"""**Last 5 min:** {active_now} nodes
**Last hour:** {active_hour} nodes  
**Last 24h:** {active_day} nodes
**Offline >24h:** {offline_24h} nodes""",
                inline=True,
            )

            # Visual coverage bar
            active_bars = int(active_day / total_nodes * 10) if total_nodes > 0 else 0
            coverage_bar = "🟩" * active_bars + "⬜" * (10 - active_bars)

            embed.add_field(name="📊 Coverage", value=coverage_bar, inline=True)

            # Recommendations
            if offline_24h > total_nodes * 0.2:
                recommendation = f"⚠️ {offline_24h} nodes offline - check connectivity"
            elif coverage_pct >= 90:
                recommendation = "✅ Excellent coverage!"
            elif coverage_pct >= 70:
                recommendation = "👍 Good coverage"
            else:
                recommendation = "⚠️ Coverage needs improvement"

            embed.add_field(name="💡 Status", value=recommendation, inline=False)

            await message.channel.send(embed=embed)

        except Exception as e:
            logger.error(f"Error getting coverage: {e}")
            await self._safe_send(
                message.channel, f"❌ Error analyzing coverage: {str(e)[:100]}"
            )

    async def cmd_quality(self, message: discord.Message):
        """Show network quality score"""
        try:
            nodes = await self._get_cached_data(
                "all_nodes", self.database.get_all_nodes, timeout=10
            )
            stats = await self._run_db_query(
                self.database.get_message_statistics, hours=24, timeout=5
            )

            if not nodes:
                await self._safe_send(
                    message.channel, "📡 No nodes for quality analysis"
                )
                return

            # Calculate quality score (0-100)
            score_components = {}

            # 1. Node activity (40 points)
            from datetime import datetime, timedelta, timezone

            now = datetime.now(timezone.utc)
            active_nodes = 0
            for node in nodes:
                last_heard = node.get("last_heard")
                if last_heard:
                    try:
                        last_heard_dt = datetime.fromisoformat(
                            last_heard.replace("Z", "+00:00")
                        )
                        if (now - last_heard_dt).total_seconds() < 86400:  # 24 hours
                            active_nodes += 1
                    except:
                        pass

            activity_score = min(40, (active_nodes / len(nodes)) * 40)
            score_components["Node Activity"] = activity_score

            # 2. Signal quality (30 points)
            snr_values = [n.get("snr") for n in nodes if n.get("snr") is not None]
            if snr_values:
                avg_snr = sum(snr_values) / len(snr_values)
                # Good SNR is > 5, excellent is > 10
                snr_score = min(30, max(0, (avg_snr + 5) / 15 * 30))
            else:
                snr_score = 15  # Neutral
            score_components["Signal Quality"] = snr_score

            # 3. Battery health (15 points)
            battery_values = [
                n.get("battery_level")
                for n in nodes
                if n.get("battery_level") is not None
            ]
            if battery_values:
                avg_battery = sum(battery_values) / len(battery_values)
                battery_score = (avg_battery / 100) * 15
            else:
                battery_score = 7.5  # Neutral
            score_components["Battery Health"] = battery_score

            # 4. Message activity (15 points)
            if stats and stats.get("total_messages", 0) > 0:
                # Good if > 100 messages per day
                msg_score = min(15, (stats["total_messages"] / 100) * 15)
            else:
                msg_score = 0
            score_components["Message Activity"] = msg_score

            # Total score
            total_score = sum(score_components.values())

            # Determine color and grade
            if total_score >= 80:
                color = 0x00FF00
                grade = "A - Excellent"
                emoji = "🟢"
            elif total_score >= 65:
                color = 0x9ACD32
                grade = "B - Good"
                emoji = "🟡"
            elif total_score >= 50:
                color = 0xFFAA00
                grade = "C - Fair"
                emoji = "🟠"
            else:
                color = 0xFF0000
                grade = "D - Needs Improvement"
                emoji = "🔴"

            embed = discord.Embed(
                title="🎯 Network Quality Score",
                description=f"{emoji} **{total_score:.0f}/100** - Grade: {grade}",
                color=color,
                timestamp=get_utc_time(),
            )

            # Score breakdown
            breakdown = "\n".join(
                [f"**{k}:** {v:.0f} pts" for k, v in score_components.items()]
            )
            embed.add_field(name="📊 Score Breakdown", value=breakdown, inline=True)

            # Recommendations
            recommendations = []
            if activity_score < 30:
                recommendations.append(
                    f"📡 Low node activity ({active_nodes}/{len(nodes)} active)"
                )
            if snr_score < 20:
                recommendations.append(
                    f"📶 Weak signal quality (avg SNR: {avg_snr:.1f} dB)"
                    if snr_values
                    else "📶 No signal data"
                )
            if battery_score < 10:
                recommendations.append(
                    f"🔋 Low battery levels (avg: {avg_battery:.0f}%)"
                    if battery_values
                    else "🔋 No battery data"
                )

            if not recommendations:
                recommendations.append("✅ Network is performing well!")

            embed.add_field(
                name="💡 Recommendations",
                value="\n".join(recommendations[:3]),
                inline=False,
            )

            await message.channel.send(embed=embed)

        except Exception as e:
            logger.error(f"Error calculating quality: {e}")
            await self._safe_send(
                message.channel, f"❌ Error calculating quality: {str(e)[:100]}"
            )

    async def cmd_heatmap(self, message: discord.Message):
        """Show activity heatmap by hour"""
        try:
            stats = await self._run_db_query(
                self.database.get_message_statistics, hours=168, timeout=10
            )

            if not stats or not stats.get("hourly_distribution"):
                await self._safe_send(message.channel, "📊 Not enough data for heatmap")
                return

            hourly_dist = stats["hourly_distribution"]

            # Create 24-hour heatmap
            hours = list(range(24))
            hourly_counts = [hourly_dist.get(h, 0) for h in hours]

            if max(hourly_counts) == 0:
                await self._safe_send(
                    message.channel, "📊 No message activity in last 7 days"
                )
                return

            # Normalize to 0-8 scale for block characters
            max_count = max(hourly_counts)
            blocks = [" ", "▁", "▂", "▃", "▄", "▅", "▆", "▇", "█"]

            # Create two rows (0-11 and 12-23)
            row1_visual = ""
            row2_visual = ""

            for i in range(12):
                count = hourly_counts[i]
                normalized = int((count / max_count) * 8) if max_count > 0 else 0
                row1_visual += blocks[normalized]

            for i in range(12, 24):
                count = hourly_counts[i]
                normalized = int((count / max_count) * 8) if max_count > 0 else 0
                row2_visual += blocks[normalized]

            # Find peak and quiet hours
            peak_hour = hourly_counts.index(max(hourly_counts))
            quiet_hour = hourly_counts.index(min(hourly_counts))

            embed = discord.Embed(
                title="📊 Activity Heatmap - Last 7 Days",
                description="Message activity by hour of day",
                color=0x9B59B6,
                timestamp=get_utc_time(),
            )

            embed.add_field(
                name="🌅 Morning (00:00 - 11:59)",
                value=f"```{row1_visual}\n{''.join([f'{h:02d}' if h % 2 == 0 else '  ' for h in range(12)])}```",
                inline=False,
            )

            embed.add_field(
                name="🌙 Evening (12:00 - 23:59)",
                value=f"```{row2_visual}\n{''.join([f'{h:02d}' if h % 2 == 0 else '  ' for h in range(12, 24)])}```",
                inline=False,
            )

            embed.add_field(
                name="📈 Peak Activity",
                value=f"**Hour:** {peak_hour}:00\n**Messages:** {hourly_counts[peak_hour]}",
                inline=True,
            )

            embed.add_field(
                name="📉 Quiet Period",
                value=f"**Hour:** {quiet_hour}:00\n**Messages:** {hourly_counts[quiet_hour]}",
                inline=True,
            )

            await message.channel.send(embed=embed)

        except Exception as e:
            logger.error(f"Error creating heatmap: {e}")
            await self._safe_send(
                message.channel, f"❌ Error creating heatmap: {str(e)[:100]}"
            )

    async def cmd_compare(self, message: discord.Message):
        """Compare two nodes"""
        content = message.content.strip()
        parts = content.split(maxsplit=2)

        if len(parts) < 3:
            await self._safe_send(
                message.channel, "❌ Use format: `$compare <node1> <node2>`"
            )
            return

        try:
            node1_name = parts[1]
            node2_name = parts[2]

            # Find both nodes
            node1 = await self._run_db_query(
                self.database.find_node_by_name, node1_name, timeout=5
            )
            node2 = await self._run_db_query(
                self.database.find_node_by_name, node2_name, timeout=5
            )

            if not node1:
                await self._safe_send(
                    message.channel, f"❌ Node '{node1_name}' not found"
                )
                return
            if not node2:
                await self._safe_send(
                    message.channel, f"❌ Node '{node2_name}' not found"
                )
                return

            embed = discord.Embed(
                title="⚖️ Node Comparison",
                description=f"**{node1['long_name']}** vs **{node2['long_name']}**",
                color=0x00BFFF,
                timestamp=get_utc_time(),
            )

            # Battery comparison
            bat1 = node1.get("battery_level")
            bat2 = node2.get("battery_level")
            if bat1 is not None and bat2 is not None:
                winner = "=" if abs(bat1 - bat2) < 5 else ("→" if bat1 > bat2 else "←")
                embed.add_field(
                    name="🔋 Battery",
                    value=f"{bat1:.0f}% {winner} {bat2:.0f}%",
                    inline=True,
                )

            # Signal comparison
            snr1 = node1.get("snr")
            snr2 = node2.get("snr")
            if snr1 is not None and snr2 is not None:
                winner = "=" if abs(snr1 - snr2) < 2 else ("→" if snr1 > snr2 else "←")
                embed.add_field(
                    name="📡 Signal (SNR)",
                    value=f"{snr1:.1f} dB {winner} {snr2:.1f} dB",
                    inline=True,
                )

            # Last heard comparison
            from datetime import datetime

            lh1 = node1.get("last_heard")
            lh2 = node2.get("last_heard")
            if lh1 and lh2:
                try:
                    dt1 = datetime.fromisoformat(lh1.replace("Z", "+00:00"))
                    dt2 = datetime.fromisoformat(lh2.replace("Z", "+00:00"))
                    winner = "→" if dt1 > dt2 else ("←" if dt2 > dt1 else "=")

                    now = datetime.utcnow()
                    age1 = (now - dt1).total_seconds() / 60
                    age2 = (now - dt2).total_seconds() / 60

                    embed.add_field(
                        name="⏱️ Last Heard",
                        value=f"{age1:.0f}m ago {winner} {age2:.0f}m ago",
                        inline=True,
                    )
                except:
                    pass

            # Temperature comparison
            temp1 = node1.get("temperature")
            temp2 = node2.get("temperature")
            if temp1 is not None and temp2 is not None:
                winner = (
                    "=" if abs(temp1 - temp2) < 2 else ("→" if temp1 < temp2 else "←")
                )  # Lower temp is better
                embed.add_field(
                    name="🌡️ Temperature",
                    value=f"{temp1:.1f}°C {winner} {temp2:.1f}°C",
                    inline=True,
                )

            # Node IDs
            embed.add_field(
                name="🆔 Node IDs",
                value=f"`{node1['node_id']}`\nvs\n`{node2['node_id']}`",
                inline=False,
            )

            await message.channel.send(embed=embed)

        except Exception as e:
            logger.error(f"Error comparing nodes: {e}")
            await self._safe_send(
                message.channel, f"❌ Error comparing nodes: {str(e)[:100]}"
            )

    async def cmd_trends(self, message: discord.Message):
        """Show 7-day network trends"""
        try:
            # Get current stats
            current_nodes = await self._get_cached_data(
                "all_nodes", self.database.get_all_nodes, timeout=10
            )
            current_stats = await self._run_db_query(
                self.database.get_message_statistics, hours=24, timeout=5
            )

            # Get week-old stats (this is a simplified version - ideally we'd store historical snapshots)
            week_stats = await self._run_db_query(
                self.database.get_message_statistics, hours=168, timeout=5
            )

            if not current_nodes or not current_stats:
                await self._safe_send(message.channel, "📊 Not enough data for trends")
                return

            embed = discord.Embed(
                title="📈 Network Trends - 7 Days",
                description="Comparing today vs last week",
                color=0x9B59B6,
                timestamp=get_utc_time(),
            )

            # Active nodes trend
            from datetime import datetime, timedelta, timezone

            now = datetime.now(timezone.utc)
            active_now = sum(
                1
                for n in current_nodes
                if n.get("last_heard")
                and (
                    now - datetime.fromisoformat(n["last_heard"].replace("Z", "+00:00"))
                ).total_seconds()
                < 86400
            )
            total_nodes = len(current_nodes)

            embed.add_field(
                name="📡 Active Nodes (24h)",
                value=f"**Current:** {active_now}/{total_nodes}\n**Trend:** Data collection in progress",
                inline=True,
            )

            # Message trend
            if current_stats.get("total_messages"):
                msg_today = current_stats["total_messages"]
                avg_per_day = (
                    week_stats.get("total_messages", 0) / 7 if week_stats else msg_today
                )

                if msg_today > avg_per_day * 1.1:
                    trend = f"📈 +{((msg_today / avg_per_day - 1) * 100):.0f}% vs avg"
                elif msg_today < avg_per_day * 0.9:
                    trend = f"📉 -{((1 - msg_today / avg_per_day) * 100):.0f}% vs avg"
                else:
                    trend = "➡️ Stable"

                embed.add_field(
                    name="💬 Messages Today",
                    value=f"**Count:** {msg_today}\n**Trend:** {trend}",
                    inline=True,
                )

            # Battery trend
            battery_values = [
                n.get("battery_level")
                for n in current_nodes
                if n.get("battery_level") is not None
            ]
            if battery_values:
                avg_battery = sum(battery_values) / len(battery_values)

                # Show battery graph
                battery_graph = self._render_unicode_graph(
                    battery_values[:20], width=20
                )

                embed.add_field(
                    name="🔋 Average Battery",
                    value=f"**Current:** {avg_battery:.0f}%\n**Sample:** `{battery_graph}`",
                    inline=False,
                )

            # Signal trend
            snr_values = [
                n.get("snr") for n in current_nodes if n.get("snr") is not None
            ]
            if snr_values:
                avg_snr = sum(snr_values) / len(snr_values)

                embed.add_field(
                    name="📶 Average Signal",
                    value=f"**SNR:** {avg_snr:.1f} dB\n**Quality:** {'Excellent' if avg_snr > 10 else 'Good' if avg_snr > 5 else 'Fair'}",
                    inline=True,
                )

            embed.set_footer(text="💡 Trends are based on available data")
            await message.channel.send(embed=embed)

        except Exception as e:
            logger.error(f"Error showing trends: {e}")
            await self._safe_send(
                message.channel, f"❌ Error showing trends: {str(e)[:100]}"
            )

    async def cmd_path(self, message: discord.Message):
        """Show routing path between two nodes"""
        content = message.content.strip()
        parts = content.split(maxsplit=2)

        if len(parts) < 3:
            await self._safe_send(
                message.channel, "❌ Use format: `$path <node1> <node2>`"
            )
            return

        try:
            node1_name = parts[1]
            node2_name = parts[2]

            # Find both nodes
            node1 = await self._run_db_query(
                self.database.find_node_by_name, node1_name, timeout=5
            )
            node2 = await self._run_db_query(
                self.database.find_node_by_name, node2_name, timeout=5
            )

            if not node1:
                await self._safe_send(
                    message.channel, f"❌ Node '{node1_name}' not found"
                )
                return
            if not node2:
                await self._safe_send(
                    message.channel, f"❌ Node '{node2_name}' not found"
                )
                return

            # Get topology
            topology = await self._run_db_query(
                self.database.get_network_topology, timeout=10
            )

            if not topology or not topology.get("connections"):
                await self._safe_send(message.channel, "📡 No routing data available")
                return

            # Simple pathfinding - look for connections
            connections = topology["connections"]

            # Find direct connection
            direct_conn = None
            for conn in connections:
                if (
                    conn["from_node"] == node1["node_id"]
                    and conn["to_node"] == node2["node_id"]
                ) or (
                    conn["from_node"] == node2["node_id"]
                    and conn["to_node"] == node1["node_id"]
                ):
                    direct_conn = conn
                    break

            embed = discord.Embed(
                title="🛣️ Routing Path Analysis",
                description=f"**{node1['long_name']}** → **{node2['long_name']}**",
                color=0x00BFFF,
                timestamp=get_utc_time(),
            )

            if direct_conn:
                # Direct connection found
                embed.add_field(
                    name="✅ Direct Connection",
                    value=f"**Hops:** {direct_conn.get('avg_hops', 0):.1f}\n**Messages:** {direct_conn.get('message_count', 0)}\n**SNR:** {direct_conn.get('avg_snr', 0):.1f} dB",
                    inline=True,
                )

                # Quality assessment
                quality = (
                    "🟢 Excellent"
                    if direct_conn.get("avg_snr", 0) > 10
                    else "🟡 Good"
                    if direct_conn.get("avg_snr", 0) > 5
                    else "🟠 Fair"
                    if direct_conn.get("avg_snr", 0) > 0
                    else "⚫ Unknown"
                )

                embed.add_field(name="📶 Link Quality", value=quality, inline=True)
            else:
                # No direct connection - look for intermediate nodes
                intermediate_nodes = []
                for conn in connections[:10]:  # Check top connections
                    if (
                        conn["from_node"] == node1["node_id"]
                        or conn["to_node"] == node1["node_id"]
                    ):
                        other_node = (
                            conn["to_node"]
                            if conn["from_node"] == node1["node_id"]
                            else conn["from_node"]
                        )
                        if other_node != node2["node_id"]:
                            intermediate_nodes.append(other_node[:16])  # Truncate ID

                if intermediate_nodes:
                    embed.add_field(
                        name="🔀 Possible Routes",
                        value=f"Path may go through:\n`{', '.join(intermediate_nodes[:5])}`",
                        inline=False,
                    )
                else:
                    embed.add_field(
                        name="❌ No Direct Path",
                        value="No routing data found between these nodes",
                        inline=False,
                    )

            embed.set_footer(text=f"{node1['node_id']} → {node2['node_id']}")
            await message.channel.send(embed=embed)

        except Exception as e:
            logger.error(f"Error analyzing path: {e}")
            await self._safe_send(
                message.channel, f"❌ Error analyzing path: {str(e)[:100]}"
            )

    def _is_admin(self, message: discord.Message) -> bool:
        try:
            if message.guild is None:
                return True
            perms = getattr(message.author, "guild_permissions", None)
            if perms is None:
                return False
            return bool(perms.administrator or perms.manage_guild)
        except Exception:
            return False

    def _parse_timestamp(self, ts: Optional[str]) -> Optional[datetime]:
        if not ts:
            return None
        try:
            return datetime.fromisoformat(ts.replace("Z", "+00:00"))
        except Exception:
            pass
        try:
            return datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
        except Exception:
            return None

    def _format_age(self, ts: Optional[str]) -> str:
        dt = self._parse_timestamp(ts) if isinstance(ts, str) else ts
        if not dt:
            return "unknown"
        now = datetime.utcnow().replace(tzinfo=dt.tzinfo)
        delta = now - dt
        seconds = max(0, int(delta.total_seconds()))
        if seconds < 60:
            return f"{seconds}s"
        minutes = seconds // 60
        if minutes < 60:
            return f"{minutes}m"
        hours = minutes // 60
        if hours < 24:
            return f"{hours}h"
        days = hours // 24
        return f"{days}d"

    def _build_sparkline(self, values: List[float]) -> str:
        if not values:
            return ""
        clean = []
        for val in values:
            try:
                clean.append(float(val))
            except Exception:
                continue
        if not clean:
            return ""
        vmin = min(clean)
        vmax = max(clean)
        if vmax == vmin:
            return "-" * len(clean)
        blocks = ["▁", "▂", "▃", "▄", "▅", "▆", "▇", "█"]
        span = vmax - vmin
        chars = []
        for val in clean:
            idx = int((val - vmin) / span * (len(blocks) - 1))
            chars.append(blocks[max(0, min(idx, len(blocks) - 1))])
        return "".join(chars)

    def _metric_slope_per_hour(self, history: List[Dict[str, Any]]) -> Optional[float]:
        points = []
        for entry in history:
            val = entry.get("value")
            ts = entry.get("timestamp")
            dt = self._parse_timestamp(ts) if isinstance(ts, str) else None
            if dt is None or val is None:
                continue
            try:
                points.append((dt, float(val)))
            except Exception:
                continue
        if len(points) < 2:
            return None
        points.sort(key=lambda item: item[0])
        dt_seconds = (points[-1][0] - points[0][0]).total_seconds()
        if dt_seconds <= 0:
            return None
        return (points[-1][1] - points[0][1]) / (dt_seconds / 3600)

    def _compute_health_score(
        self,
        node: Dict[str, Any],
        message_rate: Optional[float],
        battery_slope: Optional[float],
    ) -> Tuple[Optional[float], Dict[str, float]]:
        weights = HEALTH_SCORE.get("weights", {})
        score_components = {}
        weighted = 0.0
        total_weight = 0.0

        if battery_slope is not None:
            floor_val = HEALTH_SCORE.get("battery_slope_floor", -5.0)
            ceil_val = HEALTH_SCORE.get("battery_slope_ceiling", 0.0)
            if battery_slope <= floor_val:
                slope_score = 0.0
            elif battery_slope >= ceil_val:
                slope_score = 1.0
            else:
                slope_score = (battery_slope - floor_val) / (ceil_val - floor_val)
            score_components["battery_slope"] = slope_score * 100
            weight = float(weights.get("battery_slope", 0))
            weighted += slope_score * weight
            total_weight += weight

        uptime_seconds = node.get("uptime_seconds")
        if uptime_seconds is not None:
            try:
                uptime_hours = float(uptime_seconds) / 3600.0
                target_hours = float(HEALTH_SCORE.get("uptime_hours_target", 24.0))
                uptime_score = min(1.0, max(0.0, uptime_hours / target_hours))
                score_components["uptime"] = uptime_score * 100
                weight = float(weights.get("uptime", 0))
                weighted += uptime_score * weight
                total_weight += weight
            except Exception:
                pass

        last_heard = node.get("last_heard")
        last_dt = self._parse_timestamp(last_heard) if isinstance(last_heard, str) else None
        if last_dt:
            age_minutes = (datetime.utcnow() - last_dt.replace(tzinfo=None)).total_seconds() / 60
            good = float(HEALTH_SCORE.get("last_heard_good_minutes", 10))
            bad = float(HEALTH_SCORE.get("last_heard_bad_minutes", 120))
            if age_minutes <= good:
                heard_score = 1.0
            elif age_minutes >= bad:
                heard_score = 0.0
            else:
                heard_score = 1.0 - ((age_minutes - good) / (bad - good))
            score_components["last_heard"] = heard_score * 100
            weight = float(weights.get("last_heard", 0))
            weighted += heard_score * weight
            total_weight += weight

        if message_rate is not None:
            good_rate = float(HEALTH_SCORE.get("packet_rate_good_per_hour", 10.0))
            bad_rate = float(HEALTH_SCORE.get("packet_rate_bad_per_hour", 0.0))
            if message_rate <= bad_rate:
                rate_score = 0.0
            elif message_rate >= good_rate:
                rate_score = 1.0
            else:
                rate_score = (message_rate - bad_rate) / (good_rate - bad_rate)
            score_components["packet_rate"] = rate_score * 100
            weight = float(weights.get("packet_rate", 0))
            weighted += rate_score * weight
            total_weight += weight

        if total_weight <= 0:
            return None, score_components

        return round((weighted / total_weight) * 100, 1), score_components

    def _haversine_m(self, lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        try:
            import math

            r = 6371000.0
            lat1_r = math.radians(lat1)
            lat2_r = math.radians(lat2)
            dlat = lat2_r - lat1_r
            dlon = math.radians(lon2 - lon1)
            a = (
                math.sin(dlat / 2) ** 2
                + math.cos(lat1_r) * math.cos(lat2_r) * math.sin(dlon / 2) ** 2
            )
            c = 2 * math.asin(math.sqrt(a))
            return r * c
        except Exception:
            return 0.0

    def _build_static_map_url(self, lat: float, lon: float) -> str:
        base_url = MAP_SNAPSHOT.get("base_url", "")
        if not base_url:
            return ""
        params = {
            "center": f"{lat:.5f},{lon:.5f}",
            "zoom": int(MAP_SNAPSHOT.get("zoom", 13)),
            "size": MAP_SNAPSHOT.get("size", "600x400"),
            "markers": f"{lat:.5f},{lon:.5f},{MAP_SNAPSHOT.get('marker_color', 'red')}",
        }
        return f"{base_url}?{urlencode(params, safe=',:')}"

    async def _get_map_snapshot(self, lat: float, lon: float) -> Optional[Dict[str, str]]:
        if not MAP_SNAPSHOT.get("enabled", True):
            return None
        url = self._build_static_map_url(lat, lon)
        if not url:
            return None
        if not MAP_SNAPSHOT.get("cache_enabled", True):
            return {"url": url}

        cache_dir = MAP_SNAPSHOT.get("cache_dir", "data/map_cache")
        try:
            os.makedirs(cache_dir, exist_ok=True)
        except Exception:
            return {"url": url}

        key = f"{lat:.5f}:{lon:.5f}:{MAP_SNAPSHOT.get('zoom')}:{MAP_SNAPSHOT.get('size')}:{MAP_SNAPSHOT.get('marker_color')}"
        filename = hashlib.sha1(key.encode("utf-8")).hexdigest() + ".png"
        path = os.path.join(cache_dir, filename)
        ttl_hours = float(MAP_SNAPSHOT.get("cache_ttl_hours", 24))
        if os.path.exists(path):
            try:
                age_seconds = time.time() - os.path.getmtime(path)
                if age_seconds < ttl_hours * 3600:
                    return {"path": path}
            except Exception:
                pass

        timeout = float(MAP_SNAPSHOT.get("timeout_seconds", 5))

        def _download():
            try:
                resp = requests.get(url, timeout=timeout)
                if resp.ok and resp.content:
                    with open(path, "wb") as handle:
                        handle.write(resp.content)
                    return True
            except Exception:
                return False
            return False

        try:
            ok = await asyncio.to_thread(_download)
        except Exception:
            ok = False
        if ok and os.path.exists(path):
            return {"path": path}
        return {"url": url}

    async def _safe_send(self, channel, message: str):
        """Safely send a message to a channel with error handling"""
        try:
            await channel.send(message)
        except Exception as e:
            logger.error(f"Error sending message to channel: {e}")


class DiscordBot(discord.Client):
    """Enhanced Discord bot with Meshtastic integration and database"""

    def __init__(
        self,
        config: Config,
        meshtastic: MeshtasticInterface,
        database: MeshtasticDatabase,
    ):
        intents = discord.Intents.default()
        intents.message_content = True

        super().__init__(intents=intents)
        self.config = config
        self.meshtastic = meshtastic
        self.database = database
        self.my_node_id = None

        # Queues for communication with size limits to prevent memory exhaustion
        self.mesh_to_discord = queue.Queue(maxsize=1000)  # Limit to 1000 messages
        self.discord_to_mesh = queue.Queue(
            maxsize=100
        )  # Limit to 100 outgoing messages

        # Initialize command handler after queues are created (event loop will be set later)
        self.command_handler = CommandHandler(
            meshtastic, self.discord_to_mesh, database
        )
        self._last_mesh_rx_time = time.time()
        try:
            self._watchdog_threshold = max(
                0,
                int(getattr(config, "connection_watchdog_minutes", 10)) * 60,
            )
        except Exception:
            self._watchdog_threshold = 600
        self.command_handler.mesh_to_discord_queue = self.mesh_to_discord

        # Background task
        self.bg_task = None
        self.telemetry_task = None
        self.high_altitude_task_handle = None
        self.db_maintenance_task_handle = None
        self.flight_monitor_task_handle = None

        # Track last telemetry update hour
        self.last_telemetry_hour = datetime.now().hour

        # High altitude alert cooldown map: node_id -> last_alert_utc_str
        self._high_alt_alerts = {}
        # Movement alert cooldown map: node_id -> last_alert_epoch
        self._movement_alerts = {}
        # Flight lookup cooldown map: node_id -> last_lookup_epoch
        self._flight_lookup_last = {}
        # Flight tracking state keyed by node_id
        self._flight_tracks = {}
        # Nodes that already received initial Safe Flight message
        self._flight_initial_safe_sent = set()

        # Restore persisted queues if configured
        self._load_persisted_queues()

    def _queue_policy(self, queue_name: str) -> str:
        policy = QUEUE_PERSISTENCE.get("policy", {})
        return str(policy.get(queue_name, "drop_old")).lower()

    def _load_persisted_queues(self) -> None:
        if not QUEUE_PERSISTENCE.get("enabled", True):
            return
        path = QUEUE_PERSISTENCE.get("path", "data/queue_state.json")
        if not path or not os.path.exists(path):
            return
        try:
            with open(path, "r", encoding="utf-8") as handle:
                data = json.load(handle)
        except Exception as e:
            logger.warning(f"Failed to load persisted queues: {e}")
            return

        saved_at = data.get("saved_at")
        try:
            age_seconds = time.time() - float(saved_at or 0)
        except Exception:
            age_seconds = None
        max_age = float(QUEUE_PERSISTENCE.get("max_age_seconds", 3600))

        queues = data.get("queues", {})
        restored = {"discord_to_mesh": 0, "mesh_to_discord": 0}
        dropped = {"discord_to_mesh": 0, "mesh_to_discord": 0}

        for name, target_queue in (
            ("discord_to_mesh", self.discord_to_mesh),
            ("mesh_to_discord", self.mesh_to_discord),
        ):
            items = queues.get(name, [])
            policy = self._queue_policy(name)
            if policy == "drop":
                dropped[name] = len(items) if isinstance(items, list) else 0
                continue
            if policy == "drop_old" and age_seconds is not None and age_seconds > max_age:
                dropped[name] = len(items) if isinstance(items, list) else 0
                continue
            if not isinstance(items, list):
                continue
            for item in items:
                try:
                    target_queue.put_nowait(item)
                    restored[name] += 1
                except queue.Full:
                    dropped[name] += 1
                except Exception:
                    dropped[name] += 1

        logger.info(
            "Queue restore complete: discord_to_mesh=%s restored, %s dropped; mesh_to_discord=%s restored, %s dropped",
            restored["discord_to_mesh"],
            dropped["discord_to_mesh"],
            restored["mesh_to_discord"],
            dropped["mesh_to_discord"],
        )

    def _save_persisted_queues(self) -> None:
        if not QUEUE_PERSISTENCE.get("enabled", True):
            return
        path = QUEUE_PERSISTENCE.get("path", "data/queue_state.json")
        if not path:
            return
        data = {
            "saved_at": time.time(),
            "queues": {
                "discord_to_mesh": list(self.discord_to_mesh.queue),
                "mesh_to_discord": list(self.mesh_to_discord.queue),
            },
        }
        try:
            os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
            with open(path, "w", encoding="utf-8") as handle:
                json.dump(data, handle, ensure_ascii=True, default=str)
            logger.info(
                "Persisted queues: discord_to_mesh=%s, mesh_to_discord=%s",
                len(data["queues"]["discord_to_mesh"]),
                len(data["queues"]["mesh_to_discord"]),
            )
        except Exception as e:
            logger.warning(f"Failed to persist queues: {e}")

    def _get_adsb_config(self) -> Dict[str, Any]:
        defaults = {
            "endpoint_url": "http://127.0.0.1:8090/data/aircraft.json",
            "webui_base_url": "https://adsb.canadaverse.org/?icao=",
            "timeout_seconds": 2.5,
            "cooldown_seconds": 10,
            "flight_altitude_threshold_m": 5000,
            "probable_match_threshold": 0.8,
            "max_distance_km": 80,
            "max_altitude_delta_m": 2500,
            "max_seen_pos_seconds": 60,
            "mesh_out_of_range_seconds": 300,
            "adsb_out_of_range_seconds": 120,
            "flight_monitor_interval_seconds": 30,
            "dist_sigma_km": 20,
            "alt_sigma_m": 1200,
            "seen_sigma_s": 15,
            "min_callsign_length": 4,
        }
        try:
            cfg = dict(ADSB_LOOKUP)
        except Exception:
            cfg = {}
        defaults.update(cfg)
        return defaults

    def _normalize_callsign(self, callsign: str) -> str:
        return re.sub(r"\s+", "", callsign or "").upper()

    def _extract_callsigns(self, text: str) -> List[str]:
        if not text:
            return []
        cfg = self._get_adsb_config()
        upper = text.upper()
        pattern = re.compile(r"\b([A-Z]{2,3}\s?\d{1,4}[A-Z]?)\b")
        candidates = []
        for match in pattern.findall(upper):
            normalized = self._normalize_callsign(match)
            if len(normalized) < int(cfg.get("min_callsign_length", 4)):
                continue
            if normalized.startswith("FL") and normalized[2:].isdigit():
                continue
            if not any(ch.isdigit() for ch in normalized):
                continue
            candidates.append(normalized)
        seen = set()
        deduped = []
        for item in candidates:
            if item in seen:
                continue
            seen.add(item)
            deduped.append(item)
        return deduped

    def _text_indicates_airborne(self, text: str) -> bool:
        if not text:
            return False
        lowered = text.lower()
        keywords = [
            "in the air",
            "airborne",
            "cruise",
            "cruising",
            "flight level",
            "takeoff",
            "taking off",
            "landing",
            "on approach",
            "climbing",
            "descending",
        ]
        if any(word in lowered for word in keywords):
            return True
        if re.search(r"\bfl\s?\d{2,3}\b", lowered):
            return True
        if re.search(r"\b\d{2,5}\s?(ft|feet)\b", lowered):
            return True
        return False

    def _should_rate_limit_flight_lookup(
        self, node_id: str, cooldown_seconds: float
    ) -> bool:
        now = time.time()
        last_ts = self._flight_lookup_last.get(node_id, 0)
        if now - last_ts < cooldown_seconds:
            return True
        self._flight_lookup_last[node_id] = now
        return False

    def _schedule_flight_lookup(
        self,
        from_id: str,
        from_name: str,
        text: Optional[str] = None,
        position: Optional[Dict[str, Any]] = None,
        airborne_hint: bool = False,
    ) -> None:
        try:
            loop = self.loop
        except Exception:
            return
        if not loop or loop.is_closed():
            return
        asyncio.run_coroutine_threadsafe(
            self._run_flight_lookup(from_id, from_name, text, position, airborne_hint),
            loop,
        )

    async def _run_flight_lookup(
        self,
        from_id: str,
        from_name: str,
        text: Optional[str],
        position: Optional[Dict[str, Any]],
        airborne_hint: bool,
    ) -> None:
        cfg = self._get_adsb_config()
        cooldown_seconds = float(cfg.get("cooldown_seconds", 10))
        if self._should_rate_limit_flight_lookup(from_id, cooldown_seconds):
            return

        callsigns = self._extract_callsigns(text or "")
        last_position = position
        if last_position is None and self.database:
            try:
                last_position = await asyncio.to_thread(
                    self.database.get_last_position, from_id
                )
            except Exception:
                last_position = None

        if not callsigns:
            if last_position:
                altitude = last_position.get("altitude")
                if altitude is not None:
                    try:
                        altitude = float(altitude)
                    except Exception:
                        altitude = None
                if altitude is not None and altitude < float(
                    cfg.get("flight_altitude_threshold_m", 5000)
                ):
                    if not airborne_hint:
                        return
            elif not airborne_hint:
                return

        self._maybe_send_initial_safe_flight(from_id)

        aircraft = await self._fetch_adsb_aircraft(cfg)
        if not aircraft:
            return

        match = self._select_best_aircraft(aircraft, last_position, callsigns, cfg)
        if match:
            await self._announce_flight_match(
                from_id, from_name, match, last_position, callsigns, cfg
            )
            return

        if callsigns:
            await self._request_flight_number(
                from_id,
                from_name,
                text or "",
                cfg,
                reason="Unable to match that flight number. Please repeat it.",
                alert_type="flight_not_found",
            )
        else:
            await self._request_flight_number(
                from_id,
                from_name,
                text or "",
                cfg,
                reason="Please send your flight number when you can.",
                alert_type="flight_request",
            )

    async def _fetch_adsb_aircraft(self, cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
        endpoint = cfg.get("endpoint_url")
        timeout = float(cfg.get("timeout_seconds", 2.5))

        def _fetch() -> List[Dict[str, Any]]:
            if not endpoint:
                return []
            try:
                resp = requests.get(endpoint, timeout=timeout)
                resp.raise_for_status()
                payload = resp.json()
                return payload.get("aircraft", []) or []
            except Exception as e:
                logger.warning(f"ADSB fetch failed: {e}")
                return []

        try:
            return await asyncio.to_thread(_fetch)
        except Exception:
            return await asyncio.get_running_loop().run_in_executor(None, _fetch)

    def _select_best_aircraft(
        self,
        aircraft: List[Dict[str, Any]],
        position: Optional[Dict[str, Any]],
        callsigns: List[str],
        cfg: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        max_distance_km = float(cfg.get("max_distance_km", 80))
        max_alt_delta_m = float(cfg.get("max_altitude_delta_m", 2500))
        max_seen_pos = float(cfg.get("max_seen_pos_seconds", 60))
        min_prob = float(cfg.get("probable_match_threshold", 0.8))

        best = None
        best_score = 0.0

        for plane in aircraft:
            lat = plane.get("lat")
            lon = plane.get("lon")
            if lat is None or lon is None:
                continue

            flight = self._normalize_callsign(plane.get("flight", ""))
            if callsigns and not flight:
                continue
            if callsigns:
                matched = False
                for candidate in callsigns:
                    if flight.startswith(candidate) or candidate.startswith(flight):
                        matched = True
                        break
                if not matched:
                    continue

            seen_pos = plane.get("seen_pos")
            if seen_pos is None:
                seen_pos = plane.get("seen")
            try:
                seen_pos = float(seen_pos) if seen_pos is not None else None
            except Exception:
                seen_pos = None

            if seen_pos is not None and seen_pos > max_seen_pos:
                continue

            dist_km = None
            alt_diff_m = None
            if position:
                try:
                    dist_m = self.calculate_distance(
                        float(position.get("latitude")),
                        float(position.get("longitude")),
                        float(lat),
                        float(lon),
                    )
                    dist_km = dist_m / 1000.0
                except Exception:
                    dist_km = None

                mesh_alt = position.get("altitude")
                plane_alt_m = self._aircraft_altitude_m(plane)
                if mesh_alt is not None and plane_alt_m is not None:
                    try:
                        alt_diff_m = abs(float(mesh_alt) - plane_alt_m)
                    except Exception:
                        alt_diff_m = None

            if not callsigns and dist_km is not None and dist_km > max_distance_km:
                continue
            if not callsigns and alt_diff_m is not None and alt_diff_m > max_alt_delta_m:
                continue

            score = self._score_aircraft_match(
                dist_km, alt_diff_m, seen_pos, cfg, callsign_boost=bool(callsigns)
            )
            if score > best_score:
                best_score = score
                best = plane
                best["match_score"] = score
                best["match_distance_km"] = dist_km
                best["match_alt_diff_m"] = alt_diff_m
                best["match_seen_pos_s"] = seen_pos

        if best and best_score >= min_prob:
            return best
        if best and callsigns:
            return best
        return None

    def _aircraft_altitude_m(self, plane: Dict[str, Any]) -> Optional[float]:
        alt_ft = plane.get("alt_baro")
        if alt_ft is None:
            alt_ft = plane.get("alt_geom")
        try:
            alt_ft = float(alt_ft)
        except Exception:
            return None
        if alt_ft <= 0:
            return None
        return alt_ft * 0.3048

    def _adsb_webui_link(self, hex_id: Optional[str], cfg: Dict[str, Any]) -> Optional[str]:
        if not hex_id:
            return None
        base_url = str(cfg.get("webui_base_url") or "").strip()
        if not base_url:
            return None
        return f"{base_url}{hex_id.lower()}"

    def _score_aircraft_match(
        self,
        dist_km: Optional[float],
        alt_diff_m: Optional[float],
        seen_pos_s: Optional[float],
        cfg: Dict[str, Any],
        callsign_boost: bool = False,
    ) -> float:
        dist_sigma = float(cfg.get("dist_sigma_km", 20))
        alt_sigma = float(cfg.get("alt_sigma_m", 1200))
        seen_sigma = float(cfg.get("seen_sigma_s", 15))

        dist_score = (
            math.exp(-((dist_km / dist_sigma) ** 2))
            if dist_km is not None
            else 0.0
        )
        alt_score = (
            math.exp(-((alt_diff_m / alt_sigma) ** 2))
            if alt_diff_m is not None
            else 0.5
        )
        seen_score = (
            math.exp(-((seen_pos_s / seen_sigma) ** 2))
            if seen_pos_s is not None
            else 0.5
        )
        score = 0.45 * dist_score + 0.35 * alt_score + 0.2 * seen_score
        if callsign_boost:
            score = max(score, 1.0)
        return min(score, 1.0)

    async def _announce_flight_match(
        self,
        from_id: str,
        from_name: str,
        plane: Dict[str, Any],
        position: Optional[Dict[str, Any]],
        callsigns: List[str],
        cfg: Dict[str, Any],
    ) -> None:
        alert_channel_id = (
            getattr(self.config, "alert_channel_id", 0) or self.config.channel_id
        )
        channel = self.get_channel(alert_channel_id)
        if not channel:
            return

        flight = self._normalize_callsign(plane.get("flight", "")) or "Unknown"
        hex_id = plane.get("hex") or "Unknown"
        reg = plane.get("r") or "Unknown"
        aircraft_type = plane.get("t") or "Unknown"
        alt_m = self._aircraft_altitude_m(plane)
        gs = plane.get("gs")
        dist_km = plane.get("match_distance_km")
        prob = plane.get("match_score", 0.0)

        embed = discord.Embed(
            title="✈️ Probable Flight Match",
            description=f"Possible flight for **{from_name}**",
            color=0x00FF00,
            timestamp=get_utc_time(),
        )
        embed.add_field(name="Flight", value=flight, inline=True)
        embed.add_field(name="Hex", value=hex_id, inline=True)
        embed.add_field(name="Registration", value=reg, inline=True)
        embed.add_field(name="Type", value=aircraft_type, inline=True)
        if alt_m is not None:
            embed.add_field(name="Altitude (m)", value=f"{alt_m:.0f}", inline=True)
        if gs is not None:
            try:
                embed.add_field(
                    name="Speed (kt)", value=f"{float(gs):.0f}", inline=True
                )
            except Exception:
                pass
        if dist_km is not None:
            embed.add_field(name="Distance (km)", value=f"{dist_km:.1f}", inline=True)
        embed.add_field(name="Match", value=f"{prob * 100:.0f}%", inline=True)

        lat = plane.get("lat")
        lon = plane.get("lon")
        if lat is not None and lon is not None:
            embed.add_field(
                name="Location",
                value=f"{float(lat):.5f}, {float(lon):.5f}",
                inline=True,
            )
        webui_link = self._adsb_webui_link(hex_id if hex_id != "Unknown" else None, cfg)
        if webui_link:
            embed.add_field(name="WebUI", value=webui_link, inline=True)
        if callsigns:
            embed.set_footer(text="Matched using flight number")
        await channel.send(embed=embed)

        now_ts = time.time()
        track = self._flight_tracks.get(from_id, {})
        self._flight_tracks[from_id] = {
            "node_id": from_id,
            "node_name": from_name,
            "flight": flight,
            "hex": hex_id if hex_id != "Unknown" else None,
            "last_mesh_seen_ts": now_ts,
            "last_adsb_seen_ts": now_ts,
            "safe_flight_announced": track.get("safe_flight_announced", False),
            "adsb_out_announced": track.get("adsb_out_announced", False),
        }

    async def _request_flight_number(
        self,
        from_id: str,
        from_name: str,
        text: str,
        cfg: Dict[str, Any],
        reason: str,
        alert_type: str,
    ) -> None:
        cooldown_min = float(cfg.get("cooldown_seconds", 10)) / 60.0
        if self.database and hasattr(self.database, "recent_alert_exists"):
            try:
                recent = await asyncio.to_thread(
                    self.database.recent_alert_exists,
                    from_id,
                    alert_type,
                    cooldown_min,
                )
            except Exception:
                recent = False
            if recent:
                return

        prompt = f"✈️ {from_name}, {reason}"
        try:
            self.meshtastic.send_text(prompt, destination_id=from_id)
        except Exception as e:
            logger.error(f"Error sending flight prompt to mesh: {e}")

        alert_channel_id = (
            getattr(self.config, "alert_channel_id", 0) or self.config.channel_id
        )
        channel = self.get_channel(alert_channel_id)
        if channel:
            await channel.send(
                f"✈️ Asked **{from_name}** to share their flight number."
            )

        if self.database and hasattr(self.database, "record_alert"):
            try:
                await asyncio.to_thread(self.database.record_alert, from_id, alert_type)
            except Exception:
                pass

    def _maybe_send_initial_safe_flight(self, node_id: str) -> None:
        if not node_id or node_id in self._flight_initial_safe_sent:
            return
        try:
            self.meshtastic.send_text(f"Safe Flight {node_id}!", destination_id=node_id)
            self._flight_initial_safe_sent.add(node_id)
        except Exception as send_err:
            logger.error(
                f"Error sending initial Safe Flight to {node_id}: {send_err}"
            )

    async def setup_hook(self) -> None:
        """Setup bot when starting"""
        self.bg_task = self.loop.create_task(self.background_task())
        self.telemetry_task = self.loop.create_task(self.telemetry_update_task())
        self.high_altitude_task_handle = self.loop.create_task(
            self.high_altitude_task()
        )
        self.flight_monitor_task_handle = self.loop.create_task(
            self.flight_monitor_task()
        )
        if DB_MAINTENANCE.get("enabled", True):
            self.db_maintenance_task_handle = self.loop.create_task(
                self.db_maintenance_task()
            )
        # Boot announce suppression window
        self._boot_started_at = time.time()
        # Capture our own node ID from the Meshtastic interface if available.
        try:
            iface = getattr(self.meshtastic, "iface", None)
            my_info = getattr(iface, "myInfo", None)
            my_num = getattr(my_info, "my_node_num", None) if my_info else None
            if my_num is not None:
                try:
                    self.my_node_id = f"!{int(my_num):08x}"
                except Exception:
                    self.my_node_id = None
        except Exception:
            self.my_node_id = None

    async def on_ready(self):
        """Called when bot is ready"""
        logger.info(f"Logged in as {self.user} (ID: {self.user.id})")
        logger.info("------")

        # Set event loop for command handler
        self.command_handler.event_loop = self.loop

        # Connect to Meshtastic
        if not await self.meshtastic.connect():
            logger.error("Failed to connect to Meshtastic. Exiting.")
            await self.close()
            return

    async def on_message(self, message):
        """Handle incoming messages"""
        if message.author.id == self.user.id:
            return
        if message.channel is None or message.channel.id != self.config.channel_id:
            return

        # Handle ping/pong functionality
        if message.content.strip().lower() == "ping":
            await self._handle_ping(message)
            return

        # Handle commands
        if message.content.startswith("$"):
            await self.command_handler.handle_command(message)

    async def _handle_ping(self, message):
        """Handle ping command - send pong to mesh and announce to Discord"""
        try:
            # Create a nice embed for the ping response
            embed = discord.Embed(
                title="🏓 Ping Test",
                description="Testing mesh network connectivity...",
                color=0x00FF00,
                timestamp=get_utc_time(),
            )
            embed.add_field(
                name="📡 **Action**",
                value="Sending Pong! to mesh network",
                inline=False,
            )
            embed.set_footer(text=f"Requested by {message.author.display_name}")

            # Send initial response
            await message.channel.send(embed=embed)

            # Send pong to mesh network
            pong_sent = self.meshtastic.send_text("Pong!")
            # Small delay to prevent timing issues
            await asyncio.sleep(0.5)
            if pong_sent:
                # Update with success
                success_embed = discord.Embed(
                    title="✅ Ping Successful",
                    description="Pong! sent to mesh network successfully",
                    color=0x00FF00,
                    timestamp=get_utc_time(),
                )
                success_embed.add_field(
                    name="📡 **Status**",
                    value="✅ Message sent to Longfast Channel",
                    inline=False,
                )
                success_embed.set_footer(
                    text=f"Completed for {message.author.display_name}"
                )
                await message.channel.send(embed=success_embed)
                logger.info(f"Ping/pong handled from {message.author.name}")
            else:
                # Update with failure
                fail_embed = discord.Embed(
                    title="❌ Ping Failed",
                    description="Failed to send pong to mesh network",
                    color=0xFF0000,
                    timestamp=get_utc_time(),
                )
                fail_embed.add_field(
                    name="📡 **Status**",
                    value="❌ Unable to send to Longfast Channel",
                    inline=False,
                )
                fail_embed.set_footer(text=f"Failed for {message.author.display_name}")
                await message.channel.send(embed=fail_embed)
        except Exception as e:
            logger.error(f"Error handling ping: {e}")
            error_embed = discord.Embed(
                title="❌ Ping Error",
                description="An error occurred while testing connectivity",
                color=0xFF0000,
                timestamp=get_utc_time(),
            )
            error_embed.add_field(
                name="📡 **Error**", value=f"```{str(e)[:500]}```", inline=False
            )
            await message.channel.send(embed=error_embed)

    async def background_task(self):
        """Background task for handling queues and Meshtastic events"""
        await self.wait_until_ready()

        # Subscribe to Meshtastic events
        pub.subscribe(self.on_mesh_receive, "meshtastic.receive")
        pub.subscribe(self.on_mesh_connection, "meshtastic.connection.established")

        channel = self.get_channel(self.config.channel_id)
        if not channel:
            logger.error(f"Could not find channel with ID {self.config.channel_id}")
            return

        logger.info("Background task started")

        # Performance counters
        last_cleanup = time.time()
        cleanup_interval = 300  # 5 minutes
        last_presence_eval = 0
        presence_interval = 30  # seconds
        # Simple per-category rate counters
        rate_window_start = time.time()
        sent_counts = {"announcement": 0, "alert": 0, "command_reply": 0}
        try:
            from config import BOT_CONFIG

            rate_limits = BOT_CONFIG.get(
                "rate_limits",
                {
                    "announcement_per_min": 12,
                    "alert_per_min": 12,
                    "command_reply_per_min": 30,
                },
            )
        except Exception:
            rate_limits = {
                "announcement_per_min": 12,
                "alert_per_min": 12,
                "command_reply_per_min": 30,
            }

        while not self.is_closed():
            try:
                # Process mesh to Discord messages
                await self._process_mesh_to_discord(channel)

                # Process Discord to mesh messages
                await self._process_discord_to_mesh()

                # Process nodes periodically
                if (
                    time.time() - self.meshtastic.last_node_refresh
                    >= self.config.node_refresh_interval
                ):
                    await self._process_nodes(channel)

                # Presence evaluator
                now = time.time()
                if now - last_presence_eval >= presence_interval:
                    await self._evaluate_presence()
                    last_presence_eval = now

                # Periodic cleanup
                if now - last_cleanup >= cleanup_interval:
                    await self._periodic_cleanup()
                    last_cleanup = now
                # Reset rate window each minute
                if now - rate_window_start >= 60:
                    sent_counts = {"announcement": 0, "alert": 0, "command_reply": 0}
                    rate_window_start = now

                # Watchdog: reconnect if no mesh packets have been seen recently
                if (
                    self._watchdog_threshold
                    and (now - self._last_mesh_rx_time) > self._watchdog_threshold
                ):
                    logger.warning(
                        "No mesh packets received within watchdog window; attempting reconnect"
                    )
                    try:
                        # Close existing iface if possible
                        if self.meshtastic.iface:
                            try:
                                self.meshtastic.iface.close()
                            except Exception:
                                pass
                        await self.meshtastic.connect()
                        self._last_mesh_rx_time = time.time()
                    except Exception as watchdog_err:
                        logger.error(f"Watchdog reconnect failed: {watchdog_err}")

                await asyncio.sleep(1)  # Check every second

            except Exception as e:
                logger.error(f"Error in background task: {e}")
                await asyncio.sleep(5)

    async def _periodic_cleanup(self):
        """Perform periodic cleanup tasks"""
        try:
            # Clear command handler cache
            if hasattr(self.command_handler, "clear_cache"):
                await self.command_handler.clear_cache()

            # Clean up old database data
            if hasattr(self.database, "cleanup_old_data"):
                self.database.cleanup_old_data(30)  # Keep 30 days

            logger.debug("Periodic cleanup completed")

        except Exception as e:
            logger.error(f"Error during periodic cleanup: {e}")

    async def _evaluate_presence(self):
        """Evaluate node presence and emit transition alerts with hysteresis."""
        try:
            try:
                threshold_min = getattr(self.config, "presence_threshold_min", 60)
                hysteresis = getattr(self.config, "presence_hysteresis_factor", 2.0)
            except Exception:
                threshold_min = 60
                hysteresis = 2.0
            # Fetch nodes
            nodes = (
                await asyncio.to_thread(
                    self.database.get_presence_snapshot, 1000
                )
                if self.database
                else []
            )
            now_utc = datetime.utcnow()
            online_cutoff = now_utc - timedelta(minutes=threshold_min)
            offline_cutoff = now_utc - timedelta(
                minutes=int(threshold_min * hysteresis)
            )
            for n in nodes:
                node_id = n.get("node_id")
                if not node_id:
                    continue
                last_heard = n.get("last_heard")
                prev_state = (n.get("presence_state") or "").lower()
                # Compute new state
                if last_heard:
                    try:
                        lh_dt = datetime.fromisoformat(str(last_heard))
                    except Exception:
                        # Try sqlite datetime format
                        try:
                            lh_dt = datetime.strptime(
                                str(last_heard), "%Y-%m-%d %H:%M:%S"
                            )
                        except Exception:
                            lh_dt = None
                else:
                    lh_dt = None
                if lh_dt and lh_dt > online_cutoff:
                    new_state = "online"
                elif not lh_dt or lh_dt <= offline_cutoff:
                    new_state = "offline"
                else:
                    new_state = "unknown"
                if new_state != prev_state:
                    # Update DB state
                    try:
                        await asyncio.to_thread(
                            self.database.update_presence_state, node_id, new_state
                        )
                    except Exception as upd_err:
                        logger.debug(
                            f"Presence state update failed for {node_id}: {upd_err}"
                        )
                    # Announce transitions (dedupe via alerts table)
                    if not getattr(
                        self.config, "presence_announcements_enabled", True
                    ):
                        continue
                    try:
                        alert_type = f"presence_{new_state}"
                        recent = (
                            await asyncio.to_thread(
                                self.database.recent_alert_exists,
                                node_id,
                                alert_type,
                                threshold_min,
                            )
                            if hasattr(self.database, "recent_alert_exists")
                            else True
                        )
                        if not recent:
                            name = n.get("long_name") or node_id
                            emoji = (
                                "🟢"
                                if new_state == "online"
                                else ("🔴" if new_state == "offline" else "⚪")
                            )
                            msg = f"{emoji} Presence: **{name}** is now {new_state.upper()}"
                            ch_id = (
                                getattr(self.config, "alert_channel_id", 0)
                                or self.config.channel_id
                            )
                            ch = self.get_channel(ch_id)
                            if ch:
                                await ch.send(msg)
                            if hasattr(self.database, "record_alert"):
                                await asyncio.to_thread(
                                    self.database.record_alert, node_id, alert_type
                                )
                    except Exception as ann_err:
                        logger.debug(
                            f"Presence announce failed for {node_id}: {ann_err}"
                        )
        except Exception as e:
            logger.error(f"Error evaluating presence: {e}")

    async def telemetry_update_task(self):
        """Task for hourly telemetry updates"""
        await self.wait_until_ready()

        while not self.is_closed():
            try:
                current_hour = datetime.now().hour

                # Check if it's a new hour
                if current_hour != self.last_telemetry_hour:
                    await self._send_telemetry_update()
                    self.last_telemetry_hour = current_hour

                await asyncio.sleep(60)  # Check every minute

            except Exception as e:
                logger.error(f"Error in telemetry update task: {e}")
                await asyncio.sleep(60)

    async def db_maintenance_task(self):
        """Periodic DB backup and WAL maintenance."""
        await self.wait_until_ready()
        last_backup = 0.0
        last_checkpoint = 0.0
        last_cleanup = 0.0

        backup_interval = float(DB_MAINTENANCE.get("backup_interval_hours", 24)) * 3600
        checkpoint_interval = float(
            DB_MAINTENANCE.get("wal_checkpoint_interval_hours", 6)
        ) * 3600
        cleanup_days = int(DB_MAINTENANCE.get("cleanup_retention_days", 30))
        cleanup_interval = 24 * 3600
        backup_dir = DB_MAINTENANCE.get("backup_dir", "data/backups")
        retention_days = int(DB_MAINTENANCE.get("backup_retention_days", 7))

        while not self.is_closed():
            now = time.time()
            try:
                if backup_interval > 0 and now - last_backup >= backup_interval:
                    try:
                        os.makedirs(backup_dir, exist_ok=True)
                        ts = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
                        backup_path = os.path.join(
                            backup_dir, f"meshtastic-{ts}.db"
                        )
                        ok = await asyncio.to_thread(
                            self.database.backup_database, backup_path
                        )
                        if ok:
                            last_backup = now
                            logger.info(f"DB backup created: {backup_path}")
                    except Exception as backup_err:
                        logger.warning(f"DB backup failed: {backup_err}")

                    if retention_days > 0:
                        cutoff = now - (retention_days * 86400)
                        try:
                            for name in os.listdir(backup_dir):
                                path = os.path.join(backup_dir, name)
                                if not os.path.isfile(path):
                                    continue
                                if os.path.getmtime(path) < cutoff:
                                    os.remove(path)
                        except Exception as prune_err:
                            logger.warning(f"Backup pruning failed: {prune_err}")

                if checkpoint_interval > 0 and now - last_checkpoint >= checkpoint_interval:
                    try:
                        ok = await asyncio.to_thread(self.database.checkpoint_wal)
                        if ok:
                            last_checkpoint = now
                            logger.info("WAL checkpoint completed")
                    except Exception as ck_err:
                        logger.warning(f"WAL checkpoint failed: {ck_err}")

                if cleanup_days > 0 and now - last_cleanup >= cleanup_interval:
                    try:
                        await asyncio.to_thread(
                            self.database.cleanup_old_data, cleanup_days
                        )
                        last_cleanup = now
                    except Exception as cleanup_err:
                        logger.warning(f"DB cleanup failed: {cleanup_err}")

            except Exception as loop_err:
                logger.warning(f"DB maintenance loop error: {loop_err}")

            await asyncio.sleep(60)

    async def _process_nodes(self, channel):
        """Process and store nodes, announce new ones"""
        try:
            result = await asyncio.to_thread(self.meshtastic.process_nodes)
            if result and len(result) == 2:
                processed_nodes, new_nodes = result

                logger.info(
                    f"Node processing result: {len(processed_nodes)} processed, {len(new_nodes)} new"
                )

                # Announce new nodes
                for node in new_nodes:
                    try:
                        # Respect boot suppression window
                        try:
                            suppress_secs = getattr(
                                self.config, "boot_announce_suppress_seconds", 30
                            )
                        except Exception:
                            suppress_secs = 30
                        if (
                            hasattr(self, "_boot_started_at")
                            and (time.time() - self._boot_started_at) < suppress_secs
                        ):
                            continue
                        # Check if already announced (guard against restarts)
                        node_id = node.get("node_id")
                        if not node_id:
                            continue
                        # Fetch latest node row to check announced_at
                        # Lightweight query: rely on get_all_nodes if necessary, but prefer direct check
                        if self.database:
                            with self.database._get_connection() as conn:
                                cur = conn.cursor()
                                cur.execute(
                                    "SELECT announced_at FROM nodes WHERE node_id = ?",
                                    (node_id,),
                                )
                                row = cur.fetchone()
                                already_announced = bool(row and row[0])
                        else:
                            already_announced = False

                        if not already_announced:
                            await self._announce_new_node(channel, node)
                            # Mark as announced
                            if self.database:
                                self.database.mark_node_announced(node_id)
                    except Exception as guard_err:
                        logger.error(
                            f"Error guarding announcement for node {node.get('node_id')}: {guard_err}"
                        )
            else:
                logger.debug("No nodes processed or invalid result format")

        except Exception as e:
            logger.error(f"Error processing nodes: {e}")

    async def _announce_new_node(self, channel, node: Dict[str, Any]):
        """Announce new node with embed"""
        try:
            embed = discord.Embed(
                title="🆕 New Node Detected!",
                description=f"**{node['long_name']}** has joined the mesh network",
                color=0x00FF00,
                timestamp=get_utc_time(),
            )

            embed.add_field(name="Node ID", value=node["node_id"], inline=True)
            embed.add_field(
                name="Node Number", value=node.get("node_num", "N/A"), inline=True
            )
            embed.add_field(
                name="Hardware", value=node.get("hw_model", "Unknown"), inline=True
            )
            embed.add_field(
                name="Firmware",
                value=node.get("firmware_version", "Unknown"),
                inline=True,
            )
            embed.add_field(
                name="Hops Away", value=node.get("hops_away", 0), inline=True
            )

            await channel.send(embed=embed)
            logger.info(f"Announced new node: {node['long_name']}")

        except Exception as e:
            logger.error(f"Error announcing new node: {e}")

    async def _send_telemetry_update(self):
        """Send hourly telemetry update"""
        try:
            channel = self.get_channel(self.config.channel_id)
            if not channel:
                return

            try:
                summary = self.database.get_telemetry_summary(60)
                if not summary:
                    return
            except Exception as db_error:
                logger.error(
                    f"Database error getting telemetry summary for update: {db_error}"
                )
                return

            embed = discord.Embed(
                title="📊 Hourly Telemetry Update",
                description="Latest telemetry data from active nodes",
                color=0x0099FF,
                timestamp=get_utc_time(),
            )

            embed.add_field(
                name="Active Nodes", value=summary.get("active_nodes", 0), inline=True
            )
            embed.add_field(
                name="Total Nodes", value=summary.get("total_nodes", 0), inline=True
            )

            if summary.get("avg_battery") is not None:
                embed.add_field(
                    name="Avg Battery",
                    value=f"{summary['avg_battery']:.1f}%",
                    inline=True,
                )
            if summary.get("avg_temperature") is not None:
                embed.add_field(
                    name="Avg Temperature",
                    value=f"{summary['avg_temperature']:.1f}°C",
                    inline=True,
                )
            if summary.get("avg_humidity") is not None:
                embed.add_field(
                    name="Avg Humidity",
                    value=f"{summary['avg_humidity']:.1f}%",
                    inline=True,
                )
            if summary.get("avg_snr") is not None:
                embed.add_field(
                    name="Avg SNR", value=f"{summary['avg_snr']:.1f} dB", inline=True
                )

            await channel.send(embed=embed)
            logger.info("Sent hourly telemetry update")

        except Exception as e:
            logger.error(f"Error sending telemetry update: {e}")

    def _sanitize_user_text(self, text: Optional[str]) -> str:
        """Escape markdown and neutralize mention patterns in untrusted text."""
        if text is None:
            return ""
        try:
            safe_text = discord.utils.escape_markdown(str(text))
        except Exception:
            safe_text = str(text)
        safe_text = safe_text.replace("@everyone", "@ everyone")
        safe_text = safe_text.replace("@here", "@ here")
        safe_text = safe_text.replace("<@", "< @")
        return safe_text

    async def _process_mesh_to_discord(self, channel):
        """Process messages from mesh to Discord with improved error handling"""
        try:
            processed_count = 0
            max_batch_size = 10  # Process max 10 messages at once
            allowed_mentions = discord.AllowedMentions.none()

            while not self.mesh_to_discord.empty() and processed_count < max_batch_size:
                item = self.mesh_to_discord.get_nowait()
                try:
                    if isinstance(item, dict):
                        if item.get("type") == "text":
                            # Format as single line message
                            from_name_raw = item.get(
                                "from_name", item.get("from_id", "Unknown")
                            )
                            to_name_raw = item.get("to_name", item.get("to_id", "Unknown"))
                            raw_text = str(item.get("text", ""))
                            from_name = self._sanitize_user_text(from_name_raw)
                            to_name = self._sanitize_user_text(to_name_raw)
                            text = self._sanitize_user_text(raw_text)
                            hops = item.get("hops_away", 0)

                            # Validate message content
                            if not text.strip():
                                logger.warning(f"Empty message from {from_name}")
                                continue

                            # Format destination - use "Longfast Channel" for broadcasts
                            if to_name == "^all" or to_name == "^all(^all)":
                                destination = "Longfast Channel"
                            else:
                                destination = to_name

                            # Format hops with bunny emoji
                            hops_text = (
                                f"🐰{hops} hops" if hops is not None else "🐰0 hops"
                            )

                            # Create single line message with length limit
                            message_text = f"📨 **{from_name}** → **{destination}** {hops_text}: {text}"
                            if len(message_text) > 2000:
                                message_text = message_text[:1997] + "..."

                            await channel.send(
                                message_text, allowed_mentions=allowed_mentions
                            )
                            logger.info(
                                f"📤 DISCORD: Sent message to Discord - '{text[:30]}{'...' if len(text) > 30 else ''}' from {from_name}"
                            )

                        elif item.get("type") == "traceroute":
                            # Format traceroute information
                            from_name_raw = item.get(
                                "from_name", item.get("from_id", "Unknown")
                            )
                            to_name_raw = item.get("to_name", item.get("to_id", "Unknown"))
                            route_text_raw = item.get("route_text", "")
                            from_name = self._sanitize_user_text(from_name_raw)
                            to_name = self._sanitize_user_text(to_name_raw)
                            route_text = self._sanitize_user_text(route_text_raw)
                            hops_count = item.get("hops_count", 0)

                            # Create traceroute embed
                            embed = discord.Embed(
                                title="🛣️ Traceroute Result",
                                description=f"**{from_name}** traced route to **{to_name}**",
                                color=0x00BFFF,
                                timestamp=datetime.utcnow(),
                            )

                            embed.add_field(
                                name="📍 Route Path", value=route_text, inline=False
                            )

                            embed.add_field(
                                name="📊 Statistics",
                                value=f"Total Hops: {hops_count}",
                                inline=True,
                            )

                            embed.set_footer(text=f"Traceroute completed at")

                            await channel.send(
                                embed=embed, allowed_mentions=allowed_mentions
                            )
                            logger.info(
                                f"🛣️ DISCORD: Sent traceroute info - {from_name} → {to_name} ({hops_count} hops)"
                            )

                        elif item.get("type") == "movement":
                            # Format movement notification
                            from_name_raw = item.get(
                                "from_name", item.get("from_id", "Unknown")
                            )
                            from_name = self._sanitize_user_text(from_name_raw)
                            distance_moved = item.get("distance_moved")
                            old_lat = item.get("old_lat", 0)
                            old_lon = item.get("old_lon", 0)
                            new_lat = item.get("new_lat", 0)
                            new_lon = item.get("new_lon", 0)
                            new_alt = item.get("new_alt", 0)
                            speed_mps = item.get("speed_mps")

                            # Format coordinates for display
                            old_coords = f"{old_lat:.6f}, {old_lon:.6f}"
                            new_coords = f"{new_lat:.6f}, {new_lon:.6f}"

                            # Create movement embed
                            embed = discord.Embed(
                                title="🚶 Node is on the move!",
                                description=f"**{from_name}** has moved a significant distance",
                                color=0xFF6B35,
                                timestamp=datetime.utcnow(),
                            )

                            # Add movement details
                            movement_lines = []
                            if distance_moved is not None:
                                try:
                                    movement_lines.append(
                                        f"**Distance:** {float(distance_moved):.1f} meters"
                                    )
                                except Exception:
                                    pass
                            if speed_mps is not None:
                                try:
                                    movement_lines.append(
                                        f"**Speed:** {float(speed_mps):.1f} m/s"
                                    )
                                except Exception:
                                    pass
                            if old_lat != 0 or old_lon != 0:
                                movement_lines.append(f"**From:** `{old_coords}`")
                            if new_lat != 0 or new_lon != 0:
                                movement_lines.append(f"**To:** `{new_coords}`")
                            if new_alt != 0:
                                movement_lines.append(f"**Altitude:** {new_alt}m")
                            if not movement_lines:
                                movement_lines.append("Movement detected from position update")
                            movement_text = "\n".join(movement_lines)

                            embed.add_field(
                                name="📍 Movement Details",
                                value=movement_text,
                                inline=False,
                            )

                            # Add a fun movement indicator
                            speed_val = None
                            try:
                                if speed_mps is not None:
                                    speed_val = float(speed_mps)
                            except Exception:
                                speed_val = None

                            dist_val = None
                            try:
                                if distance_moved is not None:
                                    dist_val = float(distance_moved)
                            except Exception:
                                dist_val = None

                            if speed_val is not None:
                                if speed_val > 5:
                                    embed.add_field(
                                        name="🏃 Speed", value="Moving fast!", inline=True
                                    )
                                elif speed_val > 1:
                                    embed.add_field(
                                        name="🚶 Speed", value="On the move", inline=True
                                    )
                                else:
                                    embed.add_field(
                                        name="🐌 Speed", value="Slow movement", inline=True
                                    )
                            elif dist_val is not None and dist_val > 0:
                                if dist_val > 1000:
                                    embed.add_field(
                                        name="🏃 Speed",
                                        value="Moving fast!",
                                        inline=True,
                                    )
                                elif dist_val > 500:
                                    embed.add_field(
                                        name="🚶 Speed",
                                        value="Walking pace",
                                        inline=True,
                                    )
                                else:
                                    embed.add_field(
                                        name="🐌 Speed",
                                        value="Slow movement",
                                        inline=True,
                                    )
                            else:
                                embed.add_field(
                                    name="🚶 Status",
                                    value="Movement detected",
                                    inline=True,
                                )

                            embed.set_footer(text=f"Movement detected at")
                            await channel.send(
                                embed=embed, allowed_mentions=allowed_mentions
                            )
                            distance_label = "unknown"
                            try:
                                if distance_moved is not None:
                                    distance_label = f"{float(distance_moved):.1f}m"
                            except Exception:
                                distance_label = "unknown"
                            logger.info(
                                f"🚶 DISCORD: Sent movement notification - {from_name} moved {distance_label}"
                            )

                        # Special handling for ping messages - show pong response after a delay
                        if (
                            item.get("type") == "text"
                            and raw_text.strip().lower() == "ping"
                        ):
                            # Wait a moment for the ping message to be displayed first
                            await asyncio.sleep(1.0)

                            # Then show the pong response
                            pong_embed = discord.Embed(
                                title="🏓 Pong Response",
                                description=f"Pong! sent to mesh network in response to **{from_name}**",
                                color=0x00FF00,
                                timestamp=get_utc_time(),
                            )
                            pong_embed.set_footer(
                                text="🌍 UTC Time | Mesh network response"
                            )
                            await channel.send(
                                embed=pong_embed, allowed_mentions=allowed_mentions
                            )
                            logger.info(
                                f"Pong response announced for ping from {from_name}"
                            )

                    else:
                        # Handle other message types
                        message_text = self._sanitize_user_text(str(item))
                        message_text = f"📡 **Mesh Message:** {message_text[:1900]}"
                        await channel.send(
                            message_text, allowed_mentions=allowed_mentions
                        )

                except discord.HTTPException as e:
                    logger.error(f"Discord API error sending message: {e}")
                    # Don't re-queue the message, just log and continue
                except Exception as e:
                    logger.error(f"Error processing individual mesh message: {e}")
                finally:
                    processed_count += 1
                    self.mesh_to_discord.task_done()

        except queue.Empty:
            pass
        except Exception as e:
            logger.error(f"Error processing mesh to Discord: {e}")
            # Try to clear the queue to prevent memory buildup
            try:
                while not self.mesh_to_discord.empty():
                    try:
                        self.mesh_to_discord.get_nowait()
                        self.mesh_to_discord.task_done()
                    except queue.Empty:
                        break
            except Exception as e:
                logger.warning(f"Error clearing mesh to discord queue: {e}")

    async def _process_discord_to_mesh(self):
        """Process messages from Discord to mesh"""
        try:
            while not self.discord_to_mesh.empty():
                try:
                    message = self.discord_to_mesh.get_nowait()
                except queue.Empty:
                    break

                if message.startswith("nodenum="):
                    # Extract node ID and message
                    parts = message.split(" ", 1)
                    if len(parts) == 2:
                        node_id = parts[0][8:]  # Remove 'nodenum='
                        message_text = parts[1]
                        logger.info(
                            f"📤 MESH: Sending message to node {node_id} - '{message_text[:50]}{'...' if len(message_text) > 50 else ''}'"
                        )
                        try:
                            self.meshtastic.send_text(
                                message_text, destination_id=node_id
                            )
                            logger.info(
                                f"✅ MESH: Message sent successfully to node {node_id}"
                            )
                        except Exception as send_error:
                            logger.error(
                                f"❌ MESH: Error sending message to node {node_id}: {send_error}"
                            )
                else:
                    # Send to primary channel
                    logger.info(
                        f"📤 MESH: Sending message to primary channel - '{message[:50]}{'...' if len(message) > 50 else ''}'"
                    )
                    try:
                        self.meshtastic.send_text(message)
                        logger.info(
                            f"✅ MESH: Message sent successfully to primary channel"
                        )
                    except Exception as send_error:
                        logger.error(
                            f"❌ MESH: Error sending message to primary channel: {send_error}"
                        )

                self.discord_to_mesh.task_done()

        except queue.Empty:
            pass
        except Exception as e:
            logger.error(f"Error processing Discord to mesh: {e}")

    def on_mesh_receive(self, packet, interface):
        """Handle incoming mesh packets"""
        try:
            if "decoded" not in packet:
                logger.debug("Received packet without decoded data")
                return

            # Update watchdog timestamp
            self._last_mesh_rx_time = time.time()

            portnum = packet["decoded"]["portnum"]
            from_id = packet.get("fromId", "Unknown")
            to_id = packet.get("toId", "Primary")
            hops_away = packet.get("hopsAway", 0)
            snr = packet.get("snr", "N/A")
            rssi = packet.get("rssi", "N/A")

            # Update last_heard for the sending node as soon as we receive any packet.
            try:
                if from_id and self.database:
                    # Use UTC time for last_heard
                    ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
                    self.database.update_last_heard(from_id, ts)
            except Exception as lh_err:
                logger.error(f"Error updating last_heard for {from_id}: {lh_err}")
            try:
                if from_id in self._flight_tracks:
                    self._flight_tracks[from_id]["last_mesh_seen_ts"] = time.time()
            except Exception as track_err:
                logger.debug(f"Flight tracking update failed for {from_id}: {track_err}")

            try:
                if from_id and self.database:
                    relay_raw = packet.get("relayNode") or packet.get("relay_node")
                    relay_id = normalize_node_id(relay_raw)
                    if relay_id and relay_id != from_id:
                        self.database.update_last_relay(from_id, relay_id)
            except Exception as relay_err:
                logger.error(f"Error updating relay info for {from_id}: {relay_err}")

            # Get node display name for logging
            from_name = (
                self.database.get_node_display_name(from_id)
                if self.database
                else from_id
            )

            # Log packet reception
            logger.info(
                f"📦 PACKET RECEIVED: {portnum} from {from_name} ({from_id}) -> {to_id} | Hops: {hops_away} | SNR: {snr} | RSSI: {rssi}"
            )

            # Add to live monitor buffer
            packet_info = {
                "type": "packet",
                "portnum": portnum,
                "from_name": from_name,
                "from_id": from_id,
                "to_id": to_id,
                "hops": hops_away,
                "snr": snr,
                "rssi": rssi,
            }
            if hasattr(self, "command_handler"):
                self.command_handler.add_packet_to_buffer(packet_info)

            # Handle text messages
            if portnum == "TEXT_MESSAGE_APP":
                to_id = packet.get("toId", "Primary")
                text = packet["decoded"]["text"]

                from_name = (
                    self.database.get_node_display_name(from_id)
                    if self.database
                    else from_id
                )
                to_name = (
                    self.database.get_node_display_name(to_id)
                    if self.database
                    else to_id
                )

                # Check for ping messages from mesh
                if text.strip().lower() == "ping":
                    logger.info(f"Ping received from mesh node {from_name}")
                    # Send pong back to mesh with sender's name
                    try:
                        pong_message = f"Pong! - - > {from_name}"
                        self.meshtastic.send_text(pong_message)
                        logger.info(f"Pong sent to mesh network: {pong_message}")

                    except Exception as pong_error:
                        logger.error(f"Error sending pong to mesh: {pong_error}")

                    # Continue processing the ping message normally (don't return early)

                msg_payload = {
                    "type": "text",
                    "from_id": from_id,
                    "from_name": from_name,
                    "to_id": to_id,
                    "to_name": to_name,
                    "text": text,
                    "hops_away": packet.get("hopsAway", 0),
                    "snr": packet.get("snr"),
                    "rssi": packet.get("rssi"),
                    "timestamp": datetime.utcnow().isoformat() + "Z",
                }
                try:
                    self.mesh_to_discord.put_nowait(msg_payload)
                except queue.Full:
                    logger.warning("Mesh to Discord queue is full, dropping message")
                    # Optionally remove oldest message to make room
                    try:
                        self.mesh_to_discord.get_nowait()
                        self.mesh_to_discord.put_nowait(msg_payload)
                    except queue.Empty:
                        pass
                logger.info(
                    f"💬 MESSAGE: Queued for Discord - '{text[:50]}{'...' if len(text) > 50 else ''}' from {from_name}"
                )

                # Add text message to live monitor buffer
                text_packet_info = {
                    "type": "text",
                    "portnum": portnum,
                    "from_name": from_name,
                    "from_id": from_id,
                    "to_id": to_id,
                    "text": text,
                    "hops": hops_away,
                    "snr": snr,
                    "rssi": rssi,
                }
                if hasattr(self, "command_handler"):
                    self.command_handler.add_packet_to_buffer(text_packet_info)

                # Store message in database
                try:
                    message_data = {
                        "from_node_id": from_id,
                        "to_node_id": to_id,
                        "message_text": text,
                        "port_num": packet["decoded"]["portnum"],
                        "payload": str(packet.get("payload", "")),
                        "hops_away": packet.get("hopsAway", 0),
                        "snr": packet.get("snr"),
                        "rssi": packet.get("rssi"),
                    }
                    self.database.add_message(message_data)
                except Exception as msg_error:
                    logger.error(f"Error storing message in database: {msg_error}")

                try:
                    if from_id and from_id != "Unknown":
                        airborne_hint = self._text_indicates_airborne(text)
                        callsigns = self._extract_callsigns(text)
                        if airborne_hint or callsigns:
                            self._schedule_flight_lookup(
                                from_id,
                                from_name,
                                text=text,
                                airborne_hint=airborne_hint,
                            )
                except Exception as flight_err:
                    logger.debug(f"Flight lookup trigger failed: {flight_err}")

            # Handle telemetry packets
            elif portnum == "TELEMETRY_APP":
                try:
                    logger.info(
                        f"DEBUG TELEMETRY decoded: {str(packet.get('decoded'))[:800]}"
                    )
                except Exception:
                    pass
                if from_id and from_id != "Unknown" and from_id is not None:
                    logger.info(
                        f"📊 TELEMETRY: Processing sensor data from {from_name}"
                    )
                    self._process_telemetry_packet(packet)
                    # Record this packet in the messages table to aid topology. Telemetry
                    # packets don't have a text message, but we can log the port and
                    # metrics.  Use empty payload string for brevity.
                    try:
                        message_data = {
                            "from_node_id": from_id,
                            "to_node_id": to_id,
                            "message_text": None,
                            "port_num": packet["decoded"].get("portnum"),
                            "payload": str(packet.get("payload", "")),
                            "hops_away": packet.get("hopsAway", 0),
                            "snr": packet.get("snr"),
                            "rssi": packet.get("rssi"),
                        }
                        self.database.add_message(message_data)
                    except Exception as tel_msg_err:
                        logger.error(
                            f"Error logging telemetry packet to messages: {tel_msg_err}"
                        )
                else:
                    logger.warning(
                        f"📊 TELEMETRY: Skipping packet with invalid fromId: {from_id}"
                    )

            # Handle position packets
            elif portnum == "POSITION_APP":
                try:
                    logger.info(
                        f"DEBUG POSITION decoded: {str(packet.get('decoded'))[:800]}"
                    )
                except Exception:
                    pass
                logger.info(f"📍 POSITION: Location update from {from_name}")
                self._process_position_packet(packet)
                # Record position packets in the messages table for topology tracking.
                try:
                    message_data = {
                        "from_node_id": from_id,
                        "to_node_id": to_id,
                        "message_text": None,
                        "port_num": packet["decoded"].get("portnum"),
                        "payload": str(packet.get("payload", "")),
                        "hops_away": packet.get("hopsAway", 0),
                        "snr": packet.get("snr"),
                        "rssi": packet.get("rssi"),
                    }
                    self.database.add_message(message_data)
                except Exception as pos_msg_err:
                    logger.error(
                        f"Error logging position packet to messages: {pos_msg_err}"
                    )

            # Handle node info packets
            elif portnum == "NODEINFO_APP":
                logger.info(f"👤 NODE INFO: Node information from {from_name}")
                # Node info packets are handled by Meshtastic library automatically

            # Handle routing packets (traceroute)
            elif portnum == "ROUTING_APP":
                logger.info(f"🛣️ ROUTING: Processing traceroute from {from_name}")
                self._process_routing_packet(packet)

            # Handle neighbor info packets (topology hints)
            elif portnum == "NEIGHBORINFO_APP":
                logger.info(f"🤝 NEIGHBORS: Processing neighbor info from {from_name}")
                self._process_neighborinfo_packet(packet)

            # Handle admin packets
            elif portnum == "ADMIN_APP":
                logger.info(f"⚙️ ADMIN: Administrative message from {from_name}")
                # Admin packets are handled by Meshtastic library automatically

            # Handle other packet types
            else:
                logger.info(f"❓ UNKNOWN: {portnum} packet from {from_name}")

        except Exception as e:
            logger.error(f"Error processing mesh packet: {e}")

    def _process_telemetry_packet(self, packet):
        """Process telemetry packet and extract sensor data"""
        try:
            from_id = packet.get("fromId", "Unknown")

            # Skip if we don't have a valid node ID
            if not from_id or from_id == "Unknown" or from_id is None:
                logger.warning(
                    f"Skipping telemetry packet with invalid fromId: {from_id}"
                )
                return

            decoded = packet.get("decoded", {})
            telemetry_data = decoded.get("telemetry", {})

            if not telemetry_data:
                logger.debug(f"No telemetry data in packet from {from_id}")
                return

            # Extract different types of telemetry data
            extracted_data = {}

            # Device metrics (battery, voltage, uptime, etc.)
            if "deviceMetrics" in telemetry_data:
                device_metrics = telemetry_data["deviceMetrics"]
                if device_metrics.get("batteryLevel") is not None:
                    extracted_data["battery_level"] = device_metrics["batteryLevel"]
                if device_metrics.get("voltage") is not None:
                    extracted_data["voltage"] = device_metrics["voltage"]
                if device_metrics.get("channelUtilization") is not None:
                    extracted_data["channel_utilization"] = device_metrics[
                        "channelUtilization"
                    ]
                if device_metrics.get("airUtilTx") is not None:
                    extracted_data["air_util_tx"] = device_metrics["airUtilTx"]
                if device_metrics.get("uptimeSeconds") is not None:
                    extracted_data["uptime_seconds"] = device_metrics["uptimeSeconds"]

            # Environment metrics (temperature, humidity, pressure, etc.)
            if "environmentMetrics" in telemetry_data:
                env_metrics = telemetry_data["environmentMetrics"]
                if env_metrics.get("temperature") is not None:
                    extracted_data["temperature"] = env_metrics["temperature"]
                # Accept either relativeHumidity or humidity keys
                if env_metrics.get("relativeHumidity") is not None:
                    extracted_data["humidity"] = env_metrics["relativeHumidity"]
                elif env_metrics.get("humidity") is not None:
                    extracted_data["humidity"] = env_metrics["humidity"]
                # Accept either barometricPressure or pressure keys
                if env_metrics.get("barometricPressure") is not None:
                    extracted_data["pressure"] = env_metrics["barometricPressure"]
                elif env_metrics.get("pressure") is not None:
                    extracted_data["pressure"] = env_metrics["pressure"]
                if env_metrics.get("gasResistance") is not None:
                    extracted_data["gas_resistance"] = env_metrics["gasResistance"]

            # Air quality metrics
            if "airQualityMetrics" in telemetry_data:
                air_metrics = telemetry_data["airQualityMetrics"]
                if air_metrics.get("pm10Environmental") is not None:
                    extracted_data["pm10"] = air_metrics["pm10Environmental"]
                if air_metrics.get("pm25Environmental") is not None:
                    extracted_data["pm25"] = air_metrics["pm25Environmental"]
                if air_metrics.get("pm100Environmental") is not None:
                    extracted_data["pm100"] = air_metrics["pm100Environmental"]
                if air_metrics.get("aqi") is not None:
                    extracted_data["iaq"] = air_metrics["aqi"]

            # Power metrics
            if "powerMetrics" in telemetry_data:
                power_metrics = telemetry_data["powerMetrics"]
                if power_metrics.get("ch1Voltage") is not None:
                    extracted_data["ch1_voltage"] = power_metrics["ch1Voltage"]
                if power_metrics.get("ch2Voltage") is not None:
                    extracted_data["ch2_voltage"] = power_metrics["ch2Voltage"]
                if power_metrics.get("ch3Voltage") is not None:
                    extracted_data["ch3_voltage"] = power_metrics["ch3Voltage"]

            # Add radio metrics from packet
            if packet.get("snr") is not None:
                extracted_data["snr"] = packet["snr"]
            if packet.get("rssi") is not None:
                extracted_data["rssi"] = packet["rssi"]
            if packet.get("frequency") is not None:
                extracted_data["frequency"] = packet["frequency"]

            # Position data that can be embedded in telemetry
            pos_obj = telemetry_data.get("position") or {}
            lat = pos_obj.get("latitude") or pos_obj.get("latitude_i")
            lon = pos_obj.get("longitude") or pos_obj.get("longitude_i")
            alt = pos_obj.get("altitude")
            if alt is None:
                alt = telemetry_data.get("altitude")
            if alt is not None:
                try:
                    extracted_data["altitude"] = float(alt)
                except Exception:
                    pass
            if lat is not None and lon is not None:
                try:
                    lat_f = float(lat) / (1e7 if "latitude_i" in pos_obj else 1.0)
                    lon_f = float(lon) / (1e7 if "longitude_i" in pos_obj else 1.0)
                    extracted_data["latitude"] = lat_f
                    extracted_data["longitude"] = lon_f
                except Exception:
                    pass

            # Store telemetry data if we have any
            if extracted_data:
                try:
                    success = self.database.add_telemetry(from_id, extracted_data)
                    if success:
                        logger.info(
                            f"Stored telemetry data for {from_id}: {list(extracted_data.keys())}"
                        )
                        # Store position if telemetry carried location
                        if (
                            extracted_data.get("latitude") is not None
                            and extracted_data.get("longitude") is not None
                        ):
                            try:
                                self.database.add_position(
                                    from_id,
                                    {
                                        "latitude": extracted_data.get("latitude"),
                                        "longitude": extracted_data.get("longitude"),
                                        "altitude": extracted_data.get("altitude"),
                                        "speed": None,
                                        "heading": None,
                                        "accuracy": None,
                                        "source": "telemetry",
                                    },
                                )
                                logger.debug(
                                    f"Stored position from telemetry for {from_id}"
                                )
                            except Exception as pos_err:
                                logger.error(
                                    f"Error storing telemetry-derived position for {from_id}: {pos_err}"
                                )

                        # Add telemetry to live monitor buffer
                        if from_id and from_id != "Unknown" and from_id is not None:
                            telemetry_packet_info = {
                                "type": "telemetry",
                                "portnum": "TELEMETRY_APP",
                                "from_name": self.database.get_node_display_name(
                                    from_id
                                )
                                if self.database
                                else from_id,
                                "from_id": from_id,
                                "sensor_data": list(extracted_data.keys()),
                                "hops": 0,  # Telemetry doesn't have hops info
                                "snr": "N/A",
                                "rssi": "N/A",
                            }
                            if hasattr(self, "command_handler"):
                                self.command_handler.add_packet_to_buffer(
                                    telemetry_packet_info
                                )
                    else:
                        logger.warning(f"Failed to store telemetry data for {from_id}")
                except Exception as telemetry_error:
                    logger.error(
                        f"Error storing telemetry data for {from_id}: {telemetry_error}"
                    )
            else:
                logger.debug(f"No extractable telemetry data from {from_id}")

        except Exception as e:
            logger.error(f"Error processing telemetry packet: {e}")

    def _process_routing_packet(self, packet):
        """Process routing packet and display traceroute information in Discord"""
        try:
            from_id = packet.get("fromId", "Unknown")
            to_id = packet.get("toId", "Primary")
            decoded = packet.get("decoded", {})
            from_name = (
                self.database.get_node_display_name(from_id)
                if self.database
                else from_id
            )
            to_name = (
                self.database.get_node_display_name(to_id)
                if self.database
                else to_id
            )

            # Check if this is a RouteDiscovery packet
            if "routing" in decoded and "routeDiscovery" in decoded["routing"]:
                route_data = decoded["routing"]["routeDiscovery"]

                # Extract route information
                route = route_data.get("route", [])
                route_back = route_data.get("routeBack", [])
                snr_towards = route_data.get("snrTowards", [])
                snr_back = route_data.get("snrBack", [])

                # Build route string
                route_parts = []

                # Route towards destination
                if route:
                    route_parts.append(f"**Towards {to_name}:**")
                    current_route = f"{from_name}"

                    for i, node_num in enumerate(route):
                        node_name = (
                            self.database.get_node_display_name(f"!{node_num:08x}")
                            if self.database
                            else f"!{node_num:08x}"
                        )
                        snr = ""
                        if (
                            i < len(snr_towards) and snr_towards[i] != -128
                        ):  # -128 is UNK_SNR
                            snr = f" ({snr_towards[i] / 4:.1f}dB)"
                        current_route += f" → {node_name}{snr}"

                    # Add destination
                    if snr_towards and len(snr_towards) > len(route):
                        snr = (
                            f" ({snr_towards[-1] / 4:.1f}dB)"
                            if snr_towards[-1] != -128
                            else ""
                        )
                    else:
                        snr = ""
                    current_route += f" → {to_name}{snr}"

                    route_parts.append(current_route)

                # Route back from destination
                if route_back:
                    route_parts.append(f"**Back from {to_name}:**")
                    back_route = f"{to_name}"

                    for i, node_num in enumerate(route_back):
                        node_name = (
                            self.database.get_node_display_name(f"!{node_num:08x}")
                            if self.database
                            else f"!{node_num:08x}"
                        )
                        snr = ""
                        if i < len(snr_back) and snr_back[i] != -128:  # -128 is UNK_SNR
                            snr = f" ({snr_back[i] / 4:.1f}dB)"
                        back_route += f" → {node_name}{snr}"

                    # Add origin
                    if snr_back and len(snr_back) > len(route_back):
                        snr = (
                            f" ({snr_back[-1] / 4:.1f}dB)"
                            if snr_back[-1] != -128
                            else ""
                        )
                    else:
                        snr = ""
                    back_route += f" → {from_name}{snr}"

                    route_parts.append(back_route)

                # Create Discord message
                if route_parts:
                    route_text = "\n".join(route_parts)
                    hops_count = (
                        len(route) + len(route_back) if route_back else len(route)
                    )

                    # Queue for Discord display
                    traceroute_payload = {
                        "type": "traceroute",
                        "from_id": from_id,
                        "from_name": from_name,
                        "to_id": to_id,
                        "to_name": to_name,
                        "route_text": route_text,
                        "hops_count": hops_count,
                        "timestamp": datetime.utcnow().isoformat() + "Z",
                    }
                    try:
                        self.mesh_to_discord.put_nowait(traceroute_payload)
                    except queue.Full:
                        logger.warning(
                            "Mesh to Discord queue is full, dropping traceroute"
                        )
                        # Keep oldest messages for traceroute as they're important
                        try:
                            self.mesh_to_discord.get_nowait()
                            self.mesh_to_discord.put_nowait(traceroute_payload)
                        except queue.Empty:
                            pass
                    logger.info(
                        f"🛣️ TRACEROUTE: Queued route info - {from_name} → {to_name} ({hops_count} hops)"
                    )

                    # Add traceroute to live monitor buffer
                    traceroute_packet_info = {
                        "type": "traceroute",
                        "portnum": "ROUTING_APP",
                        "from_name": from_name,
                        "from_id": from_id,
                        "to_name": to_name,
                        "to_id": to_id,
                        "hops_count": hops_count,
                        "hops": 0,  # Route hops, not packet hops
                        "snr": "N/A",
                        "rssi": "N/A",
                    }
                    if hasattr(self, "command_handler"):
                        self.command_handler.add_packet_to_buffer(
                            traceroute_packet_info
                        )
                else:
                    logger.debug(
                        f"No route information in routing packet from {from_name}"
                    )
            else:
                logger.debug(
                    f"Routing packet from {from_name} does not contain RouteDiscovery data"
                )

        except Exception as e:
            logger.error(f"Error processing routing packet: {e}")

    def _process_neighborinfo_packet(self, packet):
        """Process neighbor info packets and log edges for topology."""
        try:
            from_id = packet.get("fromId", "Unknown")
            decoded = packet.get("decoded", {}) or {}

            neighbors_obj = (
                decoded.get("neighborinfo")
                or decoded.get("neighborInfo")
                or decoded.get("neighbors")
            )
            if isinstance(neighbors_obj, dict) and "neighbors" in neighbors_obj:
                neighbors_obj = neighbors_obj.get("neighbors")

            if not neighbors_obj or not isinstance(neighbors_obj, (list, tuple)):
                logger.debug(f"No neighbor list in packet from {from_id}")
                return

            def _normalize_node_id(val):
                if val is None:
                    return None
                try:
                    if isinstance(val, str):
                        return val if val.startswith("!") else val
                    return f"!{int(val):08x}"
                except Exception:
                    try:
                        return str(val)
                    except Exception:
                        return None

            for nb in neighbors_obj:
                if not isinstance(nb, dict):
                    continue
                nb_id_raw = (
                    nb.get("nodeId")
                    or nb.get("nodeid")
                    or nb.get("id")
                    or nb.get("node")
                    or nb.get("num")
                )
                nb_id = _normalize_node_id(nb_id_raw)
                if not nb_id:
                    continue
                snr = nb.get("snr")
                rssi = nb.get("rssi")
                hops_away = nb.get("hopsAway") or nb.get("hops")
                try:
                    if self.database:
                        self.database.add_message(
                            {
                                "from_node_id": from_id,
                                "to_node_id": nb_id,
                                "message_text": None,
                                "port_num": "NEIGHBORINFO_APP",
                                "payload": str(nb),
                                "hops_away": hops_away if hops_away is not None else 0,
                                "snr": snr,
                                "rssi": rssi,
                            }
                        )
                except Exception as nb_err:
                    logger.error(f"Error storing neighbor info for {from_id}->{nb_id}: {nb_err}")
            logger.info(f"Stored neighbor info edges from {from_id}")

        except Exception as e:
            logger.error(f"Error processing neighborinfo packet: {e}")

    def _process_position_packet(self, packet):
        """Process position packet and detect movement"""
        try:
            from_id = packet.get("fromId", "Unknown")
            if not from_id or from_id == "Unknown":
                logger.warning(
                    f"Skipping position packet with invalid fromId: {from_id}"
                )
                return
            decoded = packet.get("decoded", {})
            position_data = decoded.get("position", {}) or {}

            if not position_data:
                # Some firmware uses lat/lon at top-level decoded
                top_lat = decoded.get("latitude")
                top_lon = decoded.get("longitude")
                top_alt = decoded.get("altitude")
                if top_lat is not None and top_lon is not None:
                    position_data = {
                        "latitude": top_lat,
                        "longitude": top_lon,
                        "altitude": top_alt,
                        "speed": decoded.get("speed"),
                        "ground_track": decoded.get("ground_track"),
                        "precision_bits": decoded.get("precision_bits"),
                    }
                else:
                    logger.debug(f"No position data in packet from {from_id}")
                    return

            # Extract position coordinates with fallbacks
            def _coord(val, scale=None):
                if val is None:
                    return None
                try:
                    val = float(val)
                    if scale:
                        val = val / scale
                    return val
                except Exception:
                    return None

            new_lat = _coord(position_data.get("latitude"))
            if new_lat is None:
                new_lat = _coord(position_data.get("latitude_i"), scale=1e7)
            new_lon = _coord(position_data.get("longitude"))
            if new_lon is None:
                new_lon = _coord(position_data.get("longitude_i"), scale=1e7)
            new_alt = _coord(position_data.get("altitude")) or 0
            speed_mps = _coord(position_data.get("speed"))
            if speed_mps is None:
                speed_mps = _coord(position_data.get("groundSpeed"))
            if speed_mps is None:
                speed_mps = _coord(position_data.get("ground_speed"))
            if speed_mps is not None and speed_mps < 0:
                speed_mps = None
            heading_val = position_data.get("ground_track")
            if heading_val is None:
                heading_val = position_data.get("groundTrack")
            precision_bits = position_data.get("precision_bits")
            if precision_bits is None:
                precision_bits = position_data.get("precisionBits")

            try:
                from config import MOVEMENT_DETECTION

                movement_threshold = float(
                    MOVEMENT_DETECTION.get("distance_threshold_m", 100.0)
                )
                speed_threshold = float(
                    MOVEMENT_DETECTION.get("speed_threshold_mps", 3.0)
                )
                speed_distance_floor = float(
                    MOVEMENT_DETECTION.get("speed_distance_floor_m", 25.0)
                )
                jitter_floor = float(
                    MOVEMENT_DETECTION.get("jitter_floor_m", 15.0)
                )
                min_interval_seconds = float(
                    MOVEMENT_DETECTION.get("min_interval_seconds", 15)
                )
                alert_cooldown_seconds = float(
                    MOVEMENT_DETECTION.get("alert_cooldown_seconds", 300)
                )
            except Exception:
                movement_threshold = 100.0
                speed_threshold = 3.0
                speed_distance_floor = 25.0
                jitter_floor = 15.0
                min_interval_seconds = 15.0
                alert_cooldown_seconds = 300.0

            try:
                if precision_bits is not None and float(precision_bits) < 20:
                    jitter_floor = max(jitter_floor, 50.0)
            except Exception:
                pass

            # Skip if coordinates are invalid
            if new_lat is None or new_lon is None:
                logger.debug(f"Invalid/missing position coordinates from {from_id}")
                return
            if new_lat == 0 and new_lon == 0:
                logger.debug(f"Invalid position coordinates (0,0) from {from_id}")
                return

            def enqueue_movement(
                distance_val: float,
                speed_val: Optional[float],
                old_lat: Optional[float],
                old_lon: Optional[float],
            ):
                if alert_cooldown_seconds and alert_cooldown_seconds > 0:
                    now_ts = time.time()
                    last_alert = self._movement_alerts.get(from_id)
                    if last_alert and (now_ts - last_alert) < alert_cooldown_seconds:
                        return
                    self._movement_alerts[from_id] = now_ts

                from_name = (
                    self.database.get_node_display_name(from_id)
                    if self.database
                    else from_id
                )

                movement_payload = {
                    "type": "movement",
                    "from_id": from_id,
                    "from_name": from_name,
                    "distance_moved": distance_val,
                    "speed_mps": speed_val,
                    "old_lat": old_lat if old_lat is not None else 0,
                    "old_lon": old_lon if old_lon is not None else 0,
                    "new_lat": new_lat,
                    "new_lon": new_lon,
                    "new_alt": new_alt,
                    "timestamp": datetime.utcnow().isoformat() + "Z",
                }

                # Queue for Discord
                try:
                    self.mesh_to_discord.put_nowait(movement_payload)
                except queue.Full:
                    logger.warning(
                        "Mesh to Discord queue is full, dropping movement notification"
                    )
                    try:
                        self.mesh_to_discord.get_nowait()
                        self.mesh_to_discord.put_nowait(movement_payload)
                    except queue.Empty:
                        pass

                log_parts = []
                if distance_val is not None:
                    try:
                        log_parts.append(f"{float(distance_val):.1f}m")
                    except Exception:
                        pass
                if speed_val is not None:
                    try:
                        log_parts.append(f"{float(speed_val):.1f}m/s")
                    except Exception:
                        pass
                log_suffix = f" ({', '.join(log_parts)})" if log_parts else ""
                logger.info(f"🚶 MOVEMENT: {from_name} on the move{log_suffix}")

                # Add to live monitor buffer
                if hasattr(self, "command_handler"):
                    movement_packet_info = {
                        "type": "movement",
                        "portnum": "POSITION_APP",
                        "from_name": from_name,
                        "from_id": from_id,
                        "distance_moved": distance_val,
                        "speed_mps": speed_val,
                        "hops": 0,
                        "snr": "N/A",
                        "rssi": "N/A",
                    }
                    self.command_handler.add_packet_to_buffer(movement_packet_info)

            # Get last known position from database
            if self.database:
                def _parse_ts(value: Optional[str]) -> Optional[datetime]:
                    if not value:
                        return None
                    try:
                        return datetime.fromisoformat(
                            str(value).replace("Z", "+00:00")
                        )
                    except Exception:
                        pass
                    try:
                        return datetime.strptime(str(value), "%Y-%m-%d %H:%M:%S")
                    except Exception:
                        return None

                last_position = self.database.get_last_position(from_id)

                last_lat = None
                last_lon = None
                distance_moved = None
                movement_notified = False
                elapsed_seconds = None
                new_ts = None

                packet_time = position_data.get("time") or position_data.get(
                    "timestamp"
                )
                if packet_time is not None:
                    try:
                        if isinstance(packet_time, (int, float)):
                            new_ts = datetime.utcfromtimestamp(float(packet_time))
                        else:
                            new_ts = _parse_ts(packet_time)
                    except Exception:
                        new_ts = None
                if new_ts is None:
                    new_ts = datetime.utcnow()

                if last_position:
                    last_lat = last_position.get("latitude", 0)
                    last_lon = last_position.get("longitude", 0)
                    last_ts = _parse_ts(last_position.get("timestamp"))
                    if last_ts is not None:
                        elapsed_seconds = max(
                            0.0, (new_ts - last_ts).total_seconds()
                        )

                    # Calculate distance moved
                    if (
                        last_lat != 0
                        and last_lon != 0
                        and (
                            elapsed_seconds is None
                            or elapsed_seconds >= min_interval_seconds
                        )
                    ):
                        distance_moved = self.calculate_distance(
                            last_lat, last_lon, new_lat, new_lon
                        )

                        if (
                            distance_moved is not None
                            and distance_moved < jitter_floor
                        ):
                            distance_moved = None

                        if distance_moved is not None and distance_moved >= movement_threshold:
                            enqueue_movement(
                                distance_moved,
                                speed_mps,
                                last_lat,
                                last_lon,
                            )
                            movement_notified = True

                effective_speed = speed_mps
                if (
                    effective_speed is None
                    and distance_moved is not None
                    and elapsed_seconds
                    and elapsed_seconds > 0
                ):
                    effective_speed = distance_moved / elapsed_seconds

                # Speed-only trigger if node is reporting movement but hasn't crossed distance threshold
                if (
                    not movement_notified
                    and effective_speed is not None
                    and effective_speed >= speed_threshold
                    and distance_moved is not None
                    and distance_moved >= speed_distance_floor
                ):
                    enqueue_movement(
                        distance_moved,
                        speed_mps if speed_mps is not None else effective_speed,
                        last_lat,
                        last_lon,
                    )
                    movement_notified = True

            # Store new position in database
            if self.database:
                try:
                    position_data_to_store = {
                        "latitude": new_lat,
                        "longitude": new_lon,
                        "altitude": new_alt,
                        "speed": speed_mps if speed_mps is not None else 0,
                        "heading": heading_val if heading_val is not None else 0,
                        "accuracy": position_data.get("precision_bits", 0),
                        "source": "meshtastic",
                    }
                    self.database.add_position(from_id, position_data_to_store)
                    logger.debug(
                        f"Stored position for {from_id}: {new_lat:.6f}, {new_lon:.6f}"
                    )
                except Exception as pos_error:
                    logger.error(f"Error storing position for {from_id}: {pos_error}")

            try:
                cfg = self._get_adsb_config()
                threshold = float(cfg.get("flight_altitude_threshold_m", 5000))
                if new_alt is not None and float(new_alt) >= threshold:
                    from_name = (
                        self.database.get_node_display_name(from_id)
                        if self.database
                        else from_id
                    )
                    position_payload = {
                        "latitude": new_lat,
                        "longitude": new_lon,
                        "altitude": new_alt,
                        "timestamp": datetime.utcnow().isoformat() + "Z",
                    }
                    self._schedule_flight_lookup(
                        from_id,
                        from_name,
                        position=position_payload,
                        airborne_hint=True,
                    )
            except Exception as flight_err:
                logger.debug(f"Flight lookup trigger failed: {flight_err}")

        except Exception as e:
            logger.error(f"Error processing position packet: {e}")

    async def high_altitude_task(self):
        """Background task to announce high-altitude nodes."""
        await self.wait_until_ready()
        # Determine alert channel (fallback to main)
        alert_channel_id = (
            getattr(self.config, "alert_channel_id", 0) or self.config.channel_id
        )
        channel = self.get_channel(alert_channel_id)
        while not self.is_closed():
            try:
                # Refresh channel if not found
                if channel is None:
                    channel = self.get_channel(alert_channel_id)
                try:
                    from config import HIGH_ALTITUDE

                    threshold_m = HIGH_ALTITUDE.get("high_altitude_threshold_m", 1500)
                    cooldown_min = HIGH_ALTITUDE.get(
                        "high_altitude_cooldown_minutes", 180
                    )
                except Exception:
                    threshold_m = 1500
                    cooldown_min = 180

                if self.database and channel:
                    if hasattr(self.database, "get_nodes_with_latest_altitudes"):
                        nodes = await asyncio.to_thread(
                            self.database.get_nodes_with_latest_altitudes, 500
                        )
                    else:
                        nodes = await asyncio.to_thread(
                            self.database.get_nodes_with_latest_positions, 500
                        )
                    for n in nodes:
                        node_id = n.get("node_id")
                        if not node_id:
                            continue
                        long_name = n.get("long_name") or node_id
                        altitude = n.get("altitude")
                        altitude_source = n.get("altitude_source")
                        if altitude is None:
                            continue
                        if isinstance(altitude, str):
                            try:
                                altitude = float(altitude)
                            except Exception:
                                continue
                        if altitude >= float(threshold_m):
                            recent = (
                                await asyncio.to_thread(
                                    self.database.recent_alert_exists,
                                    node_id,
                                    "high_altitude",
                                    cooldown_min,
                                )
                                if hasattr(self.database, "recent_alert_exists")
                                else True
                            )
                            if recent:
                                continue
                            try:
                                embed = discord.Embed(
                                    title="✈️ High Altitude Node Detected",
                                    description=f"**{long_name}** appears to be at high altitude",
                                    color=0x00FF00,
                                    timestamp=get_utc_time(),
                                )
                                embed.add_field(
                                    name="Altitude (m)",
                                    value=f"{altitude:.0f}",
                                    inline=True,
                                )
                                if altitude_source:
                                    embed.add_field(
                                        name="Source",
                                        value=str(altitude_source).title(),
                                        inline=True,
                                    )
                                ts = n.get("timestamp")
                                if ts:
                                    embed.add_field(
                                        name="Last Update",
                                        value=str(ts),
                                        inline=True,
                                    )
                                lat = n.get("latitude")
                                lon = n.get("longitude")
                                if lat is not None and lon is not None:
                                    embed.add_field(
                                        name="Location",
                                        value=f"{lat:.5f}, {lon:.5f}",
                                        inline=True,
                                    )
                                    embed.add_field(
                                        name="Map",
                                        value=f"https://maps.google.com/?q={lat},{lon}",
                                        inline=True,
                                    )
                                await channel.send(embed=embed)
                                if hasattr(self.database, "record_alert"):
                                    await asyncio.to_thread(
                                        self.database.record_alert,
                                        node_id,
                                        "high_altitude",
                                    )
                            except Exception as send_err:
                                logger.error(
                                    f"Failed to send high altitude alert for {node_id}: {send_err}"
                                )
                await asyncio.sleep(60)
            except Exception as e:
                logger.error(f"Error in high_altitude_task: {e}")
                await asyncio.sleep(60)

    async def flight_monitor_task(self):
        """Monitor tracked flights for mesh or ADSB out-of-range events."""
        await self.wait_until_ready()
        while not self.is_closed():
            cfg = self._get_adsb_config()
            interval = float(cfg.get("flight_monitor_interval_seconds", 30))
            try:
                if not self._flight_tracks:
                    await asyncio.sleep(interval)
                    continue

                aircraft = await self._fetch_adsb_aircraft(cfg)
                by_hex = {}
                for plane in aircraft:
                    hex_id = plane.get("hex")
                    if hex_id:
                        by_hex[str(hex_id).lower()] = plane

                now_ts = time.time()
                mesh_out_seconds = float(cfg.get("mesh_out_of_range_seconds", 300))
                adsb_out_seconds = float(cfg.get("adsb_out_of_range_seconds", 120))
                max_seen_pos = float(cfg.get("max_seen_pos_seconds", 60))

                alert_channel_id = (
                    getattr(self.config, "alert_channel_id", 0) or self.config.channel_id
                )
                channel = self.get_channel(alert_channel_id)

                for node_id, track in list(self._flight_tracks.items()):
                    flight = track.get("flight") or "Unknown"
                    node_name = track.get("node_name") or node_id

                    last_mesh_seen = track.get("last_mesh_seen_ts")
                    if (
                        last_mesh_seen
                        and not track.get("safe_flight_announced")
                        and now_ts - float(last_mesh_seen) >= mesh_out_seconds
                    ):
                        if channel:
                            await channel.send(
                                f"✈️ Safe flight {node_name} on flight {flight}!"
                            )
                        track["safe_flight_announced"] = True

                    hex_id = track.get("hex")
                    if hex_id:
                        last_adsb_seen = track.get("last_adsb_seen_ts")
                        plane = by_hex.get(str(hex_id).lower())
                        seen_pos = None
                        if plane:
                            seen_pos = plane.get("seen_pos")
                            if seen_pos is None:
                                seen_pos = plane.get("seen")
                            try:
                                seen_pos = (
                                    float(seen_pos) if seen_pos is not None else None
                                )
                            except Exception:
                                seen_pos = None
                            if seen_pos is None or seen_pos <= max_seen_pos:
                                track["last_adsb_seen_ts"] = now_ts

                        if (
                            not track.get("adsb_out_announced")
                            and last_adsb_seen
                            and now_ts - float(last_adsb_seen) >= adsb_out_seconds
                        ):
                            if channel:
                                await channel.send(
                                    f"✈️ Flight {flight} is now out of ADSB range"
                                )
                            track["adsb_out_announced"] = True

                await asyncio.sleep(interval)
            except Exception as e:
                logger.error(f"Error in flight_monitor_task: {e}")
                await asyncio.sleep(interval)

    def calculate_distance(
        self, lat1: float, lon1: float, lat2: float, lon2: float
    ) -> float:
        """Calculate distance between two coordinates in meters using Haversine formula"""
        try:
            import math

            lat1_rad = math.radians(lat1)
            lon1_rad = math.radians(lon1)
            lat2_rad = math.radians(lat2)
            lon2_rad = math.radians(lon2)

            dlat = lat2_rad - lat1_rad
            dlon = lon2_rad - lon1_rad
            a = (
                math.sin(dlat / 2) ** 2
                + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon / 2) ** 2
            )
            c = 2 * math.asin(math.sqrt(a))

            earth_radius = 6371000  # meters
            return earth_radius * c
        except Exception as e:
            logger.error(f"Error calculating distance: {e}")
            return 0.0

    def on_mesh_connection(self, interface, topic=pub.AUTO_TOPIC):
        """Handle mesh connection events"""
        logger.info(f"Connected to Meshtastic: {interface.myInfo}")

    async def close(self):
        """Clean shutdown of the bot"""
        try:
            logger.info("Shutting down bot...")
            try:
                self._save_persisted_queues()
            except Exception as save_err:
                logger.warning(f"Queue persistence failed: {save_err}")

            # Close database connections
            if hasattr(self.database, "close_connections"):
                self.database.close_connections()

            # Close Meshtastic interface
            if self.meshtastic and self.meshtastic.iface:
                try:
                    self.meshtastic.iface.close()
                except Exception as e:
                    logger.warning(f"Error closing Meshtastic interface: {e}")

            # Cancel background tasks
            if self.bg_task and not self.bg_task.done():
                self.bg_task.cancel()
                try:
                    await self.bg_task
                except asyncio.CancelledError:
                    pass

            if self.telemetry_task and not self.telemetry_task.done():
                self.telemetry_task.cancel()
                try:
                    await self.telemetry_task
                except asyncio.CancelledError:
                    pass

            if (
                self.high_altitude_task_handle
                and not self.high_altitude_task_handle.done()
            ):
                self.high_altitude_task_handle.cancel()
                try:
                    await self.high_altitude_task_handle
                except asyncio.CancelledError:
                    pass

            if (
                self.db_maintenance_task_handle
                and not self.db_maintenance_task_handle.done()
            ):
                self.db_maintenance_task_handle.cancel()
                try:
                    await self.db_maintenance_task_handle
                except asyncio.CancelledError:
                    pass

            # Close Discord connection
            await super().close()
            logger.info("Bot shutdown complete")

        except Exception as e:
            logger.error(f"Error during bot shutdown: {e}")
            await super().close()


def main():
    """Main function to run the bot"""
    try:
        def _env_or_none(*names):
            for name in names:
                value = os.getenv(name)
                if value is not None:
                    value = value.strip()
                    if value:
                        return value
            return None

        def _env_int(name: str) -> Optional[int]:
            value = _env_or_none(name)
            if value is None:
                return None
            try:
                return int(value)
            except ValueError:
                logger.warning(f"Invalid integer for {name}: {value!r}")
                return None

        # Load configuration
        config_kwargs = {
            "discord_token": os.getenv("DISCORD_TOKEN"),
            "channel_id": int(os.getenv("DISCORD_CHANNEL_ID", "0")),
            "meshtastic_hostname": _env_or_none("MESHTASTIC_HOSTNAME"),
            "meshtastic_serial_port": _env_or_none(
                "MESHTASTIC_SERIAL_PORT", "MESHTASTIC_PORT"
            ),
        }
        alert_channel_id = _env_int("ALERT_CHANNEL_ID")
        if alert_channel_id is not None:
            config_kwargs["alert_channel_id"] = alert_channel_id
        config = Config(**config_kwargs)

        # Validate configuration
        if not config.discord_token:
            logger.error("DISCORD_TOKEN not found in environment variables")
            sys.exit(1)

        if not config.channel_id:
            logger.error(
                "DISCORD_CHANNEL_ID not found or invalid in environment variables"
            )
            sys.exit(1)

        # Initialize database
        try:
            database = MeshtasticDatabase()
            logger.info("Database initialized successfully")
        except Exception as db_error:
            logger.error(f"Failed to initialize database: {db_error}")
            sys.exit(1)

        # Create Meshtastic interface
        try:
            meshtastic_interface = MeshtasticInterface(
                config.meshtastic_hostname, database, serial_port=config.meshtastic_serial_port
            )
            logger.info("Meshtastic interface created successfully")
        except Exception as mesh_error:
            logger.error(f"Failed to create Meshtastic interface: {mesh_error}")
            sys.exit(1)

        # Create and run bot
        try:
            bot = DiscordBot(config, meshtastic_interface, database)
            logger.info("Discord bot created successfully")
            bot.run(config.discord_token)
        except Exception as bot_error:
            logger.error(f"Failed to create or run Discord bot: {bot_error}")
            sys.exit(1)

    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
