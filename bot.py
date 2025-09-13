import discord
import asyncio
import os
import sys
import io
import logging
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
from datetime import datetime, timedelta
from database import MeshtasticDatabase
import functools
import weakref
from config import BOT_CONFIG, LOGGING_CONFIG

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('bot.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

def get_utc_time():
    """Get current time in UTC"""
    return datetime.utcnow()

def format_utc_time(dt=None, format_str="%Y-%m-%d %H:%M:%S UTC"):
    """Format datetime in UTC"""
    if dt is None:
        dt = get_utc_time()
    return dt.strftime(format_str)

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
    message_max_length: int = BOT_CONFIG['message_max_length']
    node_refresh_interval: int = BOT_CONFIG['node_refresh_interval']
    active_node_threshold: int = BOT_CONFIG['active_node_threshold']  # minutes (now using config.py value)
    telemetry_update_interval: int = 3600  # 1 hour in seconds

class MeshtasticInterface:
    """Handles Meshtastic radio communication"""
    
    def __init__(self, hostname: Optional[str] = None, database: Optional[MeshtasticDatabase] = None):
        self.hostname = hostname
        self.iface = None  # Changed to match reference implementation
        self.database = database
        self.last_node_refresh = 0
        
    async def connect(self) -> bool:
        """Connect to Meshtastic radio"""
        try:
            if self.hostname and len(self.hostname) > 1:
                logger.info(f"Connecting to Meshtastic via TCP: {self.hostname}")
                self.iface = meshtastic.tcp_interface.TCPInterface(self.hostname)
            else:
                logger.info("Connecting to Meshtastic via Serial")
                self.iface = meshtastic.serial_interface.SerialInterface()
            
            # Wait for connection
            await asyncio.sleep(2)
            
            # Check connection status more safely
            try:
                if hasattr(self.iface, 'isConnected') and callable(self.iface.isConnected):
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
                logger.info(f"Attempting to send message to node {destination_id} (type: {type(destination_id)})")
                # Use the correct Meshtastic API based on the reference implementation
                try:
                    # Try the standard Meshtastic API
                    self.iface.sendText(message, destinationId=destination_id)
                    logger.info(f"Sent message to node {destination_id}: {message[:50]}...")
                except Exception as e:
                    logger.error(f"Error sending to specific node {destination_id}: {e}")
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
            if not hasattr(self.iface, 'nodes'):
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
                    node_info = {
                        'node_id': str(node_id),
                        'node_num': node_data.get('num'),
                        'long_name': str(node_data.get('user', {}).get('longName', 'Unknown')),
                        'short_name': str(node_data.get('user', {}).get('shortName', '')),
                        'macaddr': node_data.get('macaddr'),
                        'hw_model': node_data.get('hwModel'),
                        'firmware_version': node_data.get('firmwareVersion'),
                        'last_heard': datetime.fromtimestamp(node_data.get('lastHeard', time.time())).isoformat(),
                        'hops_away': node_data.get('hopsAway', 0),
                        'is_router': node_data.get('isRouter', False),
                        'is_client': node_data.get('isClient', True)
                    }
                    
                                    # Store in database
                    try:
                        success, is_new = self.database.add_or_update_node(node_info)
                        if success:
                            processed_nodes.append(node_info)
                            if is_new:
                                new_nodes.append(node_info)
                                logger.info(f"New node added: {node_info['long_name']} ({node_info['node_id']})")
                    except Exception as db_error:
                        logger.error(f"Database error for node {node_id}: {db_error}")
                        continue
                    
                    # Store telemetry if available - check for actual values
                    telemetry_data = {}
                    if node_data.get('snr') is not None:
                        telemetry_data['snr'] = node_data.get('snr')
                    if node_data.get('rssi') is not None:
                        telemetry_data['rssi'] = node_data.get('rssi')
                    if node_data.get('frequency') is not None:
                        telemetry_data['frequency'] = node_data.get('frequency')
                    if node_data.get('latitude') is not None:
                        telemetry_data['latitude'] = node_data.get('latitude')
                    if node_data.get('longitude') is not None:
                        telemetry_data['longitude'] = node_data.get('longitude')
                    if node_data.get('altitude') is not None:
                        telemetry_data['altitude'] = node_data.get('altitude')
                    if node_data.get('speed') is not None:
                        telemetry_data['speed'] = node_data.get('speed')
                    if node_data.get('heading') is not None:
                        telemetry_data['heading'] = node_data.get('heading')
                    if node_data.get('accuracy') is not None:
                        telemetry_data['accuracy'] = node_data.get('accuracy')
                    
                    # Only store telemetry if we have actual data
                    if telemetry_data:
                        try:
                            self.database.add_telemetry(node_info['node_id'], telemetry_data)
                            logger.debug(f"Stored telemetry for {node_info['long_name']}: {telemetry_data}")
                        except Exception as telemetry_error:
                            logger.error(f"Error storing telemetry for node {node_id}: {telemetry_error}")
                    
                    # Store position if available
                    if (node_data.get('latitude') is not None and 
                        node_data.get('longitude') is not None):
                        try:
                            position_data = {
                                'latitude': node_data.get('latitude'),
                                'longitude': node_data.get('longitude'),
                                'altitude': node_data.get('altitude'),
                                'speed': node_data.get('speed'),
                                'heading': node_data.get('heading'),
                                'accuracy': node_data.get('accuracy'),
                                'source': 'meshtastic'
                            }
                            self.database.add_position(node_info['node_id'], position_data)
                            logger.debug(f"Stored position for {node_info['long_name']}")
                        except Exception as position_error:
                            logger.error(f"Error storing position for node {node_id}: {position_error}")
                        
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
    
    def __init__(self, meshtastic: MeshtasticInterface, discord_to_mesh: queue.Queue, database: MeshtasticDatabase, event_loop=None):
        self.meshtastic = meshtastic
        self.discord_to_mesh = discord_to_mesh
        self.database = database
        self.event_loop = event_loop
        self.commands = {
            '$help': self.cmd_help,
            '$txt': self.cmd_send_primary,  # Changed from $sendprimary
            '$send': self.cmd_send_node,    # Changed to use fuzzy name matching
            '$activenodes': self.cmd_active_nodes,
            '$nodes': self.cmd_all_nodes,
            '$telem': self.cmd_telemetry,
            '$status': self.cmd_status,
            '$topo': self.cmd_topology_tree,
            '$topology': self.cmd_network_topology,
            '$stats': self.cmd_message_statistics,
            '$trace': self.cmd_trace_route,
            '$leaderboard': self.cmd_leaderboard,
            '$live': self.cmd_live_monitor,
            '$art': self.cmd_network_art,
            '$clear': self.cmd_clear_database,
            '$debug': self.cmd_debug_info
        }
        
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
    
    async def handle_command(self, message: discord.Message) -> bool:
        """Route command to appropriate handler with rate limiting"""
        content = message.content.strip()
        
        # Rate limiting check
        user_id = message.author.id
        now = time.time()
        
        if user_id in self._command_cooldowns:
            if now - self._command_cooldowns[user_id] < self._cooldown_duration:
                await self._safe_send(message.channel, "⏰ Please wait a moment before using another command.")
                return True
        
        # Update cooldown (only after successful command execution)
        # We'll move this to after the command is executed
        
        for cmd, handler in self.commands.items():
            if content.startswith(cmd):
                try:
                    await handler(message)
                    # Update cooldown only after successful command execution
                    self._command_cooldowns[user_id] = now
                    return True
                except Exception as e:
                    logger.error(f"Error handling command {cmd}: {e}")
                    await self._safe_send(message.channel, f"❌ Error executing command: {e}")
                    return True
        
        return False
    
    async def _get_cached_data(self, key: str, fetch_func, *args, **kwargs):
        """Get data from cache or fetch if not available (thread-safe)"""
        now = time.time()
        
        async with self._cache_lock:
            # Check cache first
            if (key in self._node_cache and 
                key in self._cache_timestamps and 
                now - self._cache_timestamps[key] < self._cache_ttl):
                return self._node_cache[key]
        
        # Fetch fresh data outside the lock to avoid blocking
        try:
            data = fetch_func(*args, **kwargs)
            
            # Update cache with lock
            async with self._cache_lock:
                self._node_cache[key] = data
                self._cache_timestamps[key] = now
            
            return data
        except Exception as e:
            logger.error(f"Error fetching data for cache key {key}: {e}")
            # Return cached data if available, even if stale
            async with self._cache_lock:
                return self._node_cache.get(key, [])
    
    async def clear_cache(self):
        """Clear all cached data (thread-safe)"""
        async with self._cache_lock:
            self._node_cache.clear()
            self._cache_timestamps.clear()
        logger.info("Command handler cache cleared")
    
    def add_packet_to_buffer(self, packet_info: dict):
        """Add packet information to the live monitor buffer (thread-safe)"""
        try:
            # Add timestamp
            packet_info['timestamp'] = datetime.utcnow().isoformat()
            
            # Thread-safe buffer update using regular lock
            with self._packet_buffer_lock:
                # Add to buffer
                self._packet_buffer.append(packet_info)
                
                # Keep only the last N packets
                if len(self._packet_buffer) > self._max_packet_buffer:
                    self._packet_buffer.pop(0)
                
        except Exception as e:
            logger.error(f"Error adding packet to buffer: {e}")
    
    def calculate_distance(self, lat1: float, lon1: float, lat2: float, lon2: float) -> float:
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
            a = math.sin(dlat/2)**2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon/2)**2
            c = 2 * math.asin(math.sqrt(a))
            
            # Earth's radius in meters
            earth_radius = 6371000
            distance = earth_radius * c
            
            return distance
        except Exception as e:
            logger.error(f"Error calculating distance: {e}")
            return 0.0
    
    async def cmd_help(self, message: discord.Message):
        """Show help information"""
        embed = discord.Embed(
            title="🤖 Meshtastic Discord Bridge Commands",
            description="Complete command reference for the mesh network bridge",
            color=0x00ff00,
            timestamp=get_utc_time()
        )
        embed.set_thumbnail(url="https://raw.githubusercontent.com/meshtastic/firmware/master/docs/assets/logo/meshtastic-logo.png")
        embed.set_footer(text="🌍 UTC Time | Use $help <command> for detailed info")
        
        # Basic Commands
        embed.add_field(
            name="📡 **Basic Commands**",
            value=f"""`$help` - Show this help message
`$txt <message>` - Send message to primary channel (max 225 chars)
`$send <longname> <message>` - Send message to specific node by name
`$activenodes` - Show nodes active in last {BOT_CONFIG['active_node_threshold']} minutes
`$nodes` - Show all known nodes
`$telem` - Show telemetry information
`$status` - Show bridge status
`ping` - Send "Pong!" to mesh network""",
            inline=False
        )
        
        # Advanced Commands
        embed.add_field(
            name="🔍 **Advanced Commands**",
            value="""`$topo` - Show visual tree of all radio connections
`$topology` - Show network topology and connections
`$stats` - Show message statistics and network activity
`$trace <node_name>` - Trace route to a specific node""",
            inline=False
        )
        
        # Epic Analytics Commands
        embed.add_field(
            name="📊 **Epic Analytics**",
            value="""`$leaderboard` - Network performance leaderboards
`$live` - Real-time network monitor (1 min)
`$art` - ASCII network art""",
            inline=False
        )
        
        # Admin Commands
        embed.add_field(
            name="🔧 **Admin Commands**",
            value="""`$debug` - Show debug information""",
            inline=False
        )
        
        embed.add_field(
            name="💡 **Command Examples**",
            value="""`$send John Hello there!`
`$send "John Doe" Hello there!` (use quotes for names with spaces)
`$trace Node123` - Trace route to Node123
`ping` - Test mesh connectivity""",
            inline=False
        )
        
        embed.set_footer(text="Use any command to get started!")
        
        await message.channel.send(embed=embed)
    
    async def cmd_send_primary(self, message: discord.Message):
        """Send message to primary channel (renamed from $sendprimary to $txt)"""
        content = message.content
        if ' ' not in content:
            await self._safe_send(message.channel, "❌ Please provide a message to send.")
            return
        
        message_text = content[content.find(' ')+1:][:225]
        if not message_text.strip():
            await self._safe_send(message.channel, "❌ Message cannot be empty.")
            return
        
        await self._safe_send(message.channel, f"📤 Sending to primary channel:\n```{message_text}```")
        try:
            self.discord_to_mesh.put_nowait(message_text)
        except queue.Full:
            logger.warning("Discord to mesh queue is full, dropping message")
            return False
    
    async def cmd_send_node(self, message: discord.Message):
        """Send message to specific node using fuzzy name matching"""
        content = message.content
        
        if not content.startswith('$send '):
            await self._safe_send(message.channel, "❌ Use format: `$send <longname> <message>`")
            return
        
        try:
            # Extract node name and message
            parts = content.split(' ', 2)
            if len(parts) < 3:
                await self._safe_send(message.channel, "❌ Use format: `$send <longname> <message>`")
                return
            
            node_name = parts[1]
            message_text = parts[2][:225]
            
            if not message_text.strip():
                await self._safe_send(message.channel, "❌ Message cannot be empty.")
                return
            
            # Find node by name using fuzzy matching
            try:
                logger.info(f"Searching for node with name: '{node_name}'")
                node = self.database.find_node_by_name(node_name)
                if not node:
                    await self._safe_send(message.channel, f"❌ No node found with name '{node_name}'. Try using `$nodes` to see available nodes.")
                    return
                
                logger.info(f"Found node: {node['long_name']} with ID: {node['node_id']}")
            except Exception as db_error:
                logger.error(f"Database error finding node by name: {db_error}")
                await self._safe_send(message.channel, "❌ Error searching for node in database.")
                return
            
            # Clean the node ID (remove any prefixes like '!' that Meshtastic doesn't expect)
            clean_node_id = node['node_id'].lstrip('!')
            
            # Log the node data for debugging
            logger.info(f"Node data: {node}")
            logger.info(f"Original node_id: '{node['node_id']}', Cleaned: '{clean_node_id}'")
            
            # Try to convert to integer format that Meshtastic expects
            try:
                # Convert hex string to integer (this is what Meshtastic typically expects)
                node_id_int = int(clean_node_id, 16)
                logger.info(f"Converted to integer: {node_id_int}")
                final_node_id = node_id_int
            except ValueError:
                # If conversion fails, use the cleaned string
                logger.info(f"Could not convert '{clean_node_id}' to integer, using string")
                final_node_id = clean_node_id
            
            await self._safe_send(message.channel, f"📤 Sending to node **{node['long_name']}** (ID: {final_node_id}):\n```{message_text}```")
            try:
                self.discord_to_mesh.put_nowait(f"nodenum={final_node_id} {message_text}")
            except queue.Full:
                logger.warning("Discord to mesh queue is full, dropping message")
                await self._safe_send(message.channel, "❌ Message queue is full, please try again later.")
                return
            logger.info(f"Sent message with node ID: {final_node_id}")
            
        except Exception as e:
            logger.error(f"Error parsing send command: {e}")
            await self._safe_send(message.channel, "❌ Error parsing command. Use format: `$send <longname> <message>`")
    
    async def cmd_active_nodes(self, message: discord.Message):
        """Show active nodes from last {} minutes""".format(BOT_CONFIG['active_node_threshold'])
        try:
            # Use caching for better performance
            nodes = await self._get_cached_data(
                f"active_nodes_{BOT_CONFIG['active_node_threshold']}", 
                self.database.get_active_nodes, 
                BOT_CONFIG['active_node_threshold']
            )
            if not nodes:
                embed = discord.Embed(
                    title="📡 Active Nodes",
                    description=f"No active nodes in the last {BOT_CONFIG['active_node_threshold']} minutes",
                    color=0xff6b6b,
                    timestamp=get_utc_time()
                )
                embed.set_thumbnail(url="https://raw.githubusercontent.com/meshtastic/firmware/master/docs/assets/logo/meshtastic-logo.png")
                embed.set_footer(text="🌍 UTC Time | Check back later for activity")
                await message.channel.send(embed=embed)
                return
        except Exception as db_error:
            logger.error(f"Database error getting active nodes: {db_error}")
            await self._safe_send(message.channel, "❌ Error retrieving node data from database.")
            return
        
        active_nodes = []
        for node in nodes:
            try:
                node_info = self._format_node_info(node)
                active_nodes.append(node_info)
            except Exception as e:
                logger.error(f"Error processing node {node.get('node_id', 'Unknown')}: {e}")
                continue
        
        response = f"📡 **Active Nodes (Last {BOT_CONFIG['active_node_threshold']} minutes):**\n" + "\n".join(active_nodes)
        try:
            await self._send_long_message(message.channel, response)
        except Exception as send_error:
            logger.error(f"Error sending message to channel: {send_error}")
            await message.channel.send("❌ Error sending message to channel.")
    
    async def cmd_all_nodes(self, message: discord.Message):
        """Show all known nodes"""
        try:
            # Use caching for better performance
            nodes = await self._get_cached_data(
                "all_nodes", 
                self.database.get_all_nodes
            )
            if not nodes:
                await message.channel.send("📡 No nodes available.")
                return
        except Exception as db_error:
            logger.error(f"Database error getting all nodes: {db_error}")
            await message.channel.send("❌ Error retrieving node data from database.")
            return
        
        node_list = []
        for node in nodes:
            try:
                node_info = self._format_node_info(node)
                node_list.append(node_info)
            except Exception as e:
                logger.error(f"Error processing node {node.get('node_id', 'Unknown')}: {e}")
                continue
        
        response = "📡 **All Known Nodes:**\n" + "\n".join(node_list)
        try:
            await self._send_long_message(message.channel, response)
        except Exception as send_error:
            logger.error(f"Error sending message to channel: {send_error}")
            await self._safe_send(message.channel, "❌ Error sending message to channel.")
    
    async def cmd_telemetry(self, message: discord.Message):
        """Show telemetry information"""
        try:
            summary = self.database.get_telemetry_summary(60)
            if not summary:
                embed = discord.Embed(
                    title="📊 Telemetry Summary",
                    description="No telemetry data available in the last 60 minutes",
                    color=0xff6b6b,
                    timestamp=get_utc_time()
                )
                embed.set_thumbnail(url="https://raw.githubusercontent.com/meshtastic/firmware/master/docs/assets/logo/meshtastic-logo.png")
                embed.set_footer(text="🌍 UTC Time | Data collection in progress...")
                await message.channel.send(embed=embed)
                return
        except Exception as db_error:
            logger.error(f"Database error getting telemetry summary: {db_error}")
            await self._safe_send(message.channel, "❌ Error retrieving telemetry data from database.")
            return
        
        # Check connection status safely
        connection_status = "❌ Disconnected"
        if self.meshtastic.iface:
            try:
                if hasattr(self.meshtastic.iface, 'isConnected') and callable(self.meshtastic.iface.isConnected):
                    if self.meshtastic.iface.isConnected():
                        connection_status = "✅ Connected"
            except Exception:
                connection_status = "❌ Disconnected"
        
        embed = discord.Embed(
            title="📊 Telemetry Summary",
            description="Last 60 minutes of network telemetry data",
            color=0x00ff00,
            timestamp=get_utc_time()
        )
        
        # Node statistics
        embed.add_field(
            name="📡 **Network Status**",
            value=f"""Total Nodes: {summary.get('total_nodes', 0)}
Active Nodes: {summary.get('active_nodes', 0)}
Connection: {connection_status}""",
            inline=True
        )
        
        # Environmental data
        env_data = ""
        if summary.get('avg_battery') is not None:
            env_data += f"🔋 Battery: {summary['avg_battery']:.1f}%\n"
        else:
            env_data += "🔋 Battery: N/A\n"
            
        if summary.get('avg_temperature') is not None:
            env_data += f"🌡️ Temperature: {summary['avg_temperature']:.1f}°C\n"
        else:
            env_data += "🌡️ Temperature: N/A\n"
            
        if summary.get('avg_humidity') is not None:
            env_data += f"💧 Humidity: {summary['avg_humidity']:.1f}%\n"
        else:
            env_data += "💧 Humidity: N/A\n"
        
        embed.add_field(
            name="🌍 **Environmental**",
            value=env_data,
            inline=True
        )
        
        # Signal quality
        signal_data = ""
        if summary.get('avg_snr') is not None:
            signal_data += f"📶 SNR: {summary['avg_snr']:.1f} dB\n"
        else:
            signal_data += "📶 SNR: N/A\n"
            
        if summary.get('avg_rssi') is not None:
            signal_data += f"📡 RSSI: {summary['avg_rssi']:.1f} dBm\n"
        else:
            signal_data += "📡 RSSI: N/A\n"
        
        embed.add_field(
            name="📶 **Signal Quality**",
            value=signal_data,
            inline=True
        )
        
        await message.channel.send(embed=embed)
    
    async def cmd_status(self, message: discord.Message):
        """Show bridge status"""
        # Check Meshtastic connection status safely
        meshtastic_status = "❌ Disconnected"
        if self.meshtastic.iface:
            try:
                if hasattr(self.meshtastic.iface, 'isConnected') and callable(self.meshtastic.iface.isConnected):
                    if self.meshtastic.iface.isConnected():
                        meshtastic_status = "✅ Connected"
            except Exception:
                meshtastic_status = "❌ Disconnected"
        
        # Get database statistics
        try:
            db_stats = self.database.get_telemetry_summary(60)
            node_count = db_stats.get('total_nodes', 0)
            active_count = db_stats.get('active_nodes', 0)
        except Exception:
            node_count = 0
            active_count = 0
        
        # Determine status color and emoji
        if meshtastic_status == "✅ Connected":
            status_color = 0x00ff00
            status_emoji = "🟢"
            status_text = "All Systems Operational"
        else:
            status_color = 0xff6b6b
            status_emoji = "🔴"
            status_text = "Service Issues Detected"
        
        embed = discord.Embed(
            title=f"{status_emoji} Bridge Status",
            description=f"**{status_text}**\n*Real-time system monitoring*",
            color=status_color,
            timestamp=get_utc_time()
        )
        embed.set_thumbnail(url="https://raw.githubusercontent.com/meshtastic/firmware/master/docs/assets/logo/meshtastic-logo.png")
        embed.set_footer(text="🌍 UTC Time | Last updated")
        
        # Service status
        embed.add_field(
            name="🖥️ **Services**",
            value=f"""Discord: ✅ Connected
Meshtastic: {meshtastic_status}
Database: ✅ Connected""",
            inline=True
        )
        
        # Network status
        embed.add_field(
            name="📡 **Network**",
            value=f"""Total Nodes: {node_count}
Active Nodes: {active_count}
Current Time: {format_utc_time()}""",
            inline=True
        )
        
        # System health
        health_score = 0
        if meshtastic_status == "✅ Connected":
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
            inline=True
        )
        
        await message.channel.send(embed=embed)
    
    async def cmd_network_topology(self, message: discord.Message):
        """Show network topology and connections with ASCII network diagram"""
        try:
            topology = self.database.get_network_topology()
            nodes = await self._get_cached_data("all_nodes", self.database.get_all_nodes)
            
            embed = discord.Embed(
                title="🌐 Network Topology",
                description="**Mesh Network Structure & Connections**\n*Real-time network visualization*",
                color=0x0099ff,
                timestamp=get_utc_time()
            )
            embed.set_thumbnail(url="https://raw.githubusercontent.com/meshtastic/firmware/master/docs/assets/logo/meshtastic-logo.png")
            embed.set_footer(text="🌍 UTC Time | Network analysis")
            
            # Create ASCII network diagram
            ascii_network = self._create_network_diagram(nodes, topology['connections'])
            
            embed.add_field(
                name="🌳 **Network Tree Diagram**",
                value=f"```\n{ascii_network}\n```",
                inline=False
            )
            
            # Network statistics
            embed.add_field(
                name="📊 **Network Stats**",
                value=f"""Total Nodes: {topology['total_nodes']}
Active Nodes: {topology['active_nodes']}
Router Nodes: {topology['router_nodes']}
Avg Hops: {topology['avg_hops']:.1f}""",
                inline=True
            )
            
            # Top connections
            if topology['connections']:
                top_connections = topology['connections'][:5]  # Top 5 connections
                connections_text = ""
                for conn in top_connections:
                    from_name = self.database.get_node_display_name(conn['from_node'])
                    to_name = self.database.get_node_display_name(conn['to_node'])
                    connections_text += f"**{from_name}** → **{to_name}**\n"
                    connections_text += f"Messages: {conn['message_count']}, Hops: {conn['avg_hops']:.1f}\n\n"
                
                embed.add_field(
                    name="🔗 **Top Connections**",
                    value=connections_text[:1024],  # Discord field limit
                    inline=True
                )
            else:
                embed.add_field(
                    name="🔗 **Connections**",
                    value="No recent connections found",
                    inline=True
                )
            
            await message.channel.send(embed=embed)
            
        except Exception as e:
            logger.error(f"Error getting network topology: {e}")
            await self._safe_send(message.channel, "❌ Error retrieving network topology.")
    
    async def cmd_topology_tree(self, message: discord.Message):
        """Show visual tree of all radio connections"""
        try:
            nodes = await self._get_cached_data("all_nodes", self.database.get_all_nodes)
            topology = self.database.get_network_topology()
            
            if not nodes:
                await self._safe_send(message.channel, "📡 **No nodes available for topology analysis**")
                return
            
            # Create readable connection tree
            connection_tree = self._create_connection_tree(nodes, topology['connections'])
            
            # Add summary info
            total_nodes = len(nodes)
            active_connections = len(topology['connections'])
            avg_hops = topology.get('avg_hops', 0)
            
            # Send the formatted tree
            await self._send_long_message(message.channel, connection_tree)
            
            # Send summary
            summary = f"\n📊 **Network Summary:** {total_nodes} radios | {active_connections} routes | {avg_hops:.1f} avg hops"
            await message.channel.send(summary)
            
        except Exception as e:
            logger.error(f"Error creating topology tree: {e}")
            await self._safe_send(message.channel, "❌ Error creating connection tree.")
    
    async def cmd_message_statistics(self, message: discord.Message):
        """Show message statistics and network activity"""
        try:
            stats = self.database.get_message_statistics(hours=24)
            
            embed = discord.Embed(
                title="📊 Message Statistics",
                description="24-hour network activity summary",
                color=0x9b59b6,
                timestamp=get_utc_time()
            )
            
            # Basic statistics
            embed.add_field(
                name="📈 **Activity**",
                value=f"""Total Messages: {stats.get('total_messages', 0)}
Unique Senders: {stats.get('unique_senders', 0)}
Unique Recipients: {stats.get('unique_recipients', 0)}""",
                inline=True
            )
            
            # Signal quality - safe formatting
            avg_hops = stats.get('avg_hops', 0) or 0
            avg_snr = stats.get('avg_snr', 0) or 0
            avg_rssi = stats.get('avg_rssi', 0) or 0
            
            embed.add_field(
                name="📶 **Signal Quality**",
                value=f"""Avg Hops: {avg_hops:.1f}
Avg SNR: {avg_snr:.1f} dB
Avg RSSI: {avg_rssi:.1f} dBm""",
                inline=True
            )
            
            # Hourly distribution
            hourly_dist = stats.get('hourly_distribution', {})
            if hourly_dist:
                # Find peak hours
                peak_hour = max(hourly_dist.items(), key=lambda x: x[1]) if hourly_dist else ("N/A", 0)
                quiet_hour = min(hourly_dist.items(), key=lambda x: x[1]) if hourly_dist else ("N/A", 0)
                
                embed.add_field(
                    name="⏰ **Activity Pattern**",
                    value=f"""Peak Hour: {peak_hour[0]}:00 ({peak_hour[1]} msgs)
Quiet Hour: {quiet_hour[0]}:00 ({quiet_hour[1]} msgs)
Active Hours: {len(hourly_dist)}""",
                    inline=True
                )
            
            await message.channel.send(embed=embed)
            
        except Exception as e:
            logger.error(f"Error getting message statistics: {e}")
            await self._safe_send(message.channel, "❌ Error retrieving message statistics.")
    
    async def cmd_trace_route(self, message: discord.Message):
        """Trace route to a specific node with visual hop-by-hop path"""
        content = message.content.strip()
        
        if not content.startswith('$trace '):
            await self._safe_send(message.channel, "❌ Use format: `$trace <node_name>`")
            return
        
        try:
            node_name = content[7:].strip()  # Remove '$trace '
            if not node_name:
                await self._safe_send(message.channel, "❌ Please specify a node name.")
                return
            
            # Find the target node
            target_node = self.database.find_node_by_name(node_name)
            if not target_node:
                await self._safe_send(message.channel, f"❌ No node found with name '{node_name}'. Try using `$nodes` to see available nodes.")
                return
            
            # Get network topology and analyze routing
            topology = self.database.get_network_topology()
            route_path = self._analyze_route_to_node(target_node['node_id'], topology)
            
            embed = discord.Embed(
                title=f"🛣️ Trace Route to {target_node['long_name']}",
                description=f"Analyzing network path to **{target_node['node_id']}**",
                color=0x00bfff,
                timestamp=get_utc_time()
            )
            
            # Target node info
            embed.add_field(
                name="🎯 **Target Node**",
                value=f"""**Name:** {target_node['long_name']}
**ID:** `{target_node['node_id']}`
**Hops Away:** {target_node.get('hops_away', 'Unknown')}
**Last Heard:** {target_node.get('last_heard', 'Unknown')}""",
                inline=True
            )
            
            # Route path visualization
            if route_path:
                route_text = self._format_route_path(route_path)
                embed.add_field(
                    name="🛤️ **Route Path**",
                    value=route_text,
                    inline=False
                )
                
                # Route statistics
                total_hops = len(route_path) - 1  # -1 because we don't count the source
                avg_snr = sum(hop.get('snr', 0) for hop in route_path[1:]) / max(1, len(route_path) - 1)
                avg_rssi = sum(hop.get('rssi', 0) for hop in route_path[1:]) / max(1, len(route_path) - 1)
                
                embed.add_field(
                    name="📊 **Route Statistics**",
                    value=f"""**Total Hops:** {total_hops}
**Avg SNR:** {avg_snr:.1f} dB
**Avg RSSI:** {avg_rssi:.1f} dBm
**Path Quality:** {self._assess_route_quality(avg_snr, total_hops)}""",
                    inline=True
                )
            else:
                embed.add_field(
                    name="🛤️ **Route Path**",
                    value="❌ **No route found** - Node may be unreachable or no recent communication data available",
                    inline=False
                )
            
            # Network overview
            embed.add_field(
                name="🌐 **Network Overview**",
                value=f"""**Active Nodes:** {topology['active_nodes']}
**Total Connections:** {len(topology['connections'])}
**Network Avg Hops:** {topology['avg_hops']:.1f}""",
                inline=True
            )
            
            # Connection quality to target
            connections_to_target = [conn for conn in topology['connections'] if conn['to_node'] == target_node['node_id']]
            if connections_to_target:
                best_connection = max(connections_to_target, key=lambda x: x['message_count'])
                from_name = self.database.get_node_display_name(best_connection['from_node'])
                
                embed.add_field(
                    name="🔗 **Best Connection**",
                    value=f"""**From:** {from_name}
**Messages:** {best_connection['message_count']}
**Avg Hops:** {best_connection['avg_hops']:.1f}
**Avg SNR:** {best_connection['avg_snr']:.1f} dB""",
                    inline=True
                )
            
            embed.set_footer(text=f"Route analysis completed at")
            await message.channel.send(embed=embed)
            
        except Exception as e:
            logger.error(f"Error tracing route: {e}")
            await self._safe_send(message.channel, "❌ Error tracing route to node.")
    
    def _analyze_route_to_node(self, target_node_id: str, topology: dict) -> list:
        """Analyze the route to a specific node based on message data"""
        try:
            # Get all messages to the target node
            with self.database._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT from_node_id, to_node_id, hops_away, snr, rssi, timestamp
                    FROM messages 
                    WHERE to_node_id = ? 
                    ORDER BY timestamp DESC 
                    LIMIT 100
                """, (target_node_id,))
                
                messages = cursor.fetchall()
            
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
                from_name = self.database.get_node_display_name(from_node_id)
                
                route_path.append({
                    'node_id': from_node_id,
                    'node_name': from_name,
                    'hops_away': hop_count,
                    'snr': snr,
                    'rssi': rssi
                })
            
            # Add target node at the end
            target_name = self.database.get_node_display_name(target_node_id)
            route_path.append({
                'node_id': target_node_id,
                'node_name': target_name,
                'hops_away': 0,
                'snr': None,
                'rssi': None
            })
            
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
            node_name = hop['node_name']
            node_id = hop['node_id']
            hops_away = hop['hops_away']
            snr = hop.get('snr')
            rssi = hop.get('rssi')
            
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
        """Show network performance leaderboards"""
        try:
            nodes = await self._get_cached_data("all_nodes", self.database.get_all_nodes)
            stats = self.database.get_message_statistics(24)
            
            if not nodes:
                await self._safe_send(message.channel, "📡 No nodes available for leaderboard.")
                return
            
            embed = discord.Embed(
                title="🏆 Network Performance Leaderboard",
                description="Top performing nodes and network statistics",
                color=0xffd700,
                timestamp=get_utc_time()
            )
            
            # Most Active Nodes (by message count)
            active_leaderboard = ""
            if stats.get('total_messages', 0) > 0:
                # This would need message count per node - simplified for now
                active_leaderboard = "📊 **Most Active Nodes**\n"
                active_leaderboard += "• Data collection in progress...\n"
                active_leaderboard += "• Check back after more activity!\n"
            else:
                active_leaderboard = "📊 **Most Active Nodes**\nNo message data available yet"
            
            embed.add_field(
                name="🏆 **Activity Leaders**",
                value=active_leaderboard,
                inline=True
            )
            
            # Best Signal Quality
            signal_leaderboard = ""
            nodes_with_signal = [n for n in nodes if n.get('snr') is not None]
            if nodes_with_signal:
                # Sort by SNR (highest first)
                sorted_nodes = sorted(nodes_with_signal, key=lambda x: x.get('snr', 0), reverse=True)
                signal_leaderboard = "📶 **Best Signal Quality**\n"
                for i, node in enumerate(sorted_nodes[:5]):
                    medal = "🥇" if i == 0 else "🥈" if i == 1 else "🥉" if i == 2 else "🏅"
                    signal_leaderboard += f"{medal} **{node['long_name']}** - {node.get('snr', 0):.1f} dB\n"
            else:
                signal_leaderboard = "📶 **Best Signal Quality**\nNo signal data available"
            
            embed.add_field(
                name="📡 **Signal Champions**",
                value=signal_leaderboard,
                inline=True
            )
            
            # Longest Uptime (simplified)
            uptime_leaderboard = "⏰ **Longest Active**\n"
            active_nodes = [n for n in nodes if n.get('last_heard')]
            if active_nodes:
                # Sort by last_heard (most recent first)
                sorted_uptime = sorted(active_nodes, key=lambda x: x.get('last_heard', ''), reverse=True)
                for i, node in enumerate(sorted_uptime[:5]):
                    medal = "🥇" if i == 0 else "🥈" if i == 1 else "🥉" if i == 2 else "🏅"
                    last_heard = node.get('last_heard', 'Unknown')
                    uptime_leaderboard += f"{medal} **{node['long_name']}** - {last_heard}\n"
            else:
                uptime_leaderboard += "No activity data available"
            
            embed.add_field(
                name="⏰ **Uptime Champions**",
                value=uptime_leaderboard,
                inline=True
            )
            
            # Network Statistics
            total_nodes = len(nodes)
            active_count = 0
            for n in nodes:
                if n.get('last_heard'):
                    try:
                        last_heard = datetime.fromisoformat(n['last_heard'].replace('Z', '+00:00'))
                        if last_heard > datetime.now() - timedelta(hours=1):
                            active_count += 1
                    except (ValueError, TypeError) as e:
                        logger.warning(f"Error parsing last_heard for node {n.get('long_name', 'Unknown')}: {e}")
                        continue
            
            embed.add_field(
                name="📊 **Network Stats**",
                value=f"""Total Nodes: {total_nodes}
Active (1h): {active_count}
Total Messages: {stats.get('total_messages', 0)}
Unique Senders: {stats.get('unique_senders', 0)}""",
                inline=False
            )
            
            await message.channel.send(embed=embed)
            
        except Exception as e:
            logger.error(f"Error creating leaderboard: {e}")
            await self._safe_send(message.channel, "❌ Error creating leaderboard.")
    
    async def cmd_network_art(self, message: discord.Message):
        """Create ASCII network art"""
        try:
            nodes = await self._get_cached_data("all_nodes", self.database.get_all_nodes)
            topology = self.database.get_network_topology()
            
            if not nodes:
                await self._safe_send(message.channel, "📡 No nodes available for network art.")
                return
            
            embed = discord.Embed(
                title="🎨 Network Art",
                description="ASCII art representation of your mesh network",
                color=0xff69b4,
                timestamp=get_utc_time()
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
                if n.get('last_heard'):
                    try:
                        last_heard = datetime.fromisoformat(n['last_heard'].replace('Z', '+00:00'))
                        if last_heard > datetime.now() - timedelta(hours=1):
                            active_nodes.append(n)
                    except (ValueError, TypeError) as e:
                        logger.warning(f"Error parsing last_heard for node {n.get('long_name', 'Unknown')}: {e}")
                        continue
            
            if active_nodes:
                art_lines.append("🟢 ACTIVE NODES:")
                for i, node in enumerate(active_nodes[:8]):  # Limit to 8 for ASCII art
                    snr = node.get('snr')
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
            if topology.get('connections'):
                art_lines.append("🔗 CONNECTIONS:")
                for i, conn in enumerate(topology['connections'][:5]):
                    from_name = self.database.get_node_display_name(conn['from_node'])[:10]
                    to_name = self.database.get_node_display_name(conn['to_node'])[:10]
                    art_lines.append(f"  {from_name} ─── {to_name}")
                
                if len(topology['connections']) > 5:
                    art_lines.append(f"  ... and {len(topology['connections']) - 5} more")
            else:
                art_lines.append("🔗 No connections detected")
            
            art_lines.append("")
            art_lines.append("=" * 40)
            art_lines.append("```")
            
            # Create the art
            art_text = "\n".join(art_lines)
            
            embed.add_field(
                name="🎨 **Network Diagram**",
                value=art_text,
                inline=False
            )
            
            # Network stats for the art
            total_nodes = len(nodes)
            active_count = len(active_nodes)
            connection_count = len(topology.get('connections', []))
            
            embed.add_field(
                name="📊 **Art Stats**",
                value=f"""Total Nodes: {total_nodes}
Active Nodes: {active_count}
Connections: {connection_count}
Art Quality: {'🎨' * min(5, total_nodes // 2)}""",
                inline=True
            )
            
            await message.channel.send(embed=embed)
            
        except Exception as e:
            logger.error(f"Error creating network art: {e}")
            await self._safe_send(message.channel, "❌ Error creating network art.")
    
    async def cmd_live_monitor(self, message: discord.Message):
        """Real-time network monitor showing live packet activity"""
        user_id = message.author.id
        
        # Check if user already has a live monitor running
        if user_id in self._live_monitors and self._live_monitors[user_id]['active']:
            # Stop the existing monitor
            try:
                logger.debug(f"Stopping live monitor for user {user_id}")
                self._live_monitors[user_id]['active'] = False
                
                if 'task' in self._live_monitors[user_id]:
                    task = self._live_monitors[user_id]['task']
                    try:
                        if not task.done():
                            task.cancel()
                            logger.debug(f"Cancelled live monitor task for user {user_id}")
                        else:
                            logger.debug(f"Live monitor task for user {user_id} was already done")
                    except Exception as task_error:
                        logger.error(f"Error cancelling task for user {user_id}: {task_error}")
                        # Continue with cleanup even if task cancellation fails
                
                await message.channel.send("🛑 **Live monitor stopped**")
                del self._live_monitors[user_id]
                logger.debug(f"Successfully stopped live monitor for user {user_id}")
                return
            except Exception as e:
                logger.error(f"Error stopping live monitor for user {user_id}: {type(e).__name__}: {str(e)}")
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
            color=0x00ff00,
            timestamp=get_utc_time()
        )
        embed.add_field(
            name="📊 **What you'll see:**",
            value="• Packet types and sources\n• Message content previews\n• Telemetry data summaries\n• Traceroute information\n• Signal quality metrics",
            inline=False
        )
        embed.set_footer(text=f"Requested by {message.author.display_name}")
        
        status_message = await message.channel.send(embed=embed)
        
        # Start the live monitoring task
        try:
            task = asyncio.create_task(self._run_live_monitor(message.channel, user_id, status_message))
            self._live_monitors[user_id] = {'active': True, 'task': task}
            logger.info(f"Started live monitor for user {message.author.display_name} ({user_id})")
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
                if user_id not in self._live_monitors or not self._live_monitors[user_id]['active']:
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
                    await self._update_live_display(channel, status_message, new_packets, time.time() - start_time)
                    last_update = time.time()
                
                # Check every 0.5 seconds
                await asyncio.sleep(0.5)
            
            # Final update
            if user_id in self._live_monitors and self._live_monitors[user_id]['active']:
                await self._finalize_live_monitor(channel, status_message, packet_count, time.time() - start_time)
                
        except asyncio.CancelledError:
            logger.info(f"Live monitor cancelled for user {user_id}")
        except Exception as e:
            logger.error(f"Error in live monitor for user {user_id}: {type(e).__name__}: {str(e)}")
            logger.error(f"Exception details: {repr(e)}")
            try:
                await channel.send(f"❌ **Live monitor error:** {str(e)}")
            except Exception as send_error:
                logger.error(f"Error sending error message: {send_error}")
        finally:
            # Clean up
            if user_id in self._live_monitors:
                del self._live_monitors[user_id]
    
    async def _update_live_display(self, channel, status_message, new_packets, elapsed_time):
        """Update the live monitor display with new packets"""
        try:
            if not new_packets:
                return
            
            # Create embed for new packets
            embed = discord.Embed(
                title="📡 Live Network Monitor",
                description=f"**Live packet monitoring** - {elapsed_time:.1f}s elapsed",
                color=0x00bfff,
                timestamp=datetime.utcnow()
            )
            
            # Add packet information
            packet_text = ""
            for packet in new_packets[-10:]:  # Show last 10 packets
                packet_type = packet.get('type', 'UNKNOWN')
                from_name = packet.get('from_name', 'Unknown')
                portnum = packet.get('portnum', 'UNKNOWN')
                hops = packet.get('hops', 0)
                snr = packet.get('snr', 'N/A')
                rssi = packet.get('rssi', 'N/A')
                
                # Format packet info
                if packet_type == 'text':
                    text_preview = packet.get('text', '')[:30]
                    if len(packet.get('text', '')) > 30:
                        text_preview += "..."
                    packet_text += f"💬 **{from_name}** ({portnum}) - `{text_preview}`\n"
                elif packet_type == 'telemetry':
                    sensor_data = packet.get('sensor_data', [])
                    sensor_summary = ", ".join(sensor_data[:3]) if sensor_data else "No data"
                    packet_text += f"📊 **{from_name}** ({portnum}) - {sensor_summary}\n"
                elif packet_type == 'traceroute':
                    to_name = packet.get('to_name', 'Unknown')
                    hops_count = packet.get('hops_count', 0)
                    packet_text += f"🛣️ **{from_name}** → **{to_name}** ({hops_count} hops)\n"
                elif packet_type == 'movement':
                    distance_moved = packet.get('distance_moved', 0)
                    packet_text += f"🚶 **{from_name}** moved {distance_moved:.1f}m\n"
                else:
                    packet_text += f"📦 **{from_name}** ({portnum}) - {packet_type}\n"
                
                # Add signal info
                packet_text += f"   └─ Hops: {hops} | SNR: {snr} | RSSI: {rssi}\n\n"
            
            if packet_text:
                embed.add_field(
                    name="📦 **Recent Packets:**",
                    value=packet_text[:1024],  # Discord field limit
                    inline=False
                )
            
            embed.set_footer(text="Type `$live` again to stop monitoring")
            
            await status_message.edit(embed=embed)
            
        except Exception as e:
            logger.error(f"Error updating live display: {e}")
    
    async def _finalize_live_monitor(self, channel, status_message, total_packets, elapsed_time):
        """Finalize the live monitor with summary"""
        try:
            embed = discord.Embed(
                title="📡 Live Network Monitor - Complete",
                description=f"**Monitoring completed** - {elapsed_time:.1f}s total",
                color=0x00ff00,
                timestamp=datetime.utcnow()
            )
            
            embed.add_field(
                name="📊 **Summary:**",
                value=f"Total Packets: {total_packets}\nDuration: {elapsed_time:.1f}s\nAverage Rate: {total_packets/elapsed_time:.1f} packets/sec",
                inline=False
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
                cursor.execute("DELETE FROM sqlite_sequence WHERE name IN ('telemetry', 'positions', 'messages')")
                
                conn.commit()
                
                logger.info("Database cleared by user command")
            
            # Clear command handler cache
            await self.clear_cache()
            
            embed = discord.Embed(
                title="🗑️ Database Cleared",
                description="All data has been cleared from the database",
                color=0xff6b6b,
                timestamp=get_utc_time()
            )
            
            embed.add_field(
                name="✅ **Cleared Tables**",
                value="• Nodes\n• Telemetry\n• Positions\n• Messages\n• Cache",
                inline=True
            )
            
            embed.add_field(
                name="🔄 **Next Steps**",
                value="The bot will now collect fresh data from the mesh network. Use `$nodes` to see new data as it's collected.",
                inline=True
            )
            
            await message.channel.send(embed=embed)
            
        except Exception as e:
            logger.error(f"Error clearing database: {e}")
            await self._safe_send(message.channel, f"❌ Error clearing database: {e}")
    
    async def cmd_debug_info(self, message: discord.Message):
        """Show debug information about database and data storage"""
        try:
            # Get database counts
            with self.database._get_connection() as conn:
                cursor = conn.cursor()
                
                # Count records in each table
                cursor.execute("SELECT COUNT(*) FROM nodes")
                node_count = cursor.fetchone()[0]
                
                cursor.execute("SELECT COUNT(*) FROM telemetry")
                telemetry_count = cursor.fetchone()[0]
                
                cursor.execute("SELECT COUNT(*) FROM positions")
                position_count = cursor.fetchone()[0]
                
                cursor.execute("SELECT COUNT(*) FROM messages")
                message_count = cursor.fetchone()[0]
            
            # Get some sample data
            nodes = self.database.get_all_nodes()
            recent_telemetry = []
            if nodes:
                # Get recent telemetry for first node
                recent_telemetry = self.database.get_telemetry_history(nodes[0]['node_id'], hours=1, limit=5)
            
            embed = discord.Embed(
                title="🔍 Debug Information",
                description="Database and data storage status",
                color=0x00bfff,
                timestamp=get_utc_time()
            )
            
            embed.add_field(
                name="📊 **Database Counts**",
                value=f"""Nodes: {node_count}
Telemetry: {telemetry_count}
Positions: {position_count}
Messages: {message_count}""",
                inline=True
            )
            
            embed.add_field(
                name="🔄 **Cache Status**",
                value=f"""Cache Entries: {len(self._node_cache)}
Cache TTL: {self._cache_ttl}s
Last Refresh: {time.time() - self.meshtastic.last_node_refresh:.1f}s ago""",
                inline=True
            )
            
            if nodes:
                embed.add_field(
                    name="📡 **Sample Node Data**",
                    value=f"""Total Nodes: {len(nodes)}
First Node: {nodes[0]['long_name']}
Has SNR: {nodes[0].get('snr') is not None}
Has Battery: {nodes[0].get('battery_level') is not None}
Last Heard: {nodes[0].get('last_heard', 'Unknown')}""",
                    inline=False
                )
            
            if recent_telemetry:
                embed.add_field(
                    name="📈 **Recent Telemetry**",
                    value=f"Found {len(recent_telemetry)} recent telemetry records for {nodes[0]['long_name']}",
                    inline=False
                )
            else:
                embed.add_field(
                    name="📈 **Recent Telemetry**",
                    value="No recent telemetry data found",
                    inline=False
                )
            
            await message.channel.send(embed=embed)
            
        except Exception as e:
            logger.error(f"Error getting debug info: {e}")
            await self._safe_send(message.channel, f"❌ Error getting debug info: {e}")
    
    def _create_signal_tree(self, excellent_nodes, good_nodes, poor_nodes, unknown_nodes):
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
                    tree_lines.append(f"│  └─ {node['long_name'][:20]} ({node.get('snr', 0):.1f}dB) +{len(excellent_nodes)-8} more")
                else:
                    tree_lines.append(f"│  ├─ {node['long_name'][:20]} ({node.get('snr', 0):.1f}dB)")
        else:
            tree_lines.append("├─ 🟢 Excellent Signal (None)")
        
        # Good signal branch
        if good_nodes:
            tree_lines.append("├─ 🟡 Good Signal")
            for i, node in enumerate(good_nodes[:6]):
                if i == len(good_nodes[:6]) - 1 and len(good_nodes) > 6:
                    tree_lines.append(f"│  └─ {node['long_name'][:20]} ({node.get('snr', 0):.1f}dB) +{len(good_nodes)-6} more")
                else:
                    tree_lines.append(f"│  ├─ {node['long_name'][:20]} ({node.get('snr', 0):.1f}dB)")
        else:
            tree_lines.append("├─ 🟡 Good Signal (None)")
        
        # Poor signal branch
        if poor_nodes:
            tree_lines.append("├─ 🔴 Poor Signal")
            for i, node in enumerate(poor_nodes[:4]):
                if i == len(poor_nodes[:4]) - 1 and len(poor_nodes) > 4:
                    tree_lines.append(f"│  └─ {node['long_name'][:20]} ({node.get('snr', 0):.1f}dB) +{len(poor_nodes)-4} more")
                else:
                    tree_lines.append(f"│  ├─ {node['long_name'][:20]} ({node.get('snr', 0):.1f}dB)")
        else:
            tree_lines.append("├─ 🔴 Poor Signal (None)")
        
        # Unknown signal branch
        if unknown_nodes:
            tree_lines.append("└─ ⚪ Unknown Signal")
            for i, node in enumerate(unknown_nodes[:4]):
                if i == len(unknown_nodes[:4]) - 1 and len(unknown_nodes) > 4:
                    tree_lines.append(f"   └─ {node['long_name'][:20]} +{len(unknown_nodes)-4} more")
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
            if n.get('last_heard'):
                try:
                    last_heard = datetime.fromisoformat(n['last_heard'].replace('Z', '+00:00'))
                    if last_heard > datetime.now() - timedelta(hours=1):
                        active_nodes.append(n)
                except (ValueError, TypeError) as e:
                    logger.warning(f"Error parsing last_heard for node {n.get('long_name', 'Unknown')}: {e}")
                    continue
        
        # Sort by hops away
        active_nodes.sort(key=lambda x: x.get('hops_away', 0))
        
        if not active_nodes:
            diagram_lines.append("⚪ No active nodes detected")
            return "\n".join(diagram_lines)
        
        # Create hierarchical diagram
        diagram_lines.append("📡 Active Network Nodes:")
        diagram_lines.append("")
        
        # Group by hops
        hop_groups = {}
        for node in active_nodes:
            hops = node.get('hops_away', 0)
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
                snr = node.get('snr')
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
                battery = node.get('battery_level')
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
                node_name = node['long_name'][:15]
                if i == len(nodes_at_hop) - 1 and len(nodes_at_hop) > 6:
                    diagram_lines.append(f"   └─ {signal_icon}{battery_icon} {node_name} +{len(nodes_at_hop)-6} more")
                else:
                    diagram_lines.append(f"   ├─ {signal_icon}{battery_icon} {node_name}")
            
            diagram_lines.append("")
        
        # Show connections if available
        if connections:
            diagram_lines.append("🔗 TOP CONNECTIONS:")
            for i, conn in enumerate(connections[:5]):
                from_name = self.database.get_node_display_name(conn['from_node'])[:12]
                to_name = self.database.get_node_display_name(conn['to_node'])[:12]
                msg_count = conn['message_count']
                avg_hops = conn['avg_hops']
                
                if i == len(connections[:5]) - 1 and len(connections) > 5:
                    diagram_lines.append(f"   {from_name} ──→ {to_name} ({msg_count}msgs) +{len(connections)-5} more")
                else:
                    diagram_lines.append(f"   {from_name} ──→ {to_name} ({msg_count}msgs)")
        
        diagram_lines.append("")
        diagram_lines.append("Legend: 🟢🟡🔴 Signal Quality | 🔋🪫🔴 Battery | ⚪❓ Unknown")
        
        return "\n".join(diagram_lines)
    
    def _format_node_info(self, node: Dict[str, Any]) -> str:
        """Format node information for display"""
        try:
            long_name = str(node.get('long_name', 'Unknown'))
            node_id = str(node.get('node_id', 'Unknown'))
            node_num = str(node.get('node_num', 'Unknown'))
            hops_away = str(node.get('hops_away', '0'))
            snr = str(node.get('snr', '?'))
            battery = f"{node.get('battery_level', 'N/A')}%" if node.get('battery_level') is not None else "N/A"
            temperature = f"{node.get('temperature', 'N/A'):.1f}°C" if node.get('temperature') is not None else "N/A"
            
            if node.get('last_heard'):
                try:
                    last_heard = datetime.fromisoformat(node['last_heard'])
                    time_str = last_heard.strftime('%H:%M:%S')
                except (ValueError, TypeError, KeyError) as e:
                    logger.debug(f"Error parsing last_heard timestamp: {e}")
                    time_str = "Unknown"
            else:
                time_str = "Unknown"
            
            return f"**{long_name}** (ID: {node_id}, Num: {node_num}) - Hops: {hops_away}, SNR: {snr}, Battery: {battery}, Temp: {temperature}, Last: {time_str}"
            
        except Exception as e:
            logger.error(f"Error formatting node info: {e}")
            return f"**Node {node.get('node_id', 'Unknown')}** - Error formatting data"
    
    def _create_connection_tree(self, nodes, connections):
        """Create readable ASCII tree for Discord showing network topology"""
        tree_lines = []
        tree_lines.append("🌐 MESH NETWORK TOPOLOGY")
        tree_lines.append("=" * 50)
        
        # Get active nodes (last 2 hours)
        active_nodes = []
        for n in nodes:
            if n.get('last_heard'):
                try:
                    last_heard = datetime.fromisoformat(n['last_heard'].replace('Z', '+00:00'))
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
            from_node = conn['from_node']
            to_node = conn['to_node']
            if from_node not in routing_map:
                routing_map[from_node] = []
            if to_node not in routing_map:
                routing_map[to_node] = []
            routing_map[from_node].append({'node': to_node, 'msgs': conn['message_count']})
            routing_map[to_node].append({'node': from_node, 'msgs': conn['message_count']})
        
        # Sort and group by hops
        active_nodes.sort(key=lambda x: x.get('hops_away') or 0)
        hop_groups = {}
        for node in active_nodes:
            hops = node.get('hops_away') or 0
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
                snr = node.get('snr')
                battery = node.get('battery_level')
                node_id = node.get('node_id')
                long_name = node.get('long_name', 'Unknown')
                
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
                node_type = "Router" if node.get('is_router') else "Client"
                type_icon = "📡" if node.get('is_router') else "📱"
                
                # Find routing parent
                via_text = ""
                if hops > 0 and node_id in routing_map:
                    for parent_hops in range(hops):
                        parent_nodes = hop_groups.get(parent_hops, [])
                        for parent in parent_nodes:
                            parent_id = parent.get('node_id')
                            if parent_id and parent_id in routing_map:
                                for route in routing_map[parent_id]:
                                    if route['node'] == node_id:
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
            sorted_conns = sorted(connections, key=lambda x: x['message_count'], reverse=True)
            for i, conn in enumerate(sorted_conns[:5]):  # Top 5 connections
                from_name = self.database.get_node_display_name(conn['from_node'])[:15]
                to_name = self.database.get_node_display_name(conn['to_node'])[:15]
                msgs = conn['message_count']
                avg_hops = conn.get('avg_hops', 0)
                
                tree_lines.append(f"  {from_name} ↔ {to_name} ({msgs} msgs, {avg_hops:.1f} avg hops)")
        
        return "\n".join(tree_lines)
    
    def _calculate_tree_depth(self, connections):
        """Calculate the maximum depth of the connection tree"""
        if not connections:
            return 0
        
        # Simple heuristic: max hops in connections + 1
        max_hops = 0
        for conn in connections:
            hops = conn.get('avg_hops', 0)
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
            snr = node.get('snr')
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
        high_battery = sum(1 for n in nodes if n.get('battery_level') is not None and n.get('battery_level') > 80)
        low_battery = sum(1 for n in nodes if n.get('battery_level') is not None and n.get('battery_level') < 40)
        
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
                chunks = [message[i:i+1900] for i in range(0, len(message), 1900)]
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
    
    async def _safe_send(self, channel, message: str):
        """Safely send a message to a channel with error handling"""
        try:
            await channel.send(message)
        except Exception as e:
            logger.error(f"Error sending message to channel: {e}")

class DiscordBot(discord.Client):
    """Enhanced Discord bot with Meshtastic integration and database"""
    
    def __init__(self, config: Config, meshtastic: MeshtasticInterface, database: MeshtasticDatabase):
        intents = discord.Intents.default()
        intents.message_content = True
        
        super().__init__(intents=intents)
        self.config = config
        self.meshtastic = meshtastic
        self.database = database
        
        # Queues for communication with size limits to prevent memory exhaustion
        self.mesh_to_discord = queue.Queue(maxsize=1000)  # Limit to 1000 messages
        self.discord_to_mesh = queue.Queue(maxsize=100)   # Limit to 100 outgoing messages
        
        # Initialize command handler after queues are created (event loop will be set later)
        self.command_handler = CommandHandler(meshtastic, self.discord_to_mesh, database)
        
        # Background task
        self.bg_task = None
        self.telemetry_task = None
        
        # Track last telemetry update hour
        self.last_telemetry_hour = datetime.now().hour
    
    async def setup_hook(self) -> None:
        """Setup bot when starting"""
        self.bg_task = self.loop.create_task(self.background_task())
        self.telemetry_task = self.loop.create_task(self.telemetry_update_task())
    
    async def on_ready(self):
        """Called when bot is ready"""
        logger.info(f'Logged in as {self.user} (ID: {self.user.id})')
        logger.info('------')
        
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
        
        # Handle ping/pong functionality
        if message.content.strip().lower() == "ping":
            await self._handle_ping(message)
            return
        
        # Handle commands
        if message.content.startswith('$'):
            await self.command_handler.handle_command(message)
    
    async def _handle_ping(self, message):
        """Handle ping command - send pong to mesh and announce to Discord"""
        try:
            # Create a nice embed for the ping response
            embed = discord.Embed(
                title="🏓 Ping Test",
                description="Testing mesh network connectivity...",
                color=0x00ff00,
                timestamp=get_utc_time()
            )
            embed.add_field(
                name="📡 **Action**",
                value="Sending Pong! to mesh network",
                inline=False
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
                    color=0x00ff00,
                    timestamp=get_utc_time()
                )
                success_embed.add_field(
                    name="📡 **Status**",
                    value="✅ Message sent to Longfast Channel",
                    inline=False
                )
                success_embed.set_footer(text=f"Completed for {message.author.display_name}")
                await message.channel.send(embed=success_embed)
                logger.info(f"Ping/pong handled from {message.author.name}")
            else:
                # Update with failure
                fail_embed = discord.Embed(
                    title="❌ Ping Failed",
                    description="Failed to send pong to mesh network",
                    color=0xff0000,
                    timestamp=get_utc_time()
                )
                fail_embed.add_field(
                    name="📡 **Status**",
                    value="❌ Unable to send to Longfast Channel",
                    inline=False
                )
                fail_embed.set_footer(text=f"Failed for {message.author.display_name}")
                await message.channel.send(embed=fail_embed)
        except Exception as e:
            logger.error(f"Error handling ping: {e}")
            error_embed = discord.Embed(
                title="❌ Ping Error",
                description="An error occurred while testing connectivity",
                color=0xff0000,
                timestamp=get_utc_time()
            )
            error_embed.add_field(
                name="📡 **Error**",
                value=f"```{str(e)[:500]}```",
                inline=False
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
        
        while not self.is_closed():
            try:
                # Process mesh to Discord messages
                await self._process_mesh_to_discord(channel)
                
                # Process Discord to mesh messages
                await self._process_discord_to_mesh()
                
                # Process nodes periodically
                if time.time() - self.meshtastic.last_node_refresh >= self.config.node_refresh_interval:
                    await self._process_nodes(channel)
                
                # Periodic cleanup
                now = time.time()
                if now - last_cleanup >= cleanup_interval:
                    await self._periodic_cleanup()
                    last_cleanup = now
                
                await asyncio.sleep(1)  # Check every second
                
            except Exception as e:
                logger.error(f"Error in background task: {e}")
                await asyncio.sleep(5)
    
    async def _periodic_cleanup(self):
        """Perform periodic cleanup tasks"""
        try:
            # Clear command handler cache
            if hasattr(self.command_handler, 'clear_cache'):
                await self.command_handler.clear_cache()
            
            # Clean up old database data
            if hasattr(self.database, 'cleanup_old_data'):
                self.database.cleanup_old_data(30)  # Keep 30 days
            
            logger.debug("Periodic cleanup completed")
            
        except Exception as e:
            logger.error(f"Error during periodic cleanup: {e}")
    
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
    
    async def _process_nodes(self, channel):
        """Process and store nodes, announce new ones"""
        try:
            result = self.meshtastic.process_nodes()
            if result and len(result) == 2:
                processed_nodes, new_nodes = result
                
                logger.info(f"Node processing result: {len(processed_nodes)} processed, {len(new_nodes)} new")
                
                # Announce new nodes
                for node in new_nodes:
                    await self._announce_new_node(channel, node)
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
                color=0x00ff00,
                timestamp=get_utc_time()
            )
            
            embed.add_field(name="Node ID", value=node['node_id'], inline=True)
            embed.add_field(name="Node Number", value=node.get('node_num', 'N/A'), inline=True)
            embed.add_field(name="Hardware", value=node.get('hw_model', 'Unknown'), inline=True)
            embed.add_field(name="Firmware", value=node.get('firmware_version', 'Unknown'), inline=True)
            embed.add_field(name="Hops Away", value=node.get('hops_away', 0), inline=True)
            
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
                logger.error(f"Database error getting telemetry summary for update: {db_error}")
                return
            
            embed = discord.Embed(
                title="📊 Hourly Telemetry Update",
                description="Latest telemetry data from active nodes",
                color=0x0099ff,
                timestamp=get_utc_time()
            )
            
            embed.add_field(name="Active Nodes", value=summary.get('active_nodes', 0), inline=True)
            embed.add_field(name="Total Nodes", value=summary.get('total_nodes', 0), inline=True)
            
            if summary.get('avg_battery') is not None:
                embed.add_field(name="Avg Battery", value=f"{summary['avg_battery']:.1f}%", inline=True)
            if summary.get('avg_temperature') is not None:
                embed.add_field(name="Avg Temperature", value=f"{summary['avg_temperature']:.1f}°C", inline=True)
            if summary.get('avg_humidity') is not None:
                embed.add_field(name="Avg Humidity", value=f"{summary['avg_humidity']:.1f}%", inline=True)
            if summary.get('avg_snr') is not None:
                embed.add_field(name="Avg SNR", value=f"{summary['avg_snr']:.1f} dB", inline=True)
            
            await channel.send(embed=embed)
            logger.info("Sent hourly telemetry update")
            
        except Exception as e:
            logger.error(f"Error sending telemetry update: {e}")
    
    
    async def _process_mesh_to_discord(self, channel):
        """Process messages from mesh to Discord with improved error handling"""
        try:
            processed_count = 0
            max_batch_size = 10  # Process max 10 messages at once
            
            while not self.mesh_to_discord.empty() and processed_count < max_batch_size:
                item = self.mesh_to_discord.get_nowait()
                try:
                    if isinstance(item, dict):
                        if item.get('type') == 'text':
                            # Format as single line message
                            from_name = item.get('from_name', item.get('from_id', 'Unknown'))
                            to_name = item.get('to_name', item.get('to_id', 'Unknown'))
                            text = str(item.get('text', ''))
                            hops = item.get('hops_away', 0)
                            
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
                            hops_text = f"🐰{hops} hops" if hops is not None else "🐰0 hops"
                            
                            # Create single line message with length limit
                            message_text = f"📨 **{from_name}** → **{destination}** {hops_text}: {text}"
                            if len(message_text) > 2000:
                                message_text = message_text[:1997] + "..."
                            
                            await channel.send(message_text)
                            logger.info(f"📤 DISCORD: Sent message to Discord - '{text[:30]}{'...' if len(text) > 30 else ''}' from {from_name}")
                            processed_count += 1
                            
                        elif item.get('type') == 'traceroute':
                            # Format traceroute information
                            from_name = item.get('from_name', item.get('from_id', 'Unknown'))
                            to_name = item.get('to_name', item.get('to_id', 'Unknown'))
                            route_text = item.get('route_text', '')
                            hops_count = item.get('hops_count', 0)
                            
                            # Create traceroute embed
                            embed = discord.Embed(
                                title="🛣️ Traceroute Result",
                                description=f"**{from_name}** traced route to **{to_name}**",
                                color=0x00bfff,
                                timestamp=datetime.utcnow()
                            )
                            
                            embed.add_field(
                                name="📍 Route Path",
                                value=route_text,
                                inline=False
                            )
                            
                            embed.add_field(
                                name="📊 Statistics",
                                value=f"Total Hops: {hops_count}",
                                inline=True
                            )
                            
                            embed.set_footer(text=f"Traceroute completed at")
                            
                            await channel.send(embed=embed)
                            logger.info(f"🛣️ DISCORD: Sent traceroute info - {from_name} → {to_name} ({hops_count} hops)")
                            processed_count += 1
                        
                        elif item.get('type') == 'movement':
                            # Format movement notification
                            from_name = item.get('from_name', item.get('from_id', 'Unknown'))
                            distance_moved = item.get('distance_moved', 0)
                            old_lat = item.get('old_lat', 0)
                            old_lon = item.get('old_lon', 0)
                            new_lat = item.get('new_lat', 0)
                            new_lon = item.get('new_lon', 0)
                            new_alt = item.get('new_alt', 0)
                            
                            # Format coordinates for display
                            old_coords = f"{old_lat:.6f}, {old_lon:.6f}"
                            new_coords = f"{new_lat:.6f}, {new_lon:.6f}"
                            
                            # Create movement embed
                            embed = discord.Embed(
                                title="🚶 Node is on the move!",
                                description=f"**{from_name}** has moved a significant distance",
                                color=0xff6b35,
                                timestamp=datetime.utcnow()
                            )
                            
                            # Add movement details
                            movement_text = f"**Distance:** {distance_moved:.1f} meters\n"
                            movement_text += f"**From:** `{old_coords}`\n"
                            movement_text += f"**To:** `{new_coords}`"
                            
                            if new_alt != 0:
                                movement_text += f"\n**Altitude:** {new_alt}m"
                            
                            embed.add_field(name="📍 Movement Details", value=movement_text, inline=False)
                            
                            # Add a fun movement indicator
                            if distance_moved > 1000:
                                embed.add_field(name="🏃 Speed", value="Moving fast!", inline=True)
                            elif distance_moved > 500:
                                embed.add_field(name="🚶 Speed", value="Walking pace", inline=True)
                            else:
                                embed.add_field(name="🐌 Speed", value="Slow movement", inline=True)
                            
                            embed.set_footer(text=f"Movement detected at")
                            await channel.send(embed=embed)
                            logger.info(f"🚶 DISCORD: Sent movement notification - {from_name} moved {distance_moved:.1f}m")
                        processed_count += 1
                        
                        # Special handling for ping messages - show pong response after a delay
                        if item.get('type') == 'text' and item.get('text', '').strip().lower() == "ping":
                            # Wait a moment for the ping message to be displayed first
                            await asyncio.sleep(1.0)
                            
                            # Then show the pong response
                            pong_embed = discord.Embed(
                                title="🏓 Pong Response",
                                description=f"Pong! sent to mesh network in response to **{from_name}**",
                                color=0x00ff00,
                                timestamp=get_utc_time()
                            )
                            pong_embed.set_footer(text="🌍 UTC Time | Mesh network response")
                            await channel.send(embed=pong_embed)
                            logger.info(f"Pong response announced for ping from {from_name}")
                        
                        
                    else:
                        # Handle other message types
                        message_text = f"📡 **Mesh Message:** {str(item)[:1900]}"
                        await channel.send(message_text)
                        processed_count += 1
                        
                except discord.HTTPException as e:
                    logger.error(f"Discord API error sending message: {e}")
                    # Don't re-queue the message, just log and continue
                except Exception as e:
                    logger.error(f"Error processing individual mesh message: {e}")
                finally:
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
                
                if message.startswith('nodenum='):
                    # Extract node ID and message
                    parts = message.split(' ', 1)
                    if len(parts) == 2:
                        node_id = parts[0][8:]  # Remove 'nodenum='
                        message_text = parts[1]
                        logger.info(f"📤 MESH: Sending message to node {node_id} - '{message_text[:50]}{'...' if len(message_text) > 50 else ''}'")
                        try:
                            self.meshtastic.send_text(message_text, destination_id=node_id)
                            logger.info(f"✅ MESH: Message sent successfully to node {node_id}")
                        except Exception as send_error:
                            logger.error(f"❌ MESH: Error sending message to node {node_id}: {send_error}")
                else:
                    # Send to primary channel
                    logger.info(f"📤 MESH: Sending message to primary channel - '{message[:50]}{'...' if len(message) > 50 else ''}'")
                    try:
                        self.meshtastic.send_text(message)
                        logger.info(f"✅ MESH: Message sent successfully to primary channel")
                    except Exception as send_error:
                        logger.error(f"❌ MESH: Error sending message to primary channel: {send_error}")
                
                self.discord_to_mesh.task_done()
                
        except queue.Empty:
            pass
        except Exception as e:
            logger.error(f"Error processing Discord to mesh: {e}")
    
    def on_mesh_receive(self, packet, interface):
        """Handle incoming mesh packets"""
        try:
            if 'decoded' not in packet:
                logger.debug("Received packet without decoded data")
                return
                
            portnum = packet['decoded']['portnum']
            from_id = packet.get('fromId', 'Unknown')
            to_id = packet.get('toId', 'Primary')
            hops_away = packet.get('hopsAway', 0)
            snr = packet.get('snr', 'N/A')
            rssi = packet.get('rssi', 'N/A')
            
            # Get node display name for logging
            from_name = self.database.get_node_display_name(from_id) if self.database else from_id
            
            # Log packet reception
            logger.info(f"📦 PACKET RECEIVED: {portnum} from {from_name} ({from_id}) -> {to_id} | Hops: {hops_away} | SNR: {snr} | RSSI: {rssi}")
            
            # Add to live monitor buffer
            packet_info = {
                'type': 'packet',
                'portnum': portnum,
                'from_name': from_name,
                'from_id': from_id,
                'to_id': to_id,
                'hops': hops_away,
                'snr': snr,
                'rssi': rssi
            }
            if hasattr(self, 'command_handler'):
                self.command_handler.add_packet_to_buffer(packet_info)
            
            # Handle text messages
            if portnum == 'TEXT_MESSAGE_APP':
                to_id = packet.get('toId', 'Primary')
                text = packet['decoded']['text']
                
                from_name = self.database.get_node_display_name(from_id) if self.database else from_id
                to_name = self.database.get_node_display_name(to_id) if self.database else to_id
                
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
                    'type': 'text',
                    'from_id': from_id,
                    'from_name': from_name,
                    'to_id': to_id,
                    'to_name': to_name,
                    'text': text,
                    'hops_away': packet.get('hopsAway', 0),
                    'snr': packet.get('snr'),
                    'rssi': packet.get('rssi'),
                    'timestamp': datetime.utcnow().isoformat() + 'Z'
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
                logger.info(f"💬 MESSAGE: Queued for Discord - '{text[:50]}{'...' if len(text) > 50 else ''}' from {from_name}")
                
                # Add text message to live monitor buffer
                text_packet_info = {
                    'type': 'text',
                    'portnum': portnum,
                    'from_name': from_name,
                    'from_id': from_id,
                    'to_id': to_id,
                    'text': text,
                    'hops': hops_away,
                    'snr': snr,
                    'rssi': rssi
                }
                if hasattr(self, 'command_handler'):
                    self.command_handler.add_packet_to_buffer(text_packet_info)
                
                # Store message in database
                try:
                    message_data = {
                        'from_node_id': from_id,
                        'to_node_id': to_id,
                        'message_text': text,
                        'port_num': packet['decoded']['portnum'],
                        'payload': str(packet.get('payload', '')),
                        'hops_away': packet.get('hopsAway', 0),
                        'snr': packet.get('snr'),
                        'rssi': packet.get('rssi')
                    }
                    self.database.add_message(message_data)
                except Exception as msg_error:
                    logger.error(f"Error storing message in database: {msg_error}")
            
            # Handle telemetry packets
            elif portnum == 'TELEMETRY_APP':
                if from_id and from_id != 'Unknown' and from_id is not None:
                    logger.info(f"📊 TELEMETRY: Processing sensor data from {from_name}")
                    self._process_telemetry_packet(packet)
                else:
                    logger.warning(f"📊 TELEMETRY: Skipping packet with invalid fromId: {from_id}")
            
            # Handle position packets
            elif portnum == 'POSITION_APP':
                logger.info(f"📍 POSITION: Location update from {from_name}")
                self._process_position_packet(packet)
                
            # Handle node info packets
            elif portnum == 'NODEINFO_APP':
                logger.info(f"👤 NODE INFO: Node information from {from_name}")
                # Node info packets are handled by Meshtastic library automatically
                
            # Handle routing packets (traceroute)
            elif portnum == 'ROUTING_APP':
                logger.info(f"🛣️ ROUTING: Processing traceroute from {from_name}")
                self._process_routing_packet(packet)
                
            # Handle admin packets
            elif portnum == 'ADMIN_APP':
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
            from_id = packet.get('fromId', 'Unknown')
            
            # Skip if we don't have a valid node ID
            if not from_id or from_id == 'Unknown' or from_id is None:
                logger.warning(f"Skipping telemetry packet with invalid fromId: {from_id}")
                return
                
            decoded = packet.get('decoded', {})
            telemetry_data = decoded.get('telemetry', {})
            
            if not telemetry_data:
                logger.debug(f"No telemetry data in packet from {from_id}")
                return
            
            # Extract different types of telemetry data
            extracted_data = {}
            
            # Device metrics (battery, voltage, uptime, etc.)
            if 'deviceMetrics' in telemetry_data:
                device_metrics = telemetry_data['deviceMetrics']
                if device_metrics.get('batteryLevel') is not None:
                    extracted_data['battery_level'] = device_metrics['batteryLevel']
                if device_metrics.get('voltage') is not None:
                    extracted_data['voltage'] = device_metrics['voltage']
                if device_metrics.get('channelUtilization') is not None:
                    extracted_data['channel_utilization'] = device_metrics['channelUtilization']
                if device_metrics.get('airUtilTx') is not None:
                    extracted_data['air_util_tx'] = device_metrics['airUtilTx']
                if device_metrics.get('uptimeSeconds') is not None:
                    extracted_data['uptime_seconds'] = device_metrics['uptimeSeconds']
            
            # Environment metrics (temperature, humidity, pressure, etc.)
            if 'environmentMetrics' in telemetry_data:
                env_metrics = telemetry_data['environmentMetrics']
                if env_metrics.get('temperature') is not None:
                    extracted_data['temperature'] = env_metrics['temperature']
                if env_metrics.get('relativeHumidity') is not None:
                    extracted_data['humidity'] = env_metrics['relativeHumidity']
                if env_metrics.get('barometricPressure') is not None:
                    extracted_data['pressure'] = env_metrics['barometricPressure']
                if env_metrics.get('gasResistance') is not None:
                    extracted_data['gas_resistance'] = env_metrics['gasResistance']
            
            # Air quality metrics
            if 'airQualityMetrics' in telemetry_data:
                air_metrics = telemetry_data['airQualityMetrics']
                if air_metrics.get('pm10Environmental') is not None:
                    extracted_data['pm10'] = air_metrics['pm10Environmental']
                if air_metrics.get('pm25Environmental') is not None:
                    extracted_data['pm25'] = air_metrics['pm25Environmental']
                if air_metrics.get('pm100Environmental') is not None:
                    extracted_data['pm100'] = air_metrics['pm100Environmental']
                if air_metrics.get('aqi') is not None:
                    extracted_data['iaq'] = air_metrics['aqi']
            
            # Power metrics
            if 'powerMetrics' in telemetry_data:
                power_metrics = telemetry_data['powerMetrics']
                if power_metrics.get('ch1Voltage') is not None:
                    extracted_data['ch1_voltage'] = power_metrics['ch1Voltage']
                if power_metrics.get('ch2Voltage') is not None:
                    extracted_data['ch2_voltage'] = power_metrics['ch2Voltage']
                if power_metrics.get('ch3Voltage') is not None:
                    extracted_data['ch3_voltage'] = power_metrics['ch3Voltage']
            
            # Add radio metrics from packet
            if packet.get('snr') is not None:
                extracted_data['snr'] = packet['snr']
            if packet.get('rssi') is not None:
                extracted_data['rssi'] = packet['rssi']
            if packet.get('frequency') is not None:
                extracted_data['frequency'] = packet['frequency']
            
            # Store telemetry data if we have any
            if extracted_data:
                try:
                    success = self.database.add_telemetry(from_id, extracted_data)
                    if success:
                        logger.info(f"Stored telemetry data for {from_id}: {list(extracted_data.keys())}")
                        
                        # Add telemetry to live monitor buffer
                        if from_id and from_id != 'Unknown' and from_id is not None:
                            telemetry_packet_info = {
                                'type': 'telemetry',
                                'portnum': 'TELEMETRY_APP',
                                'from_name': self.database.get_node_display_name(from_id) if self.database else from_id,
                                'from_id': from_id,
                                'sensor_data': list(extracted_data.keys()),
                                'hops': 0,  # Telemetry doesn't have hops info
                                'snr': 'N/A',
                                'rssi': 'N/A'
                            }
                            if hasattr(self, 'command_handler'):
                                self.command_handler.add_packet_to_buffer(telemetry_packet_info)
                    else:
                        logger.warning(f"Failed to store telemetry data for {from_id}")
                except Exception as telemetry_error:
                    logger.error(f"Error storing telemetry data for {from_id}: {telemetry_error}")
            else:
                logger.debug(f"No extractable telemetry data from {from_id}")
                
        except Exception as e:
            logger.error(f"Error processing telemetry packet: {e}")
    
    def _process_routing_packet(self, packet):
        """Process routing packet and display traceroute information in Discord"""
        try:
            from_id = packet.get('fromId', 'Unknown')
            to_id = packet.get('toId', 'Primary')
            decoded = packet.get('decoded', {})
            
            # Check if this is a RouteDiscovery packet
            if 'routing' in decoded and 'routeDiscovery' in decoded['routing']:
                route_data = decoded['routing']['routeDiscovery']
                
                # Get node display names
                from_name = self.database.get_node_display_name(from_id) if self.database else from_id
                to_name = self.database.get_node_display_name(to_id) if self.database else to_id
                
                # Extract route information
                route = route_data.get('route', [])
                route_back = route_data.get('routeBack', [])
                snr_towards = route_data.get('snrTowards', [])
                snr_back = route_data.get('snrBack', [])
                
                # Build route string
                route_parts = []
                
                # Route towards destination
                if route:
                    route_parts.append(f"**Towards {to_name}:**")
                    current_route = f"{from_name}"
                    
                    for i, node_num in enumerate(route):
                        node_name = self.database.get_node_display_name(f"!{node_num:08x}") if self.database else f"!{node_num:08x}"
                        snr = ""
                        if i < len(snr_towards) and snr_towards[i] != -128:  # -128 is UNK_SNR
                            snr = f" ({snr_towards[i]/4:.1f}dB)"
                        current_route += f" → {node_name}{snr}"
                    
                    # Add destination
                    if snr_towards and len(snr_towards) > len(route):
                        snr = f" ({snr_towards[-1]/4:.1f}dB)" if snr_towards[-1] != -128 else ""
                    else:
                        snr = ""
                    current_route += f" → {to_name}{snr}"
                    
                    route_parts.append(current_route)
                
                # Route back from destination
                if route_back:
                    route_parts.append(f"**Back from {to_name}:**")
                    back_route = f"{to_name}"
                    
                    for i, node_num in enumerate(route_back):
                        node_name = self.database.get_node_display_name(f"!{node_num:08x}") if self.database else f"!{node_num:08x}"
                        snr = ""
                        if i < len(snr_back) and snr_back[i] != -128:  # -128 is UNK_SNR
                            snr = f" ({snr_back[i]/4:.1f}dB)"
                        back_route += f" → {node_name}{snr}"
                    
                    # Add origin
                    if snr_back and len(snr_back) > len(route_back):
                        snr = f" ({snr_back[-1]/4:.1f}dB)" if snr_back[-1] != -128 else ""
                    else:
                        snr = ""
                    back_route += f" → {from_name}{snr}"
                    
                    route_parts.append(back_route)
                
                # Create Discord message
                if route_parts:
                    route_text = "\n".join(route_parts)
                    hops_count = len(route) + len(route_back) if route_back else len(route)
                    
                    # Queue for Discord display
                    traceroute_payload = {
                        'type': 'traceroute',
                        'from_id': from_id,
                        'from_name': from_name,
                        'to_id': to_id,
                        'to_name': to_name,
                        'route_text': route_text,
                        'hops_count': hops_count,
                        'timestamp': datetime.utcnow().isoformat() + 'Z'
                    }
                    try:
                        self.mesh_to_discord.put_nowait(traceroute_payload)
                    except queue.Full:
                        logger.warning("Mesh to Discord queue is full, dropping traceroute")
                        # Keep oldest messages for traceroute as they're important
                        try:
                            self.mesh_to_discord.get_nowait()
                            self.mesh_to_discord.put_nowait(traceroute_payload)
                        except queue.Empty:
                            pass
                    logger.info(f"🛣️ TRACEROUTE: Queued route info - {from_name} → {to_name} ({hops_count} hops)")
                    
                    # Add traceroute to live monitor buffer
                    traceroute_packet_info = {
                        'type': 'traceroute',
                        'portnum': 'ROUTING_APP',
                        'from_name': from_name,
                        'from_id': from_id,
                        'to_name': to_name,
                        'to_id': to_id,
                        'hops_count': hops_count,
                        'hops': 0,  # Route hops, not packet hops
                        'snr': 'N/A',
                        'rssi': 'N/A'
                    }
                    if hasattr(self, 'command_handler'):
                        self.command_handler.add_packet_to_buffer(traceroute_packet_info)
                else:
                    logger.debug(f"No route information in routing packet from {from_name}")
            else:
                logger.debug(f"Routing packet from {from_name} does not contain RouteDiscovery data")
                
        except Exception as e:
            logger.error(f"Error processing routing packet: {e}")
    
    def _process_position_packet(self, packet):
        """Process position packet and detect movement"""
        try:
            from_id = packet.get('fromId', 'Unknown')
            decoded = packet.get('decoded', {})
            position_data = decoded.get('position', {})
            
            if not position_data:
                logger.debug(f"No position data in packet from {from_id}")
                return
            
            # Extract position coordinates
            new_lat = position_data.get('latitude_i', 0) / 1e7  # Convert from integer to float
            new_lon = position_data.get('longitude_i', 0) / 1e7
            new_alt = position_data.get('altitude', 0)
            
            # Skip if coordinates are invalid (0,0)
            if new_lat == 0 and new_lon == 0:
                logger.debug(f"Invalid position coordinates (0,0) from {from_id}")
                return
            
            # Get last known position from database
            if self.database:
                last_position = self.database.get_last_position(from_id)
                
                if last_position:
                    last_lat = last_position.get('latitude', 0)
                    last_lon = last_position.get('longitude', 0)
                    
                    # Calculate distance moved
                    if last_lat != 0 and last_lon != 0:
                        distance_moved = self.calculate_distance(last_lat, last_lon, new_lat, new_lon)
                        
                        # Movement threshold: 100 meters (configurable)
                        movement_threshold = 100.0
                        
                        if distance_moved > movement_threshold:
                            # Node has moved significantly!
                            from_name = self.database.get_node_display_name(from_id) if self.database else from_id
                            
                            # Create movement notification
                            movement_payload = {
                                'type': 'movement',
                                'from_id': from_id,
                                'from_name': from_name,
                                'distance_moved': distance_moved,
                                'old_lat': last_lat,
                                'old_lon': last_lon,
                                'new_lat': new_lat,
                                'new_lon': new_lon,
                                'new_alt': new_alt,
                                'timestamp': datetime.utcnow().isoformat() + 'Z'
                            }
                            
                            # Queue for Discord
                            try:
                                self.mesh_to_discord.put_nowait(movement_payload)
                            except queue.Full:
                                logger.warning("Mesh to Discord queue is full, dropping movement notification")
                                # Movement notifications are important, so try to make room
                                try:
                                    self.mesh_to_discord.get_nowait()
                                    self.mesh_to_discord.put_nowait(movement_payload)
                                except queue.Empty:
                                    pass
                            logger.info(f"🚶 MOVEMENT: {from_name} moved {distance_moved:.1f}m from last position")
                            
                            # Add to live monitor buffer
                            if hasattr(self, 'command_handler'):
                                movement_packet_info = {
                                    'type': 'movement',
                                    'portnum': 'POSITION_APP',
                                    'from_name': from_name,
                                    'from_id': from_id,
                                    'distance_moved': distance_moved,
                                    'hops': 0,
                                    'snr': 'N/A',
                                    'rssi': 'N/A'
                                }
                                self.command_handler.add_packet_to_buffer(movement_packet_info)
            
            # Store new position in database
            if self.database:
                try:
                    position_data_to_store = {
                        'latitude': new_lat,
                        'longitude': new_lon,
                        'altitude': new_alt,
                        'speed': position_data.get('speed', 0),
                        'heading': position_data.get('ground_track', 0),
                        'accuracy': position_data.get('precision_bits', 0),
                        'source': 'meshtastic'
                    }
                    self.database.add_position(from_id, position_data_to_store)
                    logger.debug(f"Stored position for {from_id}: {new_lat:.6f}, {new_lon:.6f}")
                except Exception as pos_error:
                    logger.error(f"Error storing position for {from_id}: {pos_error}")
                    
        except Exception as e:
            logger.error(f"Error processing position packet: {e}")
    
    def on_mesh_connection(self, interface, topic=pub.AUTO_TOPIC):
        """Handle mesh connection events"""
        logger.info(f"Connected to Meshtastic: {interface.myInfo}")
    
    async def close(self):
        """Clean shutdown of the bot"""
        try:
            logger.info("Shutting down bot...")
            
            # Close database connections
            if hasattr(self.database, 'close_connections'):
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
            
            # Close Discord connection
            await super().close()
            logger.info("Bot shutdown complete")
            
        except Exception as e:
            logger.error(f"Error during bot shutdown: {e}")
            await super().close()

def main():
    """Main function to run the bot"""
    try:
        # Load configuration
        config = Config(
            discord_token=os.getenv("DISCORD_TOKEN"),
            channel_id=int(os.getenv("DISCORD_CHANNEL_ID", "0")),
            meshtastic_hostname=os.getenv("MESHTASTIC_HOSTNAME")
        )
        
        # Validate configuration
        if not config.discord_token:
            logger.error("DISCORD_TOKEN not found in environment variables")
            sys.exit(1)
        
        if not config.channel_id:
            logger.error("DISCORD_CHANNEL_ID not found or invalid in environment variables")
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
            meshtastic_interface = MeshtasticInterface(config.meshtastic_hostname, database)
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