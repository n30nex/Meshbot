"""
Database module for Meshtastic Discord Bridge Bot
Handles SQLite storage for nodes, telemetry, and position data
"""

import sqlite3
import logging
import threading
import time
import os
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timedelta
import json
from contextlib import contextmanager

logger = logging.getLogger(__name__)


class MeshtasticDatabase:
    """SQLite database manager for Meshtastic node data with connection pooling and WAL mode"""

    def __init__(self, db_path: Optional[str] = None):
        self.db_path = db_path or os.environ.get("DATABASE_PATH", "meshtastic.db")
        self._lock = threading.RLock()
        self._connection_pool = []
        self._max_connections = 5
        self._connection_timeout = 30
        self.init_database()
        self._start_maintenance_task()

    def init_database(self):
        """Initialize database tables with WAL mode and optimizations"""
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()

                # Enable WAL mode for better concurrency
                cursor.execute("PRAGMA journal_mode = WAL")
                cursor.execute("PRAGMA synchronous = NORMAL")
                cursor.execute("PRAGMA cache_size = -2000")  # 2MB cache
                cursor.execute("PRAGMA temp_store = MEMORY")
                cursor.execute("PRAGMA mmap_size = 268435456")  # 256MB
                cursor.execute("PRAGMA optimize")

                # Nodes table - stores basic node information
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS nodes (
                        node_id TEXT PRIMARY KEY,
                        node_num INTEGER,
                        long_name TEXT NOT NULL,
                        short_name TEXT,
                        macaddr TEXT,
                        hw_model TEXT,
                        firmware_version TEXT,
                        first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        last_heard TIMESTAMP,
                        hops_away INTEGER DEFAULT 0,
                        is_router BOOLEAN DEFAULT FALSE,
                        is_client BOOLEAN DEFAULT TRUE,
                        announced_at TIMESTAMP,
                        presence_state TEXT,
                        labels TEXT
                    )
                """)

                # Telemetry table - stores telemetry data
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS telemetry (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        node_id TEXT NOT NULL,
                        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        -- Device metrics
                        battery_level REAL,
                        voltage REAL,
                        channel_utilization REAL,
                        air_util_tx REAL,
                        uptime_seconds REAL,
                        -- Environment metrics
                        temperature REAL,
                        humidity REAL,
                        pressure REAL,
                        gas_resistance REAL,
                        iaq REAL,
                        -- Air quality metrics
                        pm10 REAL,
                        pm25 REAL,
                        pm100 REAL,
                        -- Power metrics
                        ch1_voltage REAL,
                        ch2_voltage REAL,
                        ch3_voltage REAL,
                        ch4_voltage REAL,
                        ch5_voltage REAL,
                        ch6_voltage REAL,
                        ch7_voltage REAL,
                        ch8_voltage REAL,
                        ch1_current REAL,
                        ch2_current REAL,
                        ch3_current REAL,
                        ch4_current REAL,
                        ch5_current REAL,
                        ch6_current REAL,
                        ch7_current REAL,
                        ch8_current REAL,
                        -- Radio metrics
                        snr REAL,
                        rssi REAL,
                        frequency REAL,
                        -- Position data
                        latitude REAL,
                        longitude REAL,
                        altitude REAL,
                        speed REAL,
                        heading REAL,
                        accuracy REAL,
                        FOREIGN KEY (node_id) REFERENCES nodes (node_id)
                    )
                """)

                # Position table - stores position data
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS positions (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        node_id TEXT NOT NULL,
                        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        latitude REAL,
                        longitude REAL,
                        altitude REAL,
                        speed REAL,
                        heading REAL,
                        accuracy REAL,
                        source TEXT,
                        FOREIGN KEY (node_id) REFERENCES nodes (node_id)
                    )
                """)

                # Messages table - stores message history
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS messages (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        from_node_id TEXT,
                        to_node_id TEXT,
                        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        message_text TEXT,
                        port_num TEXT,
                        payload TEXT,
                        hops_away INTEGER,
                        snr REAL,
                        rssi REAL,
                        FOREIGN KEY (from_node_id) REFERENCES nodes (node_id),
                        FOREIGN KEY (to_node_id) REFERENCES nodes (node_id)
                    )
                """)

                # Create indexes for better performance with safe error handling
                try:
                    cursor.execute(
                        "CREATE INDEX IF NOT EXISTS idx_nodes_last_heard ON nodes (last_heard)"
                    )
                    cursor.execute(
                        "CREATE INDEX IF NOT EXISTS idx_nodes_last_seen ON nodes (last_seen)"
                    )
                    cursor.execute(
                        "CREATE INDEX IF NOT EXISTS idx_nodes_presence ON nodes (presence_state)"
                    )
                    cursor.execute(
                        "CREATE INDEX IF NOT EXISTS idx_telemetry_timestamp ON telemetry (timestamp)"
                    )
                    cursor.execute(
                        "CREATE INDEX IF NOT EXISTS idx_positions_timestamp ON positions (timestamp)"
                    )
                    cursor.execute(
                        "CREATE INDEX IF NOT EXISTS idx_messages_timestamp ON messages (timestamp)"
                    )
                    cursor.execute(
                        "CREATE INDEX IF NOT EXISTS idx_nodes_long_name ON nodes (long_name)"
                    )
                except sqlite3.Error as idx_error:
                    logger.warning(
                        f"Error creating basic indexes (might already exist): {idx_error}"
                    )

                # Critical composite indexes for get_all_nodes query optimization
                # Drop and recreate if they exist to fix any corruption
                try:
                    cursor.execute("DROP INDEX IF EXISTS idx_telemetry_lookup")
                    cursor.execute("DROP INDEX IF EXISTS idx_positions_lookup")
                    cursor.execute("DROP INDEX IF EXISTS idx_messages_lookup")
                    cursor.execute("DROP INDEX IF EXISTS idx_messages_to_lookup")
                except sqlite3.Error:
                    pass  # Ignore errors when dropping

                # Now create fresh indexes
                try:
                    cursor.execute(
                        "CREATE INDEX idx_telemetry_lookup ON telemetry (node_id, timestamp DESC)"
                    )
                    cursor.execute(
                        "CREATE INDEX idx_positions_lookup ON positions (node_id, timestamp DESC)"
                    )
                    cursor.execute(
                        "CREATE INDEX idx_messages_lookup ON messages (from_node_id, timestamp DESC)"
                    )
                    cursor.execute(
                        "CREATE INDEX idx_messages_to_lookup ON messages (to_node_id, timestamp DESC)"
                    )
                    logger.info(
                        "Successfully created composite indexes for query optimization"
                    )
                except sqlite3.Error as idx_error:
                    logger.warning(
                        f"Could not create composite indexes: {idx_error}. Queries will be slower but functional."
                    )

                # Migrate existing telemetry table to add new columns
                self._migrate_telemetry_table(cursor)

                # Add announced_at to nodes if missing (idempotent migration)
                try:
                    cursor.execute("PRAGMA table_info(nodes)")
                    node_cols = [row[1] for row in cursor.fetchall()]
                    if "announced_at" not in node_cols:
                        cursor.execute(
                            "ALTER TABLE nodes ADD COLUMN announced_at TIMESTAMP"
                        )
                        logger.info("Added announced_at column to nodes table")
                    if "presence_state" not in node_cols:
                        cursor.execute(
                            "ALTER TABLE nodes ADD COLUMN presence_state TEXT"
                        )
                        logger.info("Added presence_state column to nodes table")
                    if "labels" not in node_cols:
                        cursor.execute("ALTER TABLE nodes ADD COLUMN labels TEXT")
                        logger.info("Added labels column to nodes table")
                except sqlite3.Error as mig_err:
                    logger.warning(
                        f"Could not ensure announced_at column on nodes: {mig_err}"
                    )

                # Alerts table for deduplication across restarts
                try:
                    cursor.execute("""
                        CREATE TABLE IF NOT EXISTS alerts (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            node_id TEXT NOT NULL,
                            type TEXT NOT NULL,
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            FOREIGN KEY (node_id) REFERENCES nodes (node_id)
                        )
                    """)
                    cursor.execute(
                        "CREATE INDEX IF NOT EXISTS idx_alerts_lookup ON alerts (node_id, type, created_at DESC)"
                    )
                except sqlite3.Error as aerr:
                    logger.warning(f"Error creating alerts table/index: {aerr}")

                conn.commit()
                logger.info("Database initialized successfully with WAL mode")

        except sqlite3.OperationalError as e:
            logger.error(f"Database operational error: {e}")
            raise
        except sqlite3.Error as e:
            logger.error(f"Database error: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error initializing database: {e}")
            raise

    def _migrate_telemetry_table(self, cursor):
        """Migrate telemetry table to add new sensor columns"""
        try:
            # Get current table schema
            cursor.execute("PRAGMA table_info(telemetry)")
            columns = [row[1] for row in cursor.fetchall()]

            # New columns to add
            new_columns = [
                ("channel_utilization", "REAL"),
                ("air_util_tx", "REAL"),
                ("uptime_seconds", "REAL"),
                ("iaq", "REAL"),
                ("pm10", "REAL"),
                ("pm25", "REAL"),
                ("pm100", "REAL"),
                ("ch1_voltage", "REAL"),
                ("ch2_voltage", "REAL"),
                ("ch3_voltage", "REAL"),
                ("ch4_voltage", "REAL"),
                ("ch5_voltage", "REAL"),
                ("ch6_voltage", "REAL"),
                ("ch7_voltage", "REAL"),
                ("ch8_voltage", "REAL"),
                ("ch1_current", "REAL"),
                ("ch2_current", "REAL"),
                ("ch3_current", "REAL"),
                ("ch4_current", "REAL"),
                ("ch5_current", "REAL"),
                ("ch6_current", "REAL"),
                ("ch7_current", "REAL"),
                ("ch8_current", "REAL"),
            ]

            # Add missing columns with validation
            for column_name, column_type in new_columns:
                if column_name not in columns:
                    # Validate column name to prevent injection (alphanumeric and underscore only)
                    if not column_name.replace("_", "").isalnum():
                        logger.error(f"Invalid column name: {column_name}")
                        continue

                    # Validate column type
                    valid_types = ["TEXT", "INTEGER", "REAL", "BLOB", "NULL"]
                    if column_type not in valid_types:
                        logger.error(f"Invalid column type: {column_type}")
                        continue

                    # Safe to execute as we've validated both inputs
                    cursor.execute(
                        f"ALTER TABLE telemetry ADD COLUMN {column_name} {column_type}"
                    )
                    logger.info(f"Added column {column_name} to telemetry table")

        except Exception as e:
            logger.error(f"Error migrating telemetry table: {e}")
            # Don't raise - this is a migration, not critical

    @contextmanager
    def _get_connection(self):
        """Get a database connection from the pool or create a new one"""
        conn = None
        try:
            with self._lock:
                # Try to get a connection from the pool
                if self._connection_pool:
                    conn = self._connection_pool.pop()
                else:
                    # Create a new connection
                    conn = sqlite3.connect(
                        self.db_path, timeout=30, check_same_thread=False
                    )
                    conn.row_factory = sqlite3.Row
                    conn.execute("PRAGMA journal_mode = WAL")
                    conn.execute("PRAGMA synchronous = NORMAL")
                    conn.execute("PRAGMA cache_size = -2000")
                    conn.execute("PRAGMA temp_store = MEMORY")

                yield conn

        except Exception as e:
            if conn:
                conn.rollback()
            raise e
        finally:
            if conn:
                try:
                    conn.commit()
                    # Return connection to pool if not full
                    with self._lock:
                        if len(self._connection_pool) < self._max_connections:
                            self._connection_pool.append(conn)
                        else:
                            conn.close()
                except Exception as e:
                    logger.warning(f"Error returning connection to pool: {e}")
                    if conn:
                        conn.close()

    def _start_maintenance_task(self):
        """Start background maintenance task"""

        def maintenance_worker():
            while True:
                try:
                    time.sleep(3600)  # Run every hour
                    self._run_maintenance()
                except Exception as e:
                    logger.error(f"Error in maintenance task: {e}")
                    time.sleep(300)  # Wait 5 minutes before retrying

        maintenance_thread = threading.Thread(target=maintenance_worker, daemon=True)
        maintenance_thread.start()
        logger.info("Database maintenance task started")

    def _run_maintenance(self):
        """Run database maintenance tasks"""
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()

                # Analyze database for query optimization
                cursor.execute("ANALYZE")

                # Clean up old data (keep 30 days)
                self.cleanup_old_data(30)

                # Vacuum if needed (check database size)
                cursor.execute("PRAGMA page_count")
                page_count = cursor.fetchone()[0]
                cursor.execute("PRAGMA page_size")
                page_size = cursor.fetchone()[0]
                db_size_mb = (page_count * page_size) / (1024 * 1024)

                if db_size_mb > 100:  # If database is larger than 100MB
                    logger.info("Running VACUUM to optimize database")
                    cursor.execute("VACUUM")

                logger.info("Database maintenance completed")

        except Exception as e:
            logger.error(f"Error during database maintenance: {e}")

    def add_or_update_node(self, node_data: Dict[str, Any]) -> Tuple[bool, bool]:
        """Add new node or update existing node information.

        Returns (success, is_new_node) where is_new_node is True only on INSERT.
        """
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()

                # Check if node exists
                cursor.execute(
                    "SELECT node_id FROM nodes WHERE node_id = ?",
                    (node_data["node_id"],),
                )
                exists = cursor.fetchone()

                if exists:
                    # Update existing node
                    # We intentionally do not override last_heard with a fallback
                    # here; callers should supply a proper timestamp or leave it
                    # unchanged if no recent contact has been recorded.
                    cursor.execute(
                        """
                        UPDATE nodes SET 
                            node_num = ?,
                            long_name = ?,
                            short_name = ?,
                            macaddr = ?,
                            hw_model = ?,
                            firmware_version = ?,
                            last_seen = CURRENT_TIMESTAMP,
                            last_heard = COALESCE(?, last_heard),
                            hops_away = ?,
                            is_router = ?,
                            is_client = ?
                        WHERE node_id = ?
                    """,
                        (
                            node_data.get("node_num"),
                            node_data.get("long_name", "Unknown"),
                            node_data.get("short_name"),
                            node_data.get("macaddr"),
                            node_data.get("hw_model"),
                            node_data.get("firmware_version"),
                            node_data.get("last_heard"),
                            node_data.get("hops_away", 0),
                            node_data.get("is_router", False),
                            node_data.get("is_client", True),
                            node_data["node_id"],
                        ),
                    )

                else:
                    # Insert new node
                    cursor.execute(
                        """
                        INSERT INTO nodes (
                            node_id, node_num, long_name, short_name, macaddr,
                            hw_model, firmware_version, last_heard, hops_away,
                            is_router, is_client, announced_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                        (
                            node_data["node_id"],
                            node_data.get("node_num"),
                            node_data.get("long_name", "Unknown"),
                            node_data.get("short_name"),
                            node_data.get("macaddr"),
                            node_data.get("hw_model"),
                            node_data.get("firmware_version"),
                            node_data.get("last_heard"),
                            node_data.get("hops_away", 0),
                            node_data.get("is_router", False),
                            node_data.get("is_client", True),
                            None,
                        ),
                    )
                    conn.commit()
                    return True, True  # (success, is_new_node)

                conn.commit()
                return True, False  # (success, not_new_node)

        except sqlite3.OperationalError as e:
            logger.error(f"Database operational error adding/updating node: {e}")
            return False, False
        except sqlite3.Error as e:
            logger.error(f"Database error adding/updating node: {e}")
            return False, False
        except Exception as e:
            logger.error(f"Unexpected error adding/updating node: {e}")
            return False, False

    def mark_node_announced(self, node_id: str, when: Optional[str] = None) -> bool:
        """Mark a node as announced to prevent duplicate 'new node' messages."""
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                if when is None:
                    when = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
                cursor.execute(
                    "UPDATE nodes SET announced_at = ? WHERE node_id = ?",
                    (when, node_id),
                )
                conn.commit()
                return cursor.rowcount > 0
        except Exception as e:
            logger.error(f"Error marking node {node_id} announced: {e}")
            return False

    def record_alert(self, node_id: str, alert_type: str) -> bool:
        """Record an alert occurrence for cooldown and dedupe."""
        try:
            with self._get_connection() as conn:
                cur = conn.cursor()
                cur.execute(
                    "INSERT INTO alerts (node_id, type) VALUES (?, ?)",
                    (node_id, alert_type),
                )
                conn.commit()
                return True
        except Exception as e:
            logger.error(f"Error recording alert {alert_type} for {node_id}: {e}")
            return False

    def recent_alert_exists(self, node_id: str, alert_type: str, minutes: int) -> bool:
        """Check if an alert of type exists within the last N minutes."""
        try:
            with self._get_connection() as conn:
                cur = conn.cursor()
                cutoff = datetime.utcnow() - timedelta(minutes=minutes)
                cutoff_str = cutoff.strftime("%Y-%m-%d %H:%M:%S")
                cur.execute(
                    """
                    SELECT 1 FROM alerts
                    WHERE node_id = ? AND type = ? AND datetime(created_at) > datetime(?)
                    LIMIT 1
                """,
                    (node_id, alert_type, cutoff_str),
                )
                return cur.fetchone() is not None
        except Exception as e:
            logger.error(f"Error checking recent alert {alert_type} for {node_id}: {e}")
            return False

    def get_presence_counts(
        self, threshold_minutes: int = 60, hysteresis_factor: float = 2.0
    ) -> Dict[str, int]:
        """Return presence counts by state based on last_heard and thresholds."""
        try:
            with self._get_connection() as conn:
                cur = conn.cursor()
                now = datetime.utcnow()
                online_cutoff = (now - timedelta(minutes=threshold_minutes)).strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
                offline_cutoff = (
                    now - timedelta(minutes=int(threshold_minutes * hysteresis_factor))
                ).strftime("%Y-%m-%d %H:%M:%S")

                # Online: last_heard within threshold
                cur.execute(
                    "SELECT COUNT(*) FROM nodes WHERE datetime(last_heard) > datetime(?)",
                    (online_cutoff,),
                )
                online = cur.fetchone()[0] or 0
                # Offline: last_heard older than hysteresis window (or NULL)
                cur.execute(
                    "SELECT COUNT(*) FROM nodes WHERE last_heard IS NULL OR datetime(last_heard) <= datetime(?)",
                    (offline_cutoff,),
                )
                offline = cur.fetchone()[0] or 0
                # Unknown: the rest
                cur.execute("SELECT COUNT(*) FROM nodes")
                total = cur.fetchone()[0] or 0
                unknown = max(0, total - online - offline)
                return {
                    "online": online,
                    "offline": offline,
                    "unknown": unknown,
                    "total": total,
                }
        except Exception as e:
            logger.error(f"Error computing presence counts: {e}")
            return {"online": 0, "offline": 0, "unknown": 0, "total": 0}

    def get_recent_positions(
        self, node_id: str, minutes: int = 10
    ) -> List[Dict[str, Any]]:
        """Return recent positions for a node within the last N minutes."""
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cutoff = datetime.utcnow() - timedelta(minutes=minutes)
                cutoff_str = cutoff.strftime("%Y-%m-%d %H:%M:%S")
                cursor.execute(
                    """
                    SELECT latitude, longitude, altitude, speed, heading, accuracy, source, timestamp
                    FROM positions
                    WHERE node_id = ? AND datetime(timestamp) > datetime(?)
                    ORDER BY timestamp DESC
                """,
                    (node_id, cutoff_str),
                )
                rows = cursor.fetchall()
                result: List[Dict[str, Any]] = []
                for row in rows:
                    result.append(
                        {
                            "latitude": row[0],
                            "longitude": row[1],
                            "altitude": row[2],
                            "speed": row[3],
                            "heading": row[4],
                            "accuracy": row[5],
                            "source": row[6],
                            "timestamp": row[7],
                        }
                    )
                return result
        except Exception as e:
            logger.error(f"Error fetching recent positions for {node_id}: {e}")
            return []

    def add_telemetry(self, node_id: str, telemetry_data: Dict[str, Any]) -> bool:
        """Add telemetry data for a node"""
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()

                cursor.execute(
                    """
                    INSERT INTO telemetry (
                        node_id, battery_level, voltage, channel_utilization, air_util_tx, uptime_seconds,
                        temperature, humidity, pressure, gas_resistance, iaq,
                        pm10, pm25, pm100,
                        ch1_voltage, ch2_voltage, ch3_voltage, ch4_voltage, ch5_voltage, ch6_voltage, ch7_voltage, ch8_voltage,
                        ch1_current, ch2_current, ch3_current, ch4_current, ch5_current, ch6_current, ch7_current, ch8_current,
                        snr, rssi, frequency,
                        latitude, longitude, altitude, speed, heading, accuracy
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                    (
                        node_id,
                        telemetry_data.get("battery_level"),
                        telemetry_data.get("voltage"),
                        telemetry_data.get("channel_utilization"),
                        telemetry_data.get("air_util_tx"),
                        telemetry_data.get("uptime_seconds"),
                        telemetry_data.get("temperature"),
                        telemetry_data.get("humidity"),
                        telemetry_data.get("pressure"),
                        telemetry_data.get("gas_resistance"),
                        telemetry_data.get("iaq"),
                        telemetry_data.get("pm10"),
                        telemetry_data.get("pm25"),
                        telemetry_data.get("pm100"),
                        telemetry_data.get("ch1_voltage"),
                        telemetry_data.get("ch2_voltage"),
                        telemetry_data.get("ch3_voltage"),
                        telemetry_data.get("ch4_voltage"),
                        telemetry_data.get("ch5_voltage"),
                        telemetry_data.get("ch6_voltage"),
                        telemetry_data.get("ch7_voltage"),
                        telemetry_data.get("ch8_voltage"),
                        telemetry_data.get("ch1_current"),
                        telemetry_data.get("ch2_current"),
                        telemetry_data.get("ch3_current"),
                        telemetry_data.get("ch4_current"),
                        telemetry_data.get("ch5_current"),
                        telemetry_data.get("ch6_current"),
                        telemetry_data.get("ch7_current"),
                        telemetry_data.get("ch8_current"),
                        telemetry_data.get("snr"),
                        telemetry_data.get("rssi"),
                        telemetry_data.get("frequency"),
                        telemetry_data.get("latitude"),
                        telemetry_data.get("longitude"),
                        telemetry_data.get("altitude"),
                        telemetry_data.get("speed"),
                        telemetry_data.get("heading"),
                        telemetry_data.get("accuracy"),
                    ),
                )

                conn.commit()
                return True

        except sqlite3.OperationalError as e:
            logger.error(f"Database operational error adding telemetry: {e}")
            return False
        except sqlite3.Error as e:
            logger.error(f"Database error adding telemetry: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error adding telemetry: {e}")
            return False

    def add_position(self, node_id: str, position_data: Dict[str, Any]) -> bool:
        """Add position data for a node"""
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()

                cursor.execute(
                    """
                    INSERT INTO positions (
                        node_id, latitude, longitude, altitude, speed, heading, accuracy, source
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                    (
                        node_id,
                        position_data.get("latitude"),
                        position_data.get("longitude"),
                        position_data.get("altitude"),
                        position_data.get("speed"),
                        position_data.get("heading"),
                        position_data.get("accuracy"),
                        position_data.get("source", "unknown"),
                    ),
                )

                conn.commit()
                return True

        except sqlite3.OperationalError as e:
            logger.error(f"Database operational error adding position: {e}")
            return False
        except sqlite3.Error as e:
            logger.error(f"Database error adding position: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error adding position: {e}")
            return False

    def get_last_position(self, node_id: str) -> Optional[Dict[str, Any]]:
        """Get the last known position for a node"""
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()

                cursor.execute(
                    """
                    SELECT latitude, longitude, altitude, speed, heading, accuracy, source, timestamp
                    FROM positions 
                    WHERE node_id = ? 
                    ORDER BY timestamp DESC 
                    LIMIT 1
                """,
                    (node_id,),
                )

                row = cursor.fetchone()
                if row:
                    return {
                        "latitude": row[0],
                        "longitude": row[1],
                        "altitude": row[2],
                        "speed": row[3],
                        "heading": row[4],
                        "accuracy": row[5],
                        "source": row[6],
                        "timestamp": row[7],
                    }
                return None

        except sqlite3.OperationalError as e:
            logger.error(f"Database operational error getting last position: {e}")
            return None
        except sqlite3.Error as e:
            logger.error(f"Database error getting last position: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error getting last position: {e}")
            return None

    def add_message(self, message_data: Dict[str, Any]) -> bool:
        """Add message to database"""
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()

                cursor.execute(
                    """
                    INSERT INTO messages (
                        from_node_id, to_node_id, message_text, port_num, payload,
                        hops_away, snr, rssi
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                    (
                        message_data.get("from_node_id"),
                        message_data.get("to_node_id"),
                        message_data.get("message_text"),
                        message_data.get("port_num"),
                        message_data.get("payload"),
                        message_data.get("hops_away"),
                        message_data.get("snr"),
                        message_data.get("rssi"),
                    ),
                )

                conn.commit()
                return True

        except sqlite3.OperationalError as e:
            logger.error(f"Database operational error adding message: {e}")
            return False
        except sqlite3.Error as e:
            logger.error(f"Database error adding message: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error adding message: {e}")
            return False

    def update_last_heard(self, node_id: str, timestamp: Optional[str] = None) -> bool:
        """Update the last_heard timestamp for a node.

        This should be called whenever a packet is received (text, telemetry,
        position, etc.) to ensure that nodes are considered active when they
        send any type of data.  If no timestamp is provided, the current
        UTC time will be used.  The timestamp is stored in
        'YYYY-MM-DD HH:MM:SS' format to ensure compatibility with SQLite's
        datetime functions.

        Returns True on success, False otherwise.
        """
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                if timestamp is None:
                    timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
                cursor.execute(
                    "UPDATE nodes SET last_heard = ? WHERE node_id = ?",
                    (timestamp, node_id),
                )
                conn.commit()
                return cursor.rowcount > 0
        except sqlite3.OperationalError as e:
            logger.error(f"Database operational error updating last_heard: {e}")
            return False
        except sqlite3.Error as e:
            logger.error(f"Database error updating last_heard: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error updating last_heard: {e}")
            return False

    def get_active_nodes(self, minutes: int = 60) -> List[Dict[str, Any]]:
        """Get nodes active in the last N minutes"""
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()

                # Determine cutoff time in UTC and format for SQL comparison.
                cutoff_time = datetime.utcnow() - timedelta(minutes=minutes)
                cutoff_str = cutoff_time.strftime("%Y-%m-%d %H:%M:%S")

                cursor.execute(
                    """
                    SELECT n.*, 
                           t.battery_level, t.voltage, t.temperature, t.humidity,
                           t.pressure, t.gas_resistance, t.iaq, t.snr, t.rssi,
                           p.latitude, p.longitude, p.altitude, p.speed, p.heading
                    FROM nodes n
                    LEFT JOIN (
                        SELECT node_id, battery_level, voltage, temperature, humidity,
                               pressure, gas_resistance, iaq, snr, rssi
                        FROM telemetry 
                        WHERE timestamp = (
                            SELECT MAX(timestamp) FROM telemetry t2 
                            WHERE t2.node_id = telemetry.node_id
                        )
                    ) t ON n.node_id = t.node_id
                    LEFT JOIN (
                        SELECT node_id, latitude, longitude, altitude, speed, heading
                        FROM positions 
                        WHERE timestamp = (
                            SELECT MAX(timestamp) FROM positions p2 
                            WHERE p2.node_id = positions.node_id
                        )
                    ) p ON n.node_id = p.node_id
                    WHERE datetime(n.last_heard) > datetime(?)
                    ORDER BY datetime(n.last_heard) DESC
                """,
                    (cutoff_str,),
                )

                columns = [description[0] for description in cursor.description]
                rows = cursor.fetchall()

                return [dict(zip(columns, row)) for row in rows]

        except sqlite3.OperationalError as e:
            logger.error(f"Database operational error getting active nodes: {e}")
            return []
        except sqlite3.Error as e:
            logger.error(f"Database error getting active nodes: {e}")
            return []
        except Exception as e:
            logger.error(f"Unexpected error getting active nodes: {e}")
            return []

    def get_all_nodes(self, limit: int = 200) -> List[Dict[str, Any]]:
        """Get all known nodes with optimized query using indexed lookups"""
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()

                # Optimized query using CTEs and proper indexes
                # This avoids correlated subqueries which are very slow
                cursor.execute(
                    """
                    WITH latest_telemetry AS (
                        SELECT 
                            node_id,
                            battery_level,
                            voltage,
                            temperature,
                            humidity,
                            pressure,
                            gas_resistance,
                            iaq,
                            snr,
                            rssi,
                            ROW_NUMBER() OVER (PARTITION BY node_id ORDER BY timestamp DESC) as rn
                        FROM telemetry
                    ),
                    latest_positions AS (
                        SELECT 
                            node_id,
                            latitude,
                            longitude,
                            altitude,
                            speed,
                            heading,
                            ROW_NUMBER() OVER (PARTITION BY node_id ORDER BY timestamp DESC) as rn
                        FROM positions
                    )
                    SELECT 
                        n.*,
                        t.battery_level,
                        t.voltage,
                        t.temperature,
                        t.humidity,
                        t.pressure,
                        t.gas_resistance,
                        t.iaq,
                        t.snr,
                        t.rssi,
                        p.latitude,
                        p.longitude,
                        p.altitude,
                        p.speed,
                        p.heading
                    FROM nodes n
                    LEFT JOIN latest_telemetry t ON n.node_id = t.node_id AND t.rn = 1
                    LEFT JOIN latest_positions p ON n.node_id = p.node_id AND p.rn = 1
                    ORDER BY datetime(n.last_heard) DESC
                    LIMIT ?
                """,
                    (limit,),
                )

                columns = [description[0] for description in cursor.description]
                rows = cursor.fetchall()

                return [dict(zip(columns, row)) for row in rows]

        except sqlite3.OperationalError as e:
            logger.error(f"Database operational error getting all nodes: {e}")
            return []
        except sqlite3.Error as e:
            logger.error(f"Database error getting all nodes: {e}")
            return []
        except Exception as e:
            logger.error(f"Unexpected error getting all nodes: {e}")
            return []

    def find_node_by_name(self, name: str) -> Optional[Dict[str, Any]]:
        """Find node by fuzzy matching on long name"""
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()

                # Try exact match first
                cursor.execute(
                    """
                    SELECT * FROM nodes WHERE long_name = ? OR short_name = ?
                """,
                    (name, name),
                )
                result = cursor.fetchone()

                if result:
                    columns = [description[0] for description in cursor.description]
                    return dict(zip(columns, result))

                # Try partial match
                cursor.execute(
                    """
                    SELECT * FROM nodes WHERE long_name LIKE ? OR short_name LIKE ?
                    ORDER BY 
                        CASE 
                            WHEN long_name = ? THEN 1
                            WHEN long_name LIKE ? THEN 2
                            WHEN short_name = ? THEN 3
                            WHEN short_name LIKE ? THEN 4
                            ELSE 5
                        END,
                        last_heard DESC
                    LIMIT 1
                """,
                    (f"%{name}%", f"%{name}%", name, f"{name}%", name, f"{name}%"),
                )

                result = cursor.fetchone()
                if result:
                    columns = [description[0] for description in cursor.description]
                    return dict(zip(columns, result))

                return None

        except sqlite3.OperationalError as e:
            logger.error(f"Database operational error finding node by name: {e}")
            return None
        except sqlite3.Error as e:
            logger.error(f"Database error finding node by name: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error finding node by name: {e}")
            return None

    def get_telemetry_summary(self, minutes: int = 60) -> Dict[str, Any]:
        """Get telemetry summary for active nodes"""
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()

                cutoff_time = datetime.utcnow() - timedelta(minutes=minutes)
                cutoff_str = cutoff_time.strftime("%Y-%m-%d %H:%M:%S")

                cursor.execute(
                    """
                    SELECT 
                        COUNT(DISTINCT n.node_id) as total_nodes,
                        COUNT(DISTINCT CASE WHEN datetime(n.last_heard) > datetime(?) THEN n.node_id END) as active_nodes,
                        AVG(t.battery_level) as avg_battery,
                        AVG(t.temperature) as avg_temperature,
                        AVG(t.humidity) as avg_humidity,
                        AVG(t.snr) as avg_snr,
                        AVG(t.rssi) as avg_rssi
                    FROM nodes n
                    LEFT JOIN telemetry t ON n.node_id = t.node_id AND datetime(t.timestamp) > datetime(?)
                """,
                    (cutoff_str, cutoff_str),
                )

                result = cursor.fetchone()
                columns = [description[0] for description in cursor.description]

                return dict(zip(columns, result))

        except sqlite3.OperationalError as e:
            logger.error(f"Database operational error getting telemetry summary: {e}")
            return {}
        except sqlite3.Error as e:
            logger.error(f"Database error getting telemetry summary: {e}")
            return {}
        except Exception as e:
            logger.error(f"Unexpected error getting telemetry summary: {e}")
            return {}

    def cleanup_old_data(self, days: int = 30):
        """Clean up old telemetry and position data"""
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()

                cutoff_time = datetime.utcnow() - timedelta(days=days)
                cutoff_str = cutoff_time.strftime("%Y-%m-%d %H:%M:%S")

                # Clean up old telemetry
                cursor.execute(
                    "DELETE FROM telemetry WHERE datetime(timestamp) < datetime(?)",
                    (cutoff_str,),
                )
                telemetry_deleted = cursor.rowcount

                # Clean up old positions
                cursor.execute(
                    "DELETE FROM positions WHERE datetime(timestamp) < datetime(?)",
                    (cutoff_str,),
                )
                positions_deleted = cursor.rowcount

                # Clean up old messages
                cursor.execute(
                    "DELETE FROM messages WHERE datetime(timestamp) < datetime(?)",
                    (cutoff_str,),
                )
                messages_deleted = cursor.rowcount

                # Clean up old alerts
                cursor.execute(
                    "DELETE FROM alerts WHERE datetime(created_at) < datetime(?)",
                    (cutoff_str,),
                )
                alerts_deleted = cursor.rowcount

                conn.commit()

                logger.info(
                    f"Cleaned up {telemetry_deleted} telemetry, {positions_deleted} positions, {messages_deleted} messages, {alerts_deleted} alerts"
                )

        except sqlite3.OperationalError as e:
            logger.error(f"Database operational error cleaning up old data: {e}")
        except sqlite3.Error as e:
            logger.error(f"Database error cleaning up old data: {e}")
        except Exception as e:
            logger.error(f"Unexpected error cleaning up old data: {e}")

    def get_node_display_name(self, node_id: str) -> str:
        """Return the best human-friendly name for a node_id (long_name > short_name > node_id)."""
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    SELECT 
                        CASE 
                            WHEN long_name IS NOT NULL AND TRIM(long_name) <> '' THEN long_name
                            WHEN short_name IS NOT NULL AND TRIM(short_name) <> '' THEN short_name
                            ELSE node_id
                        END AS display_name
                    FROM nodes WHERE node_id = ?
                    """,
                    (node_id,),
                )
                row = cursor.fetchone()
                if row and row[0]:
                    return str(row[0])
        except Exception as e:
            logger.warning(f"Failed to lookup display name for {node_id}: {e}")
        return str(node_id)

    def get_telemetry_history(
        self, node_id: str, hours: int = 24, limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Get telemetry history for a specific node"""
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()

                cutoff_time = datetime.utcnow() - timedelta(hours=hours)
                cutoff_str = cutoff_time.strftime("%Y-%m-%d %H:%M:%S")

                cursor.execute(
                    """
                    SELECT timestamp, battery_level, voltage, temperature, humidity,
                           pressure, gas_resistance, iaq, snr, rssi, frequency,
                           latitude, longitude, altitude, speed, heading, accuracy
                    FROM telemetry 
                    WHERE node_id = ? AND datetime(timestamp) > datetime(?)
                    ORDER BY timestamp DESC
                    LIMIT ?
                """,
                    (node_id, cutoff_str, limit),
                )

                columns = [description[0] for description in cursor.description]
                rows = cursor.fetchall()

                return [dict(zip(columns, row)) for row in rows]

        except Exception as e:
            logger.error(f"Error getting telemetry history: {e}")
            return []

    def get_network_topology(self) -> Dict[str, Any]:
        """Get network topology information"""
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()

                # Get node connections based on message routing
                cursor.execute("""
                    SELECT 
                        from_node_id,
                        to_node_id,
                        COUNT(*) as message_count,
                        AVG(hops_away) as avg_hops,
                        AVG(snr) as avg_snr,
                        MAX(timestamp) as last_communication
                    FROM messages 
                    WHERE datetime(timestamp) > datetime('now', '-24 hours')
                    GROUP BY from_node_id, to_node_id
                    HAVING message_count > 0
                    ORDER BY message_count DESC
                """)

                connections = []
                for row in cursor.fetchall():
                    connections.append(
                        {
                            "from_node": row[0],
                            "to_node": row[1],
                            "message_count": row[2],
                            "avg_hops": row[3],
                            "avg_snr": row[4],
                            "last_communication": row[5],
                        }
                    )

                # Get node statistics
                cursor.execute("""
                    SELECT 
                        COUNT(*) as total_nodes,
                        COUNT(CASE WHEN datetime(last_heard) > datetime('now', '-1 hour') THEN 1 END) as active_nodes,
                        COUNT(CASE WHEN is_router = 1 THEN 1 END) as router_nodes,
                        AVG(hops_away) as avg_hops
                    FROM nodes
                """)

                stats = cursor.fetchone()

                return {
                    "connections": connections,
                    "total_nodes": stats[0] or 0,
                    "active_nodes": stats[1] or 0,
                    "router_nodes": stats[2] or 0,
                    "avg_hops": stats[3] or 0,
                }

        except Exception as e:
            logger.error(f"Error getting network topology: {e}")
            return {
                "connections": [],
                "total_nodes": 0,
                "active_nodes": 0,
                "router_nodes": 0,
                "avg_hops": 0,
            }

    def get_message_statistics(self, hours: int = 24) -> Dict[str, Any]:
        """Get message statistics for the specified time period"""
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()

                cutoff_time = datetime.utcnow() - timedelta(hours=hours)

                cutoff_str = cutoff_time.strftime("%Y-%m-%d %H:%M:%S")
                cursor.execute(
                    """
                    SELECT 
                        COUNT(*) as total_messages,
                        COUNT(DISTINCT from_node_id) as unique_senders,
                        COUNT(DISTINCT to_node_id) as unique_recipients,
                        AVG(hops_away) as avg_hops,
                        AVG(snr) as avg_snr,
                        AVG(rssi) as avg_rssi
                    FROM messages 
                    WHERE datetime(timestamp) > datetime(?)
                """,
                    (cutoff_str,),
                )

                stats = cursor.fetchone()

                # Get hourly message distribution
                cursor.execute(
                    """
                    SELECT 
                        strftime('%H', timestamp) as hour,
                        COUNT(*) as message_count
                    FROM messages 
                    WHERE datetime(timestamp) > datetime(?)
                    GROUP BY strftime('%H', timestamp)
                    ORDER BY hour
                """,
                    (cutoff_str,),
                )

                hourly_distribution = {row[0]: row[1] for row in cursor.fetchall()}

                return {
                    "total_messages": stats[0] or 0,
                    "unique_senders": stats[1] or 0,
                    "unique_recipients": stats[2] or 0,
                    "avg_hops": stats[3] or 0,
                    "avg_snr": stats[4] or 0,
                    "avg_rssi": stats[5] or 0,
                    "hourly_distribution": hourly_distribution,
                }

        except Exception as e:
            logger.error(f"Error getting message statistics: {e}")
            return {}

    def close_connections(self):
        """Close all connections in the pool"""
        with self._lock:
            for conn in self._connection_pool:
                try:
                    conn.close()
                except Exception as e:
                    logger.warning(f"Error closing connection: {e}")
            self._connection_pool.clear()
            logger.info("All database connections closed")
