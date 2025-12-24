#!/usr/bin/env python3
"""
Quick script to fix corrupted database indexes
Run this BEFORE starting the bot
"""

import sqlite3
import os
import sys

DB_PATH = "meshtastic.db"

def fix_database():
    """Fix corrupted database indexes"""
    
    if not os.path.exists(DB_PATH):
        print(f"❌ Database file not found: {DB_PATH}")
        return False
    
    print(f"🔧 Fixing database: {DB_PATH}")
    
    try:
        # Remove WAL files that might be causing corruption
        if os.path.exists(f"{DB_PATH}-wal"):
            print("📁 Removing WAL file...")
            os.remove(f"{DB_PATH}-wal")
        
        if os.path.exists(f"{DB_PATH}-shm"):
            print("📁 Removing SHM file...")
            os.remove(f"{DB_PATH}-shm")
        
        # Connect to database
        print("🔌 Connecting to database...")
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # Check integrity
        print("🔍 Checking database integrity...")
        cursor.execute("PRAGMA integrity_check")
        result = cursor.fetchone()
        if result[0] != "ok":
            print(f"⚠️  Database integrity check failed: {result[0]}")
            print("   Attempting to repair...")
        else:
            print("✅ Database integrity OK")
        
        # Drop corrupted indexes
        corrupted_indexes = [
            'idx_telemetry_lookup',
            'idx_positions_lookup', 
            'idx_messages_lookup',
            'idx_messages_to_lookup'
        ]
        
        print("\n🗑️  Removing potentially corrupted indexes...")
        for idx in corrupted_indexes:
            try:
                cursor.execute(f"DROP INDEX IF EXISTS {idx}")
                print(f"   ✓ Dropped {idx}")
            except sqlite3.Error as e:
                print(f"   ⚠️  Could not drop {idx}: {e}")
        
        conn.commit()
        
        # Vacuum to clean up
        print("\n🧹 Vacuuming database to clean up...")
        cursor.execute("VACUUM")
        
        # Verify tables still exist
        print("\n📊 Verifying tables...")
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()
        required_tables = ['nodes', 'telemetry', 'positions', 'messages']
        
        found_tables = [t[0] for t in tables]
        for table in required_tables:
            if table in found_tables:
                print(f"   ✓ Table '{table}' OK")
            else:
                print(f"   ❌ Table '{table}' MISSING!")
        
        conn.close()
        
        print("\n✅ Database repair complete!")
        print("\n📝 The bot will recreate the indexes automatically on next start.")
        print("🚀 You can now run: python bot.py")
        
        return True
        
    except sqlite3.Error as e:
        print(f"\n❌ Database error: {e}")
        print("\n💡 If this persists, you may need to:")
        print("   1. Backup: mv meshtastic.db meshtastic.db.backup")
        print("   2. Start fresh: python bot.py (will create new database)")
        return False
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        return False

if __name__ == "__main__":
    print("=" * 60)
    print("  Meshtastic Bot - Database Repair Tool")
    print("=" * 60)
    print()
    
    if fix_database():
        sys.exit(0)
    else:
        sys.exit(1)

