#!/usr/bin/env python3
"""
Schema migration runner for Meshtastic Discord Bridge Bot.
"""

import argparse
from database import MeshtasticDatabase


def main() -> int:
    parser = argparse.ArgumentParser(description="Run database schema migrations")
    parser.add_argument(
        "--version",
        action="store_true",
        help="Print the current schema version and exit",
    )
    args = parser.parse_args()

    db = MeshtasticDatabase()
    if args.version:
        version = db.get_schema_version()
        print(f"Schema version: {version}")
        return 0

    before = db.get_schema_version()
    after = db.run_migrations()
    if after != before:
        print(f"Migrated schema from v{before} to v{after}")
    else:
        print(f"Schema already up to date at v{after}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
