#!/usr/bin/env python3
"""Migration runner for TWR schema."""

import json
import sys
from pathlib import Path
import psycopg2
from jinja2 import Environment, FileSystemLoader


def _run_migration(connection, migration_file: Path, granularities: dict) -> None:
    """Run a single migration file."""
    print(f"Running migration: {migration_file.name}")

    # Read migration content
    content = migration_file.read_text()

    # If it's a Jinja template, render it
    if migration_file.suffix == ".j2":
        # Create environment with loader to support imports
        env = Environment(loader=FileSystemLoader(migration_file.parent))
        template = env.get_template(migration_file.name)
        content = template.render(GRANULARITIES=granularities)

    # Execute migration
    with connection.cursor() as cur:
        try:
            cur.execute(content)
            connection.commit()
            print(f"  ✓ {migration_file.name} completed")
        except Exception as e:
            connection.rollback()
            print(f"  ✗ {migration_file.name} failed: {e}")
            raise


def run_all_migrations(connection):
    """Run all migrations in order."""
    migrations_dir = Path(__file__).parent.parent.parent / "migrations"

    # Load granularities from JSON
    with open(migrations_dir / "granularities.json") as f:
        granularities = json.load(f)

    # Get all migration files (both .sql and .sql.j2)
    migration_files = sorted(
        [f for f in migrations_dir.iterdir() if f.suffix == ".sql" or f.name.endswith(".sql.j2")]
    )

    print(f"Found {len(migration_files)} migration files")
    print("=" * 60)

    for migration_file in migration_files:
        _run_migration(connection, migration_file, granularities)

    print("=" * 60)
    print("All migrations completed successfully!")


def main():
    """Main entry point for running migrations from command line."""
    connection = psycopg2.connect(
        host="127.0.0.1", database="twr", user="twr_user", password="twr_password"
    )

    try:
        run_all_migrations(connection)
    except Exception as e:
        print(f"\nMigration failed: {e}")
        sys.exit(1)
    finally:
        connection.close()


if __name__ == "__main__":
    main()
