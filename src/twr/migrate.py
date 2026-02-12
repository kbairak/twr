#!/usr/bin/env python3
"""Migration runner for TWR schema."""

import itertools
import sys
from pathlib import Path
from jinja2 import Template

from twr.utils import GRANULARITIES, Granularity, connection


def _run_migration(connection, migration_file: Path, granularities: list[Granularity]) -> None:
    """Run a single migration file."""

    content = migration_file.read_text()
    if migration_file.suffix == ".j2":
        template = Template(content)
        content = template.render(GRANULARITIES=granularities, itertools=itertools)

    # Execute migration
    with connection.cursor() as cur:
        try:
            cur.execute(content)
            connection.commit()
        except Exception as e:
            connection.rollback()
            print(f"  ✗ {migration_file.name} failed: {e}")
            raise


def run_all_migrations(connection):
    """Run all migrations in order."""
    migrations_dir = Path(__file__).parent.parent.parent / "migrations"

    # Get all migration files (both .sql and .sql.j2)
    migration_files = sorted(
        [f for f in migrations_dir.iterdir() if f.suffix == ".sql" or f.name.endswith(".sql.j2")]
    )

    for migration_file in migration_files:
        _run_migration(connection, migration_file, GRANULARITIES)


def main():
    """Main entry point for running migrations from command line."""
    with connection() as conn:
        try:
            run_all_migrations(conn)
        except Exception as e:
            print(f"\nMigration failed: {e}")
            sys.exit(1)


if __name__ == "__main__":
    main()
