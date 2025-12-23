import asyncio
from pathlib import Path

import asyncpg
from jinja2 import Template

from performance.granularities import GRANULARITIES


async def run_all_migrations(connection: asyncpg.Connection) -> None:
    """Run all database migrations."""
    migrations_dir = Path(__file__).parent.parent.parent / "migrations"
    migration_files = sorted(
        [f for f in migrations_dir.iterdir() if f.suffix == ".sql" or f.name.endswith(".sql.j2")]
    )
    print(f"Found {len(migration_files)} migration files")
    print("=" * 60)
    for migration_file in migration_files:
        print(f"Running migration: {migration_file.name}")
        content = migration_file.read_text()
        if migration_file.suffix == ".j2":
            template = Template(content)
            content = template.render(GRANULARITIES=GRANULARITIES)
        await connection.execute(content)

    print("=" * 60)
    print("All migrations completed successfully!")


async def main() -> None:
    connection = await asyncpg.connect(
        host="127.0.0.1", database="twr", user="twr_user", password="twr_password"
    )
    try:
        await run_all_migrations(connection)
    finally:
        await connection.close()


if __name__ == "__main__":
    asyncio.run(main())
