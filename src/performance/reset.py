import asyncio

import asyncpg

from performance.drop import drop
from performance.migrate import run_all_migrations


async def reset() -> None:
    """Drop the database schema and run all migrations (re-initializing connection between steps)."""
    # First, drop the schema
    connection = await asyncpg.connect(
        host="127.0.0.1", database="twr", user="twr_user", password="twr_password"
    )
    try:
        await drop(connection)
    finally:
        await connection.close()

    # Then, reconnect and run migrations (TimescaleDB extension needs fresh connection)
    connection = await asyncpg.connect(
        host="127.0.0.1", database="twr", user="twr_user", password="twr_password"
    )
    try:
        await run_all_migrations(connection)
    finally:
        await connection.close()


async def main() -> None:
    await reset()


if __name__ == "__main__":
    asyncio.run(main())
