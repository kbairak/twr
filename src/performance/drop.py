import asyncio

import asyncpg


async def drop(connection: asyncpg.Connection):
    """Drop and recreate the public schema."""
    print("Dropping schema...")
    await connection.execute("DROP SCHEMA public CASCADE")
    print("Creating schema...")
    await connection.execute("CREATE SCHEMA public")
    print("✓ Database schema reset complete")


async def main():
    connection = await asyncpg.connect(
        host="127.0.0.1", database="twr", user="twr_user", password="twr_password"
    )
    try:
        await drop(connection)
    finally:
        await connection.close()


if __name__ == "__main__":
    asyncio.run(main())
