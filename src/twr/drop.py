"""Drop and recreate the database schema."""

import psycopg2
from psycopg2.extensions import connection as Connection


def drop_and_recreate_schema(connection: Connection) -> None:
    """Drop and recreate the public schema."""
    with connection.cursor() as cursor:
        cursor.execute("DROP SCHEMA public CASCADE")
        cursor.execute("CREATE SCHEMA public")


def main() -> None:
    """Main entry point for running drop from command line."""
    connection = psycopg2.connect(
        host="127.0.0.1",
        port=5432,
        database="twr",
        user="twr_user",
        password="twr_password",
    )
    connection.autocommit = True

    try:
        drop_and_recreate_schema(connection)
    finally:
        connection.close()


if __name__ == "__main__":
    main()
