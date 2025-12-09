"""Reset database by dropping schema and running migrations."""

import psycopg2
from drop import drop_and_recreate_schema
from migrate import run_all_migrations


def main():
    """Drop schema and run all migrations."""
    # First connection: drop schema
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

    # Second connection: run migrations (needed for TimescaleDB extension)
    connection = psycopg2.connect(
        host="127.0.0.1",
        port=5432,
        database="twr",
        user="twr_user",
        password="twr_password",
    )
    connection.autocommit = True

    try:
        run_all_migrations(connection)
    finally:
        connection.close()


if __name__ == "__main__":
    main()
