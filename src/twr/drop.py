"""Drop and recreate the database schema."""

import psycopg2


def drop_and_recreate_schema(connection):
    """Drop and recreate the public schema."""
    with connection.cursor() as cursor:
        print("Dropping schema...")
        cursor.execute("DROP SCHEMA public CASCADE")
        print("Creating schema...")
        cursor.execute("CREATE SCHEMA public")
        print("✓ Database schema reset complete")


def main():
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
