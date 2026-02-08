from contextlib import contextmanager
import json
import pathlib
from typing import TypedDict

import psycopg2


def get_conn():
    return psycopg2.connect(
        dbname="twr", user="twr_user", password="twr_password", host="localhost", port=5432
    )


@contextmanager
def connection():
    conn = psycopg2.connect(
        dbname="twr", user="twr_user", password="twr_password", host="localhost", port=5432
    )
    conn.autocommit = True
    try:
        yield conn
    finally:
        conn.close()


migrations_dir = pathlib.Path(__file__).parent.parent.parent / "migrations"
granularities_file = migrations_dir / "granularities.json"


class Granularity(TypedDict):
    suffix: str
    interval: str
    cache_retention: str | None


try:
    with open(granularities_file) as f:
        GRANULARITIES: list[Granularity] = json.load(f)
except (FileNotFoundError, json.JSONDecodeError):
    GRANULARITIES: list[Granularity] = []
