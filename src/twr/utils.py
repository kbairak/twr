import json
from contextlib import contextmanager
from typing import TypedDict

import psycopg2


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


class Granularity(TypedDict):
    suffix: str
    interval: str
    cache_retention: str | None


with open("migrations/granularities.json") as f:
    GRANULARITIES: list[Granularity] = json.load(f)
