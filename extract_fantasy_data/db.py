from contextlib import contextmanager
from typing import Iterator

import polars as pl
import psycopg


@contextmanager
def connect(database_url: str) -> Iterator[psycopg.Connection]:
    with psycopg.connect(database_url) as conn:
        yield conn


def fetch_df(conn: psycopg.Connection, sql: str) -> pl.DataFrame:
    with conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()
        columns = [desc.name for desc in cur.description] if cur.description else []
    return pl.DataFrame(rows, schema=columns, orient="row")
