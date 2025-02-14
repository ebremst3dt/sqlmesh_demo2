import pymssql
import pandas as pd
from dotenv import load_dotenv
import os
from dsn import parse_dsn, DSN


def get_database_credentials(server_url: str) -> DSN:
    load_dotenv()
    database_url = os.getenv(server_url)
    if not database_url:
        raise ValueError("SERVER_URL not found in environment variables")

    return parse_dsn(database_url)


def read(query: str, server_url: str):
    dsn = get_database_credentials(server_url=server_url)

    conn = pymssql.connect(
        server=dsn.hostname,
        user=dsn.username,
        password=dsn.password,
        database=dsn.database
    )

    try:
        if conn:
            return pd.read_sql(query, conn)

    finally:
        conn.close()
