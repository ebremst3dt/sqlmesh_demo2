import pymssql
import pandas as pd
from roskarl import env_var_dsn, DSN


def read(query: str, server_url: str):
    dsn: DSN = env_var_dsn(name=server_url)

    conn = pymssql.connect(
        server=dsn.hostname,
        user=dsn.username,
        password=dsn.password,
        database=dsn.database,
        charset="UTF-8"
    )

    try:
        if conn:
            df = pd.read_sql(query, conn)
            if not df.empty:
                yield df
            else:
                yield from ()
    finally:
        conn.close()