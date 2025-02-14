from pandas import DataFrame
from aftonfalk.mssql import MssqlDriver

def pipe(query: str) -> DataFrame:
    source_dsn="mssql://sa:Password1!@localhost:31450"

    source_driver = MssqlDriver(
        dsn=source_dsn, driver=r"{ODBC Driver 18 for SQL Server}"
    )

    data = source_driver.read(query=query)

    return DataFrame(data)
