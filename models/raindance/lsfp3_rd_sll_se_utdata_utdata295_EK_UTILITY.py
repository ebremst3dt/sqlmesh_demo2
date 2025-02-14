
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from models.raindance_src_to_raw import pipe


@model(
    columns={'D2': 'varchar(max)',
 'D3': 'varchar(max)',
 'DD3': 'varchar(max)',
 'ID': 'varchar(max)',
 'S': 'varchar(max)',
 'S1': 'varchar(max)',
 'S2': 'varchar(max)',
 'S3': 'varchar(max)',
 'ZEROS': 'varchar(max)'},
    cron="@daily"
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> pd.DataFrame:
    query = """SELECT CAST(D2 AS VARCHAR(MAX)) AS D2,
CAST(D3 AS VARCHAR(MAX)) AS D3,
CAST(DD3 AS VARCHAR(MAX)) AS DD3,
CAST(ID AS VARCHAR(MAX)) AS ID,
CAST(S AS VARCHAR(MAX)) AS S,
CAST(S1 AS VARCHAR(MAX)) AS S1,
CAST(S2 AS VARCHAR(MAX)) AS S2,
CAST(S3 AS VARCHAR(MAX)) AS S3,
CAST(ZEROS AS VARCHAR(MAX)) AS ZEROS FROM utdata.utdata295.EK_UTILITY"""
    return pipe(query=query)
