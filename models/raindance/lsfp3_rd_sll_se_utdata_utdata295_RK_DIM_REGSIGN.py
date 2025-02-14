
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from models.raindance_src_to_raw import pipe


@model(
    columns={'REGSIGN': 'varchar(max)',
 'REGSIGN2': 'varchar(max)',
 'REGSIGN2_ID_TEXT': 'varchar(max)',
 'REGSIGN_ID_TEXT': 'varchar(max)',
 'REGSIGN_TEXT': 'varchar(max)'},
    cron="@daily"
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> pd.DataFrame:
    query = """SELECT CAST(REGSIGN AS VARCHAR(MAX)) AS REGSIGN,
CAST(REGSIGN_ID_TEXT AS VARCHAR(MAX)) AS REGSIGN_ID_TEXT,
CAST(REGSIGN_TEXT AS VARCHAR(MAX)) AS REGSIGN_TEXT,
CAST(REGSIGN2 AS VARCHAR(MAX)) AS REGSIGN2,
CAST(REGSIGN2_ID_TEXT AS VARCHAR(MAX)) AS REGSIGN2_ID_TEXT FROM utdata.utdata295.RK_DIM_REGSIGN"""
    return pipe(query=query)
