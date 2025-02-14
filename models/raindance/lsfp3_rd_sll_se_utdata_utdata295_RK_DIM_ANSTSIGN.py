
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from models.raindance_src_to_raw import pipe


@model(
    columns={'ANSTSIGN': 'varchar(max)',
 'ANSTSIGN2': 'varchar(max)',
 'ANSTSIGN2_ID_TEXT': 'varchar(max)',
 'ANSTSIGN_ID_TEXT': 'varchar(max)',
 'ANSTSIGN_TEXT': 'varchar(max)'},
    cron="@daily"
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> pd.DataFrame:
    query = """SELECT CAST(ANSTSIGN AS VARCHAR(MAX)) AS ANSTSIGN,
CAST(ANSTSIGN_ID_TEXT AS VARCHAR(MAX)) AS ANSTSIGN_ID_TEXT,
CAST(ANSTSIGN_TEXT AS VARCHAR(MAX)) AS ANSTSIGN_TEXT,
CAST(ANSTSIGN2 AS VARCHAR(MAX)) AS ANSTSIGN2,
CAST(ANSTSIGN2_ID_TEXT AS VARCHAR(MAX)) AS ANSTSIGN2_ID_TEXT FROM utdata.utdata295.RK_DIM_ANSTSIGN"""
    return pipe(query=query)
