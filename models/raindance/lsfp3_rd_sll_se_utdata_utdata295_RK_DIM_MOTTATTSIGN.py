
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from models.raindance_src_to_raw import pipe


@model(
    columns={'MOTTATTSIGN': 'varchar(max)',
 'MOTTATTSIGN2': 'varchar(max)',
 'MOTTATTSIGN2_ID_TEXT': 'varchar(max)',
 'MOTTATTSIGN_ID_TEXT': 'varchar(max)',
 'MOTTATTSIGN_TEXT': 'varchar(max)'},
    cron="@daily"
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> pd.DataFrame:
    query = """SELECT CAST(MOTTATTSIGN AS VARCHAR(MAX)) AS MOTTATTSIGN,
CAST(MOTTATTSIGN_ID_TEXT AS VARCHAR(MAX)) AS MOTTATTSIGN_ID_TEXT,
CAST(MOTTATTSIGN_TEXT AS VARCHAR(MAX)) AS MOTTATTSIGN_TEXT,
CAST(MOTTATTSIGN2 AS VARCHAR(MAX)) AS MOTTATTSIGN2,
CAST(MOTTATTSIGN2_ID_TEXT AS VARCHAR(MAX)) AS MOTTATTSIGN2_ID_TEXT FROM utdata.utdata295.RK_DIM_MOTTATTSIGN"""
    return pipe(query=query)
