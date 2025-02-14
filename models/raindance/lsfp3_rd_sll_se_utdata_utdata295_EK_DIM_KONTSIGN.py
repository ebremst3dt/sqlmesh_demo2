
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from models.raindance_src_to_raw import pipe


@model(
    columns={'KONTSIGN': 'varchar(max)',
 'KONTSIGN2': 'varchar(max)',
 'KONTSIGN2_ID_TEXT': 'varchar(max)',
 'KONTSIGN_ID_TEXT': 'varchar(max)',
 'KONTSIGN_TEXT': 'varchar(max)'},
    cron="@daily"
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> pd.DataFrame:
    query = """SELECT CAST(KONTSIGN AS VARCHAR(MAX)) AS KONTSIGN,
CAST(KONTSIGN_ID_TEXT AS VARCHAR(MAX)) AS KONTSIGN_ID_TEXT,
CAST(KONTSIGN_TEXT AS VARCHAR(MAX)) AS KONTSIGN_TEXT,
CAST(KONTSIGN2 AS VARCHAR(MAX)) AS KONTSIGN2,
CAST(KONTSIGN2_ID_TEXT AS VARCHAR(MAX)) AS KONTSIGN2_ID_TEXT FROM utdata.utdata295.EK_DIM_KONTSIGN"""
    return pipe(query=query)
