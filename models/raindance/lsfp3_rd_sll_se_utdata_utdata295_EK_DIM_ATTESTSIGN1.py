
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from models.raindance_src_to_raw import pipe


@model(
    columns={'ATTESTSIGN1': 'varchar(max)',
 'ATTESTSIGN12': 'varchar(max)',
 'ATTESTSIGN12_ID_TEXT': 'varchar(max)',
 'ATTESTSIGN1_ID_TEXT': 'varchar(max)',
 'ATTESTSIGN1_TEXT': 'varchar(max)'},
    cron="@daily"
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> pd.DataFrame:
    query = """SELECT CAST(ATTESTSIGN1 AS VARCHAR(MAX)) AS ATTESTSIGN1,
CAST(ATTESTSIGN1_ID_TEXT AS VARCHAR(MAX)) AS ATTESTSIGN1_ID_TEXT,
CAST(ATTESTSIGN1_TEXT AS VARCHAR(MAX)) AS ATTESTSIGN1_TEXT,
CAST(ATTESTSIGN12 AS VARCHAR(MAX)) AS ATTESTSIGN12,
CAST(ATTESTSIGN12_ID_TEXT AS VARCHAR(MAX)) AS ATTESTSIGN12_ID_TEXT FROM utdata.utdata295.EK_DIM_ATTESTSIGN1"""
    return pipe(query=query)
