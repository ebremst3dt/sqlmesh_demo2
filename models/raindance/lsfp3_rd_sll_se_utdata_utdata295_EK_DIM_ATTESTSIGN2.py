
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from models.raindance_src_to_raw import pipe


@model(
    columns={'ATTESTSIGN2': 'varchar(max)',
 'ATTESTSIGN22': 'varchar(max)',
 'ATTESTSIGN22_ID_TEXT': 'varchar(max)',
 'ATTESTSIGN2_ID_TEXT': 'varchar(max)',
 'ATTESTSIGN2_TEXT': 'varchar(max)'},
    cron="@daily"
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> pd.DataFrame:
    query = """SELECT CAST(ATTESTSIGN2 AS VARCHAR(MAX)) AS ATTESTSIGN2,
CAST(ATTESTSIGN2_ID_TEXT AS VARCHAR(MAX)) AS ATTESTSIGN2_ID_TEXT,
CAST(ATTESTSIGN2_TEXT AS VARCHAR(MAX)) AS ATTESTSIGN2_TEXT,
CAST(ATTESTSIGN22 AS VARCHAR(MAX)) AS ATTESTSIGN22,
CAST(ATTESTSIGN22_ID_TEXT AS VARCHAR(MAX)) AS ATTESTSIGN22_ID_TEXT FROM utdata.utdata295.EK_DIM_ATTESTSIGN2"""
    return pipe(query=query)
