
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from models.raindance_src_to_raw import pipe


@model(
    columns={'AR': 'varchar(max)',
 'INTERNVERNR': 'varchar(max)',
 'INTERNVERNR_TEXT': 'varchar(max)',
 'VERDATUM': 'varchar(max)',
 'VERNRGRUPP': 'varchar(max)'},
    cron="@daily"
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> pd.DataFrame:
    query = """SELECT CAST(AR AS VARCHAR(MAX)) AS AR,
CAST(INTERNVERNR AS VARCHAR(MAX)) AS INTERNVERNR,
CAST(INTERNVERNR_TEXT AS VARCHAR(MAX)) AS INTERNVERNR_TEXT,
CAST(VERDATUM AS VARCHAR(MAX)) AS VERDATUM,
CAST(VERNRGRUPP AS VARCHAR(MAX)) AS VERNRGRUPP FROM utdata.utdata295.EK_DIM_VERNR"""
    return pipe(query=query)
