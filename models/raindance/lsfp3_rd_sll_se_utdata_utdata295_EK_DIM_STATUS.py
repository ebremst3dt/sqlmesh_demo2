
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from models.raindance_src_to_raw import pipe


@model(
    columns={'STATUS': 'varchar(max)',
 'STATUSTYP': 'varchar(max)',
 'STATUSTYP_TEXT': 'varchar(max)',
 'STATUS_TEXT': 'varchar(max)'},
    cron="@daily"
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> pd.DataFrame:
    query = """SELECT CAST(STATUS AS VARCHAR(MAX)) AS STATUS,
CAST(STATUS_TEXT AS VARCHAR(MAX)) AS STATUS_TEXT,
CAST(STATUSTYP AS VARCHAR(MAX)) AS STATUSTYP,
CAST(STATUSTYP_TEXT AS VARCHAR(MAX)) AS STATUSTYP_TEXT FROM utdata.utdata295.EK_DIM_STATUS"""
    return pipe(query=query)
