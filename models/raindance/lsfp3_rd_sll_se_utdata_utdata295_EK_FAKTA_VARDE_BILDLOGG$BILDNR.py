
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from models.raindance_src_to_raw import pipe


@model(
    columns={'BILDNR_TEXT': 'varchar(max)',
 'LOPNUMMER': 'varchar(max)',
 'VERDATUM': 'varchar(max)'},
    cron="@daily"
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> pd.DataFrame:
    query = """SELECT CAST(BILDNR_TEXT AS VARCHAR(MAX)) AS BILDNR_TEXT,
CAST(LOPNUMMER AS VARCHAR(MAX)) AS LOPNUMMER,
CAST(VERDATUM AS VARCHAR(MAX)) AS VERDATUM FROM utdata.utdata295.EK_FAKTA_VARDE_BILDLOGG$BILDNR"""
    return pipe(query=query)
