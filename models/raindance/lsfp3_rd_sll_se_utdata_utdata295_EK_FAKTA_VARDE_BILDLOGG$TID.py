
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from models.raindance_src_to_raw import pipe


@model(
    columns={'LOPNUMMER': 'varchar(max)',
 'TID_V': 'varchar(max)',
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
    query = """SELECT CAST(LOPNUMMER AS VARCHAR(MAX)) AS LOPNUMMER,
CAST(TID_V AS VARCHAR(MAX)) AS TID_V,
CAST(VERDATUM AS VARCHAR(MAX)) AS VERDATUM FROM utdata.utdata295.EK_FAKTA_VARDE_BILDLOGG$TID"""
    return pipe(query=query)
