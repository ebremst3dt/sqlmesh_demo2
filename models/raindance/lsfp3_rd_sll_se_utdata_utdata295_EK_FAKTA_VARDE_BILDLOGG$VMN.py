
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from models.raindance_src_to_raw import pipe


@model(
    columns={'LOPNUMMER': 'varchar(max)',
 'VERDATUM': 'varchar(max)',
 'VMN_TEXT': 'varchar(max)'},
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
CAST(VERDATUM AS VARCHAR(MAX)) AS VERDATUM,
CAST(VMN_TEXT AS VARCHAR(MAX)) AS VMN_TEXT FROM utdata.utdata295.EK_FAKTA_VARDE_BILDLOGG$VMN"""
    return pipe(query=query)
