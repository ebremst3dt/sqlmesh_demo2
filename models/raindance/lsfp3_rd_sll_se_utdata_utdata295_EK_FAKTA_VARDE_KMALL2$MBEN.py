
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from models.raindance_src_to_raw import pipe


@model(
    columns={'ANVID_ID': 'varchar(max)',
 'KMALL_ID': 'varchar(max)',
 'MBEN_TEXT': 'varchar(max)',
 'RAK_ID': 'varchar(max)'},
    cron="@daily"
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> pd.DataFrame:
    query = """SELECT CAST(ANVID_ID AS VARCHAR(MAX)) AS ANVID_ID,
CAST(KMALL_ID AS VARCHAR(MAX)) AS KMALL_ID,
CAST(MBEN_TEXT AS VARCHAR(MAX)) AS MBEN_TEXT,
CAST(RAK_ID AS VARCHAR(MAX)) AS RAK_ID FROM utdata.utdata295.EK_FAKTA_VARDE_KMALL2$MBEN"""
    return pipe(query=query)
