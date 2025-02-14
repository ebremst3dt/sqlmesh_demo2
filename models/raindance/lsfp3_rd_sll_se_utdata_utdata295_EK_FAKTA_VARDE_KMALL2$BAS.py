
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from models.raindance_src_to_raw import pipe


@model(
    columns={'ANVID_ID': 'varchar(max)',
 'BAS_V': 'varchar(max)',
 'KMALL_ID': 'varchar(max)',
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
CAST(BAS_V AS VARCHAR(MAX)) AS BAS_V,
CAST(KMALL_ID AS VARCHAR(MAX)) AS KMALL_ID,
CAST(RAK_ID AS VARCHAR(MAX)) AS RAK_ID FROM utdata.utdata295.EK_FAKTA_VARDE_KMALL2$BAS"""
    return pipe(query=query)
