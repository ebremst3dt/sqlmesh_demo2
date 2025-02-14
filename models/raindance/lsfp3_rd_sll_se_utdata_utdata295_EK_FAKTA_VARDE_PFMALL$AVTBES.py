
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from models.raindance_src_to_raw import pipe


@model(
    columns={'AVTBES_ID': 'varchar(max)',
 'MALLID_ID': 'varchar(max)',
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
    query = """SELECT CAST(AVTBES_ID AS VARCHAR(MAX)) AS AVTBES_ID,
CAST(MALLID_ID AS VARCHAR(MAX)) AS MALLID_ID,
CAST(RAK_ID AS VARCHAR(MAX)) AS RAK_ID FROM utdata.utdata295.EK_FAKTA_VARDE_PFMALL$AVTBES"""
    return pipe(query=query)
