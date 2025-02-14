
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from models.raindance_src_to_raw import pipe


@model(
    columns={'MALLID_ID': 'varchar(max)',
 'RAK_ID': 'varchar(max)',
 'TEXT_TEXT': 'varchar(max)'},
    cron="@daily"
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> pd.DataFrame:
    query = """SELECT CAST(MALLID_ID AS VARCHAR(MAX)) AS MALLID_ID,
CAST(RAK_ID AS VARCHAR(MAX)) AS RAK_ID,
CAST(TEXT_TEXT AS VARCHAR(MAX)) AS TEXT_TEXT FROM utdata.utdata295.EK_FAKTA_VARDE_PFMALL$TEXT"""
    return pipe(query=query)
