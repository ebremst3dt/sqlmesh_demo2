
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from models.raindance_src_to_raw import pipe


@model(
    columns={'ANSTFORM_ID': 'varchar(max)',
 'ANSTFORM_ID_TEXT': 'varchar(max)',
 'ANSTFORM_TEXT': 'varchar(max)'},
    cron="@daily"
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> pd.DataFrame:
    query = """SELECT CAST(ANSTFORM_ID AS VARCHAR(MAX)) AS ANSTFORM_ID,
CAST(ANSTFORM_ID_TEXT AS VARCHAR(MAX)) AS ANSTFORM_ID_TEXT,
CAST(ANSTFORM_TEXT AS VARCHAR(MAX)) AS ANSTFORM_TEXT FROM utdata.utdata295.EK_DIM_ANSTFORM"""
    return pipe(query=query)
