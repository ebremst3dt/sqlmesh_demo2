
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from models.raindance_src_to_raw import pipe


@model(
    columns={'RANTEDEB': 'varchar(max)', 'RANTEDEB_TEXT': 'varchar(max)'},
    cron="@daily"
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> pd.DataFrame:
    query = """SELECT CAST(RANTEDEB AS VARCHAR(MAX)) AS RANTEDEB,
CAST(RANTEDEB_TEXT AS VARCHAR(MAX)) AS RANTEDEB_TEXT FROM utdata.utdata295.RK_DIM_RANTEDEB"""
    return pipe(query=query)
