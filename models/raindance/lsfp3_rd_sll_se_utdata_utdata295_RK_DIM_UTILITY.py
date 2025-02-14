
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from models.raindance_src_to_raw import pipe


@model(
    columns={'UTILITY': 'varchar(max)', 'UTILITY_TEXT': 'varchar(max)'},
    cron="@daily"
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> pd.DataFrame:
    query = """SELECT CAST(UTILITY AS VARCHAR(MAX)) AS UTILITY,
CAST(UTILITY_TEXT AS VARCHAR(MAX)) AS UTILITY_TEXT FROM utdata.utdata295.RK_DIM_UTILITY"""
    return pipe(query=query)
