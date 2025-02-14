
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from models.raindance_src_to_raw import pipe


@model(
    columns={'IB': 'varchar(max)', 'IB_TEXT': 'varchar(max)'},
    cron="@daily"
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> pd.DataFrame:
    query = """SELECT CAST(IB AS VARCHAR(MAX)) AS IB,
CAST(IB_TEXT AS VARCHAR(MAX)) AS IB_TEXT FROM utdata.utdata295.EK_DIM_IB"""
    return pipe(query=query)
