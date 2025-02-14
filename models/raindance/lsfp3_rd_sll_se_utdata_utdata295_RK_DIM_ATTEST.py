
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from models.raindance_src_to_raw import pipe


@model(
    columns={'ATTEST': 'varchar(max)', 'ATTEST_TEXT': 'varchar(max)'},
    cron="@daily"
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> pd.DataFrame:
    query = """SELECT CAST(ATTEST AS VARCHAR(MAX)) AS ATTEST,
CAST(ATTEST_TEXT AS VARCHAR(MAX)) AS ATTEST_TEXT FROM utdata.utdata295.RK_DIM_ATTEST"""
    return pipe(query=query)
