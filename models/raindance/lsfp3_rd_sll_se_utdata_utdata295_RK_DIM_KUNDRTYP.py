
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from models.raindance_src_to_raw import pipe


@model(
    columns={'KUNDRTYP': 'varchar(max)', 'KUNDRTYP_TEXT': 'varchar(max)'},
    cron="@daily"
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> pd.DataFrame:
    query = """SELECT CAST(KUNDRTYP AS VARCHAR(MAX)) AS KUNDRTYP,
CAST(KUNDRTYP_TEXT AS VARCHAR(MAX)) AS KUNDRTYP_TEXT FROM utdata.utdata295.RK_DIM_KUNDRTYP"""
    return pipe(query=query)
