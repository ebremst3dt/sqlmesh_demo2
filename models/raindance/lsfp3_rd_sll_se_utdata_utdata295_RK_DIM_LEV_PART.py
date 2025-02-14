
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from models.raindance_src_to_raw import pipe


@model(
    columns={'PART': 'varchar(max)', 'SBID': 'varchar(max)'},
    cron="@daily"
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> pd.DataFrame:
    query = """SELECT CAST(PART AS VARCHAR(MAX)) AS PART,
CAST(SBID AS VARCHAR(MAX)) AS SBID FROM utdata.utdata295.RK_DIM_LEV_PART"""
    return pipe(query=query)
