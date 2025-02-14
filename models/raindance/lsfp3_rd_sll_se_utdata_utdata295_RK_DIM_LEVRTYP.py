
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from models.raindance_src_to_raw import pipe


@model(
    columns={'LEVRTYP': 'varchar(max)', 'LEVRTYP_TEXT': 'varchar(max)'},
    cron="@daily"
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> pd.DataFrame:
    query = """SELECT CAST(LEVRTYP AS VARCHAR(MAX)) AS LEVRTYP,
CAST(LEVRTYP_TEXT AS VARCHAR(MAX)) AS LEVRTYP_TEXT FROM utdata.utdata295.RK_DIM_LEVRTYP"""
    return pipe(query=query)
