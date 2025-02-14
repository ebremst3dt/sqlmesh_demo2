
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from models.raindance_src_to_raw import pipe


@model(
    columns={'DUMMY2': 'varchar(max)',
 'TAB_REF': 'varchar(max)',
 'TAB_REF_ID_TEXT': 'varchar(max)',
 'TAB_REF_TEXT': 'varchar(max)'},
    cron="@daily"
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> pd.DataFrame:
    query = """SELECT CAST(DUMMY2 AS VARCHAR(MAX)) AS DUMMY2,
CAST(TAB_REF AS VARCHAR(MAX)) AS TAB_REF,
CAST(TAB_REF_ID_TEXT AS VARCHAR(MAX)) AS TAB_REF_ID_TEXT,
CAST(TAB_REF_TEXT AS VARCHAR(MAX)) AS TAB_REF_TEXT FROM utdata.utdata295.RK_DIM_TAB_REF"""
    return pipe(query=query)
