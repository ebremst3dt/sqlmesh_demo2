
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from models.raindance_src_to_raw import pipe


@model(
    columns={'FAKTSTATUS': 'varchar(max)',
 'FAKTSTATUSTYP': 'varchar(max)',
 'FAKTSTATUSTYP_TEXT': 'varchar(max)',
 'FAKTSTATUS_TEXT': 'varchar(max)'},
    cron="@daily"
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> pd.DataFrame:
    query = """SELECT CAST(FAKTSTATUS AS VARCHAR(MAX)) AS FAKTSTATUS,
CAST(FAKTSTATUS_TEXT AS VARCHAR(MAX)) AS FAKTSTATUS_TEXT,
CAST(FAKTSTATUSTYP AS VARCHAR(MAX)) AS FAKTSTATUSTYP,
CAST(FAKTSTATUSTYP_TEXT AS VARCHAR(MAX)) AS FAKTSTATUSTYP_TEXT FROM utdata.utdata295.RK_DIM_STATUS"""
    return pipe(query=query)
