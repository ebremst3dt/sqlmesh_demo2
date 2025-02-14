
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from models.raindance_src_to_raw import pipe


@model(
    columns={'EXTERN': 'varchar(max)',
 'EXTERN_TEXT': 'varchar(max)',
 'RESKONTRA': 'varchar(max)',
 'RESKONTRA_TEXT': 'varchar(max)',
 'RESKTYP': 'varchar(max)',
 'RESKTYP_TEXT': 'varchar(max)'},
    cron="@daily"
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> pd.DataFrame:
    query = """SELECT CAST(EXTERN AS VARCHAR(MAX)) AS EXTERN,
CAST(EXTERN_TEXT AS VARCHAR(MAX)) AS EXTERN_TEXT,
CAST(RESKONTRA AS VARCHAR(MAX)) AS RESKONTRA,
CAST(RESKONTRA_TEXT AS VARCHAR(MAX)) AS RESKONTRA_TEXT,
CAST(RESKTYP AS VARCHAR(MAX)) AS RESKTYP,
CAST(RESKTYP_TEXT AS VARCHAR(MAX)) AS RESKTYP_TEXT FROM utdata.utdata295.RK_DIM_RESKONTRA"""
    return pipe(query=query)
