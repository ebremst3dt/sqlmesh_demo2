
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from models.raindance_src_to_raw import pipe


@model(
    columns={'PROC_GILTIG_FOM': 'varchar(max)',
 'PROC_GILTIG_TOM': 'varchar(max)',
 'PROC_ID': 'varchar(max)',
 'PROC_ID_TEXT': 'varchar(max)',
 'PROC_PASSIV': 'varchar(max)',
 'PROC_TEXT': 'varchar(max)'},
    cron="@daily"
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> pd.DataFrame:
    query = """SELECT CAST(PROC_GILTIG_FOM AS VARCHAR(MAX)) AS PROC_GILTIG_FOM,
CAST(PROC_GILTIG_TOM AS VARCHAR(MAX)) AS PROC_GILTIG_TOM,
CAST(PROC_ID AS VARCHAR(MAX)) AS PROC_ID,
CAST(PROC_ID_TEXT AS VARCHAR(MAX)) AS PROC_ID_TEXT,
CAST(PROC_PASSIV AS VARCHAR(MAX)) AS PROC_PASSIV,
CAST(PROC_TEXT AS VARCHAR(MAX)) AS PROC_TEXT FROM utdata.utdata295.EK_DIM_OBJ_PROC"""
    return pipe(query=query)
