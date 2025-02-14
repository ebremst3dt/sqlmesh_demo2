
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from models.raindance_src_to_raw import pipe


@model(
    columns={'BOPER_GILTIG_FOM': 'varchar(max)',
 'BOPER_GILTIG_TOM': 'varchar(max)',
 'BOPER_ID': 'varchar(max)',
 'BOPER_ID_TEXT': 'varchar(max)',
 'BOPER_PASSIV': 'varchar(max)',
 'BOPER_TEXT': 'varchar(max)'},
    cron="@daily"
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> pd.DataFrame:
    query = """SELECT CAST(BOPER_GILTIG_FOM AS VARCHAR(MAX)) AS BOPER_GILTIG_FOM,
CAST(BOPER_GILTIG_TOM AS VARCHAR(MAX)) AS BOPER_GILTIG_TOM,
CAST(BOPER_ID AS VARCHAR(MAX)) AS BOPER_ID,
CAST(BOPER_ID_TEXT AS VARCHAR(MAX)) AS BOPER_ID_TEXT,
CAST(BOPER_PASSIV AS VARCHAR(MAX)) AS BOPER_PASSIV,
CAST(BOPER_TEXT AS VARCHAR(MAX)) AS BOPER_TEXT FROM utdata.utdata295.EK_DIM_OBJ_BOPER"""
    return pipe(query=query)
