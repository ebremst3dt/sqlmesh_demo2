
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from models.raindance_src_to_raw import pipe


@model(
    columns={'DEFANL_GILTIG_FOM': 'varchar(max)',
 'DEFANL_GILTIG_TOM': 'varchar(max)',
 'DEFANL_ID': 'varchar(max)',
 'DEFANL_ID_TEXT': 'varchar(max)',
 'DEFANL_PASSIV': 'varchar(max)',
 'DEFANL_TEXT': 'varchar(max)'},
    cron="@daily"
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> pd.DataFrame:
    query = """SELECT CAST(DEFANL_GILTIG_FOM AS VARCHAR(MAX)) AS DEFANL_GILTIG_FOM,
CAST(DEFANL_GILTIG_TOM AS VARCHAR(MAX)) AS DEFANL_GILTIG_TOM,
CAST(DEFANL_ID AS VARCHAR(MAX)) AS DEFANL_ID,
CAST(DEFANL_ID_TEXT AS VARCHAR(MAX)) AS DEFANL_ID_TEXT,
CAST(DEFANL_PASSIV AS VARCHAR(MAX)) AS DEFANL_PASSIV,
CAST(DEFANL_TEXT AS VARCHAR(MAX)) AS DEFANL_TEXT FROM utdata.utdata295.EK_DIM_OBJ_DEFANL"""
    return pipe(query=query)
