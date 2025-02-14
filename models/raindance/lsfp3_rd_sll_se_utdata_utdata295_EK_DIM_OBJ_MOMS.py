
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from models.raindance_src_to_raw import pipe


@model(
    columns={'MOMS_GILTIG_FOM': 'varchar(max)',
 'MOMS_GILTIG_TOM': 'varchar(max)',
 'MOMS_ID': 'varchar(max)',
 'MOMS_ID_TEXT': 'varchar(max)',
 'MOMS_PASSIV': 'varchar(max)',
 'MOMS_TEXT': 'varchar(max)'},
    cron="@daily"
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> pd.DataFrame:
    query = """SELECT CAST(MOMS_GILTIG_FOM AS VARCHAR(MAX)) AS MOMS_GILTIG_FOM,
CAST(MOMS_GILTIG_TOM AS VARCHAR(MAX)) AS MOMS_GILTIG_TOM,
CAST(MOMS_ID AS VARCHAR(MAX)) AS MOMS_ID,
CAST(MOMS_ID_TEXT AS VARCHAR(MAX)) AS MOMS_ID_TEXT,
CAST(MOMS_PASSIV AS VARCHAR(MAX)) AS MOMS_PASSIV,
CAST(MOMS_TEXT AS VARCHAR(MAX)) AS MOMS_TEXT FROM utdata.utdata295.EK_DIM_OBJ_MOMS"""
    return pipe(query=query)
