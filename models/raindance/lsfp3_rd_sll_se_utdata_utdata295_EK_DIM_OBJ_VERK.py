
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from models.raindance_src_to_raw import pipe


@model(
    columns={'VERK_GILTIG_FOM': 'varchar(max)',
 'VERK_GILTIG_TOM': 'varchar(max)',
 'VERK_ID': 'varchar(max)',
 'VERK_ID_TEXT': 'varchar(max)',
 'VERK_PASSIV': 'varchar(max)',
 'VERK_TEXT': 'varchar(max)',
 'VGREN_GILTIG_FOM': 'varchar(max)',
 'VGREN_GILTIG_TOM': 'varchar(max)',
 'VGREN_ID': 'varchar(max)',
 'VGREN_ID_TEXT': 'varchar(max)',
 'VGREN_PASSIV': 'varchar(max)',
 'VGREN_TEXT': 'varchar(max)'},
    cron="@daily"
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> pd.DataFrame:
    query = """SELECT CAST(VERK_GILTIG_FOM AS VARCHAR(MAX)) AS VERK_GILTIG_FOM,
CAST(VERK_GILTIG_TOM AS VARCHAR(MAX)) AS VERK_GILTIG_TOM,
CAST(VERK_ID AS VARCHAR(MAX)) AS VERK_ID,
CAST(VERK_ID_TEXT AS VARCHAR(MAX)) AS VERK_ID_TEXT,
CAST(VERK_PASSIV AS VARCHAR(MAX)) AS VERK_PASSIV,
CAST(VERK_TEXT AS VARCHAR(MAX)) AS VERK_TEXT,
CAST(VGREN_GILTIG_FOM AS VARCHAR(MAX)) AS VGREN_GILTIG_FOM,
CAST(VGREN_GILTIG_TOM AS VARCHAR(MAX)) AS VGREN_GILTIG_TOM,
CAST(VGREN_ID AS VARCHAR(MAX)) AS VGREN_ID,
CAST(VGREN_ID_TEXT AS VARCHAR(MAX)) AS VGREN_ID_TEXT,
CAST(VGREN_PASSIV AS VARCHAR(MAX)) AS VGREN_PASSIV,
CAST(VGREN_TEXT AS VARCHAR(MAX)) AS VGREN_TEXT FROM utdata.utdata295.EK_DIM_OBJ_VERK"""
    return pipe(query=query)
