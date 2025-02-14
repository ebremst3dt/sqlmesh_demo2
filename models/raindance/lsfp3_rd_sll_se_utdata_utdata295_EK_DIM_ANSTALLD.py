
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from models.raindance_src_to_raw import pipe


@model(
    columns={'ANSTALLD_GILTIG_FOM': 'varchar(max)',
 'ANSTALLD_GILTIG_TOM': 'varchar(max)',
 'ANSTALLD_ID': 'varchar(max)',
 'ANSTALLD_ID_TEXT': 'varchar(max)',
 'ANSTALLD_PASSIV': 'varchar(max)',
 'ANSTALLD_TEXT': 'varchar(max)'},
    cron="@daily"
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> pd.DataFrame:
    query = """SELECT CAST(ANSTALLD_GILTIG_FOM AS VARCHAR(MAX)) AS ANSTALLD_GILTIG_FOM,
CAST(ANSTALLD_GILTIG_TOM AS VARCHAR(MAX)) AS ANSTALLD_GILTIG_TOM,
CAST(ANSTALLD_ID AS VARCHAR(MAX)) AS ANSTALLD_ID,
CAST(ANSTALLD_ID_TEXT AS VARCHAR(MAX)) AS ANSTALLD_ID_TEXT,
CAST(ANSTALLD_PASSIV AS VARCHAR(MAX)) AS ANSTALLD_PASSIV,
CAST(ANSTALLD_TEXT AS VARCHAR(MAX)) AS ANSTALLD_TEXT FROM utdata.utdata295.EK_DIM_ANSTALLD"""
    return pipe(query=query)
