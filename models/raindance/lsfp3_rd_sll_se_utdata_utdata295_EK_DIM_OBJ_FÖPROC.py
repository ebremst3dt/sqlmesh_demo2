
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from models.raindance_src_to_raw import pipe


@model(
    columns={'FÖPROC_GILTIG_FOM': 'varchar(max)',
 'FÖPROC_GILTIG_TOM': 'varchar(max)',
 'FÖPROC_ID': 'varchar(max)',
 'FÖPROC_ID_TEXT': 'varchar(max)',
 'FÖPROC_PASSIV': 'varchar(max)',
 'FÖPROC_TEXT': 'varchar(max)'},
    cron="@daily"
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> pd.DataFrame:
    query = """SELECT CAST(FÖPROC_GILTIG_FOM AS VARCHAR(MAX)) AS FÖPROC_GILTIG_FOM,
CAST(FÖPROC_GILTIG_TOM AS VARCHAR(MAX)) AS FÖPROC_GILTIG_TOM,
CAST(FÖPROC_ID AS VARCHAR(MAX)) AS FÖPROC_ID,
CAST(FÖPROC_ID_TEXT AS VARCHAR(MAX)) AS FÖPROC_ID_TEXT,
CAST(FÖPROC_PASSIV AS VARCHAR(MAX)) AS FÖPROC_PASSIV,
CAST(FÖPROC_TEXT AS VARCHAR(MAX)) AS FÖPROC_TEXT FROM utdata.utdata295.EK_DIM_OBJ_FÖPROC"""
    return pipe(query=query)
