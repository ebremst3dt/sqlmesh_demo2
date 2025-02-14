
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from models.raindance_src_to_raw import pipe


@model(
    columns={'LONEART_GILTIG_FOM': 'varchar(max)',
 'LONEART_GILTIG_TOM': 'varchar(max)',
 'LONEART_ID': 'varchar(max)',
 'LONEART_ID_TEXT': 'varchar(max)',
 'LONEART_PASSIV': 'varchar(max)',
 'LONEART_TEXT': 'varchar(max)'},
    cron="@daily"
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> pd.DataFrame:
    query = """SELECT CAST(LONEART_GILTIG_FOM AS VARCHAR(MAX)) AS LONEART_GILTIG_FOM,
CAST(LONEART_GILTIG_TOM AS VARCHAR(MAX)) AS LONEART_GILTIG_TOM,
CAST(LONEART_ID AS VARCHAR(MAX)) AS LONEART_ID,
CAST(LONEART_ID_TEXT AS VARCHAR(MAX)) AS LONEART_ID_TEXT,
CAST(LONEART_PASSIV AS VARCHAR(MAX)) AS LONEART_PASSIV,
CAST(LONEART_TEXT AS VARCHAR(MAX)) AS LONEART_TEXT FROM utdata.utdata295.EK_DIM_LONEART"""
    return pipe(query=query)
