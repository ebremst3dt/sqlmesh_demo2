
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from models.raindance_src_to_raw import pipe


@model(
    columns={'AVTBES_GILTIG_FOM': 'varchar(max)',
 'AVTBES_GILTIG_TOM': 'varchar(max)',
 'AVTBES_ID': 'varchar(max)',
 'AVTBES_ID_TEXT': 'varchar(max)',
 'AVTBES_PASSIV': 'varchar(max)',
 'AVTBES_TEXT': 'varchar(max)'},
    cron="@daily"
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> pd.DataFrame:
    query = """SELECT CAST(AVTBES_GILTIG_FOM AS VARCHAR(MAX)) AS AVTBES_GILTIG_FOM,
CAST(AVTBES_GILTIG_TOM AS VARCHAR(MAX)) AS AVTBES_GILTIG_TOM,
CAST(AVTBES_ID AS VARCHAR(MAX)) AS AVTBES_ID,
CAST(AVTBES_ID_TEXT AS VARCHAR(MAX)) AS AVTBES_ID_TEXT,
CAST(AVTBES_PASSIV AS VARCHAR(MAX)) AS AVTBES_PASSIV,
CAST(AVTBES_TEXT AS VARCHAR(MAX)) AS AVTBES_TEXT FROM utdata.utdata295.EK_DIM_OBJ_AVTBES"""
    return pipe(query=query)
