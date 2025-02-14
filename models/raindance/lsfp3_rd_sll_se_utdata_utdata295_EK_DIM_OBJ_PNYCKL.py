
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from models.raindance_src_to_raw import pipe


@model(
    columns={'PNYCKL_GILTIG_FOM': 'varchar(max)',
 'PNYCKL_GILTIG_TOM': 'varchar(max)',
 'PNYCKL_ID': 'varchar(max)',
 'PNYCKL_ID_TEXT': 'varchar(max)',
 'PNYCKL_PASSIV': 'varchar(max)',
 'PNYCKL_TEXT': 'varchar(max)'},
    cron="@daily"
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> pd.DataFrame:
    query = """SELECT CAST(PNYCKL_GILTIG_FOM AS VARCHAR(MAX)) AS PNYCKL_GILTIG_FOM,
CAST(PNYCKL_GILTIG_TOM AS VARCHAR(MAX)) AS PNYCKL_GILTIG_TOM,
CAST(PNYCKL_ID AS VARCHAR(MAX)) AS PNYCKL_ID,
CAST(PNYCKL_ID_TEXT AS VARCHAR(MAX)) AS PNYCKL_ID_TEXT,
CAST(PNYCKL_PASSIV AS VARCHAR(MAX)) AS PNYCKL_PASSIV,
CAST(PNYCKL_TEXT AS VARCHAR(MAX)) AS PNYCKL_TEXT FROM utdata.utdata295.EK_DIM_OBJ_PNYCKL"""
    return pipe(query=query)
