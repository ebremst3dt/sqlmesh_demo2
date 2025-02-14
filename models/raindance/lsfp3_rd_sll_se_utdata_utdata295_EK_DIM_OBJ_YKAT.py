
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from models.raindance_src_to_raw import pipe


@model(
    columns={'YKAT_GILTIG_FOM': 'varchar(max)',
 'YKAT_GILTIG_TOM': 'varchar(max)',
 'YKAT_ID': 'varchar(max)',
 'YKAT_ID_TEXT': 'varchar(max)',
 'YKAT_PASSIV': 'varchar(max)',
 'YKAT_TEXT': 'varchar(max)'},
    cron="@daily"
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> pd.DataFrame:
    query = """SELECT CAST(YKAT_GILTIG_FOM AS VARCHAR(MAX)) AS YKAT_GILTIG_FOM,
CAST(YKAT_GILTIG_TOM AS VARCHAR(MAX)) AS YKAT_GILTIG_TOM,
CAST(YKAT_ID AS VARCHAR(MAX)) AS YKAT_ID,
CAST(YKAT_ID_TEXT AS VARCHAR(MAX)) AS YKAT_ID_TEXT,
CAST(YKAT_PASSIV AS VARCHAR(MAX)) AS YKAT_PASSIV,
CAST(YKAT_TEXT AS VARCHAR(MAX)) AS YKAT_TEXT FROM utdata.utdata295.EK_DIM_OBJ_YKAT"""
    return pipe(query=query)
