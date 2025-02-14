
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from models.raindance_src_to_raw import pipe


@model(
    columns={'FRI2_GILTIG_FOM': 'varchar(max)',
 'FRI2_GILTIG_TOM': 'varchar(max)',
 'FRI2_ID': 'varchar(max)',
 'FRI2_ID_TEXT': 'varchar(max)',
 'FRI2_PASSIV': 'varchar(max)',
 'FRI2_TEXT': 'varchar(max)'},
    cron="@daily"
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> pd.DataFrame:
    query = """SELECT CAST(FRI2_GILTIG_FOM AS VARCHAR(MAX)) AS FRI2_GILTIG_FOM,
CAST(FRI2_GILTIG_TOM AS VARCHAR(MAX)) AS FRI2_GILTIG_TOM,
CAST(FRI2_ID AS VARCHAR(MAX)) AS FRI2_ID,
CAST(FRI2_ID_TEXT AS VARCHAR(MAX)) AS FRI2_ID_TEXT,
CAST(FRI2_PASSIV AS VARCHAR(MAX)) AS FRI2_PASSIV,
CAST(FRI2_TEXT AS VARCHAR(MAX)) AS FRI2_TEXT FROM utdata.utdata295.EK_DIM_OBJ_FRI2_DIR_28"""
    return pipe(query=query)
