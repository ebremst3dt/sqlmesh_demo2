
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from models.raindance_src_to_raw import pipe


@model(
    columns={'FRI1_GILTIG_FOM': 'varchar(max)',
 'FRI1_GILTIG_TOM': 'varchar(max)',
 'FRI1_ID': 'varchar(max)',
 'FRI1_ID_TEXT': 'varchar(max)',
 'FRI1_PASSIV': 'varchar(max)',
 'FRI1_TEXT': 'varchar(max)'},
    cron="@daily"
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> pd.DataFrame:
    query = """SELECT CAST(FRI1_GILTIG_FOM AS VARCHAR(MAX)) AS FRI1_GILTIG_FOM,
CAST(FRI1_GILTIG_TOM AS VARCHAR(MAX)) AS FRI1_GILTIG_TOM,
CAST(FRI1_ID AS VARCHAR(MAX)) AS FRI1_ID,
CAST(FRI1_ID_TEXT AS VARCHAR(MAX)) AS FRI1_ID_TEXT,
CAST(FRI1_PASSIV AS VARCHAR(MAX)) AS FRI1_PASSIV,
CAST(FRI1_TEXT AS VARCHAR(MAX)) AS FRI1_TEXT FROM utdata.utdata295.EK_DIM_OBJ_FRI1_DIR_28"""
    return pipe(query=query)
