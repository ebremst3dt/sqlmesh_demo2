
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from models.raindance_src_to_raw import pipe


@model(
    columns={'BOTYP_GILTIG_FOM': 'varchar(max)',
 'BOTYP_GILTIG_TOM': 'varchar(max)',
 'BOTYP_ID': 'varchar(max)',
 'BOTYP_ID_TEXT': 'varchar(max)',
 'BOTYP_PASSIV': 'varchar(max)',
 'BOTYP_TEXT': 'varchar(max)'},
    cron="@daily"
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> pd.DataFrame:
    query = """SELECT CAST(BOTYP_GILTIG_FOM AS VARCHAR(MAX)) AS BOTYP_GILTIG_FOM,
CAST(BOTYP_GILTIG_TOM AS VARCHAR(MAX)) AS BOTYP_GILTIG_TOM,
CAST(BOTYP_ID AS VARCHAR(MAX)) AS BOTYP_ID,
CAST(BOTYP_ID_TEXT AS VARCHAR(MAX)) AS BOTYP_ID_TEXT,
CAST(BOTYP_PASSIV AS VARCHAR(MAX)) AS BOTYP_PASSIV,
CAST(BOTYP_TEXT AS VARCHAR(MAX)) AS BOTYP_TEXT FROM utdata.utdata295.EK_DIM_OBJ_BOTYP"""
    return pipe(query=query)
