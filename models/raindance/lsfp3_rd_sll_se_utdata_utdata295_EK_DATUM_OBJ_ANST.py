
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from models.raindance_src_to_raw import pipe


@model(
    columns={'ANST_DATUM_FOM': 'varchar(max)',
 'ANST_DATUM_TOM': 'varchar(max)',
 'ANST_GILTIG_FOM': 'varchar(max)',
 'ANST_GILTIG_TOM': 'varchar(max)',
 'ANST_ID': 'varchar(max)',
 'ANST_ID_TEXT': 'varchar(max)',
 'ANST_PASSIV': 'varchar(max)',
 'ANST_TEXT': 'varchar(max)'},
    cron="@daily"
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> pd.DataFrame:
    query = """SELECT CAST(ANST_DATUM_FOM AS VARCHAR(MAX)) AS ANST_DATUM_FOM,
CAST(ANST_DATUM_TOM AS VARCHAR(MAX)) AS ANST_DATUM_TOM,
CAST(ANST_GILTIG_FOM AS VARCHAR(MAX)) AS ANST_GILTIG_FOM,
CAST(ANST_GILTIG_TOM AS VARCHAR(MAX)) AS ANST_GILTIG_TOM,
CAST(ANST_ID AS VARCHAR(MAX)) AS ANST_ID,
CAST(ANST_ID_TEXT AS VARCHAR(MAX)) AS ANST_ID_TEXT,
CAST(ANST_PASSIV AS VARCHAR(MAX)) AS ANST_PASSIV,
CAST(ANST_TEXT AS VARCHAR(MAX)) AS ANST_TEXT FROM utdata.utdata295.EK_DATUM_OBJ_ANST"""
    return pipe(query=query)
