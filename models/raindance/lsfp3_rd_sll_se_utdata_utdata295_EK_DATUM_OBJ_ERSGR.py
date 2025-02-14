
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from models.raindance_src_to_raw import pipe


@model(
    columns={'ERSGR_DATUM_FOM': 'varchar(max)',
 'ERSGR_DATUM_TOM': 'varchar(max)',
 'ERSGR_GILTIG_FOM': 'varchar(max)',
 'ERSGR_GILTIG_TOM': 'varchar(max)',
 'ERSGR_ID': 'varchar(max)',
 'ERSGR_ID_TEXT': 'varchar(max)',
 'ERSGR_PASSIV': 'varchar(max)',
 'ERSGR_TEXT': 'varchar(max)'},
    cron="@daily"
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> pd.DataFrame:
    query = """SELECT CAST(ERSGR_DATUM_FOM AS VARCHAR(MAX)) AS ERSGR_DATUM_FOM,
CAST(ERSGR_DATUM_TOM AS VARCHAR(MAX)) AS ERSGR_DATUM_TOM,
CAST(ERSGR_GILTIG_FOM AS VARCHAR(MAX)) AS ERSGR_GILTIG_FOM,
CAST(ERSGR_GILTIG_TOM AS VARCHAR(MAX)) AS ERSGR_GILTIG_TOM,
CAST(ERSGR_ID AS VARCHAR(MAX)) AS ERSGR_ID,
CAST(ERSGR_ID_TEXT AS VARCHAR(MAX)) AS ERSGR_ID_TEXT,
CAST(ERSGR_PASSIV AS VARCHAR(MAX)) AS ERSGR_PASSIV,
CAST(ERSGR_TEXT AS VARCHAR(MAX)) AS ERSGR_TEXT FROM utdata.utdata295.EK_DATUM_OBJ_ERSGR"""
    return pipe(query=query)
