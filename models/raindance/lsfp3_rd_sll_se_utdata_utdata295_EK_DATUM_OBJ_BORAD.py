
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from models.raindance_src_to_raw import pipe


@model(
    columns={'BORAD_DATUM_FOM': 'varchar(max)',
 'BORAD_DATUM_TOM': 'varchar(max)',
 'BORAD_GILTIG_FOM': 'varchar(max)',
 'BORAD_GILTIG_TOM': 'varchar(max)',
 'BORAD_ID': 'varchar(max)',
 'BORAD_ID_TEXT': 'varchar(max)',
 'BORAD_PASSIV': 'varchar(max)',
 'BORAD_TEXT': 'varchar(max)'},
    cron="@daily"
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> pd.DataFrame:
    query = """SELECT CAST(BORAD_DATUM_FOM AS VARCHAR(MAX)) AS BORAD_DATUM_FOM,
CAST(BORAD_DATUM_TOM AS VARCHAR(MAX)) AS BORAD_DATUM_TOM,
CAST(BORAD_GILTIG_FOM AS VARCHAR(MAX)) AS BORAD_GILTIG_FOM,
CAST(BORAD_GILTIG_TOM AS VARCHAR(MAX)) AS BORAD_GILTIG_TOM,
CAST(BORAD_ID AS VARCHAR(MAX)) AS BORAD_ID,
CAST(BORAD_ID_TEXT AS VARCHAR(MAX)) AS BORAD_ID_TEXT,
CAST(BORAD_PASSIV AS VARCHAR(MAX)) AS BORAD_PASSIV,
CAST(BORAD_TEXT AS VARCHAR(MAX)) AS BORAD_TEXT FROM utdata.utdata295.EK_DATUM_OBJ_BORAD"""
    return pipe(query=query)
