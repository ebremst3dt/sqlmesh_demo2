
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from models.raindance_src_to_raw import pipe


@model(
    columns={'VALUTA_DATUM_FOM': 'varchar(max)',
 'VALUTA_DATUM_TOM': 'varchar(max)',
 'VALUTA_GILTIG_FOM': 'varchar(max)',
 'VALUTA_GILTIG_TOM': 'varchar(max)',
 'VALUTA_ID': 'varchar(max)',
 'VALUTA_ID_TEXT': 'varchar(max)',
 'VALUTA_PASSIV': 'varchar(max)',
 'VALUTA_TEXT': 'varchar(max)'},
    cron="@daily"
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> pd.DataFrame:
    query = """SELECT CAST(VALUTA_DATUM_FOM AS VARCHAR(MAX)) AS VALUTA_DATUM_FOM,
CAST(VALUTA_DATUM_TOM AS VARCHAR(MAX)) AS VALUTA_DATUM_TOM,
CAST(VALUTA_GILTIG_FOM AS VARCHAR(MAX)) AS VALUTA_GILTIG_FOM,
CAST(VALUTA_GILTIG_TOM AS VARCHAR(MAX)) AS VALUTA_GILTIG_TOM,
CAST(VALUTA_ID AS VARCHAR(MAX)) AS VALUTA_ID,
CAST(VALUTA_ID_TEXT AS VARCHAR(MAX)) AS VALUTA_ID_TEXT,
CAST(VALUTA_PASSIV AS VARCHAR(MAX)) AS VALUTA_PASSIV,
CAST(VALUTA_TEXT AS VARCHAR(MAX)) AS VALUTA_TEXT FROM utdata.utdata295.EK_DATUM_OBJ_VALUTA"""
    return pipe(query=query)
