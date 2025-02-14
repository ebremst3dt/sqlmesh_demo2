
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from models.raindance_src_to_raw import pipe


@model(
    columns={'ARTKR_DATUM_FOM': 'varchar(max)',
 'ARTKR_DATUM_TOM': 'varchar(max)',
 'ARTKR_GILTIG_FOM': 'varchar(max)',
 'ARTKR_GILTIG_TOM': 'varchar(max)',
 'ARTKR_ID': 'varchar(max)',
 'ARTKR_ID_TEXT': 'varchar(max)',
 'ARTKR_PASSIV': 'varchar(max)',
 'ARTKR_TEXT': 'varchar(max)'},
    cron="@daily"
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> pd.DataFrame:
    query = """SELECT CAST(ARTKR_DATUM_FOM AS VARCHAR(MAX)) AS ARTKR_DATUM_FOM,
CAST(ARTKR_DATUM_TOM AS VARCHAR(MAX)) AS ARTKR_DATUM_TOM,
CAST(ARTKR_GILTIG_FOM AS VARCHAR(MAX)) AS ARTKR_GILTIG_FOM,
CAST(ARTKR_GILTIG_TOM AS VARCHAR(MAX)) AS ARTKR_GILTIG_TOM,
CAST(ARTKR_ID AS VARCHAR(MAX)) AS ARTKR_ID,
CAST(ARTKR_ID_TEXT AS VARCHAR(MAX)) AS ARTKR_ID_TEXT,
CAST(ARTKR_PASSIV AS VARCHAR(MAX)) AS ARTKR_PASSIV,
CAST(ARTKR_TEXT AS VARCHAR(MAX)) AS ARTKR_TEXT FROM utdata.utdata295.EK_DATUM_OBJ_ARTKR"""
    return pipe(query=query)
