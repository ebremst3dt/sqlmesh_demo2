
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from models.raindance_src_to_raw import pipe


@model(
    columns={'ANDRAD_AV': 'varchar(max)',
 'ANDRAD_DATUM': 'varchar(max)',
 'ANDRAD_TID': 'varchar(max)',
 'BOANTP_ID': 'varchar(max)',
 'DATUM_FOM': 'varchar(max)',
 'DATUM_TOM': 'varchar(max)',
 'KONTO_ID': 'varchar(max)',
 'KTOAV_TEXT': 'varchar(max)',
 'UTILITY': 'varchar(max)',
 'VERDATUM': 'varchar(max)',
 'X_V': 'varchar(max)'},
    cron="@daily"
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> pd.DataFrame:
    query = """SELECT CAST(ANDRAD_AV AS VARCHAR(MAX)) AS ANDRAD_AV,
CAST(ANDRAD_DATUM AS VARCHAR(MAX)) AS ANDRAD_DATUM,
CAST(ANDRAD_TID AS VARCHAR(MAX)) AS ANDRAD_TID,
CAST(BOANTP_ID AS VARCHAR(MAX)) AS BOANTP_ID,
CAST(DATUM_FOM AS VARCHAR(MAX)) AS DATUM_FOM,
CAST(DATUM_TOM AS VARCHAR(MAX)) AS DATUM_TOM,
CAST(KONTO_ID AS VARCHAR(MAX)) AS KONTO_ID,
CAST(KTOAV_TEXT AS VARCHAR(MAX)) AS KTOAV_TEXT,
CAST(UTILITY AS VARCHAR(MAX)) AS UTILITY,
CAST(VERDATUM AS VARCHAR(MAX)) AS VERDATUM,
CAST(X_V AS VARCHAR(MAX)) AS X_V FROM utdata.utdata295.EK_FAKTA_VARDE_BOANTP"""
    return pipe(query=query)
