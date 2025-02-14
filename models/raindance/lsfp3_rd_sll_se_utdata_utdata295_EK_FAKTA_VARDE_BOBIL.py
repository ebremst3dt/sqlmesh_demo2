
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from models.raindance_src_to_raw import pipe


@model(
    columns={'ANDRAD_AV': 'varchar(max)',
 'ANDRAD_DATUM': 'varchar(max)',
 'ANDRAD_TID': 'varchar(max)',
 'BIL_TEXT': 'varchar(max)',
 'BOBIL_ID': 'varchar(max)',
 'BORAD_ID': 'varchar(max)',
 'DATUM_FOM': 'varchar(max)',
 'DATUM_TOM': 'varchar(max)',
 'KONTO_ID': 'varchar(max)',
 'KST_ID': 'varchar(max)',
 'MOTP_ID': 'varchar(max)',
 'PROJ_ID': 'varchar(max)',
 'UTILITY': 'varchar(max)',
 'VERDATUM': 'varchar(max)'},
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
CAST(BIL_TEXT AS VARCHAR(MAX)) AS BIL_TEXT,
CAST(BOBIL_ID AS VARCHAR(MAX)) AS BOBIL_ID,
CAST(BORAD_ID AS VARCHAR(MAX)) AS BORAD_ID,
CAST(DATUM_FOM AS VARCHAR(MAX)) AS DATUM_FOM,
CAST(DATUM_TOM AS VARCHAR(MAX)) AS DATUM_TOM,
CAST(KONTO_ID AS VARCHAR(MAX)) AS KONTO_ID,
CAST(KST_ID AS VARCHAR(MAX)) AS KST_ID,
CAST(MOTP_ID AS VARCHAR(MAX)) AS MOTP_ID,
CAST(PROJ_ID AS VARCHAR(MAX)) AS PROJ_ID,
CAST(UTILITY AS VARCHAR(MAX)) AS UTILITY,
CAST(VERDATUM AS VARCHAR(MAX)) AS VERDATUM FROM utdata.utdata295.EK_FAKTA_VARDE_BOBIL"""
    return pipe(query=query)
