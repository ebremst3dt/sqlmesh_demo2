
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from models.raindance_src_to_raw import pipe


@model(
    columns={'ANDRAD_AV': 'varchar(max)',
 'ANDRAD_DATUM': 'varchar(max)',
 'ANDRAD_TID': 'varchar(max)',
 'DATUM_FOM': 'varchar(max)',
 'DATUM_TOM': 'varchar(max)',
 'IB1_V': 'varchar(max)',
 'KONTO_ID': 'varchar(max)',
 'UB1_V': 'varchar(max)',
 'UB2_V': 'varchar(max)',
 'UB3_V': 'varchar(max)',
 'UB4_V': 'varchar(max)',
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
CAST(DATUM_FOM AS VARCHAR(MAX)) AS DATUM_FOM,
CAST(DATUM_TOM AS VARCHAR(MAX)) AS DATUM_TOM,
CAST(IB1_V AS VARCHAR(MAX)) AS IB1_V,
CAST(KONTO_ID AS VARCHAR(MAX)) AS KONTO_ID,
CAST(UB1_V AS VARCHAR(MAX)) AS UB1_V,
CAST(UB2_V AS VARCHAR(MAX)) AS UB2_V,
CAST(UB3_V AS VARCHAR(MAX)) AS UB3_V,
CAST(UB4_V AS VARCHAR(MAX)) AS UB4_V,
CAST(UTILITY AS VARCHAR(MAX)) AS UTILITY,
CAST(VERDATUM AS VARCHAR(MAX)) AS VERDATUM FROM utdata.utdata295.EK_FAKTA_VARDE_BOSAL"""
    return pipe(query=query)
