
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from models.raindance_src_to_raw import pipe


@model(
    columns={'ANDRAD_AV': 'varchar(max)',
 'ANDRAD_DATUM': 'varchar(max)',
 'ANDRAD_TID': 'varchar(max)',
 'ANST_ID': 'varchar(max)',
 'ANTMÅN_V': 'varchar(max)',
 'BELOPP_V': 'varchar(max)',
 'DATUM_FOM': 'varchar(max)',
 'DATUM_TOM': 'varchar(max)',
 'ERSGR_ID': 'varchar(max)',
 'KST_ID': 'varchar(max)',
 'LÖNEÖK_V': 'varchar(max)',
 'OMF_V': 'varchar(max)',
 'PROJ_ID': 'varchar(max)',
 'TOTBEL_V': 'varchar(max)',
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
CAST(ANST_ID AS VARCHAR(MAX)) AS ANST_ID,
CAST(ANTMÅN_V AS VARCHAR(MAX)) AS ANTMÅN_V,
CAST(BELOPP_V AS VARCHAR(MAX)) AS BELOPP_V,
CAST(DATUM_FOM AS VARCHAR(MAX)) AS DATUM_FOM,
CAST(DATUM_TOM AS VARCHAR(MAX)) AS DATUM_TOM,
CAST(ERSGR_ID AS VARCHAR(MAX)) AS ERSGR_ID,
CAST(KST_ID AS VARCHAR(MAX)) AS KST_ID,
CAST(LÖNEÖK_V AS VARCHAR(MAX)) AS LÖNEÖK_V,
CAST(OMF_V AS VARCHAR(MAX)) AS OMF_V,
CAST(PROJ_ID AS VARCHAR(MAX)) AS PROJ_ID,
CAST(TOTBEL_V AS VARCHAR(MAX)) AS TOTBEL_V,
CAST(UTILITY AS VARCHAR(MAX)) AS UTILITY,
CAST(VERDATUM AS VARCHAR(MAX)) AS VERDATUM FROM utdata.utdata295.EK_FAKTA_VARDE_PERS"""
    return pipe(query=query)
