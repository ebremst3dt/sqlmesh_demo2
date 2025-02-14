
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from models.raindance_src_to_raw import pipe


@model(
    columns={'ANVID_TEXT': 'varchar(max)',
 'BILDNR_TEXT': 'varchar(max)',
 'BILDN_TEXT': 'varchar(max)',
 'DATUM_FOM': 'varchar(max)',
 'DATUM_TOM': 'varchar(max)',
 'DELSYS_TEXT': 'varchar(max)',
 'HHMMSS_TEXT': 'varchar(max)',
 'LOPNUMMER': 'varchar(max)',
 'TIDSQL_V': 'varchar(max)',
 'TID_V': 'varchar(max)',
 'URVAL_TEXT': 'varchar(max)',
 'UTILITY': 'varchar(max)',
 'VERDATUM': 'varchar(max)',
 'VMNR_TEXT': 'varchar(max)',
 'VMN_TEXT': 'varchar(max)'},
    cron="@daily"
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> pd.DataFrame:
    query = """SELECT CAST(ANVID_TEXT AS VARCHAR(MAX)) AS ANVID_TEXT,
CAST(BILDN_TEXT AS VARCHAR(MAX)) AS BILDN_TEXT,
CAST(BILDNR_TEXT AS VARCHAR(MAX)) AS BILDNR_TEXT,
CAST(DATUM_FOM AS VARCHAR(MAX)) AS DATUM_FOM,
CAST(DATUM_TOM AS VARCHAR(MAX)) AS DATUM_TOM,
CAST(DELSYS_TEXT AS VARCHAR(MAX)) AS DELSYS_TEXT,
CAST(HHMMSS_TEXT AS VARCHAR(MAX)) AS HHMMSS_TEXT,
CAST(LOPNUMMER AS VARCHAR(MAX)) AS LOPNUMMER,
CAST(TID_V AS VARCHAR(MAX)) AS TID_V,
CAST(TIDSQL_V AS VARCHAR(MAX)) AS TIDSQL_V,
CAST(URVAL_TEXT AS VARCHAR(MAX)) AS URVAL_TEXT,
CAST(UTILITY AS VARCHAR(MAX)) AS UTILITY,
CAST(VERDATUM AS VARCHAR(MAX)) AS VERDATUM,
CAST(VMN_TEXT AS VARCHAR(MAX)) AS VMN_TEXT,
CAST(VMNR_TEXT AS VARCHAR(MAX)) AS VMNR_TEXT FROM utdata.utdata295.EK_FAKTA_VARDE_BILDLOGG"""
    return pipe(query=query)
