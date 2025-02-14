
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from models.raindance_src_to_raw import pipe


@model(
    columns={'ANVID_ID': 'varchar(max)',
 'AVTBES_ID': 'varchar(max)',
 'BAS_V': 'varchar(max)',
 'BEN_TEXT': 'varchar(max)',
 'DATUM_FOM': 'varchar(max)',
 'DATUM_TOM': 'varchar(max)',
 'FRI1_ID': 'varchar(max)',
 'FRI2_ID': 'varchar(max)',
 'KMALL_ID': 'varchar(max)',
 'KONTO_ID': 'varchar(max)',
 'KST_ID': 'varchar(max)',
 'MBEN_TEXT': 'varchar(max)',
 'PROJ_ID': 'varchar(max)',
 'RAK_ID': 'varchar(max)',
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
    query = """SELECT CAST(ANVID_ID AS VARCHAR(MAX)) AS ANVID_ID,
CAST(AVTBES_ID AS VARCHAR(MAX)) AS AVTBES_ID,
CAST(BAS_V AS VARCHAR(MAX)) AS BAS_V,
CAST(BEN_TEXT AS VARCHAR(MAX)) AS BEN_TEXT,
CAST(DATUM_FOM AS VARCHAR(MAX)) AS DATUM_FOM,
CAST(DATUM_TOM AS VARCHAR(MAX)) AS DATUM_TOM,
CAST(FRI1_ID AS VARCHAR(MAX)) AS FRI1_ID,
CAST(FRI2_ID AS VARCHAR(MAX)) AS FRI2_ID,
CAST(KMALL_ID AS VARCHAR(MAX)) AS KMALL_ID,
CAST(KONTO_ID AS VARCHAR(MAX)) AS KONTO_ID,
CAST(KST_ID AS VARCHAR(MAX)) AS KST_ID,
CAST(MBEN_TEXT AS VARCHAR(MAX)) AS MBEN_TEXT,
CAST(PROJ_ID AS VARCHAR(MAX)) AS PROJ_ID,
CAST(RAK_ID AS VARCHAR(MAX)) AS RAK_ID,
CAST(UTILITY AS VARCHAR(MAX)) AS UTILITY,
CAST(VERDATUM AS VARCHAR(MAX)) AS VERDATUM FROM utdata.utdata295.EK_FAKTA_VARDE_KMALL2"""
    return pipe(query=query)
