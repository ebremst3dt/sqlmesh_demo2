
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from models.raindance_src_to_raw import pipe


@model(
    columns={'ARTKR_ID': 'varchar(max)',
 'AVTBES_ID': 'varchar(max)',
 'DATUM_FOM': 'varchar(max)',
 'DATUM_TOM': 'varchar(max)',
 'FRI1_ID': 'varchar(max)',
 'KONTO_ID': 'varchar(max)',
 'KST_ID': 'varchar(max)',
 'MOMS_ID': 'varchar(max)',
 'PROJ_ID': 'varchar(max)',
 'TEXT2_TEXT': 'varchar(max)',
 'TEXT_TEXT': 'varchar(max)',
 'UTFALL_V': 'varchar(max)',
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
    query = """SELECT CAST(ARTKR_ID AS VARCHAR(MAX)) AS ARTKR_ID,
CAST(AVTBES_ID AS VARCHAR(MAX)) AS AVTBES_ID,
CAST(DATUM_FOM AS VARCHAR(MAX)) AS DATUM_FOM,
CAST(DATUM_TOM AS VARCHAR(MAX)) AS DATUM_TOM,
CAST(FRI1_ID AS VARCHAR(MAX)) AS FRI1_ID,
CAST(KONTO_ID AS VARCHAR(MAX)) AS KONTO_ID,
CAST(KST_ID AS VARCHAR(MAX)) AS KST_ID,
CAST(MOMS_ID AS VARCHAR(MAX)) AS MOMS_ID,
CAST(PROJ_ID AS VARCHAR(MAX)) AS PROJ_ID,
CAST(TEXT_TEXT AS VARCHAR(MAX)) AS TEXT_TEXT,
CAST(TEXT2_TEXT AS VARCHAR(MAX)) AS TEXT2_TEXT,
CAST(UTFALL_V AS VARCHAR(MAX)) AS UTFALL_V,
CAST(UTILITY AS VARCHAR(MAX)) AS UTILITY,
CAST(VERDATUM AS VARCHAR(MAX)) AS VERDATUM FROM utdata.utdata295.EK_FAKTA_VARDE_ARTKR"""
    return pipe(query=query)
