
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from models.raindance_src_to_raw import pipe


@model(
    columns={'AVTBES_ID': 'varchar(max)',
 'DATUM_FOM': 'varchar(max)',
 'DATUM_TOM': 'varchar(max)',
 'FRI1_ID': 'varchar(max)',
 'FRI2_ID': 'varchar(max)',
 'KONTO_ID': 'varchar(max)',
 'KST_ID': 'varchar(max)',
 'MALLID_ID': 'varchar(max)',
 'PNYCKL_ID': 'varchar(max)',
 'PROJ_ID': 'varchar(max)',
 'RAK_ID': 'varchar(max)',
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
    query = """SELECT CAST(AVTBES_ID AS VARCHAR(MAX)) AS AVTBES_ID,
CAST(DATUM_FOM AS VARCHAR(MAX)) AS DATUM_FOM,
CAST(DATUM_TOM AS VARCHAR(MAX)) AS DATUM_TOM,
CAST(FRI1_ID AS VARCHAR(MAX)) AS FRI1_ID,
CAST(FRI2_ID AS VARCHAR(MAX)) AS FRI2_ID,
CAST(KONTO_ID AS VARCHAR(MAX)) AS KONTO_ID,
CAST(KST_ID AS VARCHAR(MAX)) AS KST_ID,
CAST(MALLID_ID AS VARCHAR(MAX)) AS MALLID_ID,
CAST(PNYCKL_ID AS VARCHAR(MAX)) AS PNYCKL_ID,
CAST(PROJ_ID AS VARCHAR(MAX)) AS PROJ_ID,
CAST(RAK_ID AS VARCHAR(MAX)) AS RAK_ID,
CAST(TEXT_TEXT AS VARCHAR(MAX)) AS TEXT_TEXT,
CAST(UTFALL_V AS VARCHAR(MAX)) AS UTFALL_V,
CAST(UTILITY AS VARCHAR(MAX)) AS UTILITY,
CAST(VERDATUM AS VARCHAR(MAX)) AS VERDATUM FROM utdata.utdata295.EK_FAKTA_VARDE_PFMALL"""
    return pipe(query=query)
