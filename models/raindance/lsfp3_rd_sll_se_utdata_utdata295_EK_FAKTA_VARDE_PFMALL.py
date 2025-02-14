
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from models.mssql import read


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
    query = """
	SELECT top 1000
 		CAST(AVTBES_ID AS VARCHAR(MAX)) AS avtbes_id,
		CAST(DATUM_FOM AS VARCHAR(MAX)) AS datum_fom,
		CAST(DATUM_TOM AS VARCHAR(MAX)) AS datum_tom,
		CAST(FRI1_ID AS VARCHAR(MAX)) AS fri1_id,
		CAST(FRI2_ID AS VARCHAR(MAX)) AS fri2_id,
		CAST(KONTO_ID AS VARCHAR(MAX)) AS konto_id,
		CAST(KST_ID AS VARCHAR(MAX)) AS kst_id,
		CAST(MALLID_ID AS VARCHAR(MAX)) AS mallid_id,
		CAST(PNYCKL_ID AS VARCHAR(MAX)) AS pnyckl_id,
		CAST(PROJ_ID AS VARCHAR(MAX)) AS proj_id,
		CAST(RAK_ID AS VARCHAR(MAX)) AS rak_id,
		CAST(TEXT_TEXT AS VARCHAR(MAX)) AS text_text,
		CAST(UTFALL_V AS VARCHAR(MAX)) AS utfall_v,
		CAST(UTILITY AS VARCHAR(MAX)) AS utility,
		CAST(VERDATUM AS VARCHAR(MAX)) AS verdatum 
	FROM utdata.utdata295.EK_FAKTA_VARDE_PFMALL
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
