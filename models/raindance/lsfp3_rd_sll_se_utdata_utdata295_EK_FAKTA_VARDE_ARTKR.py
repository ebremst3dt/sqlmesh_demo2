
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read


@model(
    columns={'ARTKR_ID': 'varchar(20)',
 'AVTBES_ID': 'varchar(120)',
 'DATUM_FOM': 'datetime',
 'DATUM_TOM': 'datetime',
 'FRI1_ID': 'varchar(120)',
 'KONTO_ID': 'varchar(120)',
 'KST_ID': 'varchar(120)',
 'MOMS_ID': 'varchar(120)',
 'PROJ_ID': 'varchar(120)',
 'TEXT2_TEXT': 'varchar(120)',
 'TEXT_TEXT': 'varchar(120)',
 'UTFALL_V': 'decimal',
 'UTILITY': 'int',
 'VERDATUM': 'datetime'},
    kind=ModelKindName.FULL,
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
 		CAST(ARTKR_ID AS VARCHAR(MAX)) AS artkr_id,
		CAST(AVTBES_ID AS VARCHAR(MAX)) AS avtbes_id,
		CONVERT(varchar(max), DATUM_FOM, 126) AS datum_fom,
		CONVERT(varchar(max), DATUM_TOM, 126) AS datum_tom,
		CAST(FRI1_ID AS VARCHAR(MAX)) AS fri1_id,
		CAST(KONTO_ID AS VARCHAR(MAX)) AS konto_id,
		CAST(KST_ID AS VARCHAR(MAX)) AS kst_id,
		CAST(MOMS_ID AS VARCHAR(MAX)) AS moms_id,
		CAST(PROJ_ID AS VARCHAR(MAX)) AS proj_id,
		CAST(TEXT_TEXT AS VARCHAR(MAX)) AS text_text,
		CAST(TEXT2_TEXT AS VARCHAR(MAX)) AS text2_text,
		CAST(UTFALL_V AS VARCHAR(MAX)) AS utfall_v,
		CAST(UTILITY AS VARCHAR(MAX)) AS utility,
		CONVERT(varchar(max), VERDATUM, 126) AS verdatum 
	FROM utdata.utdata295.EK_FAKTA_VARDE_ARTKR
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
