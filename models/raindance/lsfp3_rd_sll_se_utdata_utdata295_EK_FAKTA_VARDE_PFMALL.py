
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read


@model(
    columns={'AVTBES_ID': 'varchar(120)',
 'DATUM_FOM': 'datetime',
 'DATUM_TOM': 'datetime',
 'FRI1_ID': 'varchar(120)',
 'FRI2_ID': 'varchar(120)',
 'KONTO_ID': 'varchar(120)',
 'KST_ID': 'varchar(120)',
 'MALLID_ID': 'varchar(20)',
 'PNYCKL_ID': 'varchar(120)',
 'PROJ_ID': 'varchar(120)',
 'RAK_ID': 'varchar(20)',
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
 		CAST(AVTBES_ID AS VARCHAR(MAX)) AS avtbes_id,
		CONVERT(varchar(max), DATUM_FOM, 126) AS datum_fom,
		CONVERT(varchar(max), DATUM_TOM, 126) AS datum_tom,
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
		CONVERT(varchar(max), VERDATUM, 126) AS verdatum 
	FROM utdata.utdata295.EK_FAKTA_VARDE_PFMALL
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
