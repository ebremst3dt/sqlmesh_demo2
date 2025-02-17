
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read


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
 		CAST(ANVID_ID AS VARCHAR(MAX)) AS anvid_id,
		CAST(AVTBES_ID AS VARCHAR(MAX)) AS avtbes_id,
		CAST(BAS_V AS VARCHAR(MAX)) AS bas_v,
		CAST(BEN_TEXT AS VARCHAR(MAX)) AS ben_text,
		CAST(DATUM_FOM AS VARCHAR(MAX)) AS datum_fom,
		CAST(DATUM_TOM AS VARCHAR(MAX)) AS datum_tom,
		CAST(FRI1_ID AS VARCHAR(MAX)) AS fri1_id,
		CAST(FRI2_ID AS VARCHAR(MAX)) AS fri2_id,
		CAST(KMALL_ID AS VARCHAR(MAX)) AS kmall_id,
		CAST(KONTO_ID AS VARCHAR(MAX)) AS konto_id,
		CAST(KST_ID AS VARCHAR(MAX)) AS kst_id,
		CAST(MBEN_TEXT AS VARCHAR(MAX)) AS mben_text,
		CAST(PROJ_ID AS VARCHAR(MAX)) AS proj_id,
		CAST(RAK_ID AS VARCHAR(MAX)) AS rak_id,
		CAST(UTILITY AS VARCHAR(MAX)) AS utility,
		CAST(VERDATUM AS VARCHAR(MAX)) AS verdatum 
	FROM utdata.utdata295.EK_FAKTA_VARDE_KMALL2
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
