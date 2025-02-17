
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read


@model(
    columns={'ANDRAD_AV': 'varchar(max)',
 'ANDRAD_DATUM': 'varchar(max)',
 'ANDRAD_TID': 'varchar(max)',
 'ANT_TEXT': 'varchar(max)',
 'DATUM_FOM': 'varchar(max)',
 'DATUM_TOM': 'varchar(max)',
 'FRI1_ID': 'varchar(max)',
 'KONTO_ID': 'varchar(max)',
 'KST_ID': 'varchar(max)',
 'MOTP_ID': 'varchar(max)',
 'PROJ_ID': 'varchar(max)',
 'UTILITY': 'varchar(max)',
 'VERDATUM': 'varchar(max)',
 'VERK_ID': 'varchar(max)'},
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
 		CAST(ANDRAD_AV AS VARCHAR(MAX)) AS andrad_av,
		CAST(ANDRAD_DATUM AS VARCHAR(MAX)) AS andrad_datum,
		CAST(ANDRAD_TID AS VARCHAR(MAX)) AS andrad_tid,
		CAST(ANT_TEXT AS VARCHAR(MAX)) AS ant_text,
		CAST(DATUM_FOM AS VARCHAR(MAX)) AS datum_fom,
		CAST(DATUM_TOM AS VARCHAR(MAX)) AS datum_tom,
		CAST(FRI1_ID AS VARCHAR(MAX)) AS fri1_id,
		CAST(KONTO_ID AS VARCHAR(MAX)) AS konto_id,
		CAST(KST_ID AS VARCHAR(MAX)) AS kst_id,
		CAST(MOTP_ID AS VARCHAR(MAX)) AS motp_id,
		CAST(PROJ_ID AS VARCHAR(MAX)) AS proj_id,
		CAST(UTILITY AS VARCHAR(MAX)) AS utility,
		CAST(VERDATUM AS VARCHAR(MAX)) AS verdatum,
		CAST(VERK_ID AS VARCHAR(MAX)) AS verk_id 
	FROM utdata.utdata295.EK_FAKTA_VARDE_PTXTK
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
