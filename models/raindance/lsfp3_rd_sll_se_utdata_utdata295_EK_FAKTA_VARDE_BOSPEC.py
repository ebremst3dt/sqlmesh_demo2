
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read


@model(
    columns={'ANDRAD_AV': 'varchar(3)',
 'ANDRAD_DATUM': 'datetime',
 'ANDRAD_TID': 'varchar(6)',
 'BEL_V': 'numeric',
 'BORAD_ID': 'varchar(3)',
 'DATUM_FOM': 'datetime',
 'DATUM_TOM': 'datetime',
 'KONTO_ID': 'varchar(4)',
 'KST_ID': 'varchar(5)',
 'MOTP_ID': 'varchar(4)',
 'PROJ_ID': 'varchar(5)',
 'REF2_TEXT': 'varchar(80)',
 'REF3_TEXT': 'varchar(120)',
 'REF_TEXT': 'varchar(30)',
 'UTILITY': 'numeric',
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
 		CAST(ANDRAD_AV AS VARCHAR(MAX)) AS andrad_av,
		CAST(ANDRAD_DATUM AS VARCHAR(MAX)) AS andrad_datum,
		CAST(ANDRAD_TID AS VARCHAR(MAX)) AS andrad_tid,
		CAST(BEL_V AS VARCHAR(MAX)) AS bel_v,
		CAST(BORAD_ID AS VARCHAR(MAX)) AS borad_id,
		CAST(DATUM_FOM AS VARCHAR(MAX)) AS datum_fom,
		CAST(DATUM_TOM AS VARCHAR(MAX)) AS datum_tom,
		CAST(KONTO_ID AS VARCHAR(MAX)) AS konto_id,
		CAST(KST_ID AS VARCHAR(MAX)) AS kst_id,
		CAST(MOTP_ID AS VARCHAR(MAX)) AS motp_id,
		CAST(PROJ_ID AS VARCHAR(MAX)) AS proj_id,
		CAST(REF_TEXT AS VARCHAR(MAX)) AS ref_text,
		CAST(REF2_TEXT AS VARCHAR(MAX)) AS ref2_text,
		CAST(REF3_TEXT AS VARCHAR(MAX)) AS ref3_text,
		CAST(UTILITY AS VARCHAR(MAX)) AS utility,
		CAST(VERDATUM AS VARCHAR(MAX)) AS verdatum 
	FROM utdata.utdata295.EK_FAKTA_VARDE_BOSPEC
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
