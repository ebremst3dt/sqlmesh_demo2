
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
 'AVTBES_ID': 'varchar(7)',
 'BELOPP_V': 'numeric',
 'BESDAT_DATUM': 'datetime',
 'BSIGN_TEXT': 'varchar(30)',
 'DATUM_FOM': 'datetime',
 'DATUM_TOM': 'datetime',
 'KST_ID': 'varchar(5)',
 'RUBRIK_TEXT': 'varchar(120)',
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
		CONVERT(varchar(max), ANDRAD_DATUM, 126) AS andrad_datum,
		CAST(ANDRAD_TID AS VARCHAR(MAX)) AS andrad_tid,
		CAST(AVTBES_ID AS VARCHAR(MAX)) AS avtbes_id,
		CAST(BELOPP_V AS VARCHAR(MAX)) AS belopp_v,
		CONVERT(varchar(max), BESDAT_DATUM, 126) AS besdat_datum,
		CAST(BSIGN_TEXT AS VARCHAR(MAX)) AS bsign_text,
		CONVERT(varchar(max), DATUM_FOM, 126) AS datum_fom,
		CONVERT(varchar(max), DATUM_TOM, 126) AS datum_tom,
		CAST(KST_ID AS VARCHAR(MAX)) AS kst_id,
		CAST(RUBRIK_TEXT AS VARCHAR(MAX)) AS rubrik_text,
		CAST(UTILITY AS VARCHAR(MAX)) AS utility,
		CONVERT(varchar(max), VERDATUM, 126) AS verdatum 
	FROM utdata.utdata295.EK_FAKTA_VARDE_BESLUT
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
