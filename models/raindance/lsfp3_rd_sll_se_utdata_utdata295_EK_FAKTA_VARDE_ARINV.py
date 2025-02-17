
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
 'ANT_TEXT': 'varchar(120)',
 'DATUM_FOM': 'datetime',
 'DATUM_TOM': 'datetime',
 'DEFANL_ID': 'varchar(14)',
 'UTILITY': 'numeric',
 'UTR_TEXT': 'varchar(3)',
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
		CAST(ANT_TEXT AS VARCHAR(MAX)) AS ant_text,
		CONVERT(varchar(max), DATUM_FOM, 126) AS datum_fom,
		CONVERT(varchar(max), DATUM_TOM, 126) AS datum_tom,
		CAST(DEFANL_ID AS VARCHAR(MAX)) AS defanl_id,
		CAST(UTILITY AS VARCHAR(MAX)) AS utility,
		CAST(UTR_TEXT AS VARCHAR(MAX)) AS utr_text,
		CONVERT(varchar(max), VERDATUM, 126) AS verdatum 
	FROM utdata.utdata295.EK_FAKTA_VARDE_ARINV
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
