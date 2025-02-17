
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
 'DATUM_FOM': 'datetime',
 'DATUM_TOM': 'datetime',
 'DEFANL_ID': 'varchar(14)',
 'INVDAT_DATUM': 'datetime',
 'INVSGN_TEXT': 'varchar(60)',
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
		CAST(DATUM_FOM AS VARCHAR(MAX)) AS datum_fom,
		CAST(DATUM_TOM AS VARCHAR(MAX)) AS datum_tom,
		CAST(DEFANL_ID AS VARCHAR(MAX)) AS defanl_id,
		CAST(INVDAT_DATUM AS VARCHAR(MAX)) AS invdat_datum,
		CAST(INVSGN_TEXT AS VARCHAR(MAX)) AS invsgn_text,
		CAST(UTILITY AS VARCHAR(MAX)) AS utility,
		CAST(VERDATUM AS VARCHAR(MAX)) AS verdatum 
	FROM utdata.utdata295.EK_FAKTA_VARDE_ARDTXT
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
