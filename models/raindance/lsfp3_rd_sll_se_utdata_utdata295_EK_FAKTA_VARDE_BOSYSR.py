
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
 'BOPER_ID': 'varchar(1)',
 'BOTYP_ID': 'varchar(1)',
 'DATUM_FOM': 'datetime',
 'DATUM_TOM': 'datetime',
 'KONTO_ID': 'varchar(4)',
 'RAPP_TEXT': 'varchar(50)',
 'RSP_TEXT': 'varchar(3)',
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
		CAST(BOPER_ID AS VARCHAR(MAX)) AS boper_id,
		CAST(BOTYP_ID AS VARCHAR(MAX)) AS botyp_id,
		CAST(DATUM_FOM AS VARCHAR(MAX)) AS datum_fom,
		CAST(DATUM_TOM AS VARCHAR(MAX)) AS datum_tom,
		CAST(KONTO_ID AS VARCHAR(MAX)) AS konto_id,
		CAST(RAPP_TEXT AS VARCHAR(MAX)) AS rapp_text,
		CAST(RSP_TEXT AS VARCHAR(MAX)) AS rsp_text,
		CAST(UTILITY AS VARCHAR(MAX)) AS utility,
		CAST(VERDATUM AS VARCHAR(MAX)) AS verdatum 
	FROM utdata.utdata295.EK_FAKTA_VARDE_BOSYSR
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
