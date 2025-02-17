
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
 'DATUM_FOM': 'varchar(max)',
 'DATUM_TOM': 'varchar(max)',
 'IB1_V': 'varchar(max)',
 'KONTO_ID': 'varchar(max)',
 'UB1_V': 'varchar(max)',
 'UB2_V': 'varchar(max)',
 'UB3_V': 'varchar(max)',
 'UB4_V': 'varchar(max)',
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
 		CAST(ANDRAD_AV AS VARCHAR(MAX)) AS andrad_av,
		CAST(ANDRAD_DATUM AS VARCHAR(MAX)) AS andrad_datum,
		CAST(ANDRAD_TID AS VARCHAR(MAX)) AS andrad_tid,
		CAST(DATUM_FOM AS VARCHAR(MAX)) AS datum_fom,
		CAST(DATUM_TOM AS VARCHAR(MAX)) AS datum_tom,
		CAST(IB1_V AS VARCHAR(MAX)) AS ib1_v,
		CAST(KONTO_ID AS VARCHAR(MAX)) AS konto_id,
		CAST(UB1_V AS VARCHAR(MAX)) AS ub1_v,
		CAST(UB2_V AS VARCHAR(MAX)) AS ub2_v,
		CAST(UB3_V AS VARCHAR(MAX)) AS ub3_v,
		CAST(UB4_V AS VARCHAR(MAX)) AS ub4_v,
		CAST(UTILITY AS VARCHAR(MAX)) AS utility,
		CAST(VERDATUM AS VARCHAR(MAX)) AS verdatum 
	FROM utdata.utdata295.EK_FAKTA_VARDE_BOSAL
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
