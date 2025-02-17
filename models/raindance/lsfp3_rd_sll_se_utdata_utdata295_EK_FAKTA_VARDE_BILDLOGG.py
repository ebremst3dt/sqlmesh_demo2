
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read


@model(
    columns={'ANVID_TEXT': 'varchar(max)',
 'BILDNR_TEXT': 'varchar(max)',
 'BILDN_TEXT': 'varchar(max)',
 'DATUM_FOM': 'varchar(max)',
 'DATUM_TOM': 'varchar(max)',
 'DELSYS_TEXT': 'varchar(max)',
 'HHMMSS_TEXT': 'varchar(max)',
 'LOPNUMMER': 'varchar(max)',
 'TIDSQL_V': 'varchar(max)',
 'TID_V': 'varchar(max)',
 'URVAL_TEXT': 'varchar(max)',
 'UTILITY': 'varchar(max)',
 'VERDATUM': 'varchar(max)',
 'VMNR_TEXT': 'varchar(max)',
 'VMN_TEXT': 'varchar(max)'},
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
 		CAST(ANVID_TEXT AS VARCHAR(MAX)) AS anvid_text,
		CAST(BILDN_TEXT AS VARCHAR(MAX)) AS bildn_text,
		CAST(BILDNR_TEXT AS VARCHAR(MAX)) AS bildnr_text,
		CONVERT(varchar(max), DATUM_FOM, 126) AS datum_fom,
		CONVERT(varchar(max), DATUM_TOM, 126) AS datum_tom,
		CAST(DELSYS_TEXT AS VARCHAR(MAX)) AS delsys_text,
		CAST(HHMMSS_TEXT AS VARCHAR(MAX)) AS hhmmss_text,
		CAST(LOPNUMMER AS VARCHAR(MAX)) AS lopnummer,
		CAST(TID_V AS VARCHAR(MAX)) AS tid_v,
		CAST(TIDSQL_V AS VARCHAR(MAX)) AS tidsql_v,
		CAST(URVAL_TEXT AS VARCHAR(MAX)) AS urval_text,
		CAST(UTILITY AS VARCHAR(MAX)) AS utility,
		CONVERT(varchar(max), VERDATUM, 126) AS verdatum,
		CAST(VMN_TEXT AS VARCHAR(MAX)) AS vmn_text,
		CAST(VMNR_TEXT AS VARCHAR(MAX)) AS vmnr_text 
	FROM utdata.utdata295.EK_FAKTA_VARDE_BILDLOGG
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
