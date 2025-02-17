
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read


@model(
    columns={'ANVID_TEXT': 'varchar(120)',
 'BILDNR_TEXT': 'varchar(120)',
 'BILDN_TEXT': 'varchar(120)',
 'DATUM_FOM': 'datetime',
 'DATUM_TOM': 'datetime',
 'DELSYS_TEXT': 'varchar(120)',
 'HHMMSS_TEXT': 'varchar(120)',
 'LOPNUMMER': 'int',
 'TIDSQL_V': 'decimal',
 'TID_V': 'decimal',
 'URVAL_TEXT': 'varchar(120)',
 'UTILITY': 'int',
 'VERDATUM': 'datetime',
 'VMNR_TEXT': 'varchar(120)',
 'VMN_TEXT': 'varchar(120)'},
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
		CAST(DATUM_FOM AS VARCHAR(MAX)) AS datum_fom,
		CAST(DATUM_TOM AS VARCHAR(MAX)) AS datum_tom,
		CAST(DELSYS_TEXT AS VARCHAR(MAX)) AS delsys_text,
		CAST(HHMMSS_TEXT AS VARCHAR(MAX)) AS hhmmss_text,
		CAST(LOPNUMMER AS VARCHAR(MAX)) AS lopnummer,
		CAST(TID_V AS VARCHAR(MAX)) AS tid_v,
		CAST(TIDSQL_V AS VARCHAR(MAX)) AS tidsql_v,
		CAST(URVAL_TEXT AS VARCHAR(MAX)) AS urval_text,
		CAST(UTILITY AS VARCHAR(MAX)) AS utility,
		CAST(VERDATUM AS VARCHAR(MAX)) AS verdatum,
		CAST(VMN_TEXT AS VARCHAR(MAX)) AS vmn_text,
		CAST(VMNR_TEXT AS VARCHAR(MAX)) AS vmnr_text 
	FROM utdata.utdata295.EK_FAKTA_VARDE_BILDLOGG
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
