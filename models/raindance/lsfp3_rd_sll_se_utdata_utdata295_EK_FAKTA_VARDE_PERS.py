
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model import kind
from models.mssql import read


@model(
    columns={'ANDRAD_AV': 'varchar(max)',
 'ANDRAD_DATUM': 'varchar(max)',
 'ANDRAD_TID': 'varchar(max)',
 'ANST_ID': 'varchar(max)',
 'ANTMÅN_V': 'varchar(max)',
 'BELOPP_V': 'varchar(max)',
 'DATUM_FOM': 'varchar(max)',
 'DATUM_TOM': 'varchar(max)',
 'ERSGR_ID': 'varchar(max)',
 'KST_ID': 'varchar(max)',
 'LÖNEÖK_V': 'varchar(max)',
 'OMF_V': 'varchar(max)',
 'PROJ_ID': 'varchar(max)',
 'TOTBEL_V': 'varchar(max)',
 'UTILITY': 'varchar(max)',
 'VERDATUM': 'varchar(max)'},
    kind=kind.FullKind,
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
		CAST(ANST_ID AS VARCHAR(MAX)) AS anst_id,
		CAST(ANTMÅN_V AS VARCHAR(MAX)) AS antmån_v,
		CAST(BELOPP_V AS VARCHAR(MAX)) AS belopp_v,
		CAST(DATUM_FOM AS VARCHAR(MAX)) AS datum_fom,
		CAST(DATUM_TOM AS VARCHAR(MAX)) AS datum_tom,
		CAST(ERSGR_ID AS VARCHAR(MAX)) AS ersgr_id,
		CAST(KST_ID AS VARCHAR(MAX)) AS kst_id,
		CAST(LÖNEÖK_V AS VARCHAR(MAX)) AS löneök_v,
		CAST(OMF_V AS VARCHAR(MAX)) AS omf_v,
		CAST(PROJ_ID AS VARCHAR(MAX)) AS proj_id,
		CAST(TOTBEL_V AS VARCHAR(MAX)) AS totbel_v,
		CAST(UTILITY AS VARCHAR(MAX)) AS utility,
		CAST(VERDATUM AS VARCHAR(MAX)) AS verdatum 
	FROM utdata.utdata295.EK_FAKTA_VARDE_PERS
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
