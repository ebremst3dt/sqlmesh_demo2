
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
 'ANST_ID': 'varchar(20)',
 'ANTMÅN_V': 'numeric',
 'BELOPP_V': 'numeric',
 'DATUM_FOM': 'datetime',
 'DATUM_TOM': 'datetime',
 'ERSGR_ID': 'varchar(3)',
 'KST_ID': 'varchar(5)',
 'LÖNEÖK_V': 'numeric',
 'OMF_V': 'numeric',
 'PROJ_ID': 'varchar(5)',
 'TOTBEL_V': 'numeric',
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
		CAST(ANST_ID AS VARCHAR(MAX)) AS anst_id,
		CAST(ANTMÅN_V AS VARCHAR(MAX)) AS antmån_v,
		CAST(BELOPP_V AS VARCHAR(MAX)) AS belopp_v,
		CONVERT(varchar(max), DATUM_FOM, 126) AS datum_fom,
		CONVERT(varchar(max), DATUM_TOM, 126) AS datum_tom,
		CAST(ERSGR_ID AS VARCHAR(MAX)) AS ersgr_id,
		CAST(KST_ID AS VARCHAR(MAX)) AS kst_id,
		CAST(LÖNEÖK_V AS VARCHAR(MAX)) AS löneök_v,
		CAST(OMF_V AS VARCHAR(MAX)) AS omf_v,
		CAST(PROJ_ID AS VARCHAR(MAX)) AS proj_id,
		CAST(TOTBEL_V AS VARCHAR(MAX)) AS totbel_v,
		CAST(UTILITY AS VARCHAR(MAX)) AS utility,
		CONVERT(varchar(max), VERDATUM, 126) AS verdatum 
	FROM utdata.utdata295.EK_FAKTA_VARDE_PERS
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
