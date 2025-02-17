
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
 'ANM_TEXT': 'varchar(max)',
 'ANST_ID': 'varchar(max)',
 'ANTMÅN_V': 'varchar(max)',
 'BUDHEL_V': 'varchar(max)',
 'DATUM_FOM': 'varchar(max)',
 'DATUM_TOM': 'varchar(max)',
 'KONTO_ID': 'varchar(max)',
 'KST_ID': 'varchar(max)',
 'LÖNETI_V': 'varchar(max)',
 'LÖNEÖK_V': 'varchar(max)',
 'LÖN_V': 'varchar(max)',
 'OMF_V': 'varchar(max)',
 'SEMERS_V': 'varchar(max)',
 'TIM_V': 'varchar(max)',
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
		CONVERT(varchar(max), ANDRAD_DATUM, 126) AS andrad_datum,
		CAST(ANDRAD_TID AS VARCHAR(MAX)) AS andrad_tid,
		CAST(ANM_TEXT AS VARCHAR(MAX)) AS anm_text,
		CAST(ANST_ID AS VARCHAR(MAX)) AS anst_id,
		CAST(ANTMÅN_V AS VARCHAR(MAX)) AS antmån_v,
		CAST(BUDHEL_V AS VARCHAR(MAX)) AS budhel_v,
		CONVERT(varchar(max), DATUM_FOM, 126) AS datum_fom,
		CONVERT(varchar(max), DATUM_TOM, 126) AS datum_tom,
		CAST(KONTO_ID AS VARCHAR(MAX)) AS konto_id,
		CAST(KST_ID AS VARCHAR(MAX)) AS kst_id,
		CAST(LÖN_V AS VARCHAR(MAX)) AS lön_v,
		CAST(LÖNETI_V AS VARCHAR(MAX)) AS löneti_v,
		CAST(LÖNEÖK_V AS VARCHAR(MAX)) AS löneök_v,
		CAST(OMF_V AS VARCHAR(MAX)) AS omf_v,
		CAST(SEMERS_V AS VARCHAR(MAX)) AS semers_v,
		CAST(TIM_V AS VARCHAR(MAX)) AS tim_v,
		CAST(UTILITY AS VARCHAR(MAX)) AS utility,
		CONVERT(varchar(max), VERDATUM, 126) AS verdatum 
	FROM utdata.utdata295.EK_FAKTA_VARDE_PERBUD
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
