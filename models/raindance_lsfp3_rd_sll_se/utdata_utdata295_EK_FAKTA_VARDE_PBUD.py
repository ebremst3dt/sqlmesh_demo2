
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', 'ANDRAD_AV': 'varchar(max)', 'ANDRAD_DATUM': 'varchar(max)', 'ANDRAD_TID': 'varchar(max)', 'ANM_TEXT': 'varchar(max)', 'ANST_ID': 'varchar(max)', 'ANTMÅN_V': 'varchar(max)', 'BUDHEL_V': 'varchar(max)', 'DATUM_FOM': 'varchar(max)', 'DATUM_TOM': 'varchar(max)', 'KONTO_ID': 'varchar(max)', 'KST_ID': 'varchar(max)', 'LÖNETI_V': 'varchar(max)', 'LÖNEÖK_V': 'varchar(max)', 'LÖN_V': 'varchar(max)', 'OMF_V': 'varchar(max)', 'SEMERS_V': 'varchar(max)', 'UTILITY': 'varchar(max)', 'VERDATUM': 'varchar(max)'},
    kind=dict(
        name=ModelKindName.FULL
    ),
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
	SELECT * FROM (SELECT 
 		CAST(CAST(GETDATE() AS datetime2) AT TIME ZONE 'CENTRAL EUROPEAN STANDARD TIME' AT TIME ZONE 'UTC' AS datetime2) as _data_modified_utc,
		CAST(CAST(GETDATE() AS datetime2) AT TIME ZONE 'CENTRAL EUROPEAN STANDARD TIME' AT TIME ZONE 'UTC' AS datetime2) as _metadata_modified_utc,
		'lsfp3_rd_sll_se_utdata_utdata295' as _source,
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
		CAST(LÖNETI_V AS VARCHAR(MAX)) AS löneti_v,
		CAST(LÖNEÖK_V AS VARCHAR(MAX)) AS löneök_v,
		CAST(LÖN_V AS VARCHAR(MAX)) AS lön_v,
		CAST(OMF_V AS VARCHAR(MAX)) AS omf_v,
		CAST(SEMERS_V AS VARCHAR(MAX)) AS semers_v,
		CAST(UTILITY AS VARCHAR(MAX)) AS utility,
		CONVERT(varchar(max), VERDATUM, 126) AS verdatum 
	FROM utdata.utdata295.EK_FAKTA_VARDE_PBUD

	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
    