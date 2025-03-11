
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'ANDRAD_AV': 'varchar(max)', 'ANDRAD_DATUM': 'varchar(max)', 'ANDRAD_TID': 'varchar(max)', 'BOANTP_ID': 'varchar(max)', 'DATUM_FOM': 'varchar(max)', 'DATUM_TOM': 'varchar(max)', 'KONTO_ID': 'varchar(max)', 'KTOAV_TEXT': 'varchar(max)', 'UTILITY': 'varchar(max)', 'VERDATUM': 'varchar(max)', 'X_V': 'varchar(max)'},
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
	SELECT TOP 10 * FROM (SELECT 
 		CAST(CAST(GETDATE() AS datetime2) AT TIME ZONE 'CENTRAL EUROPEAN STANDARD TIME' AT TIME ZONE 'UTC' AS datetime2) as _data_modified_utc,
		CAST(CAST(GETDATE() AS datetime2) AT TIME ZONE 'CENTRAL EUROPEAN STANDARD TIME' AT TIME ZONE 'UTC' AS datetime2) as _metadata_modified_utc,
		'lsfp3_rd_sll_se_utdata_utdata295' as _source,
		CAST(ANDRAD_AV AS VARCHAR(MAX)) AS andrad_av,
		CONVERT(varchar(max), ANDRAD_DATUM, 126) AS andrad_datum,
		CAST(ANDRAD_TID AS VARCHAR(MAX)) AS andrad_tid,
		CAST(BOANTP_ID AS VARCHAR(MAX)) AS boantp_id,
		CONVERT(varchar(max), DATUM_FOM, 126) AS datum_fom,
		CONVERT(varchar(max), DATUM_TOM, 126) AS datum_tom,
		CAST(KONTO_ID AS VARCHAR(MAX)) AS konto_id,
		CAST(KTOAV_TEXT AS VARCHAR(MAX)) AS ktoav_text,
		CAST(UTILITY AS VARCHAR(MAX)) AS utility,
		CONVERT(varchar(max), VERDATUM, 126) AS verdatum,
		CAST(X_V AS VARCHAR(MAX)) AS x_v 
	FROM utdata.utdata295.EK_FAKTA_VARDE_BOANTP) y

	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
    