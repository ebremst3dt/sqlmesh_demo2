
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'K1_GILTIG_FOM': 'varchar(max)', 'K1_GILTIG_TOM': 'varchar(max)', 'K1_ID': 'varchar(max)', 'K1_ID_TEXT': 'varchar(max)', 'K1_PASSIV': 'varchar(max)', 'K1_TEXT': 'varchar(max)', 'K2_GILTIG_FOM': 'varchar(max)', 'K2_GILTIG_TOM': 'varchar(max)', 'K2_ID': 'varchar(max)', 'K2_ID_TEXT': 'varchar(max)', 'K2_PASSIV': 'varchar(max)', 'K2_TEXT': 'varchar(max)', 'KONTO_GILTIG_FOM': 'varchar(max)', 'KONTO_GILTIG_TOM': 'varchar(max)', 'KONTO_ID': 'varchar(max)', 'KONTO_ID_TEXT': 'varchar(max)', 'KONTO_PASSIV': 'varchar(max)', 'KONTO_TEXT': 'varchar(max)', 'SLLKTO_GILTIG_FOM': 'varchar(max)', 'SLLKTO_GILTIG_TOM': 'varchar(max)', 'SLLKTO_ID': 'varchar(max)', 'SLLKTO_ID_TEXT': 'varchar(max)', 'SLLKTO_PASSIV': 'varchar(max)', 'SLLKTO_TEXT': 'varchar(max)', 'ÅRL_GILTIG_FOM': 'varchar(max)', 'ÅRL_GILTIG_TOM': 'varchar(max)', 'ÅRL_ID': 'varchar(max)', 'ÅRL_ID_TEXT': 'varchar(max)', 'ÅRL_PASSIV': 'varchar(max)', 'ÅRL_TEXT': 'varchar(max)'},
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
    query = f"""
	SELECT * FROM (SELECT 
 		CAST(CAST(GETDATE() AS datetime2) AT TIME ZONE 'CENTRAL EUROPEAN STANDARD TIME' AT TIME ZONE 'UTC' AS datetime2) as _data_modified_utc,
		CAST(CAST(GETDATE() AS datetime2) AT TIME ZONE 'CENTRAL EUROPEAN STANDARD TIME' AT TIME ZONE 'UTC' AS datetime2) as _metadata_modified_utc,
		'step_rd_sll_se_steudp_udp_600' as _source,
		CONVERT(varchar(max), K1_GILTIG_FOM, 126) AS K1_GILTIG_FOM,
		CONVERT(varchar(max), K1_GILTIG_TOM, 126) AS K1_GILTIG_TOM,
		CAST(K1_ID AS VARCHAR(MAX)) AS K1_ID,
		CAST(K1_ID_TEXT AS VARCHAR(MAX)) AS K1_ID_TEXT,
		CAST(K1_PASSIV AS VARCHAR(MAX)) AS K1_PASSIV,
		CAST(K1_TEXT AS VARCHAR(MAX)) AS K1_TEXT,
		CONVERT(varchar(max), K2_GILTIG_FOM, 126) AS K2_GILTIG_FOM,
		CONVERT(varchar(max), K2_GILTIG_TOM, 126) AS K2_GILTIG_TOM,
		CAST(K2_ID AS VARCHAR(MAX)) AS K2_ID,
		CAST(K2_ID_TEXT AS VARCHAR(MAX)) AS K2_ID_TEXT,
		CAST(K2_PASSIV AS VARCHAR(MAX)) AS K2_PASSIV,
		CAST(K2_TEXT AS VARCHAR(MAX)) AS K2_TEXT,
		CONVERT(varchar(max), KONTO_GILTIG_FOM, 126) AS KONTO_GILTIG_FOM,
		CONVERT(varchar(max), KONTO_GILTIG_TOM, 126) AS KONTO_GILTIG_TOM,
		CAST(KONTO_ID AS VARCHAR(MAX)) AS KONTO_ID,
		CAST(KONTO_ID_TEXT AS VARCHAR(MAX)) AS KONTO_ID_TEXT,
		CAST(KONTO_PASSIV AS VARCHAR(MAX)) AS KONTO_PASSIV,
		CAST(KONTO_TEXT AS VARCHAR(MAX)) AS KONTO_TEXT,
		CONVERT(varchar(max), SLLKTO_GILTIG_FOM, 126) AS SLLKTO_GILTIG_FOM,
		CONVERT(varchar(max), SLLKTO_GILTIG_TOM, 126) AS SLLKTO_GILTIG_TOM,
		CAST(SLLKTO_ID AS VARCHAR(MAX)) AS SLLKTO_ID,
		CAST(SLLKTO_ID_TEXT AS VARCHAR(MAX)) AS SLLKTO_ID_TEXT,
		CAST(SLLKTO_PASSIV AS VARCHAR(MAX)) AS SLLKTO_PASSIV,
		CAST(SLLKTO_TEXT AS VARCHAR(MAX)) AS SLLKTO_TEXT,
		CONVERT(varchar(max), ÅRL_GILTIG_FOM, 126) AS ÅRL_GILTIG_FOM,
		CONVERT(varchar(max), ÅRL_GILTIG_TOM, 126) AS ÅRL_GILTIG_TOM,
		CAST(ÅRL_ID AS VARCHAR(MAX)) AS ÅRL_ID,
		CAST(ÅRL_ID_TEXT AS VARCHAR(MAX)) AS ÅRL_ID_TEXT,
		CAST(ÅRL_PASSIV AS VARCHAR(MAX)) AS ÅRL_PASSIV,
		CAST(ÅRL_TEXT AS VARCHAR(MAX)) AS ÅRL_TEXT 
	FROM steudp.udp_600.EK_DIM_OBJ_KONTO ) y

	"""
    return read(query=query, server_url="step.rd.sll.se")
    