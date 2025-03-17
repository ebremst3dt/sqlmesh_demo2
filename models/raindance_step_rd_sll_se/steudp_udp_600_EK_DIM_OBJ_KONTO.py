
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
		CONVERT(varchar(max), K1_GILTIG_FOM, 126) AS k1_giltig_fom,
		CONVERT(varchar(max), K1_GILTIG_TOM, 126) AS k1_giltig_tom,
		CAST(K1_ID AS VARCHAR(MAX)) AS k1_id,
		CAST(K1_ID_TEXT AS VARCHAR(MAX)) AS k1_id_text,
		CAST(K1_PASSIV AS VARCHAR(MAX)) AS k1_passiv,
		CAST(K1_TEXT AS VARCHAR(MAX)) AS k1_text,
		CONVERT(varchar(max), K2_GILTIG_FOM, 126) AS k2_giltig_fom,
		CONVERT(varchar(max), K2_GILTIG_TOM, 126) AS k2_giltig_tom,
		CAST(K2_ID AS VARCHAR(MAX)) AS k2_id,
		CAST(K2_ID_TEXT AS VARCHAR(MAX)) AS k2_id_text,
		CAST(K2_PASSIV AS VARCHAR(MAX)) AS k2_passiv,
		CAST(K2_TEXT AS VARCHAR(MAX)) AS k2_text,
		CONVERT(varchar(max), KONTO_GILTIG_FOM, 126) AS konto_giltig_fom,
		CONVERT(varchar(max), KONTO_GILTIG_TOM, 126) AS konto_giltig_tom,
		CAST(KONTO_ID AS VARCHAR(MAX)) AS konto_id,
		CAST(KONTO_ID_TEXT AS VARCHAR(MAX)) AS konto_id_text,
		CAST(KONTO_PASSIV AS VARCHAR(MAX)) AS konto_passiv,
		CAST(KONTO_TEXT AS VARCHAR(MAX)) AS konto_text,
		CONVERT(varchar(max), SLLKTO_GILTIG_FOM, 126) AS sllkto_giltig_fom,
		CONVERT(varchar(max), SLLKTO_GILTIG_TOM, 126) AS sllkto_giltig_tom,
		CAST(SLLKTO_ID AS VARCHAR(MAX)) AS sllkto_id,
		CAST(SLLKTO_ID_TEXT AS VARCHAR(MAX)) AS sllkto_id_text,
		CAST(SLLKTO_PASSIV AS VARCHAR(MAX)) AS sllkto_passiv,
		CAST(SLLKTO_TEXT AS VARCHAR(MAX)) AS sllkto_text,
		CONVERT(varchar(max), ÅRL_GILTIG_FOM, 126) AS årl_giltig_fom,
		CONVERT(varchar(max), ÅRL_GILTIG_TOM, 126) AS årl_giltig_tom,
		CAST(ÅRL_ID AS VARCHAR(MAX)) AS årl_id,
		CAST(ÅRL_ID_TEXT AS VARCHAR(MAX)) AS årl_id_text,
		CAST(ÅRL_PASSIV AS VARCHAR(MAX)) AS årl_passiv,
		CAST(ÅRL_TEXT AS VARCHAR(MAX)) AS årl_text 
	FROM steudp.udp_600.EK_DIM_OBJ_KONTO ) y

	"""
    return read(query=query, server_url="step.rd.sll.se")
    