
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'DI_GILTIG_FOM': 'varchar(max)', 'DI_GILTIG_TOM': 'varchar(max)', 'DI_ID': 'varchar(max)', 'DI_ID_TEXT': 'varchar(max)', 'DI_PASSIV': 'varchar(max)', 'DI_TEXT': 'varchar(max)', 'KL_GILTIG_FOM': 'varchar(max)', 'KL_GILTIG_TOM': 'varchar(max)', 'KL_ID': 'varchar(max)', 'KL_ID_TEXT': 'varchar(max)', 'KL_PASSIV': 'varchar(max)', 'KL_TEXT': 'varchar(max)', 'KST_GILTIG_FOM': 'varchar(max)', 'KST_GILTIG_TOM': 'varchar(max)', 'KST_ID': 'varchar(max)', 'KST_ID_TEXT': 'varchar(max)', 'KST_PASSIV': 'varchar(max)', 'KST_TEXT': 'varchar(max)', 'SEK_GILTIG_FOM': 'varchar(max)', 'SEK_GILTIG_TOM': 'varchar(max)', 'SEK_ID': 'varchar(max)', 'SEK_ID_TEXT': 'varchar(max)', 'SEK_PASSIV': 'varchar(max)', 'SEK_TEXT': 'varchar(max)', 'VER_GILTIG_FOM': 'varchar(max)', 'VER_GILTIG_TOM': 'varchar(max)', 'VER_ID': 'varchar(max)', 'VER_ID_TEXT': 'varchar(max)', 'VER_PASSIV': 'varchar(max)', 'VER_TEXT': 'varchar(max)'},
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
		CONVERT(varchar(max), DI_GILTIG_FOM, 126) AS di_giltig_fom,
		CONVERT(varchar(max), DI_GILTIG_TOM, 126) AS di_giltig_tom,
		CAST(DI_ID AS VARCHAR(MAX)) AS di_id,
		CAST(DI_ID_TEXT AS VARCHAR(MAX)) AS di_id_text,
		CAST(DI_PASSIV AS VARCHAR(MAX)) AS di_passiv,
		CAST(DI_TEXT AS VARCHAR(MAX)) AS di_text,
		CONVERT(varchar(max), KL_GILTIG_FOM, 126) AS kl_giltig_fom,
		CONVERT(varchar(max), KL_GILTIG_TOM, 126) AS kl_giltig_tom,
		CAST(KL_ID AS VARCHAR(MAX)) AS kl_id,
		CAST(KL_ID_TEXT AS VARCHAR(MAX)) AS kl_id_text,
		CAST(KL_PASSIV AS VARCHAR(MAX)) AS kl_passiv,
		CAST(KL_TEXT AS VARCHAR(MAX)) AS kl_text,
		CONVERT(varchar(max), KST_GILTIG_FOM, 126) AS kst_giltig_fom,
		CONVERT(varchar(max), KST_GILTIG_TOM, 126) AS kst_giltig_tom,
		CAST(KST_ID AS VARCHAR(MAX)) AS kst_id,
		CAST(KST_ID_TEXT AS VARCHAR(MAX)) AS kst_id_text,
		CAST(KST_PASSIV AS VARCHAR(MAX)) AS kst_passiv,
		CAST(KST_TEXT AS VARCHAR(MAX)) AS kst_text,
		CONVERT(varchar(max), SEK_GILTIG_FOM, 126) AS sek_giltig_fom,
		CONVERT(varchar(max), SEK_GILTIG_TOM, 126) AS sek_giltig_tom,
		CAST(SEK_ID AS VARCHAR(MAX)) AS sek_id,
		CAST(SEK_ID_TEXT AS VARCHAR(MAX)) AS sek_id_text,
		CAST(SEK_PASSIV AS VARCHAR(MAX)) AS sek_passiv,
		CAST(SEK_TEXT AS VARCHAR(MAX)) AS sek_text,
		CONVERT(varchar(max), VER_GILTIG_FOM, 126) AS ver_giltig_fom,
		CONVERT(varchar(max), VER_GILTIG_TOM, 126) AS ver_giltig_tom,
		CAST(VER_ID AS VARCHAR(MAX)) AS ver_id,
		CAST(VER_ID_TEXT AS VARCHAR(MAX)) AS ver_id_text,
		CAST(VER_PASSIV AS VARCHAR(MAX)) AS ver_passiv,
		CAST(VER_TEXT AS VARCHAR(MAX)) AS ver_text 
	FROM steudp.udp_600.EK_DIM_OBJ_KST ) y

	"""
    return read(query=query, server_url="step.rd.sll.se")
    