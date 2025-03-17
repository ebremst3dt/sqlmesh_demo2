
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'FTG_GILTIG_FOM': 'varchar(max)', 'FTG_GILTIG_TOM': 'varchar(max)', 'FTG_ID': 'varchar(max)', 'FTG_ID_TEXT': 'varchar(max)', 'FTG_PASSIV': 'varchar(max)', 'FTG_TEXT': 'varchar(max)', 'KST_GILTIG_FOM': 'varchar(max)', 'KST_GILTIG_TOM': 'varchar(max)', 'KST_ID': 'varchar(max)', 'KST_ID_TEXT': 'varchar(max)', 'KST_PASSIV': 'varchar(max)', 'KST_TEXT': 'varchar(max)', 'OMR_GILTIG_FOM': 'varchar(max)', 'OMR_GILTIG_TOM': 'varchar(max)', 'OMR_ID': 'varchar(max)', 'OMR_ID_TEXT': 'varchar(max)', 'OMR_PASSIV': 'varchar(max)', 'OMR_TEXT': 'varchar(max)', 'VERKS_GILTIG_FOM': 'varchar(max)', 'VERKS_GILTIG_TOM': 'varchar(max)', 'VERKS_ID': 'varchar(max)', 'VERKS_ID_TEXT': 'varchar(max)', 'VERKS_PASSIV': 'varchar(max)', 'VERKS_TEXT': 'varchar(max)'},
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
		'nksp_rd_sll_se_raindance_udp_udp_100' as _source,
		CONVERT(varchar(max), FTG_GILTIG_FOM, 126) AS ftg_giltig_fom,
		CONVERT(varchar(max), FTG_GILTIG_TOM, 126) AS ftg_giltig_tom,
		CAST(FTG_ID AS VARCHAR(MAX)) AS ftg_id,
		CAST(FTG_ID_TEXT AS VARCHAR(MAX)) AS ftg_id_text,
		CAST(FTG_PASSIV AS VARCHAR(MAX)) AS ftg_passiv,
		CAST(FTG_TEXT AS VARCHAR(MAX)) AS ftg_text,
		CONVERT(varchar(max), KST_GILTIG_FOM, 126) AS kst_giltig_fom,
		CONVERT(varchar(max), KST_GILTIG_TOM, 126) AS kst_giltig_tom,
		CAST(KST_ID AS VARCHAR(MAX)) AS kst_id,
		CAST(KST_ID_TEXT AS VARCHAR(MAX)) AS kst_id_text,
		CAST(KST_PASSIV AS VARCHAR(MAX)) AS kst_passiv,
		CAST(KST_TEXT AS VARCHAR(MAX)) AS kst_text,
		CONVERT(varchar(max), OMR_GILTIG_FOM, 126) AS omr_giltig_fom,
		CONVERT(varchar(max), OMR_GILTIG_TOM, 126) AS omr_giltig_tom,
		CAST(OMR_ID AS VARCHAR(MAX)) AS omr_id,
		CAST(OMR_ID_TEXT AS VARCHAR(MAX)) AS omr_id_text,
		CAST(OMR_PASSIV AS VARCHAR(MAX)) AS omr_passiv,
		CAST(OMR_TEXT AS VARCHAR(MAX)) AS omr_text,
		CONVERT(varchar(max), VERKS_GILTIG_FOM, 126) AS verks_giltig_fom,
		CONVERT(varchar(max), VERKS_GILTIG_TOM, 126) AS verks_giltig_tom,
		CAST(VERKS_ID AS VARCHAR(MAX)) AS verks_id,
		CAST(VERKS_ID_TEXT AS VARCHAR(MAX)) AS verks_id_text,
		CAST(VERKS_PASSIV AS VARCHAR(MAX)) AS verks_passiv,
		CAST(VERKS_TEXT AS VARCHAR(MAX)) AS verks_text 
	FROM raindance_udp.udp_100.EK_DIM_OBJ_KST ) y

	"""
    return read(query=query, server_url="nksp.rd.sll.se")
    