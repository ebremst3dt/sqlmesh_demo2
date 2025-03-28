
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
		CONVERT(varchar(max), FTG_GILTIG_FOM, 126) AS FTG_GILTIG_FOM,
		CONVERT(varchar(max), FTG_GILTIG_TOM, 126) AS FTG_GILTIG_TOM,
		CAST(FTG_ID AS VARCHAR(MAX)) AS FTG_ID,
		CAST(FTG_ID_TEXT AS VARCHAR(MAX)) AS FTG_ID_TEXT,
		CAST(FTG_PASSIV AS VARCHAR(MAX)) AS FTG_PASSIV,
		CAST(FTG_TEXT AS VARCHAR(MAX)) AS FTG_TEXT,
		CONVERT(varchar(max), KST_GILTIG_FOM, 126) AS KST_GILTIG_FOM,
		CONVERT(varchar(max), KST_GILTIG_TOM, 126) AS KST_GILTIG_TOM,
		CAST(KST_ID AS VARCHAR(MAX)) AS KST_ID,
		CAST(KST_ID_TEXT AS VARCHAR(MAX)) AS KST_ID_TEXT,
		CAST(KST_PASSIV AS VARCHAR(MAX)) AS KST_PASSIV,
		CAST(KST_TEXT AS VARCHAR(MAX)) AS KST_TEXT,
		CONVERT(varchar(max), OMR_GILTIG_FOM, 126) AS OMR_GILTIG_FOM,
		CONVERT(varchar(max), OMR_GILTIG_TOM, 126) AS OMR_GILTIG_TOM,
		CAST(OMR_ID AS VARCHAR(MAX)) AS OMR_ID,
		CAST(OMR_ID_TEXT AS VARCHAR(MAX)) AS OMR_ID_TEXT,
		CAST(OMR_PASSIV AS VARCHAR(MAX)) AS OMR_PASSIV,
		CAST(OMR_TEXT AS VARCHAR(MAX)) AS OMR_TEXT,
		CONVERT(varchar(max), VERKS_GILTIG_FOM, 126) AS VERKS_GILTIG_FOM,
		CONVERT(varchar(max), VERKS_GILTIG_TOM, 126) AS VERKS_GILTIG_TOM,
		CAST(VERKS_ID AS VARCHAR(MAX)) AS VERKS_ID,
		CAST(VERKS_ID_TEXT AS VARCHAR(MAX)) AS VERKS_ID_TEXT,
		CAST(VERKS_PASSIV AS VARCHAR(MAX)) AS VERKS_PASSIV,
		CAST(VERKS_TEXT AS VARCHAR(MAX)) AS VERKS_TEXT 
	FROM raindance_udp.udp_100.EK_DIM_OBJ_KST ) y

	"""
    return read(query=query, server_url="nksp.rd.sll.se")
    