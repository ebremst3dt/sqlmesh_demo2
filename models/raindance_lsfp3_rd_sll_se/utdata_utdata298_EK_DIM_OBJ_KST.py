
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'AVDEL_GILTIG_FOM': 'varchar(max)', 'AVDEL_GILTIG_TOM': 'varchar(max)', 'AVDEL_ID': 'varchar(max)', 'AVDEL_ID_TEXT': 'varchar(max)', 'AVDEL_PASSIV': 'varchar(max)', 'AVDEL_TEXT': 'varchar(max)', 'AVD_GILTIG_FOM': 'varchar(max)', 'AVD_GILTIG_TOM': 'varchar(max)', 'AVD_ID': 'varchar(max)', 'AVD_ID_TEXT': 'varchar(max)', 'AVD_PASSIV': 'varchar(max)', 'AVD_TEXT': 'varchar(max)', 'ENHET_GILTIG_FOM': 'varchar(max)', 'ENHET_GILTIG_TOM': 'varchar(max)', 'ENHET_ID': 'varchar(max)', 'ENHET_ID_TEXT': 'varchar(max)', 'ENHET_PASSIV': 'varchar(max)', 'ENHET_TEXT': 'varchar(max)', 'ENH_GILTIG_FOM': 'varchar(max)', 'ENH_GILTIG_TOM': 'varchar(max)', 'ENH_ID': 'varchar(max)', 'ENH_ID_TEXT': 'varchar(max)', 'ENH_PASSIV': 'varchar(max)', 'ENH_TEXT': 'varchar(max)', 'KST_GILTIG_FOM': 'varchar(max)', 'KST_GILTIG_TOM': 'varchar(max)', 'KST_ID': 'varchar(max)', 'KST_ID_TEXT': 'varchar(max)', 'KST_PASSIV': 'varchar(max)', 'KST_TEXT': 'varchar(max)', 'SEKT_GILTIG_FOM': 'varchar(max)', 'SEKT_GILTIG_TOM': 'varchar(max)', 'SEKT_ID': 'varchar(max)', 'SEKT_ID_TEXT': 'varchar(max)', 'SEKT_PASSIV': 'varchar(max)', 'SEKT_TEXT': 'varchar(max)', 'VO_GILTIG_FOM': 'varchar(max)', 'VO_GILTIG_TOM': 'varchar(max)', 'VO_ID': 'varchar(max)', 'VO_ID_TEXT': 'varchar(max)', 'VO_PASSIV': 'varchar(max)', 'VO_TEXT': 'varchar(max)'},
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
		'lsfp3_rd_sll_se_utdata_utdata298' as _source,
		CONVERT(varchar(max), AVDEL_GILTIG_FOM, 126) AS AVDEL_GILTIG_FOM,
		CONVERT(varchar(max), AVDEL_GILTIG_TOM, 126) AS AVDEL_GILTIG_TOM,
		CAST(AVDEL_ID AS VARCHAR(MAX)) AS AVDEL_ID,
		CAST(AVDEL_ID_TEXT AS VARCHAR(MAX)) AS AVDEL_ID_TEXT,
		CAST(AVDEL_PASSIV AS VARCHAR(MAX)) AS AVDEL_PASSIV,
		CAST(AVDEL_TEXT AS VARCHAR(MAX)) AS AVDEL_TEXT,
		CONVERT(varchar(max), AVD_GILTIG_FOM, 126) AS AVD_GILTIG_FOM,
		CONVERT(varchar(max), AVD_GILTIG_TOM, 126) AS AVD_GILTIG_TOM,
		CAST(AVD_ID AS VARCHAR(MAX)) AS AVD_ID,
		CAST(AVD_ID_TEXT AS VARCHAR(MAX)) AS AVD_ID_TEXT,
		CAST(AVD_PASSIV AS VARCHAR(MAX)) AS AVD_PASSIV,
		CAST(AVD_TEXT AS VARCHAR(MAX)) AS AVD_TEXT,
		CONVERT(varchar(max), ENHET_GILTIG_FOM, 126) AS ENHET_GILTIG_FOM,
		CONVERT(varchar(max), ENHET_GILTIG_TOM, 126) AS ENHET_GILTIG_TOM,
		CAST(ENHET_ID AS VARCHAR(MAX)) AS ENHET_ID,
		CAST(ENHET_ID_TEXT AS VARCHAR(MAX)) AS ENHET_ID_TEXT,
		CAST(ENHET_PASSIV AS VARCHAR(MAX)) AS ENHET_PASSIV,
		CAST(ENHET_TEXT AS VARCHAR(MAX)) AS ENHET_TEXT,
		CONVERT(varchar(max), ENH_GILTIG_FOM, 126) AS ENH_GILTIG_FOM,
		CONVERT(varchar(max), ENH_GILTIG_TOM, 126) AS ENH_GILTIG_TOM,
		CAST(ENH_ID AS VARCHAR(MAX)) AS ENH_ID,
		CAST(ENH_ID_TEXT AS VARCHAR(MAX)) AS ENH_ID_TEXT,
		CAST(ENH_PASSIV AS VARCHAR(MAX)) AS ENH_PASSIV,
		CAST(ENH_TEXT AS VARCHAR(MAX)) AS ENH_TEXT,
		CONVERT(varchar(max), KST_GILTIG_FOM, 126) AS KST_GILTIG_FOM,
		CONVERT(varchar(max), KST_GILTIG_TOM, 126) AS KST_GILTIG_TOM,
		CAST(KST_ID AS VARCHAR(MAX)) AS KST_ID,
		CAST(KST_ID_TEXT AS VARCHAR(MAX)) AS KST_ID_TEXT,
		CAST(KST_PASSIV AS VARCHAR(MAX)) AS KST_PASSIV,
		CAST(KST_TEXT AS VARCHAR(MAX)) AS KST_TEXT,
		CONVERT(varchar(max), SEKT_GILTIG_FOM, 126) AS SEKT_GILTIG_FOM,
		CONVERT(varchar(max), SEKT_GILTIG_TOM, 126) AS SEKT_GILTIG_TOM,
		CAST(SEKT_ID AS VARCHAR(MAX)) AS SEKT_ID,
		CAST(SEKT_ID_TEXT AS VARCHAR(MAX)) AS SEKT_ID_TEXT,
		CAST(SEKT_PASSIV AS VARCHAR(MAX)) AS SEKT_PASSIV,
		CAST(SEKT_TEXT AS VARCHAR(MAX)) AS SEKT_TEXT,
		CONVERT(varchar(max), VO_GILTIG_FOM, 126) AS VO_GILTIG_FOM,
		CONVERT(varchar(max), VO_GILTIG_TOM, 126) AS VO_GILTIG_TOM,
		CAST(VO_ID AS VARCHAR(MAX)) AS VO_ID,
		CAST(VO_ID_TEXT AS VARCHAR(MAX)) AS VO_ID_TEXT,
		CAST(VO_PASSIV AS VARCHAR(MAX)) AS VO_PASSIV,
		CAST(VO_TEXT AS VARCHAR(MAX)) AS VO_TEXT 
	FROM utdata.utdata298.EK_DIM_OBJ_KST ) y

	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
    