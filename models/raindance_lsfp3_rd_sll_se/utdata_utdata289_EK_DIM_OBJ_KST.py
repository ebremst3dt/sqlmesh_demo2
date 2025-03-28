
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'AVD_GILTIG_FOM': 'varchar(max)', 'AVD_GILTIG_TOM': 'varchar(max)', 'AVD_ID': 'varchar(max)', 'AVD_ID_TEXT': 'varchar(max)', 'AVD_PASSIV': 'varchar(max)', 'AVD_TEXT': 'varchar(max)', 'DIR_GILTIG_FOM': 'varchar(max)', 'DIR_GILTIG_TOM': 'varchar(max)', 'DIR_ID': 'varchar(max)', 'DIR_ID_TEXT': 'varchar(max)', 'DIR_PASSIV': 'varchar(max)', 'DIR_TEXT': 'varchar(max)', 'ENHET_GILTIG_FOM': 'varchar(max)', 'ENHET_GILTIG_TOM': 'varchar(max)', 'ENHET_ID': 'varchar(max)', 'ENHET_ID_TEXT': 'varchar(max)', 'ENHET_PASSIV': 'varchar(max)', 'ENHET_TEXT': 'varchar(max)', 'KST_GILTIG_FOM': 'varchar(max)', 'KST_GILTIG_TOM': 'varchar(max)', 'KST_ID': 'varchar(max)', 'KST_ID_TEXT': 'varchar(max)', 'KST_PASSIV': 'varchar(max)', 'KST_TEXT': 'varchar(max)', 'SEKT_GILTIG_FOM': 'varchar(max)', 'SEKT_GILTIG_TOM': 'varchar(max)', 'SEKT_ID': 'varchar(max)', 'SEKT_ID_TEXT': 'varchar(max)', 'SEKT_PASSIV': 'varchar(max)', 'SEKT_TEXT': 'varchar(max)'},
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
		'lsfp3_rd_sll_se_utdata_utdata289' as _source,
		CONVERT(varchar(max), AVD_GILTIG_FOM, 126) AS AVD_GILTIG_FOM,
		CONVERT(varchar(max), AVD_GILTIG_TOM, 126) AS AVD_GILTIG_TOM,
		CAST(AVD_ID AS VARCHAR(MAX)) AS AVD_ID,
		CAST(AVD_ID_TEXT AS VARCHAR(MAX)) AS AVD_ID_TEXT,
		CAST(AVD_PASSIV AS VARCHAR(MAX)) AS AVD_PASSIV,
		CAST(AVD_TEXT AS VARCHAR(MAX)) AS AVD_TEXT,
		CONVERT(varchar(max), DIR_GILTIG_FOM, 126) AS DIR_GILTIG_FOM,
		CONVERT(varchar(max), DIR_GILTIG_TOM, 126) AS DIR_GILTIG_TOM,
		CAST(DIR_ID AS VARCHAR(MAX)) AS DIR_ID,
		CAST(DIR_ID_TEXT AS VARCHAR(MAX)) AS DIR_ID_TEXT,
		CAST(DIR_PASSIV AS VARCHAR(MAX)) AS DIR_PASSIV,
		CAST(DIR_TEXT AS VARCHAR(MAX)) AS DIR_TEXT,
		CONVERT(varchar(max), ENHET_GILTIG_FOM, 126) AS ENHET_GILTIG_FOM,
		CONVERT(varchar(max), ENHET_GILTIG_TOM, 126) AS ENHET_GILTIG_TOM,
		CAST(ENHET_ID AS VARCHAR(MAX)) AS ENHET_ID,
		CAST(ENHET_ID_TEXT AS VARCHAR(MAX)) AS ENHET_ID_TEXT,
		CAST(ENHET_PASSIV AS VARCHAR(MAX)) AS ENHET_PASSIV,
		CAST(ENHET_TEXT AS VARCHAR(MAX)) AS ENHET_TEXT,
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
		CAST(SEKT_TEXT AS VARCHAR(MAX)) AS SEKT_TEXT 
	FROM utdata.utdata289.EK_DIM_OBJ_KST ) y

	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
    