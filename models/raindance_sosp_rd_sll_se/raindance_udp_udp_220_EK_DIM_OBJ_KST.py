
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'ANSVAR_GILTIG_FOM': 'varchar(max)', 'ANSVAR_GILTIG_TOM': 'varchar(max)', 'ANSVAR_ID': 'varchar(max)', 'ANSVAR_ID_TEXT': 'varchar(max)', 'ANSVAR_PASSIV': 'varchar(max)', 'ANSVAR_TEXT': 'varchar(max)', 'FOUU_GILTIG_FOM': 'varchar(max)', 'FOUU_GILTIG_TOM': 'varchar(max)', 'FOUU_ID': 'varchar(max)', 'FOUU_ID_TEXT': 'varchar(max)', 'FOUU_PASSIV': 'varchar(max)', 'FOUU_TEXT': 'varchar(max)', 'FUNK_GILTIG_FOM': 'varchar(max)', 'FUNK_GILTIG_TOM': 'varchar(max)', 'FUNK_ID': 'varchar(max)', 'FUNK_ID_TEXT': 'varchar(max)', 'FUNK_PASSIV': 'varchar(max)', 'FUNK_TEXT': 'varchar(max)', 'KSG_GILTIG_FOM': 'varchar(max)', 'KSG_GILTIG_TOM': 'varchar(max)', 'KSG_ID': 'varchar(max)', 'KSG_ID_TEXT': 'varchar(max)', 'KSG_PASSIV': 'varchar(max)', 'KSG_TEXT': 'varchar(max)', 'KST_GILTIG_FOM': 'varchar(max)', 'KST_GILTIG_TOM': 'varchar(max)', 'KST_ID': 'varchar(max)', 'KST_ID_TEXT': 'varchar(max)', 'KST_PASSIV': 'varchar(max)', 'KST_TEXT': 'varchar(max)', 'MKST_GILTIG_FOM': 'varchar(max)', 'MKST_GILTIG_TOM': 'varchar(max)', 'MKST_ID': 'varchar(max)', 'MKST_ID_TEXT': 'varchar(max)', 'MKST_PASSIV': 'varchar(max)', 'MKST_TEXT': 'varchar(max)', 'SLLVGR_GILTIG_FOM': 'varchar(max)', 'SLLVGR_GILTIG_TOM': 'varchar(max)', 'SLLVGR_ID': 'varchar(max)', 'SLLVGR_ID_TEXT': 'varchar(max)', 'SLLVGR_PASSIV': 'varchar(max)', 'SLLVGR_TEXT': 'varchar(max)', 'VKO_GILTIG_FOM': 'varchar(max)', 'VKO_GILTIG_TOM': 'varchar(max)', 'VKO_ID': 'varchar(max)', 'VKO_ID_TEXT': 'varchar(max)', 'VKO_PASSIV': 'varchar(max)', 'VKO_TEXT': 'varchar(max)'},
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
		'sosp_rd_sll_se_raindance_udp_udp_220' as _source,
		CONVERT(varchar(max), ANSVAR_GILTIG_FOM, 126) AS ANSVAR_GILTIG_FOM,
		CONVERT(varchar(max), ANSVAR_GILTIG_TOM, 126) AS ANSVAR_GILTIG_TOM,
		CAST(ANSVAR_ID AS VARCHAR(MAX)) AS ANSVAR_ID,
		CAST(ANSVAR_ID_TEXT AS VARCHAR(MAX)) AS ANSVAR_ID_TEXT,
		CAST(ANSVAR_PASSIV AS VARCHAR(MAX)) AS ANSVAR_PASSIV,
		CAST(ANSVAR_TEXT AS VARCHAR(MAX)) AS ANSVAR_TEXT,
		CONVERT(varchar(max), FOUU_GILTIG_FOM, 126) AS FOUU_GILTIG_FOM,
		CONVERT(varchar(max), FOUU_GILTIG_TOM, 126) AS FOUU_GILTIG_TOM,
		CAST(FOUU_ID AS VARCHAR(MAX)) AS FOUU_ID,
		CAST(FOUU_ID_TEXT AS VARCHAR(MAX)) AS FOUU_ID_TEXT,
		CAST(FOUU_PASSIV AS VARCHAR(MAX)) AS FOUU_PASSIV,
		CAST(FOUU_TEXT AS VARCHAR(MAX)) AS FOUU_TEXT,
		CONVERT(varchar(max), FUNK_GILTIG_FOM, 126) AS FUNK_GILTIG_FOM,
		CONVERT(varchar(max), FUNK_GILTIG_TOM, 126) AS FUNK_GILTIG_TOM,
		CAST(FUNK_ID AS VARCHAR(MAX)) AS FUNK_ID,
		CAST(FUNK_ID_TEXT AS VARCHAR(MAX)) AS FUNK_ID_TEXT,
		CAST(FUNK_PASSIV AS VARCHAR(MAX)) AS FUNK_PASSIV,
		CAST(FUNK_TEXT AS VARCHAR(MAX)) AS FUNK_TEXT,
		CONVERT(varchar(max), KSG_GILTIG_FOM, 126) AS KSG_GILTIG_FOM,
		CONVERT(varchar(max), KSG_GILTIG_TOM, 126) AS KSG_GILTIG_TOM,
		CAST(KSG_ID AS VARCHAR(MAX)) AS KSG_ID,
		CAST(KSG_ID_TEXT AS VARCHAR(MAX)) AS KSG_ID_TEXT,
		CAST(KSG_PASSIV AS VARCHAR(MAX)) AS KSG_PASSIV,
		CAST(KSG_TEXT AS VARCHAR(MAX)) AS KSG_TEXT,
		CONVERT(varchar(max), KST_GILTIG_FOM, 126) AS KST_GILTIG_FOM,
		CONVERT(varchar(max), KST_GILTIG_TOM, 126) AS KST_GILTIG_TOM,
		CAST(KST_ID AS VARCHAR(MAX)) AS KST_ID,
		CAST(KST_ID_TEXT AS VARCHAR(MAX)) AS KST_ID_TEXT,
		CAST(KST_PASSIV AS VARCHAR(MAX)) AS KST_PASSIV,
		CAST(KST_TEXT AS VARCHAR(MAX)) AS KST_TEXT,
		CONVERT(varchar(max), MKST_GILTIG_FOM, 126) AS MKST_GILTIG_FOM,
		CONVERT(varchar(max), MKST_GILTIG_TOM, 126) AS MKST_GILTIG_TOM,
		CAST(MKST_ID AS VARCHAR(MAX)) AS MKST_ID,
		CAST(MKST_ID_TEXT AS VARCHAR(MAX)) AS MKST_ID_TEXT,
		CAST(MKST_PASSIV AS VARCHAR(MAX)) AS MKST_PASSIV,
		CAST(MKST_TEXT AS VARCHAR(MAX)) AS MKST_TEXT,
		CONVERT(varchar(max), SLLVGR_GILTIG_FOM, 126) AS SLLVGR_GILTIG_FOM,
		CONVERT(varchar(max), SLLVGR_GILTIG_TOM, 126) AS SLLVGR_GILTIG_TOM,
		CAST(SLLVGR_ID AS VARCHAR(MAX)) AS SLLVGR_ID,
		CAST(SLLVGR_ID_TEXT AS VARCHAR(MAX)) AS SLLVGR_ID_TEXT,
		CAST(SLLVGR_PASSIV AS VARCHAR(MAX)) AS SLLVGR_PASSIV,
		CAST(SLLVGR_TEXT AS VARCHAR(MAX)) AS SLLVGR_TEXT,
		CONVERT(varchar(max), VKO_GILTIG_FOM, 126) AS VKO_GILTIG_FOM,
		CONVERT(varchar(max), VKO_GILTIG_TOM, 126) AS VKO_GILTIG_TOM,
		CAST(VKO_ID AS VARCHAR(MAX)) AS VKO_ID,
		CAST(VKO_ID_TEXT AS VARCHAR(MAX)) AS VKO_ID_TEXT,
		CAST(VKO_PASSIV AS VARCHAR(MAX)) AS VKO_PASSIV,
		CAST(VKO_TEXT AS VARCHAR(MAX)) AS VKO_TEXT 
	FROM raindance_udp.udp_220.EK_DIM_OBJ_KST ) y

	"""
    return read(query=query, server_url="sosp.rd.sll.se")
    