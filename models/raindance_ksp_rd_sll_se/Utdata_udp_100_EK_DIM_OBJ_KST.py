
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'DIV_GILTIG_FOM': 'varchar(max)', 'DIV_GILTIG_TOM': 'varchar(max)', 'DIV_ID': 'varchar(max)', 'DIV_ID_TEXT': 'varchar(max)', 'DIV_PASSIV': 'varchar(max)', 'DIV_TEXT': 'varchar(max)', 'IRASN1_GILTIG_FOM': 'varchar(max)', 'IRASN1_GILTIG_TOM': 'varchar(max)', 'IRASN1_ID': 'varchar(max)', 'IRASN1_ID_TEXT': 'varchar(max)', 'IRASN1_PASSIV': 'varchar(max)', 'IRASN1_TEXT': 'varchar(max)', 'IRASN2_GILTIG_FOM': 'varchar(max)', 'IRASN2_GILTIG_TOM': 'varchar(max)', 'IRASN2_ID': 'varchar(max)', 'IRASN2_ID_TEXT': 'varchar(max)', 'IRASN2_PASSIV': 'varchar(max)', 'IRASN2_TEXT': 'varchar(max)', 'IRASN3_GILTIG_FOM': 'varchar(max)', 'IRASN3_GILTIG_TOM': 'varchar(max)', 'IRASN3_ID': 'varchar(max)', 'IRASN3_ID_TEXT': 'varchar(max)', 'IRASN3_PASSIV': 'varchar(max)', 'IRASN3_TEXT': 'varchar(max)', 'KLINIK_GILTIG_FOM': 'varchar(max)', 'KLINIK_GILTIG_TOM': 'varchar(max)', 'KLINIK_ID': 'varchar(max)', 'KLINIK_ID_TEXT': 'varchar(max)', 'KLINIK_PASSIV': 'varchar(max)', 'KLINIK_TEXT': 'varchar(max)', 'KST_GILTIG_FOM': 'varchar(max)', 'KST_GILTIG_TOM': 'varchar(max)', 'KST_ID': 'varchar(max)', 'KST_ID_TEXT': 'varchar(max)', 'KST_PASSIV': 'varchar(max)', 'KST_TEXT': 'varchar(max)', 'SEKT_GILTIG_FOM': 'varchar(max)', 'SEKT_GILTIG_TOM': 'varchar(max)', 'SEKT_ID': 'varchar(max)', 'SEKT_ID_TEXT': 'varchar(max)', 'SEKT_PASSIV': 'varchar(max)', 'SEKT_TEXT': 'varchar(max)', 'SJKGR_GILTIG_FOM': 'varchar(max)', 'SJKGR_GILTIG_TOM': 'varchar(max)', 'SJKGR_ID': 'varchar(max)', 'SJKGR_ID_TEXT': 'varchar(max)', 'SJKGR_PASSIV': 'varchar(max)', 'SJKGR_TEXT': 'varchar(max)', 'VGREN_GILTIG_FOM': 'varchar(max)', 'VGREN_GILTIG_TOM': 'varchar(max)', 'VGREN_ID': 'varchar(max)', 'VGREN_ID_TEXT': 'varchar(max)', 'VGREN_PASSIV': 'varchar(max)', 'VGREN_TEXT': 'varchar(max)', 'VOMR_GILTIG_FOM': 'varchar(max)', 'VOMR_GILTIG_TOM': 'varchar(max)', 'VOMR_ID': 'varchar(max)', 'VOMR_ID_TEXT': 'varchar(max)', 'VOMR_PASSIV': 'varchar(max)', 'VOMR_TEXT': 'varchar(max)', 'VSJH_GILTIG_FOM': 'varchar(max)', 'VSJH_GILTIG_TOM': 'varchar(max)', 'VSJH_ID': 'varchar(max)', 'VSJH_ID_TEXT': 'varchar(max)', 'VSJH_PASSIV': 'varchar(max)', 'VSJH_TEXT': 'varchar(max)'},
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
		'ksp_rd_sll_se_Utdata_udp_100' as _source,
		CONVERT(varchar(max), DIV_GILTIG_FOM, 126) AS DIV_GILTIG_FOM,
		CONVERT(varchar(max), DIV_GILTIG_TOM, 126) AS DIV_GILTIG_TOM,
		CAST(DIV_ID AS VARCHAR(MAX)) AS DIV_ID,
		CAST(DIV_ID_TEXT AS VARCHAR(MAX)) AS DIV_ID_TEXT,
		CAST(DIV_PASSIV AS VARCHAR(MAX)) AS DIV_PASSIV,
		CAST(DIV_TEXT AS VARCHAR(MAX)) AS DIV_TEXT,
		CONVERT(varchar(max), IRASN1_GILTIG_FOM, 126) AS IRASN1_GILTIG_FOM,
		CONVERT(varchar(max), IRASN1_GILTIG_TOM, 126) AS IRASN1_GILTIG_TOM,
		CAST(IRASN1_ID AS VARCHAR(MAX)) AS IRASN1_ID,
		CAST(IRASN1_ID_TEXT AS VARCHAR(MAX)) AS IRASN1_ID_TEXT,
		CAST(IRASN1_PASSIV AS VARCHAR(MAX)) AS IRASN1_PASSIV,
		CAST(IRASN1_TEXT AS VARCHAR(MAX)) AS IRASN1_TEXT,
		CONVERT(varchar(max), IRASN2_GILTIG_FOM, 126) AS IRASN2_GILTIG_FOM,
		CONVERT(varchar(max), IRASN2_GILTIG_TOM, 126) AS IRASN2_GILTIG_TOM,
		CAST(IRASN2_ID AS VARCHAR(MAX)) AS IRASN2_ID,
		CAST(IRASN2_ID_TEXT AS VARCHAR(MAX)) AS IRASN2_ID_TEXT,
		CAST(IRASN2_PASSIV AS VARCHAR(MAX)) AS IRASN2_PASSIV,
		CAST(IRASN2_TEXT AS VARCHAR(MAX)) AS IRASN2_TEXT,
		CONVERT(varchar(max), IRASN3_GILTIG_FOM, 126) AS IRASN3_GILTIG_FOM,
		CONVERT(varchar(max), IRASN3_GILTIG_TOM, 126) AS IRASN3_GILTIG_TOM,
		CAST(IRASN3_ID AS VARCHAR(MAX)) AS IRASN3_ID,
		CAST(IRASN3_ID_TEXT AS VARCHAR(MAX)) AS IRASN3_ID_TEXT,
		CAST(IRASN3_PASSIV AS VARCHAR(MAX)) AS IRASN3_PASSIV,
		CAST(IRASN3_TEXT AS VARCHAR(MAX)) AS IRASN3_TEXT,
		CONVERT(varchar(max), KLINIK_GILTIG_FOM, 126) AS KLINIK_GILTIG_FOM,
		CONVERT(varchar(max), KLINIK_GILTIG_TOM, 126) AS KLINIK_GILTIG_TOM,
		CAST(KLINIK_ID AS VARCHAR(MAX)) AS KLINIK_ID,
		CAST(KLINIK_ID_TEXT AS VARCHAR(MAX)) AS KLINIK_ID_TEXT,
		CAST(KLINIK_PASSIV AS VARCHAR(MAX)) AS KLINIK_PASSIV,
		CAST(KLINIK_TEXT AS VARCHAR(MAX)) AS KLINIK_TEXT,
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
		CONVERT(varchar(max), SJKGR_GILTIG_FOM, 126) AS SJKGR_GILTIG_FOM,
		CONVERT(varchar(max), SJKGR_GILTIG_TOM, 126) AS SJKGR_GILTIG_TOM,
		CAST(SJKGR_ID AS VARCHAR(MAX)) AS SJKGR_ID,
		CAST(SJKGR_ID_TEXT AS VARCHAR(MAX)) AS SJKGR_ID_TEXT,
		CAST(SJKGR_PASSIV AS VARCHAR(MAX)) AS SJKGR_PASSIV,
		CAST(SJKGR_TEXT AS VARCHAR(MAX)) AS SJKGR_TEXT,
		CONVERT(varchar(max), VGREN_GILTIG_FOM, 126) AS VGREN_GILTIG_FOM,
		CONVERT(varchar(max), VGREN_GILTIG_TOM, 126) AS VGREN_GILTIG_TOM,
		CAST(VGREN_ID AS VARCHAR(MAX)) AS VGREN_ID,
		CAST(VGREN_ID_TEXT AS VARCHAR(MAX)) AS VGREN_ID_TEXT,
		CAST(VGREN_PASSIV AS VARCHAR(MAX)) AS VGREN_PASSIV,
		CAST(VGREN_TEXT AS VARCHAR(MAX)) AS VGREN_TEXT,
		CONVERT(varchar(max), VOMR_GILTIG_FOM, 126) AS VOMR_GILTIG_FOM,
		CONVERT(varchar(max), VOMR_GILTIG_TOM, 126) AS VOMR_GILTIG_TOM,
		CAST(VOMR_ID AS VARCHAR(MAX)) AS VOMR_ID,
		CAST(VOMR_ID_TEXT AS VARCHAR(MAX)) AS VOMR_ID_TEXT,
		CAST(VOMR_PASSIV AS VARCHAR(MAX)) AS VOMR_PASSIV,
		CAST(VOMR_TEXT AS VARCHAR(MAX)) AS VOMR_TEXT,
		CONVERT(varchar(max), VSJH_GILTIG_FOM, 126) AS VSJH_GILTIG_FOM,
		CONVERT(varchar(max), VSJH_GILTIG_TOM, 126) AS VSJH_GILTIG_TOM,
		CAST(VSJH_ID AS VARCHAR(MAX)) AS VSJH_ID,
		CAST(VSJH_ID_TEXT AS VARCHAR(MAX)) AS VSJH_ID_TEXT,
		CAST(VSJH_PASSIV AS VARCHAR(MAX)) AS VSJH_PASSIV,
		CAST(VSJH_TEXT AS VARCHAR(MAX)) AS VSJH_TEXT 
	FROM Utdata.udp_100.EK_DIM_OBJ_KST ) y

	"""
    return read(query=query, server_url="ksp.rd.sll.se")
    