
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
		CONVERT(varchar(max), DIV_GILTIG_FOM, 126) AS div_giltig_fom,
		CONVERT(varchar(max), DIV_GILTIG_TOM, 126) AS div_giltig_tom,
		CAST(DIV_ID AS VARCHAR(MAX)) AS div_id,
		CAST(DIV_ID_TEXT AS VARCHAR(MAX)) AS div_id_text,
		CAST(DIV_PASSIV AS VARCHAR(MAX)) AS div_passiv,
		CAST(DIV_TEXT AS VARCHAR(MAX)) AS div_text,
		CONVERT(varchar(max), IRASN1_GILTIG_FOM, 126) AS irasn1_giltig_fom,
		CONVERT(varchar(max), IRASN1_GILTIG_TOM, 126) AS irasn1_giltig_tom,
		CAST(IRASN1_ID AS VARCHAR(MAX)) AS irasn1_id,
		CAST(IRASN1_ID_TEXT AS VARCHAR(MAX)) AS irasn1_id_text,
		CAST(IRASN1_PASSIV AS VARCHAR(MAX)) AS irasn1_passiv,
		CAST(IRASN1_TEXT AS VARCHAR(MAX)) AS irasn1_text,
		CONVERT(varchar(max), IRASN2_GILTIG_FOM, 126) AS irasn2_giltig_fom,
		CONVERT(varchar(max), IRASN2_GILTIG_TOM, 126) AS irasn2_giltig_tom,
		CAST(IRASN2_ID AS VARCHAR(MAX)) AS irasn2_id,
		CAST(IRASN2_ID_TEXT AS VARCHAR(MAX)) AS irasn2_id_text,
		CAST(IRASN2_PASSIV AS VARCHAR(MAX)) AS irasn2_passiv,
		CAST(IRASN2_TEXT AS VARCHAR(MAX)) AS irasn2_text,
		CONVERT(varchar(max), IRASN3_GILTIG_FOM, 126) AS irasn3_giltig_fom,
		CONVERT(varchar(max), IRASN3_GILTIG_TOM, 126) AS irasn3_giltig_tom,
		CAST(IRASN3_ID AS VARCHAR(MAX)) AS irasn3_id,
		CAST(IRASN3_ID_TEXT AS VARCHAR(MAX)) AS irasn3_id_text,
		CAST(IRASN3_PASSIV AS VARCHAR(MAX)) AS irasn3_passiv,
		CAST(IRASN3_TEXT AS VARCHAR(MAX)) AS irasn3_text,
		CONVERT(varchar(max), KLINIK_GILTIG_FOM, 126) AS klinik_giltig_fom,
		CONVERT(varchar(max), KLINIK_GILTIG_TOM, 126) AS klinik_giltig_tom,
		CAST(KLINIK_ID AS VARCHAR(MAX)) AS klinik_id,
		CAST(KLINIK_ID_TEXT AS VARCHAR(MAX)) AS klinik_id_text,
		CAST(KLINIK_PASSIV AS VARCHAR(MAX)) AS klinik_passiv,
		CAST(KLINIK_TEXT AS VARCHAR(MAX)) AS klinik_text,
		CONVERT(varchar(max), KST_GILTIG_FOM, 126) AS kst_giltig_fom,
		CONVERT(varchar(max), KST_GILTIG_TOM, 126) AS kst_giltig_tom,
		CAST(KST_ID AS VARCHAR(MAX)) AS kst_id,
		CAST(KST_ID_TEXT AS VARCHAR(MAX)) AS kst_id_text,
		CAST(KST_PASSIV AS VARCHAR(MAX)) AS kst_passiv,
		CAST(KST_TEXT AS VARCHAR(MAX)) AS kst_text,
		CONVERT(varchar(max), SEKT_GILTIG_FOM, 126) AS sekt_giltig_fom,
		CONVERT(varchar(max), SEKT_GILTIG_TOM, 126) AS sekt_giltig_tom,
		CAST(SEKT_ID AS VARCHAR(MAX)) AS sekt_id,
		CAST(SEKT_ID_TEXT AS VARCHAR(MAX)) AS sekt_id_text,
		CAST(SEKT_PASSIV AS VARCHAR(MAX)) AS sekt_passiv,
		CAST(SEKT_TEXT AS VARCHAR(MAX)) AS sekt_text,
		CONVERT(varchar(max), SJKGR_GILTIG_FOM, 126) AS sjkgr_giltig_fom,
		CONVERT(varchar(max), SJKGR_GILTIG_TOM, 126) AS sjkgr_giltig_tom,
		CAST(SJKGR_ID AS VARCHAR(MAX)) AS sjkgr_id,
		CAST(SJKGR_ID_TEXT AS VARCHAR(MAX)) AS sjkgr_id_text,
		CAST(SJKGR_PASSIV AS VARCHAR(MAX)) AS sjkgr_passiv,
		CAST(SJKGR_TEXT AS VARCHAR(MAX)) AS sjkgr_text,
		CONVERT(varchar(max), VGREN_GILTIG_FOM, 126) AS vgren_giltig_fom,
		CONVERT(varchar(max), VGREN_GILTIG_TOM, 126) AS vgren_giltig_tom,
		CAST(VGREN_ID AS VARCHAR(MAX)) AS vgren_id,
		CAST(VGREN_ID_TEXT AS VARCHAR(MAX)) AS vgren_id_text,
		CAST(VGREN_PASSIV AS VARCHAR(MAX)) AS vgren_passiv,
		CAST(VGREN_TEXT AS VARCHAR(MAX)) AS vgren_text,
		CONVERT(varchar(max), VOMR_GILTIG_FOM, 126) AS vomr_giltig_fom,
		CONVERT(varchar(max), VOMR_GILTIG_TOM, 126) AS vomr_giltig_tom,
		CAST(VOMR_ID AS VARCHAR(MAX)) AS vomr_id,
		CAST(VOMR_ID_TEXT AS VARCHAR(MAX)) AS vomr_id_text,
		CAST(VOMR_PASSIV AS VARCHAR(MAX)) AS vomr_passiv,
		CAST(VOMR_TEXT AS VARCHAR(MAX)) AS vomr_text,
		CONVERT(varchar(max), VSJH_GILTIG_FOM, 126) AS vsjh_giltig_fom,
		CONVERT(varchar(max), VSJH_GILTIG_TOM, 126) AS vsjh_giltig_tom,
		CAST(VSJH_ID AS VARCHAR(MAX)) AS vsjh_id,
		CAST(VSJH_ID_TEXT AS VARCHAR(MAX)) AS vsjh_id_text,
		CAST(VSJH_PASSIV AS VARCHAR(MAX)) AS vsjh_passiv,
		CAST(VSJH_TEXT AS VARCHAR(MAX)) AS vsjh_text 
	FROM Utdata.udp_100.EK_DIM_OBJ_KST ) y

	"""
    return read(query=query, server_url="ksp.rd.sll.se")
    