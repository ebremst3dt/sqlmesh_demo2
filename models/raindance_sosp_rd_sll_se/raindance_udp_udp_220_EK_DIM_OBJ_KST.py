
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
		CONVERT(varchar(max), ANSVAR_GILTIG_FOM, 126) AS ansvar_giltig_fom,
		CONVERT(varchar(max), ANSVAR_GILTIG_TOM, 126) AS ansvar_giltig_tom,
		CAST(ANSVAR_ID AS VARCHAR(MAX)) AS ansvar_id,
		CAST(ANSVAR_ID_TEXT AS VARCHAR(MAX)) AS ansvar_id_text,
		CAST(ANSVAR_PASSIV AS VARCHAR(MAX)) AS ansvar_passiv,
		CAST(ANSVAR_TEXT AS VARCHAR(MAX)) AS ansvar_text,
		CONVERT(varchar(max), FOUU_GILTIG_FOM, 126) AS fouu_giltig_fom,
		CONVERT(varchar(max), FOUU_GILTIG_TOM, 126) AS fouu_giltig_tom,
		CAST(FOUU_ID AS VARCHAR(MAX)) AS fouu_id,
		CAST(FOUU_ID_TEXT AS VARCHAR(MAX)) AS fouu_id_text,
		CAST(FOUU_PASSIV AS VARCHAR(MAX)) AS fouu_passiv,
		CAST(FOUU_TEXT AS VARCHAR(MAX)) AS fouu_text,
		CONVERT(varchar(max), FUNK_GILTIG_FOM, 126) AS funk_giltig_fom,
		CONVERT(varchar(max), FUNK_GILTIG_TOM, 126) AS funk_giltig_tom,
		CAST(FUNK_ID AS VARCHAR(MAX)) AS funk_id,
		CAST(FUNK_ID_TEXT AS VARCHAR(MAX)) AS funk_id_text,
		CAST(FUNK_PASSIV AS VARCHAR(MAX)) AS funk_passiv,
		CAST(FUNK_TEXT AS VARCHAR(MAX)) AS funk_text,
		CONVERT(varchar(max), KSG_GILTIG_FOM, 126) AS ksg_giltig_fom,
		CONVERT(varchar(max), KSG_GILTIG_TOM, 126) AS ksg_giltig_tom,
		CAST(KSG_ID AS VARCHAR(MAX)) AS ksg_id,
		CAST(KSG_ID_TEXT AS VARCHAR(MAX)) AS ksg_id_text,
		CAST(KSG_PASSIV AS VARCHAR(MAX)) AS ksg_passiv,
		CAST(KSG_TEXT AS VARCHAR(MAX)) AS ksg_text,
		CONVERT(varchar(max), KST_GILTIG_FOM, 126) AS kst_giltig_fom,
		CONVERT(varchar(max), KST_GILTIG_TOM, 126) AS kst_giltig_tom,
		CAST(KST_ID AS VARCHAR(MAX)) AS kst_id,
		CAST(KST_ID_TEXT AS VARCHAR(MAX)) AS kst_id_text,
		CAST(KST_PASSIV AS VARCHAR(MAX)) AS kst_passiv,
		CAST(KST_TEXT AS VARCHAR(MAX)) AS kst_text,
		CONVERT(varchar(max), MKST_GILTIG_FOM, 126) AS mkst_giltig_fom,
		CONVERT(varchar(max), MKST_GILTIG_TOM, 126) AS mkst_giltig_tom,
		CAST(MKST_ID AS VARCHAR(MAX)) AS mkst_id,
		CAST(MKST_ID_TEXT AS VARCHAR(MAX)) AS mkst_id_text,
		CAST(MKST_PASSIV AS VARCHAR(MAX)) AS mkst_passiv,
		CAST(MKST_TEXT AS VARCHAR(MAX)) AS mkst_text,
		CONVERT(varchar(max), SLLVGR_GILTIG_FOM, 126) AS sllvgr_giltig_fom,
		CONVERT(varchar(max), SLLVGR_GILTIG_TOM, 126) AS sllvgr_giltig_tom,
		CAST(SLLVGR_ID AS VARCHAR(MAX)) AS sllvgr_id,
		CAST(SLLVGR_ID_TEXT AS VARCHAR(MAX)) AS sllvgr_id_text,
		CAST(SLLVGR_PASSIV AS VARCHAR(MAX)) AS sllvgr_passiv,
		CAST(SLLVGR_TEXT AS VARCHAR(MAX)) AS sllvgr_text,
		CONVERT(varchar(max), VKO_GILTIG_FOM, 126) AS vko_giltig_fom,
		CONVERT(varchar(max), VKO_GILTIG_TOM, 126) AS vko_giltig_tom,
		CAST(VKO_ID AS VARCHAR(MAX)) AS vko_id,
		CAST(VKO_ID_TEXT AS VARCHAR(MAX)) AS vko_id_text,
		CAST(VKO_PASSIV AS VARCHAR(MAX)) AS vko_passiv,
		CAST(VKO_TEXT AS VARCHAR(MAX)) AS vko_text 
	FROM raindance_udp.udp_220.EK_DIM_OBJ_KST ) y

	"""
    return read(query=query, server_url="sosp.rd.sll.se")
    