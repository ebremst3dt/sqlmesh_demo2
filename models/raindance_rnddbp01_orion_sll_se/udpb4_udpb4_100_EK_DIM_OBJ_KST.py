
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'ENH_GILTIG_FOM': 'varchar(max)', 'ENH_GILTIG_TOM': 'varchar(max)', 'ENH_ID': 'varchar(max)', 'ENH_ID_TEXT': 'varchar(max)', 'ENH_PASSIV': 'varchar(max)', 'ENH_TEXT': 'varchar(max)', 'KST_GILTIG_FOM': 'varchar(max)', 'KST_GILTIG_TOM': 'varchar(max)', 'KST_ID': 'varchar(max)', 'KST_ID_TEXT': 'varchar(max)', 'KST_PASSIV': 'varchar(max)', 'KST_TEXT': 'varchar(max)', 'NSO_GILTIG_FOM': 'varchar(max)', 'NSO_GILTIG_TOM': 'varchar(max)', 'NSO_ID': 'varchar(max)', 'NSO_ID_TEXT': 'varchar(max)', 'NSO_PASSIV': 'varchar(max)', 'NSO_TEXT': 'varchar(max)', 'RE_GILTIG_FOM': 'varchar(max)', 'RE_GILTIG_TOM': 'varchar(max)', 'RE_ID': 'varchar(max)', 'RE_ID_TEXT': 'varchar(max)', 'RE_PASSIV': 'varchar(max)', 'RE_TEXT': 'varchar(max)', 'SA_GILTIG_FOM': 'varchar(max)', 'SA_GILTIG_TOM': 'varchar(max)', 'SA_ID': 'varchar(max)', 'SA_ID_TEXT': 'varchar(max)', 'SA_PASSIV': 'varchar(max)', 'SA_TEXT': 'varchar(max)', 'VG_GILTIG_FOM': 'varchar(max)', 'VG_GILTIG_TOM': 'varchar(max)', 'VG_ID': 'varchar(max)', 'VG_ID_TEXT': 'varchar(max)', 'VG_PASSIV': 'varchar(max)', 'VG_TEXT': 'varchar(max)', 'VT_GILTIG_FOM': 'varchar(max)', 'VT_GILTIG_TOM': 'varchar(max)', 'VT_ID': 'varchar(max)', 'VT_ID_TEXT': 'varchar(max)', 'VT_PASSIV': 'varchar(max)', 'VT_TEXT': 'varchar(max)'},
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
		'rnddbp01_orion_sll_se_udpb4_udpb4_100' as _source,
		CONVERT(varchar(max), ENH_GILTIG_FOM, 126) AS enh_giltig_fom,
		CONVERT(varchar(max), ENH_GILTIG_TOM, 126) AS enh_giltig_tom,
		CAST(ENH_ID AS VARCHAR(MAX)) AS enh_id,
		CAST(ENH_ID_TEXT AS VARCHAR(MAX)) AS enh_id_text,
		CAST(ENH_PASSIV AS VARCHAR(MAX)) AS enh_passiv,
		CAST(ENH_TEXT AS VARCHAR(MAX)) AS enh_text,
		CONVERT(varchar(max), KST_GILTIG_FOM, 126) AS kst_giltig_fom,
		CONVERT(varchar(max), KST_GILTIG_TOM, 126) AS kst_giltig_tom,
		CAST(KST_ID AS VARCHAR(MAX)) AS kst_id,
		CAST(KST_ID_TEXT AS VARCHAR(MAX)) AS kst_id_text,
		CAST(KST_PASSIV AS VARCHAR(MAX)) AS kst_passiv,
		CAST(KST_TEXT AS VARCHAR(MAX)) AS kst_text,
		CONVERT(varchar(max), NSO_GILTIG_FOM, 126) AS nso_giltig_fom,
		CONVERT(varchar(max), NSO_GILTIG_TOM, 126) AS nso_giltig_tom,
		CAST(NSO_ID AS VARCHAR(MAX)) AS nso_id,
		CAST(NSO_ID_TEXT AS VARCHAR(MAX)) AS nso_id_text,
		CAST(NSO_PASSIV AS VARCHAR(MAX)) AS nso_passiv,
		CAST(NSO_TEXT AS VARCHAR(MAX)) AS nso_text,
		CONVERT(varchar(max), RE_GILTIG_FOM, 126) AS re_giltig_fom,
		CONVERT(varchar(max), RE_GILTIG_TOM, 126) AS re_giltig_tom,
		CAST(RE_ID AS VARCHAR(MAX)) AS re_id,
		CAST(RE_ID_TEXT AS VARCHAR(MAX)) AS re_id_text,
		CAST(RE_PASSIV AS VARCHAR(MAX)) AS re_passiv,
		CAST(RE_TEXT AS VARCHAR(MAX)) AS re_text,
		CONVERT(varchar(max), SA_GILTIG_FOM, 126) AS sa_giltig_fom,
		CONVERT(varchar(max), SA_GILTIG_TOM, 126) AS sa_giltig_tom,
		CAST(SA_ID AS VARCHAR(MAX)) AS sa_id,
		CAST(SA_ID_TEXT AS VARCHAR(MAX)) AS sa_id_text,
		CAST(SA_PASSIV AS VARCHAR(MAX)) AS sa_passiv,
		CAST(SA_TEXT AS VARCHAR(MAX)) AS sa_text,
		CONVERT(varchar(max), VG_GILTIG_FOM, 126) AS vg_giltig_fom,
		CONVERT(varchar(max), VG_GILTIG_TOM, 126) AS vg_giltig_tom,
		CAST(VG_ID AS VARCHAR(MAX)) AS vg_id,
		CAST(VG_ID_TEXT AS VARCHAR(MAX)) AS vg_id_text,
		CAST(VG_PASSIV AS VARCHAR(MAX)) AS vg_passiv,
		CAST(VG_TEXT AS VARCHAR(MAX)) AS vg_text,
		CONVERT(varchar(max), VT_GILTIG_FOM, 126) AS vt_giltig_fom,
		CONVERT(varchar(max), VT_GILTIG_TOM, 126) AS vt_giltig_tom,
		CAST(VT_ID AS VARCHAR(MAX)) AS vt_id,
		CAST(VT_ID_TEXT AS VARCHAR(MAX)) AS vt_id_text,
		CAST(VT_PASSIV AS VARCHAR(MAX)) AS vt_passiv,
		CAST(VT_TEXT AS VARCHAR(MAX)) AS vt_text 
	FROM udpb4.udpb4_100.EK_DIM_OBJ_KST ) y

	"""
    return read(query=query, server_url="rnddbp01.orion.sll.se")
    