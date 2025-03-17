
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read

        
@model(
    columns={'_data_modified_utc': 'date', '_metadata_modified_utc': 'datetime2', '_source': 'varchar(max)', 'KST_GILTIG_FOM': 'varchar(max)', 'KST_GILTIG_TOM': 'varchar(max)', 'KST_ID': 'varchar(max)', 'KST_ID_TEXT': 'varchar(max)', 'KST_PASSIV': 'varchar(max)', 'KST_TEXT': 'varchar(max)', 'SEKT_GILTIG_FOM': 'varchar(max)', 'SEKT_GILTIG_TOM': 'varchar(max)', 'SEKT_ID': 'varchar(max)', 'SEKT_ID_TEXT': 'varchar(max)', 'SEKT_PASSIV': 'varchar(max)', 'SEKT_TEXT': 'varchar(max)', 'VERK_GILTIG_FOM': 'varchar(max)', 'VERK_GILTIG_TOM': 'varchar(max)', 'VERK_ID': 'varchar(max)', 'VERK_ID_TEXT': 'varchar(max)', 'VERK_PASSIV': 'varchar(max)', 'VERK_TEXT': 'varchar(max)', 'VGREN_GILTIG_FOM': 'varchar(max)', 'VGREN_GILTIG_TOM': 'varchar(max)', 'VGREN_ID': 'varchar(max)', 'VGREN_ID_TEXT': 'varchar(max)', 'VGREN_PASSIV': 'varchar(max)', 'VGREN_TEXT': 'varchar(max)', 'VO_GILTIG_FOM': 'varchar(max)', 'VO_GILTIG_TOM': 'varchar(max)', 'VO_ID': 'varchar(max)', 'VO_ID_TEXT': 'varchar(max)', 'VO_PASSIV': 'varchar(max)', 'VO_TEXT': 'varchar(max)', 'V_GILTIG_FOM': 'varchar(max)', 'V_GILTIG_TOM': 'varchar(max)', 'V_ID': 'varchar(max)', 'V_ID_TEXT': 'varchar(max)', 'V_PASSIV': 'varchar(max)', 'V_TEXT': 'varchar(max)'},
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
		'dsp_rd_sll_se_raindance_udp_udp_150' as _source,
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
		CONVERT(varchar(max), VERK_GILTIG_FOM, 126) AS verk_giltig_fom,
		CONVERT(varchar(max), VERK_GILTIG_TOM, 126) AS verk_giltig_tom,
		CAST(VERK_ID AS VARCHAR(MAX)) AS verk_id,
		CAST(VERK_ID_TEXT AS VARCHAR(MAX)) AS verk_id_text,
		CAST(VERK_PASSIV AS VARCHAR(MAX)) AS verk_passiv,
		CAST(VERK_TEXT AS VARCHAR(MAX)) AS verk_text,
		CONVERT(varchar(max), VGREN_GILTIG_FOM, 126) AS vgren_giltig_fom,
		CONVERT(varchar(max), VGREN_GILTIG_TOM, 126) AS vgren_giltig_tom,
		CAST(VGREN_ID AS VARCHAR(MAX)) AS vgren_id,
		CAST(VGREN_ID_TEXT AS VARCHAR(MAX)) AS vgren_id_text,
		CAST(VGREN_PASSIV AS VARCHAR(MAX)) AS vgren_passiv,
		CAST(VGREN_TEXT AS VARCHAR(MAX)) AS vgren_text,
		CONVERT(varchar(max), VO_GILTIG_FOM, 126) AS vo_giltig_fom,
		CONVERT(varchar(max), VO_GILTIG_TOM, 126) AS vo_giltig_tom,
		CAST(VO_ID AS VARCHAR(MAX)) AS vo_id,
		CAST(VO_ID_TEXT AS VARCHAR(MAX)) AS vo_id_text,
		CAST(VO_PASSIV AS VARCHAR(MAX)) AS vo_passiv,
		CAST(VO_TEXT AS VARCHAR(MAX)) AS vo_text,
		CONVERT(varchar(max), V_GILTIG_FOM, 126) AS v_giltig_fom,
		CONVERT(varchar(max), V_GILTIG_TOM, 126) AS v_giltig_tom,
		CAST(V_ID AS VARCHAR(MAX)) AS v_id,
		CAST(V_ID_TEXT AS VARCHAR(MAX)) AS v_id_text,
		CAST(V_PASSIV AS VARCHAR(MAX)) AS v_passiv,
		CAST(V_TEXT AS VARCHAR(MAX)) AS v_text 
	FROM raindance_udp.udp_150.EK_DIM_OBJ_KST ) y

	"""
    return read(query=query, server_url="dsp.rd.sll.se")
    