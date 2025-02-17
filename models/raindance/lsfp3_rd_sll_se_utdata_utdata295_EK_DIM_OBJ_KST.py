
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read


@model(
    columns={'AVDEL_GILTIG_FOM': 'datetime',
 'AVDEL_GILTIG_TOM': 'datetime',
 'AVDEL_ID': 'varchar(3)',
 'AVDEL_ID_TEXT': 'varchar(34)',
 'AVDEL_PASSIV': 'bit',
 'AVDEL_TEXT': 'varchar(30)',
 'AVD_GILTIG_FOM': 'datetime',
 'AVD_GILTIG_TOM': 'datetime',
 'AVD_ID': 'varchar(3)',
 'AVD_ID_TEXT': 'varchar(34)',
 'AVD_PASSIV': 'bit',
 'AVD_TEXT': 'varchar(30)',
 'DIR_GILTIG_FOM': 'datetime',
 'DIR_GILTIG_TOM': 'datetime',
 'DIR_ID': 'varchar(2)',
 'DIR_ID_TEXT': 'varchar(33)',
 'DIR_PASSIV': 'bit',
 'DIR_TEXT': 'varchar(30)',
 'ENHET_GILTIG_FOM': 'datetime',
 'ENHET_GILTIG_TOM': 'datetime',
 'ENHET_ID': 'varchar(4)',
 'ENHET_ID_TEXT': 'varchar(35)',
 'ENHET_PASSIV': 'bit',
 'ENHET_TEXT': 'varchar(30)',
 'ENH_GILTIG_FOM': 'datetime',
 'ENH_GILTIG_TOM': 'datetime',
 'ENH_ID': 'varchar(4)',
 'ENH_ID_TEXT': 'varchar(35)',
 'ENH_PASSIV': 'bit',
 'ENH_TEXT': 'varchar(30)',
 'KST_GILTIG_FOM': 'datetime',
 'KST_GILTIG_TOM': 'datetime',
 'KST_ID': 'varchar(5)',
 'KST_ID_TEXT': 'varchar(36)',
 'KST_PASSIV': 'bit',
 'KST_TEXT': 'varchar(30)',
 'RL_GILTIG_FOM': 'datetime',
 'RL_GILTIG_TOM': 'datetime',
 'RL_ID': 'varchar(2)',
 'RL_ID_TEXT': 'varchar(33)',
 'RL_PASSIV': 'bit',
 'RL_TEXT': 'varchar(30)',
 'SEKTN_GILTIG_FOM': 'datetime',
 'SEKTN_GILTIG_TOM': 'datetime',
 'SEKTN_ID': 'varchar(5)',
 'SEKTN_ID_TEXT': 'varchar(36)',
 'SEKTN_PASSIV': 'bit',
 'SEKTN_TEXT': 'varchar(30)',
 'SEKT_GILTIG_FOM': 'datetime',
 'SEKT_GILTIG_TOM': 'datetime',
 'SEKT_ID': 'varchar(5)',
 'SEKT_ID_TEXT': 'varchar(36)',
 'SEKT_PASSIV': 'bit',
 'SEKT_TEXT': 'varchar(30)'},
    kind=ModelKindName.FULL,
    cron="@daily"
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> pd.DataFrame:
    query = """
	SELECT top 1000
 		CONVERT(varchar(max), AVD_GILTIG_FOM, 126) AS avd_giltig_fom,
		CONVERT(varchar(max), AVD_GILTIG_TOM, 126) AS avd_giltig_tom,
		CAST(AVD_ID AS VARCHAR(MAX)) AS avd_id,
		CAST(AVD_ID_TEXT AS VARCHAR(MAX)) AS avd_id_text,
		CAST(AVD_PASSIV AS VARCHAR(MAX)) AS avd_passiv,
		CAST(AVD_TEXT AS VARCHAR(MAX)) AS avd_text,
		CONVERT(varchar(max), AVDEL_GILTIG_FOM, 126) AS avdel_giltig_fom,
		CONVERT(varchar(max), AVDEL_GILTIG_TOM, 126) AS avdel_giltig_tom,
		CAST(AVDEL_ID AS VARCHAR(MAX)) AS avdel_id,
		CAST(AVDEL_ID_TEXT AS VARCHAR(MAX)) AS avdel_id_text,
		CAST(AVDEL_PASSIV AS VARCHAR(MAX)) AS avdel_passiv,
		CAST(AVDEL_TEXT AS VARCHAR(MAX)) AS avdel_text,
		CONVERT(varchar(max), DIR_GILTIG_FOM, 126) AS dir_giltig_fom,
		CONVERT(varchar(max), DIR_GILTIG_TOM, 126) AS dir_giltig_tom,
		CAST(DIR_ID AS VARCHAR(MAX)) AS dir_id,
		CAST(DIR_ID_TEXT AS VARCHAR(MAX)) AS dir_id_text,
		CAST(DIR_PASSIV AS VARCHAR(MAX)) AS dir_passiv,
		CAST(DIR_TEXT AS VARCHAR(MAX)) AS dir_text,
		CONVERT(varchar(max), ENH_GILTIG_FOM, 126) AS enh_giltig_fom,
		CONVERT(varchar(max), ENH_GILTIG_TOM, 126) AS enh_giltig_tom,
		CAST(ENH_ID AS VARCHAR(MAX)) AS enh_id,
		CAST(ENH_ID_TEXT AS VARCHAR(MAX)) AS enh_id_text,
		CAST(ENH_PASSIV AS VARCHAR(MAX)) AS enh_passiv,
		CAST(ENH_TEXT AS VARCHAR(MAX)) AS enh_text,
		CONVERT(varchar(max), ENHET_GILTIG_FOM, 126) AS enhet_giltig_fom,
		CONVERT(varchar(max), ENHET_GILTIG_TOM, 126) AS enhet_giltig_tom,
		CAST(ENHET_ID AS VARCHAR(MAX)) AS enhet_id,
		CAST(ENHET_ID_TEXT AS VARCHAR(MAX)) AS enhet_id_text,
		CAST(ENHET_PASSIV AS VARCHAR(MAX)) AS enhet_passiv,
		CAST(ENHET_TEXT AS VARCHAR(MAX)) AS enhet_text,
		CONVERT(varchar(max), KST_GILTIG_FOM, 126) AS kst_giltig_fom,
		CONVERT(varchar(max), KST_GILTIG_TOM, 126) AS kst_giltig_tom,
		CAST(KST_ID AS VARCHAR(MAX)) AS kst_id,
		CAST(KST_ID_TEXT AS VARCHAR(MAX)) AS kst_id_text,
		CAST(KST_PASSIV AS VARCHAR(MAX)) AS kst_passiv,
		CAST(KST_TEXT AS VARCHAR(MAX)) AS kst_text,
		CONVERT(varchar(max), RL_GILTIG_FOM, 126) AS rl_giltig_fom,
		CONVERT(varchar(max), RL_GILTIG_TOM, 126) AS rl_giltig_tom,
		CAST(RL_ID AS VARCHAR(MAX)) AS rl_id,
		CAST(RL_ID_TEXT AS VARCHAR(MAX)) AS rl_id_text,
		CAST(RL_PASSIV AS VARCHAR(MAX)) AS rl_passiv,
		CAST(RL_TEXT AS VARCHAR(MAX)) AS rl_text,
		CONVERT(varchar(max), SEKT_GILTIG_FOM, 126) AS sekt_giltig_fom,
		CONVERT(varchar(max), SEKT_GILTIG_TOM, 126) AS sekt_giltig_tom,
		CAST(SEKT_ID AS VARCHAR(MAX)) AS sekt_id,
		CAST(SEKT_ID_TEXT AS VARCHAR(MAX)) AS sekt_id_text,
		CAST(SEKT_PASSIV AS VARCHAR(MAX)) AS sekt_passiv,
		CAST(SEKT_TEXT AS VARCHAR(MAX)) AS sekt_text,
		CONVERT(varchar(max), SEKTN_GILTIG_FOM, 126) AS sektn_giltig_fom,
		CONVERT(varchar(max), SEKTN_GILTIG_TOM, 126) AS sektn_giltig_tom,
		CAST(SEKTN_ID AS VARCHAR(MAX)) AS sektn_id,
		CAST(SEKTN_ID_TEXT AS VARCHAR(MAX)) AS sektn_id_text,
		CAST(SEKTN_PASSIV AS VARCHAR(MAX)) AS sektn_passiv,
		CAST(SEKTN_TEXT AS VARCHAR(MAX)) AS sektn_text 
	FROM utdata.utdata295.EK_DIM_OBJ_KST
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
