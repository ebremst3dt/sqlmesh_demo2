
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model import kind
from models.mssql import read


@model(
    columns={'AVDEL_GILTIG_FOM': 'varchar(max)',
 'AVDEL_GILTIG_TOM': 'varchar(max)',
 'AVDEL_ID': 'varchar(max)',
 'AVDEL_ID_TEXT': 'varchar(max)',
 'AVDEL_PASSIV': 'varchar(max)',
 'AVDEL_TEXT': 'varchar(max)',
 'AVD_GILTIG_FOM': 'varchar(max)',
 'AVD_GILTIG_TOM': 'varchar(max)',
 'AVD_ID': 'varchar(max)',
 'AVD_ID_TEXT': 'varchar(max)',
 'AVD_PASSIV': 'varchar(max)',
 'AVD_TEXT': 'varchar(max)',
 'DIR_GILTIG_FOM': 'varchar(max)',
 'DIR_GILTIG_TOM': 'varchar(max)',
 'DIR_ID': 'varchar(max)',
 'DIR_ID_TEXT': 'varchar(max)',
 'DIR_PASSIV': 'varchar(max)',
 'DIR_TEXT': 'varchar(max)',
 'ENHET_GILTIG_FOM': 'varchar(max)',
 'ENHET_GILTIG_TOM': 'varchar(max)',
 'ENHET_ID': 'varchar(max)',
 'ENHET_ID_TEXT': 'varchar(max)',
 'ENHET_PASSIV': 'varchar(max)',
 'ENHET_TEXT': 'varchar(max)',
 'ENH_GILTIG_FOM': 'varchar(max)',
 'ENH_GILTIG_TOM': 'varchar(max)',
 'ENH_ID': 'varchar(max)',
 'ENH_ID_TEXT': 'varchar(max)',
 'ENH_PASSIV': 'varchar(max)',
 'ENH_TEXT': 'varchar(max)',
 'KST_DATUM_FOM': 'varchar(max)',
 'KST_DATUM_TOM': 'varchar(max)',
 'KST_GILTIG_FOM': 'varchar(max)',
 'KST_GILTIG_TOM': 'varchar(max)',
 'KST_ID': 'varchar(max)',
 'KST_ID_TEXT': 'varchar(max)',
 'KST_PASSIV': 'varchar(max)',
 'KST_TEXT': 'varchar(max)',
 'RL_GILTIG_FOM': 'varchar(max)',
 'RL_GILTIG_TOM': 'varchar(max)',
 'RL_ID': 'varchar(max)',
 'RL_ID_TEXT': 'varchar(max)',
 'RL_PASSIV': 'varchar(max)',
 'RL_TEXT': 'varchar(max)',
 'SEKTN_GILTIG_FOM': 'varchar(max)',
 'SEKTN_GILTIG_TOM': 'varchar(max)',
 'SEKTN_ID': 'varchar(max)',
 'SEKTN_ID_TEXT': 'varchar(max)',
 'SEKTN_PASSIV': 'varchar(max)',
 'SEKTN_TEXT': 'varchar(max)',
 'SEKT_GILTIG_FOM': 'varchar(max)',
 'SEKT_GILTIG_TOM': 'varchar(max)',
 'SEKT_ID': 'varchar(max)',
 'SEKT_ID_TEXT': 'varchar(max)',
 'SEKT_PASSIV': 'varchar(max)',
 'SEKT_TEXT': 'varchar(max)'},
    kind=kind.FullKind,
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
 		CAST(AVD_GILTIG_FOM AS VARCHAR(MAX)) AS avd_giltig_fom,
		CAST(AVD_GILTIG_TOM AS VARCHAR(MAX)) AS avd_giltig_tom,
		CAST(AVD_ID AS VARCHAR(MAX)) AS avd_id,
		CAST(AVD_ID_TEXT AS VARCHAR(MAX)) AS avd_id_text,
		CAST(AVD_PASSIV AS VARCHAR(MAX)) AS avd_passiv,
		CAST(AVD_TEXT AS VARCHAR(MAX)) AS avd_text,
		CAST(AVDEL_GILTIG_FOM AS VARCHAR(MAX)) AS avdel_giltig_fom,
		CAST(AVDEL_GILTIG_TOM AS VARCHAR(MAX)) AS avdel_giltig_tom,
		CAST(AVDEL_ID AS VARCHAR(MAX)) AS avdel_id,
		CAST(AVDEL_ID_TEXT AS VARCHAR(MAX)) AS avdel_id_text,
		CAST(AVDEL_PASSIV AS VARCHAR(MAX)) AS avdel_passiv,
		CAST(AVDEL_TEXT AS VARCHAR(MAX)) AS avdel_text,
		CAST(DIR_GILTIG_FOM AS VARCHAR(MAX)) AS dir_giltig_fom,
		CAST(DIR_GILTIG_TOM AS VARCHAR(MAX)) AS dir_giltig_tom,
		CAST(DIR_ID AS VARCHAR(MAX)) AS dir_id,
		CAST(DIR_ID_TEXT AS VARCHAR(MAX)) AS dir_id_text,
		CAST(DIR_PASSIV AS VARCHAR(MAX)) AS dir_passiv,
		CAST(DIR_TEXT AS VARCHAR(MAX)) AS dir_text,
		CAST(ENH_GILTIG_FOM AS VARCHAR(MAX)) AS enh_giltig_fom,
		CAST(ENH_GILTIG_TOM AS VARCHAR(MAX)) AS enh_giltig_tom,
		CAST(ENH_ID AS VARCHAR(MAX)) AS enh_id,
		CAST(ENH_ID_TEXT AS VARCHAR(MAX)) AS enh_id_text,
		CAST(ENH_PASSIV AS VARCHAR(MAX)) AS enh_passiv,
		CAST(ENH_TEXT AS VARCHAR(MAX)) AS enh_text,
		CAST(ENHET_GILTIG_FOM AS VARCHAR(MAX)) AS enhet_giltig_fom,
		CAST(ENHET_GILTIG_TOM AS VARCHAR(MAX)) AS enhet_giltig_tom,
		CAST(ENHET_ID AS VARCHAR(MAX)) AS enhet_id,
		CAST(ENHET_ID_TEXT AS VARCHAR(MAX)) AS enhet_id_text,
		CAST(ENHET_PASSIV AS VARCHAR(MAX)) AS enhet_passiv,
		CAST(ENHET_TEXT AS VARCHAR(MAX)) AS enhet_text,
		CAST(KST_DATUM_FOM AS VARCHAR(MAX)) AS kst_datum_fom,
		CAST(KST_DATUM_TOM AS VARCHAR(MAX)) AS kst_datum_tom,
		CAST(KST_GILTIG_FOM AS VARCHAR(MAX)) AS kst_giltig_fom,
		CAST(KST_GILTIG_TOM AS VARCHAR(MAX)) AS kst_giltig_tom,
		CAST(KST_ID AS VARCHAR(MAX)) AS kst_id,
		CAST(KST_ID_TEXT AS VARCHAR(MAX)) AS kst_id_text,
		CAST(KST_PASSIV AS VARCHAR(MAX)) AS kst_passiv,
		CAST(KST_TEXT AS VARCHAR(MAX)) AS kst_text,
		CAST(RL_GILTIG_FOM AS VARCHAR(MAX)) AS rl_giltig_fom,
		CAST(RL_GILTIG_TOM AS VARCHAR(MAX)) AS rl_giltig_tom,
		CAST(RL_ID AS VARCHAR(MAX)) AS rl_id,
		CAST(RL_ID_TEXT AS VARCHAR(MAX)) AS rl_id_text,
		CAST(RL_PASSIV AS VARCHAR(MAX)) AS rl_passiv,
		CAST(RL_TEXT AS VARCHAR(MAX)) AS rl_text,
		CAST(SEKT_GILTIG_FOM AS VARCHAR(MAX)) AS sekt_giltig_fom,
		CAST(SEKT_GILTIG_TOM AS VARCHAR(MAX)) AS sekt_giltig_tom,
		CAST(SEKT_ID AS VARCHAR(MAX)) AS sekt_id,
		CAST(SEKT_ID_TEXT AS VARCHAR(MAX)) AS sekt_id_text,
		CAST(SEKT_PASSIV AS VARCHAR(MAX)) AS sekt_passiv,
		CAST(SEKT_TEXT AS VARCHAR(MAX)) AS sekt_text,
		CAST(SEKTN_GILTIG_FOM AS VARCHAR(MAX)) AS sektn_giltig_fom,
		CAST(SEKTN_GILTIG_TOM AS VARCHAR(MAX)) AS sektn_giltig_tom,
		CAST(SEKTN_ID AS VARCHAR(MAX)) AS sektn_id,
		CAST(SEKTN_ID_TEXT AS VARCHAR(MAX)) AS sektn_id_text,
		CAST(SEKTN_PASSIV AS VARCHAR(MAX)) AS sektn_passiv,
		CAST(SEKTN_TEXT AS VARCHAR(MAX)) AS sektn_text 
	FROM utdata.utdata295.EK_DATUM_OBJ_KST
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
