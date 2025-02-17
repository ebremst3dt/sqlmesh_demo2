
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read


@model(
    columns={'AVD_GILTIG_FOM': 'varchar(max)',
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
 'KST_GILTIG_FOM': 'varchar(max)',
 'KST_GILTIG_TOM': 'varchar(max)',
 'KST_ID': 'varchar(max)',
 'KST_ID_TEXT': 'varchar(max)',
 'KST_PASSIV': 'varchar(max)',
 'KST_TEXT': 'varchar(max)',
 'SEKT_GILTIG_FOM': 'varchar(max)',
 'SEKT_GILTIG_TOM': 'varchar(max)',
 'SEKT_ID': 'varchar(max)',
 'SEKT_ID_TEXT': 'varchar(max)',
 'SEKT_PASSIV': 'varchar(max)',
 'SEKT_TEXT': 'varchar(max)'},
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
 		CAST(AVD_GILTIG_FOM AS VARCHAR(MAX)) AS avd_giltig_fom,
		CAST(AVD_GILTIG_TOM AS VARCHAR(MAX)) AS avd_giltig_tom,
		CAST(AVD_ID AS VARCHAR(MAX)) AS avd_id,
		CAST(AVD_ID_TEXT AS VARCHAR(MAX)) AS avd_id_text,
		CAST(AVD_PASSIV AS VARCHAR(MAX)) AS avd_passiv,
		CAST(AVD_TEXT AS VARCHAR(MAX)) AS avd_text,
		CAST(DIR_GILTIG_FOM AS VARCHAR(MAX)) AS dir_giltig_fom,
		CAST(DIR_GILTIG_TOM AS VARCHAR(MAX)) AS dir_giltig_tom,
		CAST(DIR_ID AS VARCHAR(MAX)) AS dir_id,
		CAST(DIR_ID_TEXT AS VARCHAR(MAX)) AS dir_id_text,
		CAST(DIR_PASSIV AS VARCHAR(MAX)) AS dir_passiv,
		CAST(DIR_TEXT AS VARCHAR(MAX)) AS dir_text,
		CAST(ENHET_GILTIG_FOM AS VARCHAR(MAX)) AS enhet_giltig_fom,
		CAST(ENHET_GILTIG_TOM AS VARCHAR(MAX)) AS enhet_giltig_tom,
		CAST(ENHET_ID AS VARCHAR(MAX)) AS enhet_id,
		CAST(ENHET_ID_TEXT AS VARCHAR(MAX)) AS enhet_id_text,
		CAST(ENHET_PASSIV AS VARCHAR(MAX)) AS enhet_passiv,
		CAST(ENHET_TEXT AS VARCHAR(MAX)) AS enhet_text,
		CAST(KST_GILTIG_FOM AS VARCHAR(MAX)) AS kst_giltig_fom,
		CAST(KST_GILTIG_TOM AS VARCHAR(MAX)) AS kst_giltig_tom,
		CAST(KST_ID AS VARCHAR(MAX)) AS kst_id,
		CAST(KST_ID_TEXT AS VARCHAR(MAX)) AS kst_id_text,
		CAST(KST_PASSIV AS VARCHAR(MAX)) AS kst_passiv,
		CAST(KST_TEXT AS VARCHAR(MAX)) AS kst_text,
		CAST(SEKT_GILTIG_FOM AS VARCHAR(MAX)) AS sekt_giltig_fom,
		CAST(SEKT_GILTIG_TOM AS VARCHAR(MAX)) AS sekt_giltig_tom,
		CAST(SEKT_ID AS VARCHAR(MAX)) AS sekt_id,
		CAST(SEKT_ID_TEXT AS VARCHAR(MAX)) AS sekt_id_text,
		CAST(SEKT_PASSIV AS VARCHAR(MAX)) AS sekt_passiv,
		CAST(SEKT_TEXT AS VARCHAR(MAX)) AS sekt_text 
	FROM utdata.utdata295.EK_DIM_OBJ_KST_DIR_28
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
