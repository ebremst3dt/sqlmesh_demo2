
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from models.raindance_src_to_raw import pipe


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
    cron="@daily"
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> pd.DataFrame:
    query = """SELECT CAST(AVD_GILTIG_FOM AS VARCHAR(MAX)) AS AVD_GILTIG_FOM,
CAST(AVD_GILTIG_TOM AS VARCHAR(MAX)) AS AVD_GILTIG_TOM,
CAST(AVD_ID AS VARCHAR(MAX)) AS AVD_ID,
CAST(AVD_ID_TEXT AS VARCHAR(MAX)) AS AVD_ID_TEXT,
CAST(AVD_PASSIV AS VARCHAR(MAX)) AS AVD_PASSIV,
CAST(AVD_TEXT AS VARCHAR(MAX)) AS AVD_TEXT,
CAST(DIR_GILTIG_FOM AS VARCHAR(MAX)) AS DIR_GILTIG_FOM,
CAST(DIR_GILTIG_TOM AS VARCHAR(MAX)) AS DIR_GILTIG_TOM,
CAST(DIR_ID AS VARCHAR(MAX)) AS DIR_ID,
CAST(DIR_ID_TEXT AS VARCHAR(MAX)) AS DIR_ID_TEXT,
CAST(DIR_PASSIV AS VARCHAR(MAX)) AS DIR_PASSIV,
CAST(DIR_TEXT AS VARCHAR(MAX)) AS DIR_TEXT,
CAST(ENHET_GILTIG_FOM AS VARCHAR(MAX)) AS ENHET_GILTIG_FOM,
CAST(ENHET_GILTIG_TOM AS VARCHAR(MAX)) AS ENHET_GILTIG_TOM,
CAST(ENHET_ID AS VARCHAR(MAX)) AS ENHET_ID,
CAST(ENHET_ID_TEXT AS VARCHAR(MAX)) AS ENHET_ID_TEXT,
CAST(ENHET_PASSIV AS VARCHAR(MAX)) AS ENHET_PASSIV,
CAST(ENHET_TEXT AS VARCHAR(MAX)) AS ENHET_TEXT,
CAST(KST_GILTIG_FOM AS VARCHAR(MAX)) AS KST_GILTIG_FOM,
CAST(KST_GILTIG_TOM AS VARCHAR(MAX)) AS KST_GILTIG_TOM,
CAST(KST_ID AS VARCHAR(MAX)) AS KST_ID,
CAST(KST_ID_TEXT AS VARCHAR(MAX)) AS KST_ID_TEXT,
CAST(KST_PASSIV AS VARCHAR(MAX)) AS KST_PASSIV,
CAST(KST_TEXT AS VARCHAR(MAX)) AS KST_TEXT,
CAST(SEKT_GILTIG_FOM AS VARCHAR(MAX)) AS SEKT_GILTIG_FOM,
CAST(SEKT_GILTIG_TOM AS VARCHAR(MAX)) AS SEKT_GILTIG_TOM,
CAST(SEKT_ID AS VARCHAR(MAX)) AS SEKT_ID,
CAST(SEKT_ID_TEXT AS VARCHAR(MAX)) AS SEKT_ID_TEXT,
CAST(SEKT_PASSIV AS VARCHAR(MAX)) AS SEKT_PASSIV,
CAST(SEKT_TEXT AS VARCHAR(MAX)) AS SEKT_TEXT FROM utdata.utdata295.EK_DIM_OBJ_KST_DIR_28"""
    return pipe(query=query)
