
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read


@model(
    columns={'AVSLÅR_GILTIG_FOM': 'datetime',
 'AVSLÅR_GILTIG_TOM': 'datetime',
 'AVSLÅR_ID': 'varchar(2)',
 'AVSLÅR_ID_TEXT': 'varchar(33)',
 'AVSLÅR_PASSIV': 'bit',
 'AVSLÅR_TEXT': 'varchar(30)',
 'FIFORM_GILTIG_FOM': 'datetime',
 'FIFORM_GILTIG_TOM': 'datetime',
 'FIFORM_ID': 'varchar(3)',
 'FIFORM_ID_TEXT': 'varchar(34)',
 'FIFORM_PASSIV': 'bit',
 'FIFORM_TEXT': 'varchar(30)',
 'PROENH_GILTIG_FOM': 'datetime',
 'PROENH_GILTIG_TOM': 'datetime',
 'PROENH_ID': 'varchar(6)',
 'PROENH_ID_TEXT': 'varchar(37)',
 'PROENH_PASSIV': 'bit',
 'PROENH_TEXT': 'varchar(30)',
 'PROJL_GILTIG_FOM': 'datetime',
 'PROJL_GILTIG_TOM': 'datetime',
 'PROJL_ID': 'varchar(4)',
 'PROJL_ID_TEXT': 'varchar(35)',
 'PROJL_PASSIV': 'bit',
 'PROJL_TEXT': 'varchar(30)',
 'PROJ_GILTIG_FOM': 'datetime',
 'PROJ_GILTIG_TOM': 'datetime',
 'PROJ_ID': 'varchar(5)',
 'PROJ_ID_TEXT': 'varchar(36)',
 'PROJ_PASSIV': 'bit',
 'PROJ_TEXT': 'varchar(30)'},
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
 		CONVERT(varchar(max), AVSLÅR_GILTIG_FOM, 126) AS avslår_giltig_fom,
		CONVERT(varchar(max), AVSLÅR_GILTIG_TOM, 126) AS avslår_giltig_tom,
		CAST(AVSLÅR_ID AS VARCHAR(MAX)) AS avslår_id,
		CAST(AVSLÅR_ID_TEXT AS VARCHAR(MAX)) AS avslår_id_text,
		CAST(AVSLÅR_PASSIV AS VARCHAR(MAX)) AS avslår_passiv,
		CAST(AVSLÅR_TEXT AS VARCHAR(MAX)) AS avslår_text,
		CONVERT(varchar(max), FIFORM_GILTIG_FOM, 126) AS fiform_giltig_fom,
		CONVERT(varchar(max), FIFORM_GILTIG_TOM, 126) AS fiform_giltig_tom,
		CAST(FIFORM_ID AS VARCHAR(MAX)) AS fiform_id,
		CAST(FIFORM_ID_TEXT AS VARCHAR(MAX)) AS fiform_id_text,
		CAST(FIFORM_PASSIV AS VARCHAR(MAX)) AS fiform_passiv,
		CAST(FIFORM_TEXT AS VARCHAR(MAX)) AS fiform_text,
		CONVERT(varchar(max), PROENH_GILTIG_FOM, 126) AS proenh_giltig_fom,
		CONVERT(varchar(max), PROENH_GILTIG_TOM, 126) AS proenh_giltig_tom,
		CAST(PROENH_ID AS VARCHAR(MAX)) AS proenh_id,
		CAST(PROENH_ID_TEXT AS VARCHAR(MAX)) AS proenh_id_text,
		CAST(PROENH_PASSIV AS VARCHAR(MAX)) AS proenh_passiv,
		CAST(PROENH_TEXT AS VARCHAR(MAX)) AS proenh_text,
		CONVERT(varchar(max), PROJ_GILTIG_FOM, 126) AS proj_giltig_fom,
		CONVERT(varchar(max), PROJ_GILTIG_TOM, 126) AS proj_giltig_tom,
		CAST(PROJ_ID AS VARCHAR(MAX)) AS proj_id,
		CAST(PROJ_ID_TEXT AS VARCHAR(MAX)) AS proj_id_text,
		CAST(PROJ_PASSIV AS VARCHAR(MAX)) AS proj_passiv,
		CAST(PROJ_TEXT AS VARCHAR(MAX)) AS proj_text,
		CONVERT(varchar(max), PROJL_GILTIG_FOM, 126) AS projl_giltig_fom,
		CONVERT(varchar(max), PROJL_GILTIG_TOM, 126) AS projl_giltig_tom,
		CAST(PROJL_ID AS VARCHAR(MAX)) AS projl_id,
		CAST(PROJL_ID_TEXT AS VARCHAR(MAX)) AS projl_id_text,
		CAST(PROJL_PASSIV AS VARCHAR(MAX)) AS projl_passiv,
		CAST(PROJL_TEXT AS VARCHAR(MAX)) AS projl_text 
	FROM utdata.utdata295.EK_DIM_OBJ_PROJ_DIR_28
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
