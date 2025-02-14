
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from models.mssql import read


@model(
    columns={'AVSLÅR_GILTIG_FOM': 'varchar(max)',
 'AVSLÅR_GILTIG_TOM': 'varchar(max)',
 'AVSLÅR_ID': 'varchar(max)',
 'AVSLÅR_ID_TEXT': 'varchar(max)',
 'AVSLÅR_PASSIV': 'varchar(max)',
 'AVSLÅR_TEXT': 'varchar(max)',
 'FIFORM_GILTIG_FOM': 'varchar(max)',
 'FIFORM_GILTIG_TOM': 'varchar(max)',
 'FIFORM_ID': 'varchar(max)',
 'FIFORM_ID_TEXT': 'varchar(max)',
 'FIFORM_PASSIV': 'varchar(max)',
 'FIFORM_TEXT': 'varchar(max)',
 'PROENH_GILTIG_FOM': 'varchar(max)',
 'PROENH_GILTIG_TOM': 'varchar(max)',
 'PROENH_ID': 'varchar(max)',
 'PROENH_ID_TEXT': 'varchar(max)',
 'PROENH_PASSIV': 'varchar(max)',
 'PROENH_TEXT': 'varchar(max)',
 'PROJL_GILTIG_FOM': 'varchar(max)',
 'PROJL_GILTIG_TOM': 'varchar(max)',
 'PROJL_ID': 'varchar(max)',
 'PROJL_ID_TEXT': 'varchar(max)',
 'PROJL_PASSIV': 'varchar(max)',
 'PROJL_TEXT': 'varchar(max)',
 'PROJ_DATUM_FOM': 'varchar(max)',
 'PROJ_DATUM_TOM': 'varchar(max)',
 'PROJ_GILTIG_FOM': 'varchar(max)',
 'PROJ_GILTIG_TOM': 'varchar(max)',
 'PROJ_ID': 'varchar(max)',
 'PROJ_ID_TEXT': 'varchar(max)',
 'PROJ_PASSIV': 'varchar(max)',
 'PROJ_TEXT': 'varchar(max)'},
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
 		CAST(AVSLÅR_GILTIG_FOM AS VARCHAR(MAX)) AS avslår_giltig_fom,
		CAST(AVSLÅR_GILTIG_TOM AS VARCHAR(MAX)) AS avslår_giltig_tom,
		CAST(AVSLÅR_ID AS VARCHAR(MAX)) AS avslår_id,
		CAST(AVSLÅR_ID_TEXT AS VARCHAR(MAX)) AS avslår_id_text,
		CAST(AVSLÅR_PASSIV AS VARCHAR(MAX)) AS avslår_passiv,
		CAST(AVSLÅR_TEXT AS VARCHAR(MAX)) AS avslår_text,
		CAST(FIFORM_GILTIG_FOM AS VARCHAR(MAX)) AS fiform_giltig_fom,
		CAST(FIFORM_GILTIG_TOM AS VARCHAR(MAX)) AS fiform_giltig_tom,
		CAST(FIFORM_ID AS VARCHAR(MAX)) AS fiform_id,
		CAST(FIFORM_ID_TEXT AS VARCHAR(MAX)) AS fiform_id_text,
		CAST(FIFORM_PASSIV AS VARCHAR(MAX)) AS fiform_passiv,
		CAST(FIFORM_TEXT AS VARCHAR(MAX)) AS fiform_text,
		CAST(PROENH_GILTIG_FOM AS VARCHAR(MAX)) AS proenh_giltig_fom,
		CAST(PROENH_GILTIG_TOM AS VARCHAR(MAX)) AS proenh_giltig_tom,
		CAST(PROENH_ID AS VARCHAR(MAX)) AS proenh_id,
		CAST(PROENH_ID_TEXT AS VARCHAR(MAX)) AS proenh_id_text,
		CAST(PROENH_PASSIV AS VARCHAR(MAX)) AS proenh_passiv,
		CAST(PROENH_TEXT AS VARCHAR(MAX)) AS proenh_text,
		CAST(PROJ_DATUM_FOM AS VARCHAR(MAX)) AS proj_datum_fom,
		CAST(PROJ_DATUM_TOM AS VARCHAR(MAX)) AS proj_datum_tom,
		CAST(PROJ_GILTIG_FOM AS VARCHAR(MAX)) AS proj_giltig_fom,
		CAST(PROJ_GILTIG_TOM AS VARCHAR(MAX)) AS proj_giltig_tom,
		CAST(PROJ_ID AS VARCHAR(MAX)) AS proj_id,
		CAST(PROJ_ID_TEXT AS VARCHAR(MAX)) AS proj_id_text,
		CAST(PROJ_PASSIV AS VARCHAR(MAX)) AS proj_passiv,
		CAST(PROJ_TEXT AS VARCHAR(MAX)) AS proj_text,
		CAST(PROJL_GILTIG_FOM AS VARCHAR(MAX)) AS projl_giltig_fom,
		CAST(PROJL_GILTIG_TOM AS VARCHAR(MAX)) AS projl_giltig_tom,
		CAST(PROJL_ID AS VARCHAR(MAX)) AS projl_id,
		CAST(PROJL_ID_TEXT AS VARCHAR(MAX)) AS projl_id_text,
		CAST(PROJL_PASSIV AS VARCHAR(MAX)) AS projl_passiv,
		CAST(PROJL_TEXT AS VARCHAR(MAX)) AS projl_text 
	FROM utdata.utdata295.EK_DATUM_OBJ_PROJ
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
