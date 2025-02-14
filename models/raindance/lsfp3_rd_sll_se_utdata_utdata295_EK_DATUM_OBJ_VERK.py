
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from models.mssql import read


@model(
    columns={'VERK_DATUM_FOM': 'varchar(max)',
 'VERK_DATUM_TOM': 'varchar(max)',
 'VERK_GILTIG_FOM': 'varchar(max)',
 'VERK_GILTIG_TOM': 'varchar(max)',
 'VERK_ID': 'varchar(max)',
 'VERK_ID_TEXT': 'varchar(max)',
 'VERK_PASSIV': 'varchar(max)',
 'VERK_TEXT': 'varchar(max)',
 'VGREN_GILTIG_FOM': 'varchar(max)',
 'VGREN_GILTIG_TOM': 'varchar(max)',
 'VGREN_ID': 'varchar(max)',
 'VGREN_ID_TEXT': 'varchar(max)',
 'VGREN_PASSIV': 'varchar(max)',
 'VGREN_TEXT': 'varchar(max)'},
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
 		CAST(VERK_DATUM_FOM AS VARCHAR(MAX)) AS verk_datum_fom,
		CAST(VERK_DATUM_TOM AS VARCHAR(MAX)) AS verk_datum_tom,
		CAST(VERK_GILTIG_FOM AS VARCHAR(MAX)) AS verk_giltig_fom,
		CAST(VERK_GILTIG_TOM AS VARCHAR(MAX)) AS verk_giltig_tom,
		CAST(VERK_ID AS VARCHAR(MAX)) AS verk_id,
		CAST(VERK_ID_TEXT AS VARCHAR(MAX)) AS verk_id_text,
		CAST(VERK_PASSIV AS VARCHAR(MAX)) AS verk_passiv,
		CAST(VERK_TEXT AS VARCHAR(MAX)) AS verk_text,
		CAST(VGREN_GILTIG_FOM AS VARCHAR(MAX)) AS vgren_giltig_fom,
		CAST(VGREN_GILTIG_TOM AS VARCHAR(MAX)) AS vgren_giltig_tom,
		CAST(VGREN_ID AS VARCHAR(MAX)) AS vgren_id,
		CAST(VGREN_ID_TEXT AS VARCHAR(MAX)) AS vgren_id_text,
		CAST(VGREN_PASSIV AS VARCHAR(MAX)) AS vgren_passiv,
		CAST(VGREN_TEXT AS VARCHAR(MAX)) AS vgren_text 
	FROM utdata.utdata295.EK_DATUM_OBJ_VERK
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
