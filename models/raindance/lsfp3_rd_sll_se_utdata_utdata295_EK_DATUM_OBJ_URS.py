
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model import kind
from models.mssql import read


@model(
    columns={'URS_DATUM_FOM': 'varchar(max)',
 'URS_DATUM_TOM': 'varchar(max)',
 'URS_GILTIG_FOM': 'varchar(max)',
 'URS_GILTIG_TOM': 'varchar(max)',
 'URS_ID': 'varchar(max)',
 'URS_ID_TEXT': 'varchar(max)',
 'URS_PASSIV': 'varchar(max)',
 'URS_TEXT': 'varchar(max)'},
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
 		CAST(URS_DATUM_FOM AS VARCHAR(MAX)) AS urs_datum_fom,
		CAST(URS_DATUM_TOM AS VARCHAR(MAX)) AS urs_datum_tom,
		CAST(URS_GILTIG_FOM AS VARCHAR(MAX)) AS urs_giltig_fom,
		CAST(URS_GILTIG_TOM AS VARCHAR(MAX)) AS urs_giltig_tom,
		CAST(URS_ID AS VARCHAR(MAX)) AS urs_id,
		CAST(URS_ID_TEXT AS VARCHAR(MAX)) AS urs_id_text,
		CAST(URS_PASSIV AS VARCHAR(MAX)) AS urs_passiv,
		CAST(URS_TEXT AS VARCHAR(MAX)) AS urs_text 
	FROM utdata.utdata295.EK_DATUM_OBJ_URS
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
