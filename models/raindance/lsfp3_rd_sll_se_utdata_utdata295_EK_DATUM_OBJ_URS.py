
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
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
	SELECT TOP 1000 top 1000
 		CONVERT(varchar(max), URS_DATUM_FOM, 126) AS urs_datum_fom,
		CONVERT(varchar(max), URS_DATUM_TOM, 126) AS urs_datum_tom,
		CONVERT(varchar(max), URS_GILTIG_FOM, 126) AS urs_giltig_fom,
		CONVERT(varchar(max), URS_GILTIG_TOM, 126) AS urs_giltig_tom,
		CAST(URS_ID AS VARCHAR(MAX)) AS urs_id,
		CAST(URS_ID_TEXT AS VARCHAR(MAX)) AS urs_id_text,
		CAST(URS_PASSIV AS VARCHAR(MAX)) AS urs_passiv,
		CAST(URS_TEXT AS VARCHAR(MAX)) AS urs_text 
	FROM utdata.utdata295.EK_DATUM_OBJ_URS
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
