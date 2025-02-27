
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read


@model(
    columns={'BOPER_DATUM_FOM': 'varchar(max)',
 'BOPER_DATUM_TOM': 'varchar(max)',
 'BOPER_GILTIG_FOM': 'varchar(max)',
 'BOPER_GILTIG_TOM': 'varchar(max)',
 'BOPER_ID': 'varchar(max)',
 'BOPER_ID_TEXT': 'varchar(max)',
 'BOPER_PASSIV': 'varchar(max)',
 'BOPER_TEXT': 'varchar(max)'},
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
 		CONVERT(varchar(max), BOPER_DATUM_FOM, 126) AS boper_datum_fom,
		CONVERT(varchar(max), BOPER_DATUM_TOM, 126) AS boper_datum_tom,
		CONVERT(varchar(max), BOPER_GILTIG_FOM, 126) AS boper_giltig_fom,
		CONVERT(varchar(max), BOPER_GILTIG_TOM, 126) AS boper_giltig_tom,
		CAST(BOPER_ID AS VARCHAR(MAX)) AS boper_id,
		CAST(BOPER_ID_TEXT AS VARCHAR(MAX)) AS boper_id_text,
		CAST(BOPER_PASSIV AS VARCHAR(MAX)) AS boper_passiv,
		CAST(BOPER_TEXT AS VARCHAR(MAX)) AS boper_text 
	FROM utdata.utdata295.EK_DATUM_OBJ_BOPER
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
