
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read


@model(
    columns={'DEFANL_DATUM_FOM': 'varchar(max)',
 'DEFANL_DATUM_TOM': 'varchar(max)',
 'DEFANL_GILTIG_FOM': 'varchar(max)',
 'DEFANL_GILTIG_TOM': 'varchar(max)',
 'DEFANL_ID': 'varchar(max)',
 'DEFANL_ID_TEXT': 'varchar(max)',
 'DEFANL_PASSIV': 'varchar(max)',
 'DEFANL_TEXT': 'varchar(max)'},
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
 		CONVERT(varchar(max), DEFANL_DATUM_FOM, 126) AS defanl_datum_fom,
		CONVERT(varchar(max), DEFANL_DATUM_TOM, 126) AS defanl_datum_tom,
		CONVERT(varchar(max), DEFANL_GILTIG_FOM, 126) AS defanl_giltig_fom,
		CONVERT(varchar(max), DEFANL_GILTIG_TOM, 126) AS defanl_giltig_tom,
		CAST(DEFANL_ID AS VARCHAR(MAX)) AS defanl_id,
		CAST(DEFANL_ID_TEXT AS VARCHAR(MAX)) AS defanl_id_text,
		CAST(DEFANL_PASSIV AS VARCHAR(MAX)) AS defanl_passiv,
		CAST(DEFANL_TEXT AS VARCHAR(MAX)) AS defanl_text 
	FROM utdata.utdata295.EK_DATUM_OBJ_DEFANL
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
