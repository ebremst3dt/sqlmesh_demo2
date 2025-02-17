
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read


@model(
    columns={'DEFANL_GILTIG_FOM': 'datetime',
 'DEFANL_GILTIG_TOM': 'datetime',
 'DEFANL_ID': 'varchar(14)',
 'DEFANL_ID_TEXT': 'varchar(45)',
 'DEFANL_PASSIV': 'bit',
 'DEFANL_TEXT': 'varchar(30)'},
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
 		CONVERT(varchar(max), DEFANL_GILTIG_FOM, 126) AS defanl_giltig_fom,
		CONVERT(varchar(max), DEFANL_GILTIG_TOM, 126) AS defanl_giltig_tom,
		CAST(DEFANL_ID AS VARCHAR(MAX)) AS defanl_id,
		CAST(DEFANL_ID_TEXT AS VARCHAR(MAX)) AS defanl_id_text,
		CAST(DEFANL_PASSIV AS VARCHAR(MAX)) AS defanl_passiv,
		CAST(DEFANL_TEXT AS VARCHAR(MAX)) AS defanl_text 
	FROM utdata.utdata295.EK_DIM_OBJ_DEFANL
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
