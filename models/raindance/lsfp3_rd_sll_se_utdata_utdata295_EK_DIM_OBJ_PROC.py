
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read


@model(
    columns={'PROC_GILTIG_FOM': 'varchar(max)',
 'PROC_GILTIG_TOM': 'varchar(max)',
 'PROC_ID': 'varchar(max)',
 'PROC_ID_TEXT': 'varchar(max)',
 'PROC_PASSIV': 'varchar(max)',
 'PROC_TEXT': 'varchar(max)'},
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
 		CONVERT(varchar(max), PROC_GILTIG_FOM, 126) AS proc_giltig_fom,
		CONVERT(varchar(max), PROC_GILTIG_TOM, 126) AS proc_giltig_tom,
		CAST(PROC_ID AS VARCHAR(MAX)) AS proc_id,
		CAST(PROC_ID_TEXT AS VARCHAR(MAX)) AS proc_id_text,
		CAST(PROC_PASSIV AS VARCHAR(MAX)) AS proc_passiv,
		CAST(PROC_TEXT AS VARCHAR(MAX)) AS proc_text 
	FROM utdata.utdata295.EK_DIM_OBJ_PROC
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
