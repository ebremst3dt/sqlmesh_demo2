
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read


@model(
    columns={'PNYCKL_GILTIG_FOM': 'varchar(max)',
 'PNYCKL_GILTIG_TOM': 'varchar(max)',
 'PNYCKL_ID': 'varchar(max)',
 'PNYCKL_ID_TEXT': 'varchar(max)',
 'PNYCKL_PASSIV': 'varchar(max)',
 'PNYCKL_TEXT': 'varchar(max)'},
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
 		CONVERT(varchar(max), PNYCKL_GILTIG_FOM, 126) AS pnyckl_giltig_fom,
		CONVERT(varchar(max), PNYCKL_GILTIG_TOM, 126) AS pnyckl_giltig_tom,
		CAST(PNYCKL_ID AS VARCHAR(MAX)) AS pnyckl_id,
		CAST(PNYCKL_ID_TEXT AS VARCHAR(MAX)) AS pnyckl_id_text,
		CAST(PNYCKL_PASSIV AS VARCHAR(MAX)) AS pnyckl_passiv,
		CAST(PNYCKL_TEXT AS VARCHAR(MAX)) AS pnyckl_text 
	FROM utdata.utdata295.EK_DIM_OBJ_PNYCKL
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
