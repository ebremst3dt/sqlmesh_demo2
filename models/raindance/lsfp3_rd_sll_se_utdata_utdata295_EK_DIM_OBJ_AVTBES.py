
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read


@model(
    columns={'AVTBES_GILTIG_FOM': 'varchar(max)',
 'AVTBES_GILTIG_TOM': 'varchar(max)',
 'AVTBES_ID': 'varchar(max)',
 'AVTBES_ID_TEXT': 'varchar(max)',
 'AVTBES_PASSIV': 'varchar(max)',
 'AVTBES_TEXT': 'varchar(max)'},
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
 		CONVERT(varchar(max), AVTBES_GILTIG_FOM, 126) AS avtbes_giltig_fom,
		CONVERT(varchar(max), AVTBES_GILTIG_TOM, 126) AS avtbes_giltig_tom,
		CAST(AVTBES_ID AS VARCHAR(MAX)) AS avtbes_id,
		CAST(AVTBES_ID_TEXT AS VARCHAR(MAX)) AS avtbes_id_text,
		CAST(AVTBES_PASSIV AS VARCHAR(MAX)) AS avtbes_passiv,
		CAST(AVTBES_TEXT AS VARCHAR(MAX)) AS avtbes_text 
	FROM utdata.utdata295.EK_DIM_OBJ_AVTBES
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
