
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read


@model(
    columns={'AVTBES_GILTIG_FOM': 'datetime',
 'AVTBES_GILTIG_TOM': 'datetime',
 'AVTBES_ID': 'varchar(7)',
 'AVTBES_ID_TEXT': 'varchar(38)',
 'AVTBES_PASSIV': 'bit',
 'AVTBES_TEXT': 'varchar(30)'},
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
 		CONVERT(varchar(max), AVTBES_GILTIG_FOM, 126) AS avtbes_giltig_fom,
		CONVERT(varchar(max), AVTBES_GILTIG_TOM, 126) AS avtbes_giltig_tom,
		CAST(AVTBES_ID AS VARCHAR(MAX)) AS avtbes_id,
		CAST(AVTBES_ID_TEXT AS VARCHAR(MAX)) AS avtbes_id_text,
		CAST(AVTBES_PASSIV AS VARCHAR(MAX)) AS avtbes_passiv,
		CAST(AVTBES_TEXT AS VARCHAR(MAX)) AS avtbes_text 
	FROM utdata.utdata295.EK_DIM_OBJ_AVTBES
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
