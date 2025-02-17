
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read


@model(
    columns={'LONEART_GILTIG_FOM': 'datetime',
 'LONEART_GILTIG_TOM': 'datetime',
 'LONEART_ID': 'varchar(20)',
 'LONEART_ID_TEXT': 'varchar(50)',
 'LONEART_PASSIV': 'bit',
 'LONEART_TEXT': 'varchar(50)'},
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
 		CONVERT(varchar(max), LONEART_GILTIG_FOM, 126) AS loneart_giltig_fom,
		CONVERT(varchar(max), LONEART_GILTIG_TOM, 126) AS loneart_giltig_tom,
		CAST(LONEART_ID AS VARCHAR(MAX)) AS loneart_id,
		CAST(LONEART_ID_TEXT AS VARCHAR(MAX)) AS loneart_id_text,
		CAST(LONEART_PASSIV AS VARCHAR(MAX)) AS loneart_passiv,
		CAST(LONEART_TEXT AS VARCHAR(MAX)) AS loneart_text 
	FROM utdata.utdata295.EK_DIM_LONEART
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
