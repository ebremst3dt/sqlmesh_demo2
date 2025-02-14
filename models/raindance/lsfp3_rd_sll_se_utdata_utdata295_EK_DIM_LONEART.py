
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from models.mssql import read


@model(
    columns={'LONEART_GILTIG_FOM': 'varchar(max)',
 'LONEART_GILTIG_TOM': 'varchar(max)',
 'LONEART_ID': 'varchar(max)',
 'LONEART_ID_TEXT': 'varchar(max)',
 'LONEART_PASSIV': 'varchar(max)',
 'LONEART_TEXT': 'varchar(max)'},
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
 		CAST(LONEART_GILTIG_FOM AS VARCHAR(MAX)) AS loneart_giltig_fom,
		CAST(LONEART_GILTIG_TOM AS VARCHAR(MAX)) AS loneart_giltig_tom,
		CAST(LONEART_ID AS VARCHAR(MAX)) AS loneart_id,
		CAST(LONEART_ID_TEXT AS VARCHAR(MAX)) AS loneart_id_text,
		CAST(LONEART_PASSIV AS VARCHAR(MAX)) AS loneart_passiv,
		CAST(LONEART_TEXT AS VARCHAR(MAX)) AS loneart_text 
	FROM utdata.utdata295.EK_DIM_LONEART
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
