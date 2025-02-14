
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from models.mssql import read


@model(
    columns={'ANSTFORM_ID': 'varchar(max)',
 'ANSTFORM_ID_TEXT': 'varchar(max)',
 'ANSTFORM_TEXT': 'varchar(max)'},
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
 		CAST(ANSTFORM_ID AS VARCHAR(MAX)) AS anstform_id,
		CAST(ANSTFORM_ID_TEXT AS VARCHAR(MAX)) AS anstform_id_text,
		CAST(ANSTFORM_TEXT AS VARCHAR(MAX)) AS anstform_text 
	FROM utdata.utdata295.EK_DIM_ANSTFORM
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
