
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model import kind
from models.mssql import read


@model(
    columns={'IB': 'varchar(max)', 'IB_TEXT': 'varchar(max)'},
    kind=kind.FullKind,
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
 		CAST(IB AS VARCHAR(MAX)) AS ib,
		CAST(IB_TEXT AS VARCHAR(MAX)) AS ib_text 
	FROM utdata.utdata295.EK_DIM_IB
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
