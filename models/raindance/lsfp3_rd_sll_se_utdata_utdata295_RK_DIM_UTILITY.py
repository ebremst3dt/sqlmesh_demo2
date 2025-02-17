
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read


@model(
    columns={'UTILITY': 'numeric', 'UTILITY_TEXT': 'varchar(30)'},
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
 		CAST(UTILITY AS VARCHAR(MAX)) AS utility,
		CAST(UTILITY_TEXT AS VARCHAR(MAX)) AS utility_text 
	FROM utdata.utdata295.RK_DIM_UTILITY
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
