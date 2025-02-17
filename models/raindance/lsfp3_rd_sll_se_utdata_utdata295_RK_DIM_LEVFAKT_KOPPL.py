
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read


@model(
    columns={'KORR1': 'numeric',
 'KORR10': 'numeric',
 'KORR2': 'numeric',
 'KORR3': 'numeric',
 'KORR4': 'numeric',
 'KORR5': 'numeric',
 'KORR6': 'numeric',
 'KORR7': 'numeric',
 'KORR8': 'numeric',
 'KORR9': 'numeric',
 'NR': 'numeric'},
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
 		CAST(KORR1 AS VARCHAR(MAX)) AS korr1,
		CAST(KORR10 AS VARCHAR(MAX)) AS korr10,
		CAST(KORR2 AS VARCHAR(MAX)) AS korr2,
		CAST(KORR3 AS VARCHAR(MAX)) AS korr3,
		CAST(KORR4 AS VARCHAR(MAX)) AS korr4,
		CAST(KORR5 AS VARCHAR(MAX)) AS korr5,
		CAST(KORR6 AS VARCHAR(MAX)) AS korr6,
		CAST(KORR7 AS VARCHAR(MAX)) AS korr7,
		CAST(KORR8 AS VARCHAR(MAX)) AS korr8,
		CAST(KORR9 AS VARCHAR(MAX)) AS korr9,
		CAST(NR AS VARCHAR(MAX)) AS nr 
	FROM utdata.utdata295.RK_DIM_LEVFAKT_KOPPL
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
