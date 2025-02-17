
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read


@model(
    columns={'D2': 'varchar(6)',
 'D3': 'varchar(6)',
 'DD3': 'varchar(6)',
 'ID': 'numeric',
 'S': 'varchar(3)',
 'S1': 'varchar(3)',
 'S2': 'varchar(3)',
 'S3': 'varchar(3)',
 'ZEROS': 'bit'},
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
 		CAST(D2 AS VARCHAR(MAX)) AS d2,
		CAST(D3 AS VARCHAR(MAX)) AS d3,
		CAST(DD3 AS VARCHAR(MAX)) AS dd3,
		CAST(ID AS VARCHAR(MAX)) AS id,
		CAST(S AS VARCHAR(MAX)) AS s,
		CAST(S1 AS VARCHAR(MAX)) AS s1,
		CAST(S2 AS VARCHAR(MAX)) AS s2,
		CAST(S3 AS VARCHAR(MAX)) AS s3,
		CAST(ZEROS AS VARCHAR(MAX)) AS zeros 
	FROM utdata.utdata295.EK_UTILITY
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
