
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read


@model(
    columns={'PART': 'varchar(max)', 'SBID': 'varchar(max)'},
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
 		CAST(PART AS VARCHAR(MAX)) AS part,
		CAST(SBID AS VARCHAR(MAX)) AS sbid 
	FROM utdata.utdata295.RK_DIM_LEV_PART
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
