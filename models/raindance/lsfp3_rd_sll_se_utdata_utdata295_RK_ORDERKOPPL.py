
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read


@model(
    columns={'FAKTNR': 'varchar(max)', 'ORDERNR': 'varchar(max)'},
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
 		CAST(FAKTNR AS VARCHAR(MAX)) AS faktnr,
		CAST(ORDERNR AS VARCHAR(MAX)) AS ordernr 
	FROM utdata.utdata295.RK_ORDERKOPPL
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
