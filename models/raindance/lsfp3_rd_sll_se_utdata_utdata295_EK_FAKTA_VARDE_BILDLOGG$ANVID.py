
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read


@model(
    columns={'ANVID_TEXT': 'varchar(max)',
 'LOPNUMMER': 'varchar(max)',
 'VERDATUM': 'varchar(max)'},
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
 		CAST(ANVID_TEXT AS VARCHAR(MAX)) AS anvid_text,
		CAST(LOPNUMMER AS VARCHAR(MAX)) AS lopnummer,
		CAST(VERDATUM AS VARCHAR(MAX)) AS verdatum 
	FROM utdata.utdata295.EK_FAKTA_VARDE_BILDLOGG$ANVID
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
