
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read


@model(
    columns={'LOPNUMMER': 'int', 'VERDATUM': 'datetime', 'VMN_TEXT': 'varchar(120)'},
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
 		CAST(LOPNUMMER AS VARCHAR(MAX)) AS lopnummer,
		CONVERT(varchar(max), VERDATUM, 126) AS verdatum,
		CAST(VMN_TEXT AS VARCHAR(MAX)) AS vmn_text 
	FROM utdata.utdata295.EK_FAKTA_VARDE_BILDLOGG$VMN
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
