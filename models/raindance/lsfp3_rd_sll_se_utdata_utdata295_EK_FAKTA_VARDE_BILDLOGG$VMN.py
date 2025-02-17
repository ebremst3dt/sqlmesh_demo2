
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model import kind
from models.mssql import read


@model(
    columns={'LOPNUMMER': 'varchar(max)',
 'VERDATUM': 'varchar(max)',
 'VMN_TEXT': 'varchar(max)'},
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
 		CAST(LOPNUMMER AS VARCHAR(MAX)) AS lopnummer,
		CAST(VERDATUM AS VARCHAR(MAX)) AS verdatum,
		CAST(VMN_TEXT AS VARCHAR(MAX)) AS vmn_text 
	FROM utdata.utdata295.EK_FAKTA_VARDE_BILDLOGG$VMN
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
