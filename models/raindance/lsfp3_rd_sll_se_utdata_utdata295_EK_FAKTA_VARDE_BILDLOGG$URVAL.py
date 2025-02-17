
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model import kind
from models.mssql import read


@model(
    columns={'LOPNUMMER': 'varchar(max)',
 'URVAL_TEXT': 'varchar(max)',
 'VERDATUM': 'varchar(max)'},
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
		CAST(URVAL_TEXT AS VARCHAR(MAX)) AS urval_text,
		CAST(VERDATUM AS VARCHAR(MAX)) AS verdatum 
	FROM utdata.utdata295.EK_FAKTA_VARDE_BILDLOGG$URVAL
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
