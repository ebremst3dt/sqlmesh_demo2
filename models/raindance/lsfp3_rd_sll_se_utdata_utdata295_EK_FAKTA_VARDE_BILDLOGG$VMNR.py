
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from models.mssql import read


@model(
    columns={'LOPNUMMER': 'varchar(max)',
 'VERDATUM': 'varchar(max)',
 'VMNR_TEXT': 'varchar(max)'},
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
		CAST(VMNR_TEXT AS VARCHAR(MAX)) AS vmnr_text 
	FROM utdata.utdata295.EK_FAKTA_VARDE_BILDLOGG$VMNR
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
