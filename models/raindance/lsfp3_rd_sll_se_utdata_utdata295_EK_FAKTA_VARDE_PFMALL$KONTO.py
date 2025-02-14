
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from models.mssql import read


@model(
    columns={'KONTO_ID': 'varchar(max)',
 'MALLID_ID': 'varchar(max)',
 'RAK_ID': 'varchar(max)'},
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
 		CAST(KONTO_ID AS VARCHAR(MAX)) AS konto_id,
		CAST(MALLID_ID AS VARCHAR(MAX)) AS mallid_id,
		CAST(RAK_ID AS VARCHAR(MAX)) AS rak_id 
	FROM utdata.utdata295.EK_FAKTA_VARDE_PFMALL$KONTO
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
