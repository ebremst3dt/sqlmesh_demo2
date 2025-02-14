
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from models.mssql import read


@model(
    columns={'ANVID_ID': 'varchar(max)',
 'BAS_V': 'varchar(max)',
 'KMALL_ID': 'varchar(max)',
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
 		CAST(ANVID_ID AS VARCHAR(MAX)) AS anvid_id,
		CAST(BAS_V AS VARCHAR(MAX)) AS bas_v,
		CAST(KMALL_ID AS VARCHAR(MAX)) AS kmall_id,
		CAST(RAK_ID AS VARCHAR(MAX)) AS rak_id 
	FROM utdata.utdata295.EK_FAKTA_VARDE_KMALL2$BAS
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
