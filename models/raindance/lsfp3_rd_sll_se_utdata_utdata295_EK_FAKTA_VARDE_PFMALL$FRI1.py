
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from models.mssql import read


@model(
    columns={'FRI1_ID': 'varchar(max)',
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
 		CAST(FRI1_ID AS VARCHAR(MAX)) AS fri1_id,
		CAST(MALLID_ID AS VARCHAR(MAX)) AS mallid_id,
		CAST(RAK_ID AS VARCHAR(MAX)) AS rak_id 
	FROM utdata.utdata295.EK_FAKTA_VARDE_PFMALL$FRI1
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
