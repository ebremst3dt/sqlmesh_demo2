
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from models.mssql import read


@model(
    columns={'ATTEST': 'varchar(max)', 'ATTEST_TEXT': 'varchar(max)'},
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
 		CAST(ATTEST AS VARCHAR(MAX)) AS attest,
		CAST(ATTEST_TEXT AS VARCHAR(MAX)) AS attest_text 
	FROM utdata.utdata295.RK_DIM_ATTEST
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
