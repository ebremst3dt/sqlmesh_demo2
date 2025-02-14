
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from models.mssql import read


@model(
    columns={'KUNDRTYP': 'varchar(max)', 'KUNDRTYP_TEXT': 'varchar(max)'},
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
 		CAST(KUNDRTYP AS VARCHAR(MAX)) AS kundrtyp,
		CAST(KUNDRTYP_TEXT AS VARCHAR(MAX)) AS kundrtyp_text 
	FROM utdata.utdata295.RK_DIM_KUNDRTYP
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
