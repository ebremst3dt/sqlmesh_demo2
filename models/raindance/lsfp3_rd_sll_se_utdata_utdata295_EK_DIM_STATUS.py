
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read


@model(
    columns={'STATUS': 'varchar(1)',
 'STATUSTYP': 'varchar(1)',
 'STATUSTYP_TEXT': 'varchar(20)',
 'STATUS_TEXT': 'varchar(20)'},
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
 		CAST(STATUS AS VARCHAR(MAX)) AS status,
		CAST(STATUS_TEXT AS VARCHAR(MAX)) AS status_text,
		CAST(STATUSTYP AS VARCHAR(MAX)) AS statustyp,
		CAST(STATUSTYP_TEXT AS VARCHAR(MAX)) AS statustyp_text 
	FROM utdata.utdata295.EK_DIM_STATUS
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
