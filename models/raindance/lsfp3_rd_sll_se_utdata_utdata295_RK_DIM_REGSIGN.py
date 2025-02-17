
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read


@model(
    columns={'REGSIGN': 'varchar(3)',
 'REGSIGN2': 'varchar(30)',
 'REGSIGN2_ID_TEXT': 'varchar(62)',
 'REGSIGN_ID_TEXT': 'varchar(34)',
 'REGSIGN_TEXT': 'varchar(30)'},
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
 		CAST(REGSIGN AS VARCHAR(MAX)) AS regsign,
		CAST(REGSIGN_ID_TEXT AS VARCHAR(MAX)) AS regsign_id_text,
		CAST(REGSIGN_TEXT AS VARCHAR(MAX)) AS regsign_text,
		CAST(REGSIGN2 AS VARCHAR(MAX)) AS regsign2,
		CAST(REGSIGN2_ID_TEXT AS VARCHAR(MAX)) AS regsign2_id_text 
	FROM utdata.utdata295.RK_DIM_REGSIGN
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
