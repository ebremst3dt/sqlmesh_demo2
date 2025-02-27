
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read


@model(
    columns={'MOTTATTSIGN': 'varchar(max)',
 'MOTTATTSIGN2': 'varchar(max)',
 'MOTTATTSIGN2_ID_TEXT': 'varchar(max)',
 'MOTTATTSIGN_ID_TEXT': 'varchar(max)',
 'MOTTATTSIGN_TEXT': 'varchar(max)'},
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
	SELECT TOP 1000 top 1000
 		CAST(MOTTATTSIGN AS VARCHAR(MAX)) AS mottattsign,
		CAST(MOTTATTSIGN_ID_TEXT AS VARCHAR(MAX)) AS mottattsign_id_text,
		CAST(MOTTATTSIGN_TEXT AS VARCHAR(MAX)) AS mottattsign_text,
		CAST(MOTTATTSIGN2 AS VARCHAR(MAX)) AS mottattsign2,
		CAST(MOTTATTSIGN2_ID_TEXT AS VARCHAR(MAX)) AS mottattsign2_id_text 
	FROM utdata.utdata295.RK_DIM_MOTTATTSIGN
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
