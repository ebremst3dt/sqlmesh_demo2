
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read


@model(
    columns={'KONTSIGN': 'varchar(3)',
 'KONTSIGN2': 'varchar(30)',
 'KONTSIGN2_ID_TEXT': 'varchar(62)',
 'KONTSIGN_ID_TEXT': 'varchar(34)',
 'KONTSIGN_TEXT': 'varchar(30)'},
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
 		CAST(KONTSIGN AS VARCHAR(MAX)) AS kontsign,
		CAST(KONTSIGN_ID_TEXT AS VARCHAR(MAX)) AS kontsign_id_text,
		CAST(KONTSIGN_TEXT AS VARCHAR(MAX)) AS kontsign_text,
		CAST(KONTSIGN2 AS VARCHAR(MAX)) AS kontsign2,
		CAST(KONTSIGN2_ID_TEXT AS VARCHAR(MAX)) AS kontsign2_id_text 
	FROM utdata.utdata295.EK_DIM_KONTSIGN
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
