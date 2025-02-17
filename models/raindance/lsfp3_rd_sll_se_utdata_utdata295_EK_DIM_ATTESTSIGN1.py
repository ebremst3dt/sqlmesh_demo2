
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read


@model(
    columns={'ATTESTSIGN1': 'varchar(3)',
 'ATTESTSIGN12': 'varchar(30)',
 'ATTESTSIGN12_ID_TEXT': 'varchar(62)',
 'ATTESTSIGN1_ID_TEXT': 'varchar(34)',
 'ATTESTSIGN1_TEXT': 'varchar(30)'},
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
 		CAST(ATTESTSIGN1 AS VARCHAR(MAX)) AS attestsign1,
		CAST(ATTESTSIGN1_ID_TEXT AS VARCHAR(MAX)) AS attestsign1_id_text,
		CAST(ATTESTSIGN1_TEXT AS VARCHAR(MAX)) AS attestsign1_text,
		CAST(ATTESTSIGN12 AS VARCHAR(MAX)) AS attestsign12,
		CAST(ATTESTSIGN12_ID_TEXT AS VARCHAR(MAX)) AS attestsign12_id_text 
	FROM utdata.utdata295.EK_DIM_ATTESTSIGN1
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
