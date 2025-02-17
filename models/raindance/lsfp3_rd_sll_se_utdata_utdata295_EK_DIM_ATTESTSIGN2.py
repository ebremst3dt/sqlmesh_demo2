
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read


@model(
    columns={'ATTESTSIGN2': 'varchar(3)',
 'ATTESTSIGN22': 'varchar(30)',
 'ATTESTSIGN22_ID_TEXT': 'varchar(62)',
 'ATTESTSIGN2_ID_TEXT': 'varchar(34)',
 'ATTESTSIGN2_TEXT': 'varchar(30)'},
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
 		CAST(ATTESTSIGN2 AS VARCHAR(MAX)) AS attestsign2,
		CAST(ATTESTSIGN2_ID_TEXT AS VARCHAR(MAX)) AS attestsign2_id_text,
		CAST(ATTESTSIGN2_TEXT AS VARCHAR(MAX)) AS attestsign2_text,
		CAST(ATTESTSIGN22 AS VARCHAR(MAX)) AS attestsign22,
		CAST(ATTESTSIGN22_ID_TEXT AS VARCHAR(MAX)) AS attestsign22_id_text 
	FROM utdata.utdata295.EK_DIM_ATTESTSIGN2
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
