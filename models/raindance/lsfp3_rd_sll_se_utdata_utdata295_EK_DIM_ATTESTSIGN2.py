
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model import kind
from models.mssql import read


@model(
    columns={'ATTESTSIGN2': 'varchar(max)',
 'ATTESTSIGN22': 'varchar(max)',
 'ATTESTSIGN22_ID_TEXT': 'varchar(max)',
 'ATTESTSIGN2_ID_TEXT': 'varchar(max)',
 'ATTESTSIGN2_TEXT': 'varchar(max)'},
    kind=kind.FullKind,
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
