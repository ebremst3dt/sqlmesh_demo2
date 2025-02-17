
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model import kind
from models.mssql import read


@model(
    columns={'MALLID_ID': 'varchar(max)',
 'RAK_ID': 'varchar(max)',
 'UTFALL_V': 'varchar(max)'},
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
 		CAST(MALLID_ID AS VARCHAR(MAX)) AS mallid_id,
		CAST(RAK_ID AS VARCHAR(MAX)) AS rak_id,
		CAST(UTFALL_V AS VARCHAR(MAX)) AS utfall_v 
	FROM utdata.utdata295.EK_FAKTA_VARDE_PFMALL$UTFALL
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
