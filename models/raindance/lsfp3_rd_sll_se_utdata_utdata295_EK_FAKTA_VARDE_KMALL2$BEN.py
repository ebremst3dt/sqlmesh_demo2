
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model import kind
from models.mssql import read


@model(
    columns={'ANVID_ID': 'varchar(max)',
 'BEN_TEXT': 'varchar(max)',
 'KMALL_ID': 'varchar(max)',
 'RAK_ID': 'varchar(max)'},
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
 		CAST(ANVID_ID AS VARCHAR(MAX)) AS anvid_id,
		CAST(BEN_TEXT AS VARCHAR(MAX)) AS ben_text,
		CAST(KMALL_ID AS VARCHAR(MAX)) AS kmall_id,
		CAST(RAK_ID AS VARCHAR(MAX)) AS rak_id 
	FROM utdata.utdata295.EK_FAKTA_VARDE_KMALL2$BEN
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
