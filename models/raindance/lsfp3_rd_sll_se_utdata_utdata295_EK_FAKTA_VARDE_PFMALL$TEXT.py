
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read


@model(
    columns={'MALLID_ID': 'varchar(20)',
 'RAK_ID': 'varchar(20)',
 'TEXT_TEXT': 'varchar(120)'},
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
 		CAST(MALLID_ID AS VARCHAR(MAX)) AS mallid_id,
		CAST(RAK_ID AS VARCHAR(MAX)) AS rak_id,
		CAST(TEXT_TEXT AS VARCHAR(MAX)) AS text_text 
	FROM utdata.utdata295.EK_FAKTA_VARDE_PFMALL$TEXT
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
