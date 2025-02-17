
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read


@model(
    columns={'AR': 'varchar(max)',
 'INTERNVERNR': 'varchar(max)',
 'INTERNVERNR_TEXT': 'varchar(max)',
 'VERDATUM': 'varchar(max)',
 'VERNRGRUPP': 'varchar(max)'},
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
 		CAST(AR AS VARCHAR(MAX)) AS ar,
		CAST(INTERNVERNR AS VARCHAR(MAX)) AS internvernr,
		CAST(INTERNVERNR_TEXT AS VARCHAR(MAX)) AS internvernr_text,
		CONVERT(varchar(max), VERDATUM, 126) AS verdatum,
		CAST(VERNRGRUPP AS VARCHAR(MAX)) AS vernrgrupp 
	FROM utdata.utdata295.EK_DIM_VERNR
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
