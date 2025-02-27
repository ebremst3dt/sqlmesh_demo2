
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read


@model(
    columns={'PERSONALKAT_ID': 'varchar(max)',
 'PERSONALKAT_ID_TEXT': 'varchar(max)',
 'PERSONALKAT_TEXT': 'varchar(max)'},
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
 		CAST(PERSONALKAT_ID AS VARCHAR(MAX)) AS personalkat_id,
		CAST(PERSONALKAT_ID_TEXT AS VARCHAR(MAX)) AS personalkat_id_text,
		CAST(PERSONALKAT_TEXT AS VARCHAR(MAX)) AS personalkat_text 
	FROM utdata.utdata295.EK_DIM_PERSONALKAT
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
