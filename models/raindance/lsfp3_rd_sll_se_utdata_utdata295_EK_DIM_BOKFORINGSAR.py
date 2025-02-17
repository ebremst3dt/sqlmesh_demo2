
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read


@model(
    columns={'BOKFORINGSAR': 'varchar(max)',
 'BOKFORINGSARSLUT': 'varchar(max)',
 'BOKFORINGSAR_TEXT': 'varchar(max)'},
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
 		CAST(BOKFORINGSAR AS VARCHAR(MAX)) AS bokforingsar,
		CAST(BOKFORINGSAR_TEXT AS VARCHAR(MAX)) AS bokforingsar_text,
		CAST(BOKFORINGSARSLUT AS VARCHAR(MAX)) AS bokforingsarslut 
	FROM utdata.utdata295.EK_DIM_BOKFORINGSAR
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
