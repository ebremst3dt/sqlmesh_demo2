
import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from models.mssql import read


@model(
    columns={'DELSYSTEM': 'varchar(max)',
 'DELSYSTEM_TEXT': 'varchar(max)',
 'VERTYP': 'varchar(max)',
 'VERTYP_PASSIV': 'varchar(max)',
 'VERTYP_TEXT': 'varchar(max)'},
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
 		CAST(DELSYSTEM AS VARCHAR(MAX)) AS delsystem,
		CAST(DELSYSTEM_TEXT AS VARCHAR(MAX)) AS delsystem_text,
		CAST(VERTYP AS VARCHAR(MAX)) AS vertyp,
		CAST(VERTYP_PASSIV AS VARCHAR(MAX)) AS vertyp_passiv,
		CAST(VERTYP_TEXT AS VARCHAR(MAX)) AS vertyp_text 
	FROM utdata.utdata295.EK_DIM_VERTYP
	"""
    return read(query=query, server_url="lsfp3.rd.sll.se")
